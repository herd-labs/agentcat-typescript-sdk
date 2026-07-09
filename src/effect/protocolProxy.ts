import { Cause, Effect, Layer, Option, Predicate, Schema } from "effect";
import type { Scope } from "effect";
import { RpcServer } from "effect/unstable/rpc";
import type { RpcMessage } from "effect/unstable/rpc";
import { HttpEffect, HttpServerRequest } from "effect/unstable/http";
import type { HttpServerResponse } from "effect/unstable/http";
import { PublishEventRequestEventTypeEnum } from "agentcat-api";
import type {
  CompatibleRequestHandlerExtra,
  ErrorData,
  ServerClientInfoLike,
  UnredactedEvent,
  UserIdentity,
} from "../types.js";
import { publishEvent } from "../modules/eventQueue.js";
import {
  applyIdentityForContext,
  getSessionState,
} from "../modules/internal.js";
import { captureException } from "../modules/exceptions.js";
import {
  addContextParameterToTools,
  type ContextParameterTool,
} from "../modules/context-parameters.js";
import { validateTags } from "../modules/validation.js";
import { writeToLog } from "../modules/logging.js";
import { GET_MORE_TOOLS_NAME } from "../modules/constants.js";
import {
  CurrentAgentCatRequest,
  OBSERVED_TAGS,
  rememberClientInfo,
  resolveSession,
  trackPendingRequest,
  withSessionScope,
  type DeferredInitialize,
  type EffectTracking,
  type ObservedTag,
  type PendingRequest,
} from "./session.js";
import type { EffectOptionResolver, EffectRequestHeaders } from "./types.js";

type ProtocolSend = RpcServer.Protocol["Service"]["send"];
type ProtocolTransferables = Parameters<ProtocolSend>[2];
type RunHandler = (
  clientId: number,
  message: RpcMessage.FromClientEncoded,
) => Effect.Effect<void>;

type EncodedCauseReason = Extract<
  RpcMessage.ExitEncoded<unknown, unknown>,
  { _tag: "Failure" }
>["cause"][number];
const isNonEmptyString = Schema.is(Schema.NonEmptyString);
const isToolCallPayload = Schema.is(
  Schema.Struct({ arguments: Schema.Unknown }),
);
const isContextArgumentPayload = Schema.is(
  Schema.Struct({ context: Schema.String }),
);

/**
 * The capture seam (decision 1 in docs/plans/effect-mcp-support.md): a
 * proxying `RpcServer.Protocol` layer that observes every wire-encoded MCP
 * request on `run` and correlates the encoded `Exit` on `send`, emitting
 * mcpInitialize / mcpToolsCall / mcpToolsList events through the shared
 * pipeline. Also performs context-parameter injection into the encoded
 * `ListToolsResult` and context-argument stripping from `tools/call` payloads
 * (decision 4). All non-message protocol fields pass through untouched.
 */
export const wrapProtocol = (
  tracking: EffectTracking,
): Layer.Layer<RpcServer.Protocol, never, RpcServer.Protocol> =>
  Layer.effect(RpcServer.Protocol)(
    Effect.gen(function* () {
      const inner = yield* RpcServer.Protocol;
      return RpcServer.Protocol.of({
        ...inner,
        run: (f) =>
          inner.run((clientId, message) =>
            message._tag === "Request"
              ? observeRequest(tracking, f, clientId, message)
              : f(clientId, message),
          ),
        send: (clientId, response, transferables) =>
          observeSend(tracking, inner.send, clientId, response, transferables),
      });
    }),
  );

/**
 * Wraps the HTTP route effect registered by the layerHttp mirror. The route
 * effect completes only after every request `Exit` has been collected
 * (non-streaming JSON-RPC serialization), which happens strictly after
 * McpServer's initialize handler appended its own pre-response handler — so a
 * handler appended here runs after it in the chain and can read the minted
 * `mcp-session-id` off the final response headers.
 */
export const wrapHttpRouteEffect = (
  tracking: EffectTracking,
  httpEffect: Effect.Effect<
    HttpServerResponse.HttpServerResponse,
    never,
    Scope.Scope | HttpServerRequest.HttpServerRequest
  >,
): Effect.Effect<
  HttpServerResponse.HttpServerResponse,
  never,
  Scope.Scope | HttpServerRequest.HttpServerRequest
> =>
  Effect.gen(function* () {
    const response = yield* httpEffect;
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (tracking.pendingHttpInit.has(request)) {
      yield* HttpEffect.appendPreResponseHandler((req, res) =>
        Effect.sync(() => {
          finalizeHttpInitialize(tracking, req, res);
          return res;
        }),
      );
    }
    return response;
  });

// ---------------------------------------------------------------------------
// run side: request observation
// ---------------------------------------------------------------------------

interface PreparedRequest {
  readonly forward: RpcMessage.FromClientEncoded;
  readonly mcpSessionId: string | undefined;
  readonly pending?: PendingRequest;
}

const observeRequest = (
  tracking: EffectTracking,
  f: RunHandler,
  clientId: number,
  message: RpcMessage.RequestEncoded,
): Effect.Effect<void> => {
  let prepared: PreparedRequest;
  try {
    prepared = prepareRequest(tracking, clientId, message);
  } catch (error) {
    writeToLog(`Warning: AgentCat failed to observe an MCP request - ${error}`);
    return f(clientId, message);
  }
  const { forward, mcpSessionId, pending } = prepared;
  const provide = (effect: Effect.Effect<void>): Effect.Effect<void> =>
    Effect.provideService(effect, CurrentAgentCatRequest, {
      tracking,
      clientId,
      mcpSessionId,
    });
  if (!pending) {
    return provide(f(clientId, forward));
  }

  return Effect.gen(function* () {
    const tracingEnabled = tracking.data.options.enableTracing === true;
    let identity: UserIdentity | null | undefined;
    if (tracingEnabled && tracking.options.identify !== undefined) {
      identity = yield* resolveOption(
        tracking.options.identify,
        pending.payload,
        pending.headers,
        "identify",
      );
    }
    if (tracingEnabled && tracking.options.eventTags !== undefined) {
      const rawTags = yield* resolveOption(
        tracking.options.eventTags,
        pending.payload,
        pending.headers,
        "eventTags",
      );
      pending.tags = rawTags ? safeValidateTags(rawTags) : null;
    }
    if (tracingEnabled && tracking.options.eventProperties !== undefined) {
      pending.properties = yield* resolveOption(
        tracking.options.eventProperties,
        pending.payload,
        pending.headers,
        "eventProperties",
      );
    }

    if (pending.tag === "initialize") {
      const clientInfo = readClientInfo(pending.payload);
      let deferredToHttp = false;
      if (tracking.httpMintObservation) {
        const httpRequest = yield* Effect.serviceOption(
          HttpServerRequest.HttpServerRequest,
        );
        if (Option.isSome(httpRequest)) {
          const deferred: DeferredInitialize = {
            pending,
            clientInfo,
            identity,
          };
          pending.deferred = deferred;
          tracking.pendingHttpInit.set(httpRequest.value, deferred);
          deferredToHttp = true;
        }
      }
      if (!deferredToHttp) {
        completeRequestObservation(tracking, pending, clientInfo, identity);
      }
    } else {
      completeRequestObservation(tracking, pending, undefined, identity);
    }

    return yield* provide(f(clientId, forward));
  });
};

const prepareRequest = (
  tracking: EffectTracking,
  clientId: number,
  message: RpcMessage.RequestEncoded,
): PreparedRequest => {
  const headers = headersToRecord(message.headers);
  const headerSessionId = headers["mcp-session-id"];
  const mcpSessionId = isNonEmptyString(headerSessionId)
    ? headerSessionId
    : undefined;
  const tag = toObservedTag(message.tag);
  if (!tag) {
    return { forward: message, mcpSessionId };
  }

  let forward: RpcMessage.RequestEncoded = message;
  const resourceName = resourceNameForRequest(tag, message.payload);
  let userIntent: string | undefined;

  if (tag === "tools/call" && isToolCallPayload(message.payload)) {
    const args = message.payload.arguments;
    if (isContextArgumentPayload(args)) {
      if (resourceName === GET_MORE_TOOLS_NAME) {
        // get_more_tools declares its own real context parameter: capture the
        // intent but forward the argument untouched.
        userIntent = args.context;
      } else if (tracking.data.options.enableToolCallContext === true) {
        userIntent = args.context;
        const { context: _context, ...strippedArgs } = args;
        forward = {
          ...message,
          payload: { ...message.payload, arguments: strippedArgs },
        };
      }
    }
  }

  const pending: PendingRequest = {
    tag,
    clientId,
    requestId: message.id,
    payload: message.payload,
    headers,
    mcpSessionId,
    timestamp: new Date(),
    startedAt: Date.now(),
    resourceName,
    userIntent,
  };
  trackPendingRequest(tracking, pendingKey(clientId, message.id), pending);
  return { forward, mcpSessionId, pending };
};

/**
 * Resolves the session and applies identity for a request whose session is
 * fully determined at request time (everything except HTTP initialize).
 */
const completeRequestObservation = (
  tracking: EffectTracking,
  pending: PendingRequest,
  clientInfo: ServerClientInfoLike | undefined,
  identity: UserIdentity | null | undefined,
): void => {
  try {
    if (pending.tag === "initialize") {
      rememberClientInfo(tracking, pending.clientId, clientInfo);
    }
    pending.session = resolveSession(
      tracking,
      pending.mcpSessionId,
      pending.clientId,
    );
    if (pending.tag === "initialize" && clientInfo) {
      pending.session = { ...pending.session, clientInfo };
    }
    if (identity !== undefined) {
      applyIdentity(tracking, pending, identity);
    }
  } catch (error) {
    writeToLog(
      `Warning: AgentCat failed to resolve the session for an MCP request - ${error}`,
    );
  }
};

/**
 * Feeds a pre-resolved identity through the shared merge/dedup +
 * publish-on-change pipeline, scoped to the request's session.
 */
const applyIdentity = (
  tracking: EffectTracking,
  pending: PendingRequest,
  identity: UserIdentity | null,
): void => {
  const session = pending.session;
  if (!session) {
    return;
  }
  const rawParams = Predicate.isObject(pending.payload) ? pending.payload : {};
  const paramsName = rawParams["name"];
  const { name: _name, ...restParams } = rawParams;
  const identifyParams: { name?: string } & Record<string, unknown> =
    typeof paramsName === "string"
      ? { ...restParams, name: paramsName }
      : restParams;
  const identifyRequest = { method: pending.tag, params: identifyParams };
  const extra: CompatibleRequestHandlerExtra = {
    sessionId: session.mcpSessionId,
    headers: pending.headers,
  };
  withSessionScope(tracking, session, () =>
    applyIdentityForContext(tracking.ctx, identity, identifyRequest, extra),
  );
};

// ---------------------------------------------------------------------------
// send side: Exit correlation and event emission
// ---------------------------------------------------------------------------

const observeSend = (
  tracking: EffectTracking,
  innerSend: ProtocolSend,
  clientId: number,
  response: RpcMessage.FromServerEncoded,
  transferables: ProtocolTransferables,
): Effect.Effect<void> => {
  try {
    if (response._tag === "Exit") {
      const key = pendingKey(clientId, response.requestId);
      const pending = tracking.pendingRequests.get(key);
      if (pending) {
        tracking.pendingRequests.delete(key);
        const durationMs = Math.max(0, Date.now() - pending.startedAt);
        if (pending.deferred) {
          // HTTP initialize: emission happens in the pre-response hook once
          // the minted mcp-session-id is known.
          pending.deferred.exit = response.exit;
          pending.deferred.durationMs = durationMs;
        } else if (pending.tag === "tools/list") {
          let outgoing = response;
          try {
            outgoing = injectContextIntoListExit(tracking, response);
          } catch (error) {
            writeToLog(
              `Warning: AgentCat failed to inject context into tools/list response - ${error}`,
            );
          }
          emitEvent(tracking, pending, outgoing.exit, durationMs);
          return innerSend(clientId, outgoing, transferables);
        } else {
          emitEvent(tracking, pending, response.exit, durationMs);
        }
      }
    }
  } catch (error) {
    writeToLog(
      `Warning: AgentCat failed to observe an MCP response - ${error}`,
    );
  }
  return innerSend(clientId, response, transferables);
};

/**
 * Injects the `context` parameter into every tool of an encoded
 * `ListToolsResult`, honoring the shared skip rules (existing context,
 * oneOf/allOf/anyOf, get_more_tools).
 */
const injectContextIntoListExit = (
  tracking: EffectTracking,
  response: RpcMessage.ResponseExitEncoded,
): RpcMessage.ResponseExitEncoded => {
  if (tracking.data.options.enableToolCallContext !== true) {
    return response;
  }
  const exit = response.exit;
  if (
    exit._tag !== "Success" ||
    !Predicate.isObject(exit.value) ||
    !Array.isArray(exit.value.tools)
  ) {
    return response;
  }
  const tools: ContextParameterTool[] = exit.value.tools;
  const mutated = addContextParameterToTools(
    tools,
    tracking.data.options.customContextDescription,
  );
  return {
    ...response,
    exit: {
      _tag: "Success",
      value: { ...exit.value, tools: mutated },
    },
  };
};

const emitEvent = (
  tracking: EffectTracking,
  pending: PendingRequest,
  exit: RpcMessage.ExitEncoded<unknown, unknown>,
  durationMs: number,
): void => {
  const session = pending.session;
  if (!session) {
    return;
  }
  const event: UnredactedEvent = {
    sessionId: session.sessionId,
    eventType: eventTypeForTag(pending.tag),
    timestamp: pending.timestamp,
    duration: durationMs,
    parameters: {
      request: { method: pending.tag, params: pending.payload },
      extra: {
        headers: pending.headers,
        sessionId: session.mcpSessionId,
      },
    },
    redactionFn: tracking.data.options.redactSensitiveInformation,
  };
  if (pending.tag === "initialize" || pending.tag === "tools/call") {
    // Parity with the official-SDK path's fallback for these two.
    event.resourceName = pending.resourceName || "Unknown Tool Name";
  } else if (pending.resourceName !== undefined) {
    event.resourceName = pending.resourceName;
  }
  if (pending.userIntent !== undefined) {
    event.userIntent = pending.userIntent;
  }
  if (pending.tags) {
    event.tags = pending.tags;
  }
  if (pending.properties) {
    event.properties = pending.properties;
  }

  if (exit._tag === "Success") {
    event.response = exit.value;
    if (
      pending.tag === "tools/call" &&
      Predicate.isObject(exit.value) &&
      exit.value.isError === true
    ) {
      event.isError = true;
      event.error = captureException(exit.value);
    } else if (pending.tag === "tools/list") {
      event.isError = false;
    }
  } else {
    event.isError = true;
    event.error = errorDataFromEncodedCause(exit.cause);
  }

  withSessionScope(tracking, session, () => publishEvent(tracking.ctx, event));
};

/**
 * Completes a deferred HTTP initialize: binds clientInfo to the minted
 * session, applies identity, and emits the mcpInitialize event. Runs inside
 * the pre-response chain, after McpServer's handler set the session headers.
 */
const finalizeHttpInitialize = (
  tracking: EffectTracking,
  request: HttpServerRequest.HttpServerRequest,
  response: HttpServerResponse.HttpServerResponse,
): void => {
  try {
    const deferred = tracking.pendingHttpInit.get(request);
    if (!deferred) {
      return;
    }
    tracking.pendingHttpInit.delete(request);
    const pending = deferred.pending;
    const headerSessionId = response.headers["mcp-session-id"];
    const minted = isNonEmptyString(headerSessionId)
      ? headerSessionId
      : undefined;
    if (minted) {
      const state = getSessionState(tracking.ctx, minted);
      if (deferred.clientInfo) {
        state.clientInfo = deferred.clientInfo;
      }
      pending.session = resolveSession(tracking, minted, pending.clientId);
    } else {
      rememberClientInfo(tracking, pending.clientId, deferred.clientInfo);
      pending.session = resolveSession(
        tracking,
        pending.mcpSessionId,
        pending.clientId,
      );
      if (deferred.clientInfo) {
        pending.session = {
          ...pending.session,
          clientInfo: deferred.clientInfo,
        };
      }
    }
    if (deferred.identity !== undefined) {
      applyIdentity(tracking, pending, deferred.identity);
    }
    if (deferred.exit !== undefined && deferred.durationMs !== undefined) {
      emitEvent(tracking, pending, deferred.exit, deferred.durationMs);
    }
  } catch (error) {
    writeToLog(
      `Warning: AgentCat failed to finalize an initialize observation - ${error}`,
    );
  }
};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

const pendingKey = (clientId: number, requestId: string): string =>
  `${clientId}:${requestId}`;

const toObservedTag = (tag: string): ObservedTag | undefined =>
  OBSERVED_TAGS.find((observed) => observed === tag);

/**
 * The primary resource a request addresses, mirrored into
 * `event.resourceName`: tool/prompt name, resource uri, or completion ref.
 */
const resourceNameForRequest = (
  tag: ObservedTag,
  payload: unknown,
): string | undefined => {
  if (!Predicate.isObject(payload)) {
    return undefined;
  }
  switch (tag) {
    case "tools/call":
    case "prompts/get":
      const name = payload["name"];
      return isNonEmptyString(name) ? name : undefined;
    case "resources/read":
    case "resources/subscribe":
    case "resources/unsubscribe":
      const uri = payload["uri"];
      return isNonEmptyString(uri) ? uri : undefined;
    case "completion/complete": {
      const ref = payload.ref;
      if (!Predicate.isObject(ref)) {
        return undefined;
      }
      const name = ref["name"];
      if (isNonEmptyString(name)) {
        return name;
      }
      const uri = ref["uri"];
      return isNonEmptyString(uri) ? uri : undefined;
    }
    default:
      return undefined;
  }
};

const eventTypeForTag = (tag: ObservedTag): string => {
  switch (tag) {
    case "initialize":
      return PublishEventRequestEventTypeEnum.mcpInitialize;
    case "tools/call":
      return PublishEventRequestEventTypeEnum.mcpToolsCall;
    case "tools/list":
      return PublishEventRequestEventTypeEnum.mcpToolsList;
    case "resources/list":
      return PublishEventRequestEventTypeEnum.mcpResourcesList;
    case "resources/templates/list":
      return PublishEventRequestEventTypeEnum.mcpResourcesTemplatesList;
    case "resources/read":
      return PublishEventRequestEventTypeEnum.mcpResourcesRead;
    case "resources/subscribe":
      return PublishEventRequestEventTypeEnum.mcpResourcesSubscribe;
    case "resources/unsubscribe":
      return PublishEventRequestEventTypeEnum.mcpResourcesUnsubscribe;
    case "prompts/list":
      return PublishEventRequestEventTypeEnum.mcpPromptsList;
    case "prompts/get":
      return PublishEventRequestEventTypeEnum.mcpPromptsGet;
    case "completion/complete":
      return PublishEventRequestEventTypeEnum.mcpCompletionComplete;
    case "logging/setLevel":
      return PublishEventRequestEventTypeEnum.mcpLoggingSetLevel;
    default: {
      const exhaustive: never = tag;
      return exhaustive;
    }
  }
};

const headersToRecord = (
  headers: ReadonlyArray<readonly [string, string]>,
): Record<string, string> => {
  const record: Record<string, string> = {};
  for (const [key, value] of headers) {
    record[key.toLowerCase()] = value;
  }
  return record;
};

const readClientInfo = (payload: unknown): ServerClientInfoLike | undefined => {
  const clientInfo = Predicate.isObject(payload)
    ? payload["clientInfo"]
    : undefined;
  if (!Predicate.isObject(clientInfo)) {
    return undefined;
  }
  const { name, version } = clientInfo;
  const serverClientInfo: ServerClientInfoLike = {};
  if (typeof name === "string") {
    serverClientInfo.name = name;
  }
  if (typeof version === "string") {
    serverClientInfo.version = version;
  }
  return serverClientInfo;
};

const errorDataFromEncodedCause = (
  cause: ReadonlyArray<EncodedCauseReason>,
): ErrorData => {
  for (const reason of cause) {
    if (reason._tag === "Fail") {
      return captureException(reason.error);
    }
    if (reason._tag === "Die") {
      return captureException(reason.defect);
    }
  }
  return { message: "Request interrupted", platform: "javascript" };
};

/**
 * Evaluates a user-supplied option resolver (Effect in the request fiber, or
 * `(payload, headers)` callback), swallowing failures with a log — matching
 * the official path's error semantics for identify/eventTags/eventProperties.
 */
const resolveOption = <A>(
  resolver: EffectOptionResolver<A>,
  payload: unknown,
  headers: EffectRequestHeaders,
  label: string,
): Effect.Effect<A | null> => {
  if (Effect.isEffect(resolver)) {
    return Effect.matchCause(resolver, {
      onFailure: (cause) => {
        writeToLog(`${label} effect error: ${Cause.pretty(cause)}`);
        return null;
      },
      onSuccess: (value) => value ?? null,
    });
  }
  return Effect.promise(async () => {
    try {
      return (await resolver(payload, headers)) ?? null;
    } catch (error) {
      writeToLog(`${label} callback error: ${error}`);
      return null;
    }
  });
};

const safeValidateTags = (
  tags: Record<string, string>,
): Record<string, string> | null => {
  try {
    return validateTags(tags);
  } catch (error) {
    writeToLog(`eventTags callback error: ${error}`);
    return null;
  }
};
