/**
 * Acceptance suite for `agentcat/effect` under a router-middleware wrapper
 * topology: `layerHttp` composed BEHIND an `HttpRouter.middleware`-style auth
 * wrapper that wraps the registered route effect, provides a user service,
 * short-circuits unauthorized requests, and appends its own persist-style
 * pre-response handler AFTER the route effect — plus verbatim forwarding of
 * an extra `clientSessions` server option (a patched-effect key that
 * unpatched effect must ignore without breaking the server).
 *
 * Session-id hygiene: identity dedup runs through a module-global LRU keyed
 * by the DERIVED ses_ id, shared across every test in a worker. Each test
 * therefore uses a run-unique projectId so its derived ids can never collide
 * with another test (or a previous run).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { randomUUID } from "node:crypto";
import { Context, Effect, Layer, Option, Predicate, Schema } from "effect";
import type { Types } from "effect";
import { McpServer, Tool, Toolkit } from "effect/unstable/ai";
import {
  Headers,
  HttpEffect,
  HttpRouter,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/unstable/http";
import { PublishEventRequestEventTypeEnum } from "agentcat-api";
import { layerHttp } from "../effect/index.js";
import { eventQueue } from "../modules/eventQueue.js";
import { deriveSessionIdFromMCPSession } from "../modules/session.js";
import type { UnredactedEvent } from "../types.js";

/** Parses a JSON-RPC POST response body (plain JSON or SSE-framed). */
const parseJsonRpcBody = async (res: Response): Promise<unknown[]> => {
  const text = await res.text();
  const contentType = res.headers.get("content-type") ?? "";
  if (contentType.includes("text/event-stream")) {
    return text
      .split("\n")
      .filter((line) => line.startsWith("data:"))
      .map((line): unknown => JSON.parse(line.slice(5).trim()));
  }
  if (text.trim().length === 0) return [];
  const parsed: unknown = JSON.parse(text);
  return Array.isArray(parsed) ? parsed : [parsed];
};

/** First JSON-RPC message in the body that carries a `result`. */
const firstResult = (
  messages: unknown[],
): Record<string, unknown> | undefined => {
  for (const message of messages) {
    if (Predicate.isObject(message) && Predicate.isObject(message.result)) {
      return message.result;
    }
  }
  return undefined;
};

/** Service the auth middleware provides to every authorized request. */
class TestUser extends Context.Service<
  TestUser,
  { readonly id: string; readonly name: string }
>()("middleware/TestUser") {}

const TEST_USER = { id: "app-user-1", name: "App User" } as const;

const EchoTool = Tool.make("echo", {
  description: "Echo a message back",
  parameters: Schema.Struct({ message: Schema.String }),
  success: Schema.String,
});

const echoToolkit = Toolkit.make(EchoTool);

interface WrappedServer {
  /** Arguments the echo handler actually received (post context-stripping). */
  readonly received: Array<Record<string, unknown>>;
  /** What the persist-style pre-response handler observed, in order. */
  readonly persistedSessionIds: Array<string | undefined>;
  /** Extra server option forwarded verbatim; unpatched effect ignores it. */
  readonly clientSessions: Map<string, unknown>;
  readonly post: (
    body: object,
    options?: { sessionId?: string; authorized?: boolean },
  ) => Promise<Response>;
  readonly dispose: () => Promise<void>;
}

/**
 * Builds one independent web handler mirroring the wrapped composition:
 *
 *   McpServer.toolkit(Tools).pipe(
 *     Layer.provide(layerHttp({ ...info, clientSessions, path }).pipe(
 *       Layer.provide(authMiddleware),
 *     )),
 *   )
 *
 * The auth middleware wraps the route effect registered by `layerHttp`:
 * 401-short-circuits requests without an `authorization` header, provides
 * `TestUser` to the route fiber, and — for initialize-shaped requests (no
 * `mcp-session-id` header) — appends a persist-style pre-response handler
 * AFTER the route effect succeeded, so it chains after both the library's
 * session-id setter and agentcat's initialize observer.
 */
const makeWrappedServer = (projectId: string): WrappedServer => {
  const received: Array<Record<string, unknown>> = [];
  const persistedSessionIds: Array<string | undefined> = [];
  const clientSessions = new Map<string, unknown>();

  const authMiddleware = HttpRouter.middleware<{ provides: TestUser }>()(
    Effect.gen(function* () {
      return (
        httpEffect: Effect.Effect<
          HttpServerResponse.HttpServerResponse,
          Types.unhandled,
          TestUser
        >,
      ) =>
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const auth = Headers.get(request.headers, "authorization");
          if (Option.isNone(auth)) {
            return HttpServerResponse.text("Unauthorized", { status: 401 });
          }
          const withUser = Effect.provideService(httpEffect, TestUser, {
            id: TEST_USER.id,
            name: TEST_USER.name,
          });
          const sessionHeader = Headers.get(request.headers, "mcp-session-id");
          if (Option.isSome(sessionHeader)) {
            return yield* withUser;
          }
          // Persist-style hook: appended AFTER the route effect so it runs
          // after the session-id setter and agentcat's observer in the chain.
          return yield* withUser.pipe(
            Effect.tap(() =>
              HttpEffect.appendPreResponseHandler((_req, res) =>
                Effect.sync(() => {
                  persistedSessionIds.push(res.headers["mcp-session-id"]);
                  return res;
                }),
              ),
            ),
          );
        });
    }),
  ).layer;

  const serverLayer = McpServer.toolkit(echoToolkit).pipe(
    Layer.provide(
      echoToolkit.toLayer({
        echo: (params) =>
          Effect.sync(() => {
            received.push({ ...params });
            return `echo: ${params.message}`;
          }),
      }),
    ),
    Layer.provideMerge(
      layerHttp(
        {
          name: "router-middleware-server",
          version: "1.0.0",
          path: "/mcp",
          // Patched-effect option forwarded verbatim through the combinator.
          clientSessions,
        },
        projectId,
        {
          identify: Effect.map(Effect.serviceOption(TestUser), (user) =>
            Option.isSome(user)
              ? { userId: user.value.id, userName: user.value.name }
              : null,
          ),
        },
      ).pipe(Layer.provide(authMiddleware)),
    ),
  );

  const { handler, dispose } = HttpRouter.toWebHandler(serverLayer);
  const post = (
    body: object,
    options?: { sessionId?: string; authorized?: boolean },
  ) =>
    handler(
      new Request("http://localhost/mcp", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          accept: "application/json, text/event-stream",
          ...(options?.authorized === false
            ? {}
            : { authorization: "Bearer test-secret" }),
          ...(options?.sessionId
            ? { "mcp-session-id": options.sessionId }
            : {}),
        },
        body: JSON.stringify(body),
      }),
    );
  return { received, persistedSessionIds, clientSessions, post, dispose };
};

const initialize = async (
  server: WrappedServer,
  clientInfo: { name: string; version: string },
): Promise<string> => {
  const res = await server.post({
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo,
    },
  });
  const minted = res.headers.get("mcp-session-id");
  expect(minted).toBeTruthy();
  const result = firstResult(await parseJsonRpcBody(res));
  expect(result).toBeDefined();
  const ack = await server.post(
    { jsonrpc: "2.0", method: "notifications/initialized" },
    { sessionId: minted ?? undefined },
  );
  await ack.text();
  return minted ?? "";
};

const callEcho = async (
  server: WrappedServer,
  id: number,
  args: Record<string, unknown>,
  sessionId: string,
): Promise<unknown[]> => {
  const res = await server.post(
    {
      jsonrpc: "2.0",
      id,
      method: "tools/call",
      params: { name: "echo", arguments: args },
    },
    { sessionId },
  );
  return parseJsonRpcBody(res);
};

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe("effect layerHttp under router middleware wrapper", () => {
  let events: UnredactedEvent[] = [];

  const byType = (type: string): UnredactedEvent[] =>
    events.filter((e) => e.eventType === type);

  beforeEach(() => {
    events = [];
    vi.spyOn(eventQueue, "add").mockImplementation((event) => {
      events.push(event);
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("chains mint -> agentcat finalize -> persist on one initialize response", async () => {
    const projectId = `proj_router_chain_${randomUUID()}`;
    const server = makeWrappedServer(projectId);
    try {
      // initialize helper already asserts the server answered with a result:
      // the extra `clientSessions` key did not break the composition.
      const minted = await initialize(server, {
        name: "router-chain-client",
        version: "1.0.0",
      });

      // [library mint]: the response header carries the minted session id.
      // [persist]: the wrapper's handler — appended AFTER the route effect,
      // hence after the session-id setter — observed the SAME minted id on
      // the response headers. Exactly once: the `notifications/initialized`
      // ack carried a session header, so no persist hook was appended for it.
      expect(server.persistedSessionIds).toEqual([minted]);

      // [agentcat finalize]: exactly one initialize event, on the session
      // deterministically derived from the id minted on this same response.
      const inits = byType(PublishEventRequestEventTypeEnum.mcpInitialize);
      expect(inits).toHaveLength(1);
      expect(inits[0].sessionId).toBe(
        deriveSessionIdFromMCPSession(minted, projectId),
      );
      expect(inits[0].projectId).toBe(projectId);

      // Verbatim forwarding: unpatched effect ignores the extra option;
      // patched effect records the minted session in the forwarded map.
      expect([0, 1]).toContain(server.clientSessions.size);
      if (server.clientSessions.size > 0) {
        expect(server.clientSessions.has(minted)).toBe(true);
      }
    } finally {
      await server.dispose();
    }
  }, 20000);

  it("resolves identity from the middleware-provided service and stamps the actor", async () => {
    const projectId = `proj_router_identify_${randomUUID()}`;
    const server = makeWrappedServer(projectId);
    try {
      const minted = await initialize(server, {
        name: "router-identify-client",
        version: "1.0.0",
      });
      const derived = deriveSessionIdFromMCPSession(minted, projectId);

      // The identify resolver ran in the route fiber WRAPPED by the
      // middleware, so `Effect.serviceOption(TestUser)` saw the service.
      const identifies = byType(
        PublishEventRequestEventTypeEnum.agentcatIdentify,
      );
      expect(identifies).toHaveLength(1);
      expect(identifies[0].sessionId).toBe(derived);
      expect(identifies[0].identifyActorGivenId).toBe("app-user-1");
      expect(identifies[0].identifyActorName).toBe("App User");

      // Subsequent tools/call events on that session carry the actor, and
      // the unchanged identity publishes no further identify events.
      await callEcho(server, 2, { message: "first" }, minted);
      await callEcho(server, 3, { message: "second" }, minted);
      const calls = byType(PublishEventRequestEventTypeEnum.mcpToolsCall);
      expect(calls).toHaveLength(2);
      for (const call of calls) {
        expect(call.sessionId).toBe(derived);
        expect(call.identifyActorGivenId).toBe("app-user-1");
        expect(call.identifyActorName).toBe("App User");
      }
      expect(
        byType(PublishEventRequestEventTypeEnum.agentcatIdentify),
      ).toHaveLength(1);
    } finally {
      await server.dispose();
    }
  }, 20000);

  it("401-short-circuits unauthorized requests without poisoning the session", async () => {
    const projectId = `proj_router_auth_${randomUUID()}`;
    const server = makeWrappedServer(projectId);
    try {
      const minted = await initialize(server, {
        name: "router-auth-client",
        version: "1.0.0",
      });
      const eventCountAfterInit = events.length;

      // Unauthorized tools/call on the live session: the middleware answers
      // before the route effect ever runs.
      const denied = await server.post(
        {
          jsonrpc: "2.0",
          id: 2,
          method: "tools/call",
          params: { name: "echo", arguments: { message: "sneaky" } },
        },
        { sessionId: minted, authorized: false },
      );
      expect(denied.status).toBe(401);
      expect(await denied.text()).toBe("Unauthorized");
      // ZERO events captured for the rejected request, and the tool handler
      // never ran.
      expect(events.length).toBe(eventCountAfterInit);
      expect(server.received).toHaveLength(0);

      // The same session keeps working afterwards: no pending-state poisoning.
      const body = await callEcho(server, 3, { message: "back" }, minted);
      const result = firstResult(body);
      expect(result).toBeDefined();
      expect(result?.isError).toBeFalsy();
      expect(server.received).toEqual([{ message: "back" }]);

      const calls = byType(PublishEventRequestEventTypeEnum.mcpToolsCall);
      expect(calls).toHaveLength(1);
      expect(calls[0].sessionId).toBe(
        deriveSessionIdFromMCPSession(minted, projectId),
      );
      expect(calls[0].resourceName).toBe("echo");
      expect(calls[0].identifyActorGivenId).toBe("app-user-1");
    } finally {
      await server.dispose();
    }
  }, 20000);

  it("round-trips context through the wrapper and keeps per-session clientInfo", async () => {
    const projectId = `proj_router_context_${randomUUID()}`;
    const server = makeWrappedServer(projectId);
    try {
      const minted = await initialize(server, {
        name: "router-context-client",
        version: "9.9.9",
      });

      // The injected `context` parameter is client-visible on tools/list.
      const listRes = await server.post(
        { jsonrpc: "2.0", id: 2, method: "tools/list", params: {} },
        { sessionId: minted },
      );
      const listResult = firstResult(await parseJsonRpcBody(listRes));
      expect(listResult).toBeDefined();
      const tools = Array.isArray(listResult?.tools) ? listResult.tools : [];
      const echo = tools.find(
        (t) => Predicate.isObject(t) && t.name === "echo",
      );
      if (
        Predicate.isObject(echo) &&
        Predicate.isObject(echo.inputSchema) &&
        Predicate.isObject(echo.inputSchema.properties)
      ) {
        expect(echo.inputSchema.properties.context).toBeDefined();
      } else {
        expect.unreachable("echo tool without inputSchema.properties");
      }

      // The context argument becomes userIntent and never reaches the handler.
      const body = await callEcho(
        server,
        3,
        { message: "hi", context: "deploy the rollout" },
        minted,
      );
      const callResult = firstResult(body);
      expect(callResult).toBeDefined();
      expect(callResult?.isError).toBeFalsy();
      expect(server.received).toEqual([{ message: "hi" }]);

      const calls = byType(PublishEventRequestEventTypeEnum.mcpToolsCall);
      expect(calls).toHaveLength(1);
      expect(calls[0].userIntent).toBe("deploy the rollout");
      expect(calls[0].resourceName).toBe("echo");
      // Per-session clientInfo from the initialize payload survived the
      // wrapper onto later events of the same session.
      expect(calls[0].clientName).toBe("router-context-client");
      expect(calls[0].clientVersion).toBe("9.9.9");
    } finally {
      await server.dispose();
    }
  }, 20000);
});
