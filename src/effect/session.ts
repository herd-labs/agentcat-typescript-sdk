import { Context, pipe, Record, Result, Struct } from "effect";
import type { RpcMessage } from "effect/unstable/rpc";
import type {
  AgentCatData,
  ServerClientInfoLike,
  UserIdentity,
} from "../types.js";
import {
  createTrackingContext,
  getSessionState,
  type TrackingContext,
} from "../modules/internal.js";
import {
  deriveSessionIdFromMCPSession,
  newSessionId,
} from "../modules/session.js";
import { INACTIVITY_TIMEOUT_IN_MINUTES } from "../modules/constants.js";
import type { AgentCatEffectOptions } from "./types.js";

/**
 * Session identity resolved for one observed MCP request (decision 6 in
 * docs/plans/effect-mcp-support.md):
 *
 * - MCP session id present (HTTP header / minted at initialize) → the
 *   deterministic `ses_` id derived via {@link deriveSessionIdFromMCPSession},
 *   giving cross-replica continuity for free.
 * - No MCP session id (stdio, custom transports) → a generated KSUID with
 *   30-minute inactivity rotation, as on the official-SDK path.
 */
export interface ResolvedSession {
  readonly sessionId: string;
  readonly mcpSessionId?: string;
  readonly source: "mcp" | "agentcat";
  readonly clientInfo?: ServerClientInfoLike;
}

/**
 * MCP request tags observed by the protocol proxy — every wire-visible
 * request method with a declared agentcat-api event type. `ping` is
 * deliberately excluded (keepalive noise); client notifications never
 * produce an Exit at this seam and are not observable as request/response
 * pairs.
 */
export const OBSERVED_TAGS = [
  "initialize",
  "tools/call",
  "tools/list",
  "resources/list",
  "resources/templates/list",
  "resources/read",
  "resources/subscribe",
  "resources/unsubscribe",
  "prompts/list",
  "prompts/get",
  "completion/complete",
  "logging/setLevel",
] as const;

export type ObservedTag = (typeof OBSERVED_TAGS)[number];

/**
 * Per-request bookkeeping created on the `run` side of the protocol proxy and
 * completed on the `send` side (Exit correlation) — keyed
 * `${clientId}:${requestId}`.
 */
export interface PendingRequest {
  readonly tag: ObservedTag;
  readonly clientId: number;
  readonly requestId: string;
  /** Original wire payload (pre context-strip), used for event parameters. */
  readonly payload: unknown;
  readonly headers: Record<string, string>;
  readonly mcpSessionId: string | undefined;
  readonly timestamp: Date;
  readonly startedAt: number;
  resourceName?: string;
  userIntent?: string;
  session?: ResolvedSession;
  tags?: Record<string, string> | null;
  properties?: Record<string, unknown> | null;
  /** Set when the event is deferred to the HTTP pre-response hook. */
  deferred?: DeferredInitialize;
}

/**
 * An HTTP `initialize` whose event emission waits for the pre-response hook,
 * where the `mcp-session-id` minted by McpServer becomes readable on the
 * response headers.
 */
export interface DeferredInitialize {
  readonly pending: PendingRequest;
  readonly clientInfo: ServerClientInfoLike | undefined;
  /** Resolved identity; `undefined` means identify was not configured. */
  readonly identity: UserIdentity | null | undefined;
  exit?: RpcMessage.ExitEncoded<unknown, unknown>;
  durationMs?: number;
}

// Bounded to survive pathological clients that never receive responses.
const MAX_PENDING_REQUESTS = 10_000;
// Bounds the per-clientId clientInfo cache (stdio uses a single clientId;
// header-less HTTP clients mint one per POST).
const MAX_CLIENT_INFO_ENTRIES = 1000;

/**
 * All state owned by one layer-combinator invocation: the TrackingContext
 * (pipeline token), its AgentCatData, the Effect-shaped options, and the
 * request/session correlation tables used by the protocol proxy.
 */
export interface EffectTracking {
  readonly ctx: TrackingContext;
  readonly data: AgentCatData;
  readonly options: AgentCatEffectOptions;
  /** Mutable snapshot backing the context's SessionInfoProvider. */
  readonly snapshot: {
    readonly serverName: string;
    readonly serverVersion: string;
    clientName: string | undefined;
    clientVersion: string | undefined;
  };
  /** Generated-session slot for requests without an MCP session id. */
  readonly generated: { sessionId: string; lastActivity: Date };
  readonly pendingRequests: Map<string, PendingRequest>;
  /** HTTP initializes awaiting the minted session id, keyed by HttpServerRequest. */
  readonly pendingHttpInit: WeakMap<object, DeferredInitialize>;
  readonly clientInfoByClientId: Map<number, ServerClientInfoLike>;
  /**
   * True when the combinator registered the HTTP route itself and wrapped the
   * route effect, so the minted `mcp-session-id` is observable at response
   * time (layerHttp). False for layer/layerStdio.
   */
  httpMintObservation: boolean;
}

/**
 * Fiber-context service provided by the protocol proxy around every forwarded
 * request; request handlers inherit it, enabling `publishCustomEvent` to
 * resolve the current session without arguments.
 */
export class CurrentAgentCatRequest extends Context.Service<
  CurrentAgentCatRequest,
  {
    readonly tracking: EffectTracking;
    readonly clientId: number;
    readonly mcpSessionId: string | undefined;
  }
>()("agentcat/effect/CurrentAgentCatRequest") {}

/**
 * Creates the per-combinator tracking state (decision 5): AgentCatData plus a
 * standalone TrackingContext whose SessionInfoProvider reads the mutable
 * snapshot updated per publish by {@link withSessionScope}.
 */
export const makeEffectTracking = (
  serverInfo: { readonly name: string; readonly version: string },
  projectId: string | null,
  options: AgentCatEffectOptions,
): EffectTracking => {
  const optionalDataOptions = pipe(
    {
      customContextDescription: options.customContextDescription,
      redactSensitiveInformation: options.redactSensitiveInformation,
    },
    Record.filterMap(Result.fromNullishOr(() => undefined)),
  );
  const data: AgentCatData = {
    projectId: projectId || "",
    sessionId: newSessionId(),
    lastActivity: new Date(),
    identifiedSessions: new Map<string, UserIdentity>(),
    sessionInfo: {},
    options: {
      enableReportMissing: options.enableReportMissing ?? true,
      enableTracing: options.enableTracing ?? true,
      enableToolCallContext: options.enableToolCallContext ?? true,
      ...optionalDataOptions,
    },
    sessionSource: "agentcat",
  };
  const snapshot = {
    serverName: serverInfo.name,
    serverVersion: serverInfo.version,
    clientName: undefined,
    clientVersion: undefined,
  };
  const ctx = createTrackingContext(data, () => ({
    serverName: snapshot.serverName,
    serverVersion: snapshot.serverVersion,
    ...pipe(
      {
        clientName: snapshot.clientName,
        clientVersion: snapshot.clientVersion,
      },
      Record.filterMap(Result.fromNullishOr(() => undefined)),
    ),
  }));
  return {
    ctx,
    data,
    options,
    snapshot,
    generated: { sessionId: data.sessionId, lastActivity: new Date() },
    pendingRequests: new Map(),
    pendingHttpInit: new WeakMap(),
    clientInfoByClientId: new Map(),
    httpMintObservation: false,
  };
};

/**
 * Resolves the session for one request per decision 6. Derivation results are
 * cached on the per-session shadow state, so repeated requests on one MCP
 * session hash only once.
 */
export const resolveSession = (
  tracking: EffectTracking,
  mcpSessionId: string | undefined,
  clientId: number,
): ResolvedSession => {
  if (mcpSessionId) {
    const state = getSessionState(tracking.ctx, mcpSessionId);
    if (!state.derivedSessionId) {
      state.derivedSessionId = deriveSessionIdFromMCPSession(
        mcpSessionId,
        tracking.data.projectId || undefined,
      );
    }
    state.lastActivity = new Date();
    const optionalSession = pipe(
      { clientInfo: state.clientInfo },
      Record.filterMap(Result.fromNullishOr(() => undefined)),
    );
    return {
      sessionId: state.derivedSessionId,
      mcpSessionId,
      source: "mcp",
      ...optionalSession,
    };
  }

  const generated = tracking.generated;
  const timeoutMs = INACTIVITY_TIMEOUT_IN_MINUTES * 60 * 1000;
  if (Date.now() - generated.lastActivity.getTime() > timeoutMs) {
    generated.sessionId = newSessionId();
  }
  generated.lastActivity = new Date();
  const optionalSession = pipe(
    { clientInfo: tracking.clientInfoByClientId.get(clientId) },
    Record.filterMap(Result.fromNullishOr(() => undefined)),
  );
  return {
    sessionId: generated.sessionId,
    source: "agentcat",
    ...optionalSession,
  };
};

/**
 * Retains initialize clientInfo for transports without an MCP session id,
 * keyed by protocol clientId (constant 0 on stdio).
 */
export const rememberClientInfo = (
  tracking: EffectTracking,
  clientId: number,
  clientInfo: ServerClientInfoLike | undefined,
): void => {
  if (!clientInfo) {
    return;
  }
  const cache = tracking.clientInfoByClientId;
  if (!cache.has(clientId) && cache.size >= MAX_CLIENT_INFO_ENTRIES) {
    const oldestKey = cache.keys().next().value;
    if (oldestKey !== undefined) {
      cache.delete(oldestKey);
    }
  }
  cache.set(clientId, clientInfo);
};

/** Inserts a pending request, evicting the oldest entry beyond the bound. */
export const trackPendingRequest = (
  tracking: EffectTracking,
  key: string,
  pending: PendingRequest,
): void => {
  const requests = tracking.pendingRequests;
  if (!requests.has(key) && requests.size >= MAX_PENDING_REQUESTS) {
    const oldestKey = requests.keys().next().value;
    if (oldestKey !== undefined) {
      requests.delete(oldestKey);
    }
  }
  requests.set(key, pending);
};

/**
 * Runs `fn` with the shared AgentCatData slots and the SessionInfoProvider
 * snapshot pointed at `session`. Every publish on the Effect path goes
 * through this scope; it must stay synchronous (no awaits between the slot
 * writes and the publish), which keeps concurrent sessions race-free on the
 * single-threaded runtime.
 */
export const withSessionScope = <T>(
  tracking: EffectTracking,
  session: ResolvedSession,
  fn: () => T,
): T => {
  const { data, snapshot } = tracking;
  data.sessionId = session.sessionId;
  if (session.mcpSessionId === undefined) {
    delete data.lastMcpSessionId;
  } else {
    data.lastMcpSessionId = session.mcpSessionId;
  }
  data.sessionSource = session.source;
  // Defeat the legacy "only fill clientInfo when unset" gate in
  // buildSessionInfo: on the multi-session Effect path the provider snapshot
  // is authoritative for every publish.
  data.sessionInfo = Struct.omit(data.sessionInfo, [
    "clientName",
    "clientVersion",
  ]);
  snapshot.clientName = session.clientInfo?.name;
  snapshot.clientVersion = session.clientInfo?.version;
  return fn();
};
