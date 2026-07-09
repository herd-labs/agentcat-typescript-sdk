import {
  AgentCatData,
  MCPServerLike,
  ServerClientInfoLike,
  UserIdentity,
  CompatibleRequestHandlerExtra,
  UnredactedEvent,
} from "../types.js";
import { PublishEventRequestEventTypeEnum } from "agentcat-api";
import { publishEvent } from "./eventQueue.js";
import { writeToLog } from "./logging.js";
import { validateTags } from "./validation.js";

/**
 * Simple LRU cache for session identities.
 * Prevents memory leaks by capping at maxSize entries.
 * This cache persists across server instance restarts.
 */
class IdentityCache {
  private cache: Map<string, { identity: UserIdentity; timestamp: number }>;
  private maxSize: number;

  constructor(maxSize: number = 1000) {
    this.cache = new Map();
    this.maxSize = maxSize;
  }

  get(sessionId: string): UserIdentity | undefined {
    const entry = this.cache.get(sessionId);
    if (entry) {
      // Update timestamp on access (LRU behavior)
      entry.timestamp = Date.now();
      // Move to end (most recently used)
      this.cache.delete(sessionId);
      this.cache.set(sessionId, entry);
      return entry.identity;
    }
    return undefined;
  }

  set(sessionId: string, identity: UserIdentity): void {
    // Remove if already exists (to re-add at end)
    this.cache.delete(sessionId);

    // Evict oldest if at capacity
    if (this.cache.size >= this.maxSize) {
      const oldestKey = this.cache.keys().next().value;
      if (oldestKey !== undefined) {
        this.cache.delete(oldestKey);
      }
    }

    this.cache.set(sessionId, { identity, timestamp: Date.now() });
  }

  has(sessionId: string): boolean {
    return this.cache.has(sessionId);
  }

  size(): number {
    return this.cache.size;
  }
}

// Global identity cache shared across all server instances
// This prevents duplicate identify events when server objects are recreated
const _globalIdentityCache = new IdentityCache(1000);

/**
 * Snapshot of server/client identification info consumed when building
 * SessionInfo for outgoing events.
 */
export interface SessionInfoSnapshot {
  serverName?: string;
  serverVersion?: string;
  clientName?: string;
  clientVersion?: string;
}

/**
 * Lazily resolves server/client identification. The official-SDK
 * implementation reads the live server object (`getClientVersion()` /
 * `_serverInfo`); other integrations (e.g. Effect) can supply a provider
 * built from layer options + captured initialize payloads instead.
 *
 * `includeClientInfo` preserves the legacy official-SDK behavior of only
 * calling `getClientVersion()` when the current AgentCatData snapshot does
 * not already have a client name.
 */
export type SessionInfoProvider = (
  includeClientInfo: boolean,
) => SessionInfoSnapshot;

/**
 * Per-MCP-session state, keyed by the MCP protocol session id.
 *
 * Shadow-only for now: the single mutable slots on AgentCatData
 * (sessionId/sessionInfo/lastMcpSessionId/lastActivity) remain the source of
 * truth for the official-SDK path; this map is populated in parallel so a
 * multi-session integration (e.g. Effect HTTP) can consume it without the
 * last-writer-wins behavior of the legacy slots.
 */
export interface SessionState {
  clientInfo?: ServerClientInfoLike;
  identity?: UserIdentity;
  lastActivity?: Date;
  derivedSessionId?: string;
}

// Bounded like the identity cache to prevent unbounded growth on
// long-lived servers that see many MCP sessions.
const MAX_TRACKED_SESSIONS = 1000;

/**
 * Opaque tracking token that owns a tracked server's AgentCatData (via the
 * WeakMap below) plus the SessionInfoProvider used to resolve server/client
 * info. The event pipeline operates on this token; MCPServerLike is just an
 * adapter resolved through the server -> context lookup.
 */
export class TrackingContext {
  readonly sessionInfoProvider: SessionInfoProvider;
  readonly sessions = new Map<string, SessionState>();

  constructor(sessionInfoProvider: SessionInfoProvider) {
    this.sessionInfoProvider = sessionInfoProvider;
  }
}

// Internal tracking storage, keyed by the opaque TrackingContext token
const _serverTracking = new WeakMap<TrackingContext, AgentCatData>();
// Adapter lookup: official-SDK server object -> its TrackingContext
const _serverContexts = new WeakMap<MCPServerLike, TrackingContext>();

export function isTrackingContext(value: unknown): value is TrackingContext {
  return value instanceof TrackingContext;
}

/**
 * Reads server/client identification off an official-SDK server object.
 */
export function readOfficialSdkSessionInfo(
  server: MCPServerLike,
  includeClientInfo: boolean,
): SessionInfoSnapshot {
  const clientInfo = includeClientInfo ? server.getClientVersion() : undefined;
  return {
    serverName: server._serverInfo?.name,
    serverVersion: server._serverInfo?.version,
    clientName: clientInfo?.name,
    clientVersion: clientInfo?.version,
  };
}

/**
 * Creates a standalone TrackingContext (no server object involved).
 * Entry point for integrations that drive the pipeline context-first.
 */
export function createTrackingContext(
  data: AgentCatData,
  sessionInfoProvider: SessionInfoProvider,
): TrackingContext {
  const ctx = new TrackingContext(sessionInfoProvider);
  _serverTracking.set(ctx, data);
  return ctx;
}

export function getTrackingContext(
  server: MCPServerLike,
): TrackingContext | undefined {
  return _serverContexts.get(server);
}

export function getContextTrackingData(
  ctx: TrackingContext,
): AgentCatData | undefined {
  return _serverTracking.get(ctx);
}

export function setContextTrackingData(
  ctx: TrackingContext,
  data: AgentCatData,
): void {
  _serverTracking.set(ctx, data);
}

/**
 * Gets (creating if absent) the per-MCP-session shadow state for a context.
 */
export function getSessionState(
  ctx: TrackingContext,
  mcpSessionId: string,
): SessionState {
  let state = ctx.sessions.get(mcpSessionId);
  if (!state) {
    if (ctx.sessions.size >= MAX_TRACKED_SESSIONS) {
      const oldestKey = ctx.sessions.keys().next().value;
      if (oldestKey !== undefined) {
        ctx.sessions.delete(oldestKey);
      }
    }
    state = {};
    ctx.sessions.set(mcpSessionId, state);
  }
  return state;
}

export function getServerTrackingData(
  server: MCPServerLike,
): AgentCatData | undefined {
  const ctx = _serverContexts.get(server);
  return ctx ? _serverTracking.get(ctx) : undefined;
}

export function setServerTrackingData(
  server: MCPServerLike,
  data: AgentCatData,
): void {
  let ctx = _serverContexts.get(server);
  if (!ctx) {
    ctx = new TrackingContext((includeClientInfo) =>
      readOfficialSdkSessionInfo(server, includeClientInfo),
    );
    _serverContexts.set(server, ctx);
  }
  _serverTracking.set(ctx, data);
}

/**
 * Deep comparison of two UserIdentity objects
 */
export function areIdentitiesEqual(a: UserIdentity, b: UserIdentity): boolean {
  if (a.userId !== b.userId) return false;
  if (a.userName !== b.userName) return false;

  // Deep compare userData objects
  const aData = a.userData || {};
  const bData = b.userData || {};

  const aKeys = Object.keys(aData);
  const bKeys = Object.keys(bData);

  if (aKeys.length !== bKeys.length) return false;

  for (const key of aKeys) {
    if (!(key in bData)) return false;
    if (JSON.stringify(aData[key]) !== JSON.stringify(bData[key])) return false;
  }

  return true;
}

/**
 * Merges two UserIdentity objects, overwriting userId and userName,
 * but merging userData fields
 */
export function mergeIdentities(
  previous: UserIdentity | undefined,
  next: UserIdentity,
): UserIdentity {
  if (!previous) {
    return next;
  }

  return {
    userId: next.userId,
    userName: next.userName,
    userData: {
      ...(previous.userData || {}),
      ...(next.userData || {}),
    },
  };
}

/**
 * Minimal request shape the identify pipeline itself reads; the full request
 * object flows through to the user-supplied callbacks untouched.
 */
export interface IdentifyRequestLike {
  params?: { name?: string };
}

/**
 * Handles user identification for a request.
 * Calls the identify function if configured, compares with previous identity,
 * and publishes an identify event only if the identity has changed.
 *
 * @param server - The MCP server instance
 * @param data - The server tracking data
 * @param request - The request object to pass to identify function
 * @param extra - Optional extra parameters containing headers, sessionId, etc.
 */
export async function handleIdentify(
  server: MCPServerLike,
  data: AgentCatData,
  request: any,
  extra?: CompatibleRequestHandlerExtra,
): Promise<void> {
  return handleIdentifyCore(
    getTrackingContext(server),
    (event) => publishEvent(server, event),
    data,
    request,
    extra,
  );
}

/**
 * Context-first variant of handleIdentify for callers that have no server
 * object (e.g. the Effect integration).
 */
export async function handleIdentifyForContext(
  ctx: TrackingContext,
  request: IdentifyRequestLike,
  extra?: CompatibleRequestHandlerExtra,
): Promise<void> {
  const data = getContextTrackingData(ctx);
  if (!data) {
    return;
  }
  return handleIdentifyCore(
    ctx,
    (event) => publishEvent(ctx, event),
    data,
    request,
    extra,
  );
}

async function handleIdentifyCore(
  ctx: TrackingContext | undefined,
  publish: (event: UnredactedEvent) => void,
  data: AgentCatData,
  request: IdentifyRequestLike,
  extra?: CompatibleRequestHandlerExtra,
): Promise<void> {
  if (!data.options.identify) {
    return;
  }

  const sessionId = data.sessionId;
  const identifyEvent = buildIdentifyEvent(data, request, extra);

  try {
    const identityResult = await data.options.identify(request, extra);
    applyResolvedIdentity(
      ctx,
      publish,
      data,
      identityResult,
      identifyEvent,
      sessionId,
      extra,
    );
  } catch (error) {
    writeToLog(
      `Error: User supplied identify function threw an error while identifying session ${sessionId} - ${error}`,
    );
  }
}

function buildIdentifyEvent(
  data: AgentCatData,
  request: IdentifyRequestLike,
  extra?: CompatibleRequestHandlerExtra,
): UnredactedEvent {
  return {
    sessionId: data.sessionId,
    resourceName: request.params?.name || "Unknown",
    eventType: PublishEventRequestEventTypeEnum.agentcatIdentify,
    parameters: {
      request: request,
      extra: extra,
    },
    timestamp: new Date(),
    redactionFn: data.options.redactSensitiveInformation,
  };
}

/**
 * Merge/dedup + publish-on-change core shared by the callback path
 * (handleIdentifyCore) and the pre-resolved path (applyIdentityForContext).
 */
function applyResolvedIdentity(
  ctx: TrackingContext | undefined,
  publish: (event: UnredactedEvent) => void,
  data: AgentCatData,
  identityResult: UserIdentity | null | undefined,
  identifyEvent: UnredactedEvent,
  sessionId: string,
  extra?: CompatibleRequestHandlerExtra,
): void {
  if (identityResult) {
    // Now use the (possibly updated) sessionId for all subsequent operations
    const currentSessionId = data.sessionId;

    // Check global cache first (works across server instance restarts)
    const previousIdentity = _globalIdentityCache.get(currentSessionId);

    // Merge identities (overwrite userId/userName, merge userData)
    const mergedIdentity = mergeIdentities(previousIdentity, identityResult);

    // Only publish if identity has changed
    const hasChanged =
      !previousIdentity ||
      !areIdentitiesEqual(previousIdentity, mergedIdentity);

    // Update BOTH caches to keep them in sync
    // Global cache: persists across server instances
    _globalIdentityCache.set(currentSessionId, mergedIdentity);
    // Per-server cache: used by getSessionInfo() for fast local access
    data.identifiedSessions.set(data.sessionId, mergedIdentity);
    // Shadow per-session state, keyed by MCP session id (see SessionState)
    const mcpSessionId = extra?.sessionId ?? data.lastMcpSessionId;
    if (ctx && mcpSessionId) {
      getSessionState(ctx, mcpSessionId).identity = mergedIdentity;
    }

    if (hasChanged) {
      writeToLog(
        `Identified session ${currentSessionId} (actor: ${mergedIdentity.userId})`,
      );
      publish(identifyEvent);
    }
  } else {
    writeToLog(
      `Warning: Supplied identify function returned null for session ${sessionId}`,
    );
  }
}

/**
 * Applies a pre-resolved identity to a context: global-LRU merge/dedup,
 * per-session caches, and identify-event publication on change. Entry point
 * for the Effect path, where the identity value is produced by an Effect (or
 * callback) evaluated in the request fiber rather than by
 * data.options.identify.
 */
export function applyIdentityForContext(
  ctx: TrackingContext,
  identity: UserIdentity | null,
  request: IdentifyRequestLike,
  extra?: CompatibleRequestHandlerExtra,
): void {
  const data = getContextTrackingData(ctx);
  if (!data) {
    return;
  }
  try {
    applyResolvedIdentity(
      ctx,
      (event) => publishEvent(ctx, event),
      data,
      identity,
      buildIdentifyEvent(data, request, extra),
      data.sessionId,
      extra,
    );
  } catch (error) {
    writeToLog(
      `Error: Failed to apply identity for session ${data.sessionId} - ${error}`,
    );
  }
}

/**
 * Resolves the eventTags callback, validates the result, and returns validated tags.
 * Returns null if no callback configured, callback returns nullish, or callback throws.
 */
export async function resolveEventTags(
  data: AgentCatData,
  request: any,
  extra?: CompatibleRequestHandlerExtra,
): Promise<Record<string, string> | null> {
  if (!data.options.eventTags) return null;
  try {
    const raw = (await data.options.eventTags(request, extra)) ?? null;
    if (!raw) return null;
    return validateTags(raw);
  } catch (e) {
    writeToLog(`eventTags callback error: ${e}`);
    return null;
  }
}

/**
 * Resolves the eventProperties callback and returns the result.
 * Returns null if no callback configured, callback returns nullish, or callback throws.
 */
export async function resolveEventProperties(
  data: AgentCatData,
  request: any,
  extra?: CompatibleRequestHandlerExtra,
): Promise<Record<string, any> | null> {
  if (!data.options.eventProperties) return null;
  try {
    return (await data.options.eventProperties(request, extra)) ?? null;
  } catch (e) {
    writeToLog(`eventProperties callback error: ${e}`);
    return null;
  }
}

/**
 * Context-first variant of resolveEventTags: resolves the tracked data from
 * the context token instead of requiring the caller to carry it.
 */
export async function resolveEventTagsForContext(
  ctx: TrackingContext,
  request: unknown,
  extra?: CompatibleRequestHandlerExtra,
): Promise<Record<string, string> | null> {
  const data = getContextTrackingData(ctx);
  if (!data) return null;
  return resolveEventTags(data, request, extra);
}

/**
 * Context-first variant of resolveEventProperties.
 */
export async function resolveEventPropertiesForContext(
  ctx: TrackingContext,
  request: unknown,
  extra?: CompatibleRequestHandlerExtra,
): Promise<Record<string, any> | null> {
  const data = getContextTrackingData(ctx);
  if (!data) return null;
  return resolveEventProperties(data, request, extra);
}
