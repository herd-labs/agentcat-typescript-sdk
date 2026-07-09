import type { Effect } from "effect";
import type { HttpRouter } from "effect/unstable/http";
import type { ExporterConfig, RedactFunction, UserIdentity } from "../types.js";

/**
 * Headers of the observed MCP request (lower-cased keys). On HTTP transports
 * these include the HTTP request headers (e.g. `mcp-session-id`,
 * `authorization`); on stdio they are the JSON-RPC message headers, usually
 * empty.
 */
export type EffectRequestHeaders = Readonly<Record<string, string>>;

/**
 * A user-supplied option resolver for the Effect path.
 *
 * - As an `Effect`, it is evaluated in the request fiber, so services
 *   provided by host middleware (e.g. a `CurrentUser` from an auth layer) are
 *   readable via `Effect.serviceOption`.
 * - As a callback, it receives the wire-encoded request payload and the
 *   request headers.
 *
 * Failures are swallowed and logged; a `null` result means "nothing".
 */
export type EffectOptionResolver<A> =
  | Effect.Effect<A | null, unknown>
  | ((
      payload: unknown,
      headers: EffectRequestHeaders,
    ) => A | null | Promise<A | null>);

/**
 * Options for the `agentcat/effect` layer combinators. Mirrors
 * {@link AgentCatOptions} with the SDK-callback shapes replaced by
 * Effect-friendly resolvers (decision 7 in docs/plans/effect-mcp-support.md).
 */
export interface AgentCatEffectOptions {
  enableReportMissing?: boolean;
  enableTracing?: boolean;
  enableToolCallContext?: boolean;
  customContextDescription?: string;
  identify?: EffectOptionResolver<UserIdentity>;
  eventTags?: EffectOptionResolver<Record<string, string>>;
  eventProperties?: EffectOptionResolver<Record<string, unknown>>;
  redactSensitiveInformation?: RedactFunction;
  exporters?: Record<string, ExporterConfig>;
  apiBaseUrl?: string;
  disableDiagnostics?: boolean;
}

/**
 * Options forwarded verbatim to `McpServer.layer` (decision 2). Extra keys —
 * e.g. a `clientSessions` map on patched effect builds — pass through
 * untouched.
 */
export interface McpServerLayerOptions {
  readonly name: string;
  readonly version: string;
  readonly extensions?: Record<`${string}/${string}`, unknown> | undefined;
}

/** Options forwarded verbatim to the HTTP composition (`McpServer.layerHttp` mirror). */
export interface McpServerHttpLayerOptions extends McpServerLayerOptions {
  readonly path: HttpRouter.PathInput;
}
