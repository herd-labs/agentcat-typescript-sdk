import { Effect, Layer, pipe, Record, Result, Stdio } from "effect";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import { HttpRouter } from "effect/unstable/http";
import { McpSchema, McpServer } from "effect/unstable/ai";
import { writeToLog } from "../modules/logging.js";
import { eventQueue, setTelemetryManager } from "../modules/eventQueue.js";
import { TelemetryManager } from "../modules/telemetry.js";
import { initDiagnostics } from "../modules/diagnostics.js";
import type {
  AgentCatEffectOptions,
  McpServerHttpLayerOptions,
  McpServerLayerOptions,
} from "./types.js";
import { makeEffectTracking, type EffectTracking } from "./session.js";
import { wrapHttpRouteEffect, wrapProtocol } from "./protocolProxy.js";
import { reportMissingToolkit } from "./reportMissing.js";

/**
 * What the combinators provide — identical to upstream `McpServer.layer`:
 * the McpServer registry plus the per-request McpServerClient.
 */
export type AgentCatMcpServices =
  McpServer.McpServer | McpSchema.McpServerClient;

/** BYO-protocol layer: mirrors `McpServer.layer`. */
export type AgentCatServerLayer = Layer.Layer<
  AgentCatMcpServices,
  never,
  RpcServer.Protocol
>;

/** stdio layer: mirrors `McpServer.layerStdio`. */
export type AgentCatStdioLayer = Layer.Layer<
  AgentCatMcpServices,
  never,
  Stdio.Stdio
>;

/** HTTP layer: mirrors `McpServer.layerHttp`. */
export type AgentCatHttpLayer = Layer.Layer<
  AgentCatMcpServices,
  never,
  HttpRouter.HttpRouter
>;

/**
 * Drop-in mirror of `McpServer.layer` with agentcat capture (decision 2 in
 * docs/plans/effect-mcp-support.md): composes `McpServer.layer(serverOptions)`
 * over a proxied `RpcServer.Protocol` taken from the surrounding layer graph.
 * `serverOptions` is forwarded verbatim — the generic parameter admits extra
 * keys (e.g. a `clientSessions` map on patched effect builds) without
 * excess-property errors.
 *
 * Note: with a bring-your-own protocol the minted HTTP `mcp-session-id` is
 * not observable at initialize time; prefer {@link layerHttp} for HTTP
 * transports.
 *
 * Compose once per server (like `track()`, tracking state is created per
 * combinator call).
 */
export const layer = <const O extends McpServerLayerOptions>(
  serverOptions: O,
  projectId: string | null,
  options: AgentCatEffectOptions = {},
): AgentCatServerLayer => {
  const tracking = setupTracking(serverOptions, projectId, options, false);
  if (!tracking) {
    return McpServer.layer(serverOptions);
  }
  return composeServerLayer(tracking, serverOptions, wrapProtocol(tracking));
};

/**
 * Drop-in mirror of `McpServer.layerStdio` with agentcat capture: the proxied
 * protocol over `RpcServer.layerProtocolStdio` + NDJSON-RPC serialization.
 */
export const layerStdio = <const O extends McpServerLayerOptions>(
  serverOptions: O,
  projectId: string | null,
  options: AgentCatEffectOptions = {},
): AgentCatStdioLayer =>
  layer(serverOptions, projectId, options).pipe(
    Layer.provide(RpcServer.layerProtocolStdio),
    Layer.provide(RpcSerialization.layerNdJsonRpc()),
  );

/**
 * Drop-in mirror of `McpServer.layerHttp` with agentcat capture. Registers
 * the JSON-RPC POST route on the current `HttpRouter` itself (mirroring
 * `RpcServer.layerProtocolHttp`) so the route effect can be wrapped to
 * observe the `mcp-session-id` minted at initialize — the basis for
 * per-session clientInfo and deterministic cross-replica session ids.
 */
export const layerHttp = <const O extends McpServerHttpLayerOptions>(
  serverOptions: O,
  projectId: string | null,
  options: AgentCatEffectOptions = {},
): AgentCatHttpLayer => {
  const tracking = setupTracking(serverOptions, projectId, options, true);
  if (!tracking) {
    return McpServer.layerHttp(serverOptions);
  }
  const httpProtocol = Layer.effect(RpcServer.Protocol)(
    Effect.gen(function* () {
      const { httpEffect, protocol } =
        yield* RpcServer.makeProtocolWithHttpEffect;
      const router = yield* HttpRouter.HttpRouter;
      yield* router.add(
        "POST",
        serverOptions.path,
        wrapHttpRouteEffect(tracking, httpEffect),
      );
      return protocol;
    }),
  );
  const protocolLayer = wrapProtocol(tracking).pipe(
    Layer.provide(httpProtocol),
    Layer.provide(RpcSerialization.layerJsonRpc()),
  );
  return composeServerLayer(tracking, serverOptions, protocolLayer);
};

const composeServerLayer = <R>(
  tracking: EffectTracking,
  serverOptions: McpServerLayerOptions,
  protocolLayer: Layer.Layer<RpcServer.Protocol, never, R>,
): Layer.Layer<AgentCatMcpServices, never, R> => {
  const base = McpServer.layer(serverOptions).pipe(
    Layer.provide(protocolLayer),
  );
  if (tracking.data.options.enableReportMissing !== true) {
    return base;
  }
  // The toolkit layer registers on the memoized McpServer registry shared
  // with `base` within one layer graph build.
  return Layer.mergeAll(base, reportMissingToolkit);
};

/**
 * Mirrors the wiring half of `track()`: diagnostics beacon, API base URL,
 * telemetry exporters, and tracking-state construction. Failures never break
 * the server — the combinators fall back to the plain upstream composition.
 */
const setupTracking = (
  serverOptions: McpServerLayerOptions,
  projectId: string | null,
  options: AgentCatEffectOptions,
  httpMintObservation: boolean,
): EffectTracking | undefined => {
  try {
    const optionalDiagnosticsOptions = pipe(
      { disabled: options.disableDiagnostics },
      Record.filterMap(Result.fromNullishOr(() => undefined)),
    );
    initDiagnostics({
      projectId,
      ...optionalDiagnosticsOptions,
    });

    const apiBaseUrl =
      options.apiBaseUrl ||
      process.env["AGENTCAT_API_URL"] ||
      process.env["MCPCAT_API_URL"];
    if (apiBaseUrl) {
      eventQueue.configure(apiBaseUrl);
    }

    writeToLog(
      `AgentCat setup started | project ${projectId || "(telemetry-only)"} | server effect`,
    );

    if (options.exporters) {
      setTelemetryManager(new TelemetryManager(options.exporters));
      writeToLog(
        `Initialized telemetry with ${Object.keys(options.exporters).length} exporters`,
      );
    }

    if (!projectId && !options.exporters) {
      writeToLog(
        "Warning: No projectId provided and no exporters configured. Events will not be sent anywhere.",
      );
    }

    const tracking = makeEffectTracking(serverOptions, projectId, options);
    tracking.httpMintObservation = httpMintObservation;

    const exporterCount = options.exporters
      ? Object.keys(options.exporters).length
      : 0;
    writeToLog(
      `AgentCat setup complete | project ${projectId || "(telemetry-only)"} | tracing=${tracking.data.options.enableTracing} context=${tracking.data.options.enableToolCallContext} reportMissing=${tracking.data.options.enableReportMissing} exporters=${exporterCount}`,
    );

    return tracking;
  } catch (error) {
    writeToLog(`Warning: Failed to track server - ${error}`);
    return undefined;
  }
};
