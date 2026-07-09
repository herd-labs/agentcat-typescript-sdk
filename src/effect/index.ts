/**
 * agentcat/effect — analytics capture for MCP servers built with Effect's
 * `McpServer` (`effect/unstable/ai`, effect v4). ESM-only; the root
 * `agentcat` entry never imports effect, and this entry never imports
 * `@modelcontextprotocol/sdk`.
 *
 * See docs/plans/effect-mcp-support.md for the architecture.
 */
export {
  layer,
  layerHttp,
  layerStdio,
  type AgentCatHttpLayer,
  type AgentCatMcpServices,
  type AgentCatServerLayer,
  type AgentCatStdioLayer,
} from "./layers.js";
export {
  publishCustomEvent,
  type PublishCustomEventFallback,
} from "./customEvent.js";
export type {
  AgentCatEffectOptions,
  EffectOptionResolver,
  EffectRequestHeaders,
  McpServerHttpLayerOptions,
  McpServerLayerOptions,
} from "./types.js";
export type {
  CustomEventData,
  ExporterConfig,
  RedactFunction,
  UserIdentity,
} from "../types.js";
