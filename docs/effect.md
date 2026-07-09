# AgentCat for Effect MCP servers (`agentcat/effect`)

Analytics capture for MCP servers built with Effect's `McpServer` (`effect/unstable/ai`, effect v4). Everything the official-SDK path captures via `track()` — tool calls with arguments/results/errors/duration, initialize + client info, `tools/list`, session continuity, `identify`, context-parameter injection, `get_more_tools`, redaction, and the exporter fan-out — for Effect layer graphs.

## Install

```bash
npm install -S agentcat effect
```

- `effect` is an **optional peer dependency** (`^4.0.0-beta.85` — effect v4 betas at or above the tested floor, and stable 4.x once released). The root `agentcat` entry never loads it.
- `agentcat/effect` is **ESM-only** (effect v4 is pure ESM). The root entry keeps its CJS build.
- Requires the `effect/unstable/ai` `McpServer` (effect v4). effect v3 / `@effect/ai` is not supported.

## Usage

The combinators are drop-in mirrors of `McpServer.layerStdio`, `McpServer.layerHttp`, and `McpServer.layer` with two extra arguments: your AgentCat project id (or `null` for telemetry-only mode) and options.

```ts
import { Effect, Layer, Option } from "effect";
import { NodeStdio } from "@effect/platform-node";
import * as AgentCat from "agentcat/effect";

const Server = Layer.mergeAll(MyToolkit, MyResources).pipe(
  Layer.provide(
    AgentCat.layerStdio(
      { name: "demo", version: "1.0.0" }, // same options as McpServer.layerStdio
      "proj_0000000",
      {
        enableToolCallContext: true,
        enableReportMissing: true,
        identify: Effect.gen(function* () {
          // Runs in the request fiber: services provided by your host
          // middleware (auth, etc.) are visible here.
          const user = yield* Effect.serviceOption(CurrentUser);
          return Option.isSome(user)
            ? { userId: user.value.id, userName: user.value.name }
            : null;
        }),
        redactSensitiveInformation: (text) => redact(text),
        exporters: {
          otlp: { type: "otlp", endpoint: "http://localhost:4318/v1/traces" },
        },
      },
    ),
  ),
  Layer.provide(NodeStdio.layer),
);
```

Over HTTP, pass the router path exactly like `McpServer.layerHttp`:

```ts
AgentCat.layerHttp(
  { name: "demo", version: "1.0.0", path: "/mcp" },
  "proj_0000000",
);
// Layer<McpServer | McpServerClient, never, HttpRouter.HttpRouter>
```

For custom transports there is a bring-your-own-protocol variant mirroring `McpServer.layer`:

```ts
AgentCat.layer({ name: "demo", version: "1.0.0" }, "proj_0000000");
// Layer<McpServer | McpServerClient, never, RpcServer.Protocol>
```

Server options are forwarded to `McpServer.layer` verbatim, so options accepted by your effect build (including patched builds) pass through untouched.

## Options

`AgentCatEffectOptions` mirrors the root entry's `AgentCatOptions`, with the request-callback shapes replaced by Effect-friendly resolvers:

| Option                             | Type                                                                                         | Default              |
| ---------------------------------- | -------------------------------------------------------------------------------------------- | -------------------- |
| `enableTracing`                    | `boolean`                                                                                    | `true`               |
| `enableToolCallContext`            | `boolean`                                                                                    | `true`               |
| `enableReportMissing`              | `boolean`                                                                                    | `true`               |
| `customContextDescription`         | `string`                                                                                     | built-in description |
| `identify`                         | `Effect<UserIdentity \| null> \| (payload, headers) => UserIdentity \| null \| Promise<...>` | —                    |
| `eventTags`                        | `Effect<Record<string, string> \| null> \| (payload, headers) => ...`                        | —                    |
| `eventProperties`                  | `Effect<Record<string, unknown> \| null> \| (payload, headers) => ...`                       | —                    |
| `redactSensitiveInformation`       | `(text: string) => Promise<string>`                                                          | —                    |
| `exporters`                        | same as root entry                                                                           | —                    |
| `apiBaseUrl`, `disableDiagnostics` | same as root entry                                                                           | —                    |

`identify`/`eventTags`/`eventProperties` given as an **Effect** are evaluated in the MCP request fiber, so anything your HTTP middleware provides (e.g. a `CurrentUser` service from auth) is readable with `Effect.serviceOption`. Given as a **callback**, they receive the wire-encoded request payload and the request headers (on HTTP these include headers like `authorization` and `mcp-session-id`). Failures are logged and swallowed — analytics never break your server.

## Custom events

Inside any tool/resource/prompt handler of a server composed with the AgentCat combinators, publish custom events without any wiring — the session resolves from the request fiber:

```ts
import * as AgentCat from "agentcat/effect";

const handlers = myToolkit.toLayer({
  my_tool: (args) =>
    Effect.gen(function* () {
      yield* AgentCat.publishCustomEvent({ message: "did the thing" });
      return result;
    }),
});
```

Outside a request fiber, pass an explicit session:

```ts
AgentCat.publishCustomEvent(eventData, {
  sessionId: "my-session-id",
  projectId: "proj_0000000",
});
```

Fallback publishes a raw session/project event. It does not inherit per-layer request context such as `redactSensitiveInformation`, client info, or actor identity; call `publishCustomEvent` inside a request fiber when those fields should be attached automatically.

## Session semantics

- **HTTP**: sessions key off the `mcp-session-id` header minted by `McpServer` at `initialize`. AgentCat derives a deterministic `ses_` id from it, so multiple replicas serving one MCP session report the same session id with no shared state and no sticky sessions. Client info (name/version) is tracked per session.
- **stdio**: one generated session per process, rotating after 30 minutes of inactivity — same as the official-SDK path.
- **`AgentCat.layer` (BYO protocol) on HTTP**: the minted session id is not observable at `initialize` time, so initialize events fall back to a generated session; requests that carry the header still derive correctly. Prefer `AgentCat.layerHttp` for HTTP.

## Captured events

Every wire-visible MCP request method with a first-class AgentCat event type is captured, with request/response payloads, duration, errors, and session attribution:

| MCP method                            | Event type                                  | `resourceName`             |
| ------------------------------------- | ------------------------------------------- | -------------------------- |
| `initialize`                          | `mcp:initialize`                            | —                          |
| `tools/list`                          | `mcp:tools/list`                            | —                          |
| `tools/call`                          | `mcp:tools/call`                            | tool name                  |
| `resources/list`                      | `mcp:resources/list`                        | —                          |
| `resources/templates/list`            | `mcp:resources/templates/list`              | —                          |
| `resources/read`                      | `mcp:resources/read`                        | resource uri               |
| `resources/subscribe` / `unsubscribe` | `mcp:resources/subscribe` / `…/unsubscribe` | resource uri               |
| `prompts/list`                        | `mcp:prompts/list`                          | —                          |
| `prompts/get`                         | `mcp:prompts/get`                           | prompt name                |
| `completion/complete`                 | `mcp:completion/complete`                   | prompt name / resource uri |
| `logging/setLevel`                    | `mcp:logging/setLevel`                      | —                          |

`ping` is deliberately not captured (keepalive noise). Resource and prompt capture is a superset of the official-SDK `track()` path, which only hooks `initialize`, `tools/list`, and `tools/call`.

## Notes

- Compose a combinator **once** per server, like calling `track()` once. Each call creates its own tracking state.
- `get_more_tools` is registered as a real Effect toolkit tool when `enableReportMissing` is on; its calls surface as regular tool-call events with the caller's intent.
- With `enableToolCallContext` on, AgentCat injects a `context` parameter into your tools' schemas in `tools/list` responses and strips it back out of `tools/call` requests before your handlers decode them — handlers never see it.
- If one process uses both `track()` (official SDK) and `agentcat/effect`, each entry runs its own event pipeline instance; configure exporters on both if you need them on both.
