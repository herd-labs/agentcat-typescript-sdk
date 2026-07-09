/**
 * Acceptance tests for the `agentcat/effect` stdio path (`layerStdio`), per
 * docs/plans/effect-mcp-support.md §7: a real `McpServer` toolkit composed
 * over the public layer combinators, driven through a fake stdio transport
 * (`Stdio.layerTest`), with captured events asserted via a stubbed
 * `eventQueue.add` — no network.
 *
 * Harness notes (Phase 0 finding): `makeProtocolStdio` interrupts the fiber
 * that built it when stdin terminates, so the server is built inside
 * `Effect.forkChild(Effect.scoped(...))` and results are handed out through a
 * `Deferred`.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  Deferred,
  Effect,
  Layer,
  Predicate,
  Queue,
  Schema,
  Sink,
  Stdio,
  Stream,
} from "effect";
import { McpServer, Tool, Toolkit } from "effect/unstable/ai";
import { layerStdio, publishCustomEvent } from "../effect/index.js";
import type { AgentCatEffectOptions } from "../effect/index.js";
import { eventQueue } from "../modules/eventQueue.js";
import {
  GET_MORE_TOOLS_CONTEXT_DESCRIPTION,
  GET_MORE_TOOLS_NAME,
} from "../modules/constants.js";
import type { UnredactedEvent } from "../types.js";

const asRecord = (u: unknown, label: string): Record<string, unknown> => {
  if (!Predicate.isObject(u)) {
    throw new Error(`expected ${label} to be a record, got ${typeof u}`);
  }
  return u;
};

const asArray = (u: unknown, label: string): unknown[] => {
  if (!Array.isArray(u)) {
    throw new Error(`expected ${label} to be an array, got ${typeof u}`);
  }
  return u;
};

/** Extracts the joined text of a CallToolResult's text content parts. */
const contentText = (result: Record<string, unknown>): string =>
  asArray(result.content, "result.content")
    .map((part) => {
      const record = asRecord(part, "content part");
      return typeof record.text === "string" ? record.text : "";
    })
    .join(" ");

const toolByName = (
  tools: unknown[],
  name: string,
): Record<string, unknown> => {
  const tool = tools.find((t) => Predicate.isObject(t) && t.name === name);
  return asRecord(tool, `tool ${name}`);
};

// ---------------------------------------------------------------------------
// Fixture: a two-tool server (one succeeding, one throwing)
// ---------------------------------------------------------------------------

const EchoTool = Tool.make("echo", {
  description: "Echo a message back",
  parameters: Schema.Struct({ message: Schema.String }),
  success: Schema.String,
});

const BoomTool = Tool.make("boom", {
  description: "Always fails",
  parameters: Schema.Struct({ message: Schema.String }),
  success: Schema.String,
});

const fixtureToolkit = Toolkit.make(EchoTool, BoomTool);

interface FixtureHooks {
  /** Receives the (post-strip) arguments each echo handler call saw. */
  readonly received?: Array<Record<string, unknown>>;
  /** When set, the echo handler publishes a custom event with this message. */
  readonly customEventMessage?: string;
}

const fixtureHandlers = (hooks: FixtureHooks = {}) =>
  McpServer.toolkit(fixtureToolkit).pipe(
    Layer.provide(
      fixtureToolkit.toLayer({
        echo: (params: { readonly message: string }) =>
          Effect.gen(function* () {
            hooks.received?.push({ ...params });
            if (hooks.customEventMessage !== undefined) {
              yield* publishCustomEvent({
                message: hooks.customEventMessage,
              });
            }
            return `echo: ${params.message}`;
          }),
        boom: () =>
          Effect.sync<string>(() => {
            throw new Error("boom: intentional stdio failure");
          }),
      }),
    ),
  );

const SERVER_INFO = { name: "stdio-server", version: "9.9.9" } as const;

const makeServer = (
  projectId: string,
  options: AgentCatEffectOptions = {},
  hooks: FixtureHooks = {},
) =>
  fixtureHandlers(hooks).pipe(
    Layer.provideMerge(layerStdio(SERVER_INFO, projectId, options)),
  );

// ---------------------------------------------------------------------------
// stdio harness
// ---------------------------------------------------------------------------

type JsonRecord = Record<string, unknown>;

interface StdioClient {
  readonly request: (
    method: string,
    params?: unknown,
  ) => Effect.Effect<JsonRecord, unknown>;
  readonly notify: (method: string) => Effect.Effect<void, unknown>;
}

const runStdio = async <A>(
  serverLayer: Layer.Layer<unknown, unknown, Stdio.Stdio>,
  drive: (client: StdioClient) => Effect.Effect<A, unknown>,
): Promise<A> => {
  const program = Effect.gen(function* () {
    const done = yield* Deferred.make<A, unknown>();
    yield* Effect.forkChild(
      Effect.scoped(
        Effect.gen(function* () {
          const stdinQueue = yield* Queue.make<Uint8Array>();
          const responses = yield* Queue.make<JsonRecord>();
          const decoder = new TextDecoder();
          let buffer = "";
          const feed = (chunk: string | Uint8Array) => {
            buffer +=
              typeof chunk === "string"
                ? chunk
                : decoder.decode(chunk, { stream: true });
            let newline = buffer.indexOf("\n");
            while (newline >= 0) {
              const line = buffer.slice(0, newline).trim();
              buffer = buffer.slice(newline + 1);
              if (line.length > 0) {
                const parsed: unknown = JSON.parse(line);
                if (
                  Predicate.isObject(parsed) &&
                  parsed.id !== undefined &&
                  parsed.id !== null
                ) {
                  Queue.offerUnsafe(responses, parsed);
                }
              }
              newline = buffer.indexOf("\n");
            }
          };
          const stdioLayer = Stdio.layerTest({
            stdin: Stream.fromQueue(stdinQueue),
            stdout: () =>
              Sink.forEach((chunk: string | Uint8Array) =>
                Effect.sync(() => feed(chunk)),
              ),
          });
          yield* Layer.build(serverLayer.pipe(Layer.provide(stdioLayer)));
          const encoder = new TextEncoder();
          const write = (message: object) =>
            Queue.offer(
              stdinQueue,
              encoder.encode(`${JSON.stringify(message)}\n`),
            );
          const takeMatching = (
            id: number,
          ): Effect.Effect<JsonRecord, unknown> =>
            Effect.flatMap(Queue.take(responses), (res) =>
              String(res.id) === String(id)
                ? Effect.succeed(res)
                : takeMatching(id),
            ).pipe(Effect.timeout(5000));
          let nextId = 0;
          const client: StdioClient = {
            request: (method, params) =>
              Effect.suspend(() => {
                nextId += 1;
                const id = nextId;
                return Effect.flatMap(
                  write({ jsonrpc: "2.0", id, method, params: params ?? {} }),
                  () => takeMatching(id),
                );
              }),
            notify: (method) => write({ jsonrpc: "2.0", method }),
          };
          const result = yield* drive(client);
          yield* Deferred.succeed(done, result);
        }),
      ).pipe(Effect.catchCause((cause) => Deferred.failCause(done, cause))),
    );
    return yield* Deferred.await(done).pipe(Effect.timeout(15000));
  });
  return await Effect.runPromise(program);
};

const initialize = (
  client: StdioClient,
  clientInfo: { name: string; version: string } = {
    name: "stdio-client",
    version: "1.2.3",
  },
): Effect.Effect<JsonRecord, unknown> =>
  Effect.gen(function* () {
    const res = yield* client.request("initialize", {
      protocolVersion: "2025-06-18",
      capabilities: {},
      clientInfo,
    });
    yield* client.notify("notifications/initialized");
    return res;
  });

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe("agentcat/effect layerStdio", () => {
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

  it("initialize emits one mcp:initialize event with client/server identity", async () => {
    const initRes = await runStdio(makeServer("proj_stdio_init"), (client) =>
      initialize(client),
    );

    // Wire: the initialize result carries the server's own identity.
    const initResult = asRecord(initRes.result, "initialize result");
    const serverInfo = asRecord(initResult.serverInfo, "serverInfo");
    expect(serverInfo.name).toBe("stdio-server");
    expect(serverInfo.version).toBe("9.9.9");

    const inits = byType("mcp:initialize");
    expect(inits).toHaveLength(1);
    const init = inits[0];
    expect(init.clientName).toBe("stdio-client");
    expect(init.clientVersion).toBe("1.2.3");
    expect(init.serverName).toBe("stdio-server");
    expect(init.serverVersion).toBe("9.9.9");
    expect(init.projectId).toBe("proj_stdio_init");
    expect(init.resourceName).toBe("Unknown Tool Name");
    expect(init.sessionId).toMatch(/^ses_/);
    expect(init.duration).toBeTypeOf("number");
    expect(init.duration).toBeGreaterThanOrEqual(0);

    // The captured response is the encoded InitializeResult envelope.
    const eventResponse = asRecord(init.response, "initialize event response");
    const responseServerInfo = asRecord(
      eventResponse.serverInfo,
      "event response serverInfo",
    );
    expect(responseServerInfo.name).toBe("stdio-server");
  }, 20000);

  it("tools/list injects context everywhere except get_more_tools' own parameter", async () => {
    const listRes = await runStdio(makeServer("proj_stdio_list"), (client) =>
      Effect.gen(function* () {
        yield* initialize(client);
        return yield* client.request("tools/list", {});
      }),
    );

    const listResult = asRecord(listRes.result, "tools/list result");
    const tools = asArray(listResult.tools, "tools");
    const names = tools.map((t) => asRecord(t, "tool").name);
    expect(names).toContain("echo");
    expect(names).toContain("boom");
    expect(names).toContain(GET_MORE_TOOLS_NAME);

    // Every tool carries a context parameter, and it is required.
    for (const name of ["echo", "boom", GET_MORE_TOOLS_NAME]) {
      const tool = toolByName(tools, name);
      const schema = asRecord(tool.inputSchema, `${name} inputSchema`);
      const properties = asRecord(schema.properties, `${name} properties`);
      const context = asRecord(properties.context, `${name} context`);
      expect(context.type).toBe("string");
      const required = asArray(schema.required, `${name} required`);
      expect(required).toContain("context");
    }

    // Regular tools got the injected parameter on top of their own schema...
    const echoSchema = asRecord(
      toolByName(tools, "echo").inputSchema,
      "echo inputSchema",
    );
    expect(asArray(echoSchema.required, "echo required")).toEqual([
      "message",
      "context",
    ]);
    const echoContext = asRecord(
      asRecord(echoSchema.properties, "echo properties").context,
      "echo context",
    );

    // ...while get_more_tools keeps its own context parameter untouched: its
    // description differs from the injected one and required is not doubled.
    const moreSchema = asRecord(
      toolByName(tools, GET_MORE_TOOLS_NAME).inputSchema,
      "get_more_tools inputSchema",
    );
    const moreContext = asRecord(
      asRecord(moreSchema.properties, "get_more_tools properties").context,
      "get_more_tools context",
    );
    expect(moreContext.description).toBe(GET_MORE_TOOLS_CONTEXT_DESCRIPTION);
    expect(echoContext.description).not.toBe(moreContext.description);
    expect(asArray(moreSchema.required, "get_more_tools required")).toEqual([
      "context",
    ]);

    // Event: exactly one mcp:tools/list whose response is the mutated list.
    const lists = byType("mcp:tools/list");
    expect(lists).toHaveLength(1);
    const list = lists[0];
    expect(list.isError).toBe(false);
    // The list event shares the connection's generated session.
    expect(list.sessionId).toBe(byType("mcp:initialize")[0].sessionId);
    const eventTools = asArray(
      asRecord(list.response, "tools/list event response").tools,
      "event tools",
    );
    const eventEcho = toolByName(eventTools, "echo");
    const eventEchoProps = asRecord(
      asRecord(eventEcho.inputSchema, "event echo inputSchema").properties,
      "event echo properties",
    );
    expect(eventEchoProps.context).toBeDefined();
  }, 20000);

  it("tools/call strips context for the handler, captures intent, and keeps session continuity", async () => {
    const received: Array<Record<string, unknown>> = [];
    const callRes = await runStdio(
      makeServer(
        "proj_stdio_call",
        {},
        {
          received,
          customEventMessage: "from handler",
        },
      ),
      (client) =>
        Effect.gen(function* () {
          yield* initialize(client);
          return yield* client.request("tools/call", {
            name: "echo",
            arguments: { message: "hi", context: "finding the answer" },
          });
        }),
    );

    // Wire: successful CallToolResult with the handler's return value.
    const callResult = asRecord(callRes.result, "tools/call result");
    expect(callResult.isError).toBe(false);
    expect(contentText(callResult)).toContain("echo: hi");

    // The handler saw the arguments WITHOUT the injected context parameter.
    expect(received).toEqual([{ message: "hi" }]);

    const init = byType("mcp:initialize")[0];
    expect(init).toBeDefined();
    const calls = byType("mcp:tools/call");
    expect(calls).toHaveLength(1);
    const call = calls[0];
    expect(call.resourceName).toBe("echo");
    expect(call.userIntent).toBe("finding the answer");
    expect(call.isError).toBeFalsy();
    expect(
      contentText(asRecord(call.response, "call event response")),
    ).toContain("echo: hi");
    // Session continuity across the stdio connection.
    expect(call.sessionId).toMatch(/^ses_/);
    expect(call.sessionId).toBe(init.sessionId);
    // clientInfo captured at initialize propagates to later session events.
    expect(call.clientName).toBe("stdio-client");
    expect(call.clientVersion).toBe("1.2.3");
    const parameters = asRecord(call.parameters, "call parameters");
    expect(asRecord(parameters.request, "call request").method).toBe(
      "tools/call",
    );

    // publishCustomEvent inside the handler lands on the same session.
    const customs = byType("agentcat:custom");
    expect(customs).toHaveLength(1);
    expect(customs[0].sessionId).toBe(init.sessionId);
    expect(customs[0].userIntent).toBe("from handler");
  }, 20000);

  it("a failing tool surfaces isError on the wire and on the event with an error message", async () => {
    const callRes = await runStdio(makeServer("proj_stdio_boom"), (client) =>
      Effect.gen(function* () {
        yield* initialize(client);
        return yield* client.request("tools/call", {
          name: "boom",
          arguments: { message: "x", context: "triggering a failure" },
        });
      }),
    );

    // Wire: effect wraps handler failures into an isError CallToolResult.
    const callResult = asRecord(callRes.result, "tools/call result");
    expect(callResult.isError).toBe(true);
    expect(contentText(callResult)).toContain(
      "boom: intentional stdio failure",
    );

    const calls = byType("mcp:tools/call");
    expect(calls).toHaveLength(1);
    const call = calls[0];
    expect(call.resourceName).toBe("boom");
    expect(call.isError).toBe(true);
    const error = asRecord(call.error, "call event error");
    expect(error.message).toBeTypeOf("string");
    expect(error.message).toContain("boom: intentional stdio failure");
  }, 20000);

  it("get_more_tools answers the canned text and keeps its context argument un-stripped", async () => {
    const callRes = await runStdio(makeServer("proj_stdio_more"), (client) =>
      Effect.gen(function* () {
        yield* initialize(client);
        return yield* client.request("tools/call", {
          name: GET_MORE_TOOLS_NAME,
          arguments: { context: "need a database tool" },
        });
      }),
    );

    const callResult = asRecord(callRes.result, "get_more_tools result");
    expect(callResult.isError).toBe(false);
    expect(contentText(callResult)).toContain(
      "Unfortunately, we have shown you the full tool list",
    );

    const calls = byType("mcp:tools/call");
    expect(calls).toHaveLength(1);
    const call = calls[0];
    expect(call.resourceName).toBe(GET_MORE_TOOLS_NAME);
    expect(call.userIntent).toBe("need a database tool");
    expect(call.isError).toBeFalsy();
    // get_more_tools declares its own real context parameter: the forwarded
    // arguments (and hence the captured request payload) keep it.
    const parameters = asRecord(call.parameters, "call parameters");
    const request = asRecord(parameters.request, "call request");
    const params = asRecord(request.params, "call request params");
    const args = asRecord(params.arguments, "call request arguments");
    expect(args.context).toBe("need a database tool");
  }, 20000);

  it("enableToolCallContext:false + enableReportMissing:false disable injection and get_more_tools", async () => {
    const listRes = await runStdio(
      makeServer("proj_stdio_optout", {
        enableToolCallContext: false,
        enableReportMissing: false,
      }),
      (client) =>
        Effect.gen(function* () {
          yield* initialize(client);
          return yield* client.request("tools/list", {});
        }),
    );

    const listResult = asRecord(listRes.result, "tools/list result");
    const tools = asArray(listResult.tools, "tools");
    const names = tools.map((t) => asRecord(t, "tool").name).sort();
    expect(names).toEqual(["boom", "echo"]);
    for (const name of ["echo", "boom"]) {
      const schema = asRecord(
        toolByName(tools, name).inputSchema,
        `${name} inputSchema`,
      );
      const properties = asRecord(schema.properties, `${name} properties`);
      expect(properties.context).toBeUndefined();
      expect(asArray(schema.required, `${name} required`)).toEqual(["message"]);
    }
  }, 20000);

  it("enableTracing:false still answers requests but captures no events", async () => {
    const received: Array<Record<string, unknown>> = [];
    const result = await runStdio(
      makeServer("proj_stdio_notrace", { enableTracing: false }, { received }),
      (client) =>
        Effect.gen(function* () {
          const initRes = yield* initialize(client);
          const listRes = yield* client.request("tools/list", {});
          const callRes = yield* client.request("tools/call", {
            name: "echo",
            arguments: { message: "quiet", context: "should not be traced" },
          });
          return { initRes, listRes, callRes };
        }),
    );

    // The server behaves normally on the wire...
    const initResult = asRecord(result.initRes.result, "initialize result");
    expect(asRecord(initResult.serverInfo, "serverInfo").name).toBe(
      "stdio-server",
    );
    const tools = asArray(
      asRecord(result.listRes.result, "tools/list result").tools,
      "tools",
    );
    expect(tools.length).toBeGreaterThan(0);
    const callResult = asRecord(result.callRes.result, "tools/call result");
    expect(callResult.isError).toBe(false);
    expect(contentText(callResult)).toContain("echo: quiet");
    expect(received).toEqual([{ message: "quiet" }]);

    // ...while nothing reaches the event queue.
    expect(events).toHaveLength(0);
  }, 20000);

  it(
    "callback identify publishes once and stamps actor/tags/properties on session events",
    { timeout: 20000 },
    async () => {
      await runStdio(
        makeServer("proj_stdio_identity", {
          identify: () => ({ userId: "user-1", userName: "Ada" }),
          eventTags: () => ({ team: "qa" }),
          eventProperties: () => ({ plan: "pro" }),
        }),
        (client) =>
          Effect.gen(function* () {
            yield* initialize(client);
            yield* client.request("tools/call", {
              name: "echo",
              arguments: { message: "first", context: "first call" },
            });
            return yield* client.request("tools/call", {
              name: "echo",
              arguments: { message: "second", context: "second call" },
            });
          }),
      );

      const init = byType("mcp:initialize")[0];
      expect(init).toBeDefined();

      // Same identity resolved on every request → exactly one identify event.
      const identifies = byType("agentcat:identify");
      expect(identifies).toHaveLength(1);
      expect(identifies[0].sessionId).toBe(init.sessionId);

      // Subsequent events for the session carry the actor fields, and the
      // resolved tags/properties land on each traced event.
      const calls = byType("mcp:tools/call");
      expect(calls).toHaveLength(2);
      for (const call of calls) {
        expect(call.sessionId).toBe(init.sessionId);
        expect(call.identifyActorGivenId).toBe("user-1");
        expect(call.identifyActorName).toBe("Ada");
        expect(call.tags).toEqual({ team: "qa" });
        expect(call.properties).toEqual({ plan: "pro" });
      }
    },
  );
});
