/**
 * Unit suite for the agentcat/effect protocol proxy (plan §7): exercises
 * `wrapProtocol` over a scripted in-memory `RpcServer.Protocol` — no
 * McpServer, no transport. Wire messages are injected by invoking the run
 * callback the proxy hands to the inner protocol; responses are driven
 * through the wrapped `send`. Events are captured at `eventQueue.add`.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Deferred, Effect, Layer, Predicate, Queue } from "effect";
import { RpcServer } from "effect/unstable/rpc";
import type { RpcMessage } from "effect/unstable/rpc";
import { PublishEventRequestEventTypeEnum } from "agentcat-api";
import { wrapProtocol } from "../effect/protocolProxy.js";
import { makeEffectTracking } from "../effect/session.js";
import type { AgentCatEffectOptions } from "../effect/types.js";
import { eventQueue } from "../modules/eventQueue.js";
import { deriveSessionIdFromMCPSession } from "../modules/session.js";
import type { UnredactedEvent } from "../types.js";

/** Runtime narrowing that fails the test with a pointed message. */
const asRecord = (u: unknown, label: string): Record<string, unknown> => {
  if (!Predicate.isObject(u)) {
    throw new Error(`expected ${label} to be a record, got: ${String(u)}`);
  }
  return u;
};

type RunHandler = (
  clientId: number,
  message: RpcMessage.FromClientEncoded,
) => Effect.Effect<void>;

const request = (
  id: string,
  tag: string,
  payload: unknown,
  headers: ReadonlyArray<[string, string]> = [],
): RpcMessage.FromClientEncoded => ({
  _tag: "Request",
  id,
  tag,
  payload,
  headers,
});

const success = (value: unknown): RpcMessage.ExitEncoded<unknown, unknown> => ({
  _tag: "Success",
  value,
});

interface Harness {
  /** Injects a wire message as if the transport received it. */
  readonly push: (
    message: RpcMessage.FromClientEncoded,
    clientId?: number,
  ) => Effect.Effect<void>;
  /** Drives a terminal Exit through the wrapped protocol's `send`. */
  readonly sendExit: (
    requestId: string,
    exit: RpcMessage.ExitEncoded<unknown, unknown>,
    clientId?: number,
  ) => Effect.Effect<void>;
  /** Messages the proxy forwarded to the server-side run callback. */
  readonly forwarded: ReadonlyArray<RpcMessage.FromClientEncoded>;
  /** Responses the proxy handed to the inner protocol's `send`. */
  readonly sent: ReadonlyArray<RpcMessage.FromServerEncoded>;
}

/**
 * Builds `wrapProtocol(makeEffectTracking(...))` over a scripted in-memory
 * Protocol and runs `body` against the wrapped service.
 */
const withHarness = (
  projectId: string,
  options: AgentCatEffectOptions,
  body: (harness: Harness) => Effect.Effect<void>,
): Promise<void> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const tracking = makeEffectTracking(
        { name: "unit", version: "0" },
        projectId,
        options,
      );
      const forwarded: Array<RpcMessage.FromClientEncoded> = [];
      const sent: Array<RpcMessage.FromServerEncoded> = [];
      const captured = yield* Deferred.make<RunHandler>();
      const disconnects = yield* Queue.make<number>();
      const inner = RpcServer.Protocol.of({
        run: (f) =>
          Deferred.succeed(captured, f).pipe(Effect.andThen(Effect.never)),
        disconnects,
        send: (_clientId, response) =>
          Effect.sync(() => {
            sent.push(response);
          }),
        end: (_clientId) => Effect.void,
        clientIds: Effect.succeed(new Set<number>()),
        initialMessage: Effect.succeedNone,
        supportsAck: false,
        supportsTransferables: false,
        supportsSpanPropagation: false,
      });
      const proto = yield* Effect.gen(function* () {
        return yield* RpcServer.Protocol;
      }).pipe(
        Effect.provide(
          wrapProtocol(tracking).pipe(
            Layer.provide(Layer.succeed(RpcServer.Protocol, inner)),
          ),
        ),
      );
      // run() never completes; the child fiber is interrupted when this
      // program ends. The Deferred hands out the wrapped run callback.
      yield* Effect.forkChild(
        proto.run((_clientId, message) =>
          Effect.sync(() => {
            forwarded.push(message);
          }),
        ),
      );
      const push = yield* Deferred.await(captured);
      yield* body({
        push: (message, clientId = 0) => push(clientId, message),
        sendExit: (requestId, exit, clientId = 0) =>
          proto.send(clientId, { _tag: "Exit", requestId, exit }),
        forwarded,
        sent,
      });
    }),
  );

describe("effect protocol proxy (unit)", () => {
  let events: UnredactedEvent[] = [];

  beforeEach(() => {
    events = [];
    vi.spyOn(eventQueue, "add").mockImplementation((event) => {
      events.push(event);
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("correlates one event per Request/Exit pair and ignores unknown requestIds", async () => {
    await withHarness("proj_unit_corr", {}, (h) =>
      Effect.gen(function* () {
        yield* h.push(request("1", "tools/call", { name: "t", arguments: {} }));
        expect(h.forwarded).toHaveLength(1);
        // No event until the Exit correlates.
        expect(events).toHaveLength(0);

        yield* h.sendExit(
          "1",
          success({ content: [{ type: "text", text: "ok" }] }),
        );
        expect(events).toHaveLength(1);
        const event = events[0];
        expect(event.eventType).toBe(
          PublishEventRequestEventTypeEnum.mcpToolsCall,
        );
        expect(event.resourceName).toBe("t");
        expect(event.projectId).toBe("proj_unit_corr");
        expect(typeof event.duration).toBe("number");
        expect(event.duration).toBeGreaterThanOrEqual(0);
        expect(event.isError).toBeFalsy();
        expect(h.sent).toHaveLength(1);

        // Exit for a requestId that was never observed: forwarded, no event.
        yield* h.sendExit("999", success({ content: [] }));
        expect(events).toHaveLength(1);
        expect(h.sent).toHaveLength(2);
        const unknown = h.sent[1];
        expect(unknown._tag).toBe("Exit");
        if (unknown._tag === "Exit") {
          expect(unknown.requestId).toBe("999");
        }

        // A duplicate Exit for the already-consumed request emits nothing.
        yield* h.sendExit("1", success({ content: [] }));
        expect(events).toHaveLength(1);
        expect(h.sent).toHaveLength(3);
      }),
    );
  }, 20000);

  it("maps CallToolResult isError and encoded Failure causes onto the event", async () => {
    await withHarness("proj_unit_iserr", {}, (h) =>
      Effect.gen(function* () {
        // Success exit carrying a CallToolResult error.
        yield* h.push(
          request("1", "tools/call", { name: "e1", arguments: {} }),
        );
        const toolError = {
          content: [{ type: "text", text: "boom" }],
          isError: true,
        };
        yield* h.sendExit("1", success(toolError));
        expect(events).toHaveLength(1);
        expect(events[0].isError).toBe(true);
        expect(events[0].error).toBeDefined();
        expect(events[0].error?.message).toContain("boom");
        expect(events[0].response).toEqual(toolError);

        // Encoded Failure with a Fail reason.
        yield* h.push(
          request("2", "tools/call", { name: "e2", arguments: {} }),
        );
        yield* h.sendExit("2", {
          _tag: "Failure",
          cause: [{ _tag: "Fail", error: { message: "bad" } }],
        });
        expect(events).toHaveLength(2);
        expect(events[1].isError).toBe(true);
        expect(events[1].error).toBeDefined();
        expect(events[1].error?.message).toContain("bad");

        // Encoded Failure with only an Interrupt reason.
        yield* h.push(
          request("3", "tools/call", { name: "e3", arguments: {} }),
        );
        yield* h.sendExit("3", {
          _tag: "Failure",
          cause: [{ _tag: "Interrupt", fiberId: undefined }],
        });
        expect(events).toHaveLength(3);
        expect(events[2].isError).toBe(true);
        expect(events[2].error?.message).toBe("Request interrupted");
      }),
    );
  }, 20000);

  it("strips the context argument before forwarding and preserves it on the event", async () => {
    await withHarness("proj_unit_ctx", {}, (h) =>
      Effect.gen(function* () {
        yield* h.push(
          request("1", "tools/call", {
            name: "t",
            arguments: { a: 1, context: "why" },
          }),
        );
        const fwd = h.forwarded[0];
        expect(fwd._tag).toBe("Request");
        if (fwd._tag === "Request") {
          const payload = asRecord(fwd.payload, "forwarded payload");
          expect(payload.name).toBe("t");
          // context removed, sibling arguments intact.
          expect(payload.arguments).toEqual({ a: 1 });
        }

        yield* h.sendExit("1", success({ content: [] }));
        expect(events).toHaveLength(1);
        const event = events[0];
        expect(event.userIntent).toBe("why");
        const params = asRecord(event.parameters, "event parameters");
        const req = asRecord(params.request, "request envelope");
        expect(req.method).toBe("tools/call");
        const original = asRecord(req.params, "original params");
        // The event carries the pre-strip payload.
        expect(original.arguments).toEqual({ a: 1, context: "why" });
      }),
    );
  }, 20000);

  it("forwards arguments untouched and captures no intent when enableToolCallContext is false", async () => {
    await withHarness(
      "proj_unit_ctxoff",
      { enableToolCallContext: false },
      (h) =>
        Effect.gen(function* () {
          yield* h.push(
            request("1", "tools/call", {
              name: "t",
              arguments: { a: 1, context: "why" },
            }),
          );
          const fwd = h.forwarded[0];
          expect(fwd._tag).toBe("Request");
          if (fwd._tag === "Request") {
            const payload = asRecord(fwd.payload, "forwarded payload");
            expect(payload.arguments).toEqual({ a: 1, context: "why" });
          }
          yield* h.sendExit("1", success({ content: [] }));
          expect(events).toHaveLength(1);
          expect(events[0].userIntent).toBeUndefined();
        }),
    );
  }, 20000);

  it("injects context into the encoded ListToolsResult and emits the mutated response", async () => {
    await withHarness("proj_unit_list", {}, (h) =>
      Effect.gen(function* () {
        const choiceSchema = {
          oneOf: [{ type: "object" }, { type: "string" }],
        };
        yield* h.push(request("1", "tools/list", {}));
        yield* h.sendExit(
          "1",
          success({
            tools: [
              {
                name: "t",
                inputSchema: { type: "object", properties: {}, required: [] },
              },
              { name: "choice", inputSchema: choiceSchema },
            ],
          }),
        );

        expect(h.sent).toHaveLength(1);
        const outgoing = h.sent[0];
        expect(outgoing._tag).toBe("Exit");
        if (outgoing._tag !== "Exit") {
          return;
        }
        expect(outgoing.exit._tag).toBe("Success");
        if (outgoing.exit._tag !== "Success") {
          return;
        }
        const value = asRecord(outgoing.exit.value, "list result");
        expect(Array.isArray(value.tools)).toBe(true);
        const tools = Array.isArray(value.tools) ? value.tools : [];
        expect(tools).toHaveLength(2);

        // Plain object schema: context injected and required on the wire.
        const injected = asRecord(tools[0], "injected tool");
        const schema = asRecord(injected.inputSchema, "injected schema");
        const properties = asRecord(schema.properties, "schema properties");
        const context = asRecord(properties.context, "context property");
        expect(context.type).toBe("string");
        expect(typeof context.description).toBe("string");
        expect(schema.required).toContain("context");

        // oneOf schema: skip rule keeps it byte-identical.
        const skipped = asRecord(tools[1], "skipped tool");
        expect(skipped.inputSchema).toEqual(choiceSchema);

        // The emitted event carries the mutated response that hit the wire.
        expect(events).toHaveLength(1);
        const event = events[0];
        expect(event.eventType).toBe(
          PublishEventRequestEventTypeEnum.mcpToolsList,
        );
        expect(event.isError).toBe(false);
        expect(event.resourceName).toBeUndefined();
        expect(event.response).toEqual(value);
      }),
    );
  }, 20000);

  it("emits tools/list event and sends original response when context injection fails", async () => {
    await withHarness("proj_unit_list_inject_fail", {}, (h) =>
      Effect.gen(function* () {
        const badSchema: Record<string, unknown> = { type: "object" };
        Object.defineProperty(badSchema, "properties", {
          get: () => {
            throw new Error("schema getter exploded");
          },
        });
        const listResult = {
          tools: [{ name: "bad", inputSchema: badSchema }],
        };

        yield* h.push(request("1", "tools/list", {}));
        yield* h.sendExit("1", success(listResult));

        expect(h.sent).toHaveLength(1);
        const outgoing = h.sent[0];
        expect(outgoing._tag).toBe("Exit");
        if (outgoing._tag !== "Exit") {
          return;
        }
        expect(outgoing.exit._tag).toBe("Success");
        if (outgoing.exit._tag !== "Success") {
          return;
        }
        expect(outgoing.exit.value).toBe(listResult);

        expect(events).toHaveLength(1);
        expect(events[0].eventType).toBe(
          PublishEventRequestEventTypeEnum.mcpToolsList,
        );
        expect(events[0].response).toBe(listResult);

        yield* h.push(request("2", "tools/call", { name: "t", arguments: {} }));
        yield* h.sendExit("2", success({ content: [] }));
        expect(events).toHaveLength(2);
        expect(events[1].eventType).toBe(
          PublishEventRequestEventTypeEnum.mcpToolsCall,
        );
        expect(events[1].resourceName).toBe("t");
      }),
    );
  }, 20000);

  it("passes non-observed tags through untouched and survives requests that never exit", async () => {
    await withHarness("proj_unit_passthru", {}, (h) =>
      Effect.gen(function* () {
        // Unobserved tag (`ping` is deliberately excluded as keepalive
        // noise): forwarded verbatim, no event on request or Exit.
        const ping = request("1", "ping", {}, [
          ["mcp-session-id", "unit-sess-passthru-91d2"],
        ]);
        yield* h.push(ping);
        expect(h.forwarded).toHaveLength(1);
        expect(h.forwarded[0]).toEqual(ping);
        yield* h.sendExit("1", success({}));
        expect(events).toHaveLength(0);
        expect(h.sent).toHaveLength(1);

        // Notification-style tag (never answered): forwarded, no event.
        yield* h.push(request("2", "notifications/initialized", {}));
        expect(h.forwarded).toHaveLength(2);
        expect(events).toHaveLength(0);

        // An observed request that never receives an Exit must not poison a
        // later identical request: the retry correlates and emits exactly one
        // event.
        yield* h.push(request("7", "tools/call", { name: "t", arguments: {} }));
        yield* h.push(request("7", "tools/call", { name: "t", arguments: {} }));
        yield* h.sendExit("7", success({ content: [] }));
        expect(events).toHaveLength(1);
        expect(events[0].resourceName).toBe("t");
        expect(events[0].eventType).toBe(
          PublishEventRequestEventTypeEnum.mcpToolsCall,
        );
        expect(h.sent).toHaveLength(2);
      }),
    );
  }, 20000);

  it("captures resources/read with the uri resourceName, response, duration, and derived session", async () => {
    const projectId = "proj_unit_res_read";
    const mcpSessionId = "unit-sess-read-5b21";
    await withHarness(projectId, {}, (h) =>
      Effect.gen(function* () {
        yield* h.push(
          request("1", "resources/read", { uri: "file://x" }, [
            ["mcp-session-id", mcpSessionId],
          ]),
        );
        // Observed but never mutated: the payload forwards untouched.
        expect(h.forwarded).toHaveLength(1);
        const fwd = h.forwarded[0];
        expect(fwd._tag).toBe("Request");
        if (fwd._tag === "Request") {
          expect(fwd.payload).toEqual({ uri: "file://x" });
        }
        expect(events).toHaveLength(0);

        const contents = { contents: [{ uri: "file://x", text: "hi" }] };
        yield* h.sendExit("1", success(contents));
        expect(events).toHaveLength(1);
        const event = events[0];
        expect(event.eventType).toBe(
          PublishEventRequestEventTypeEnum.mcpResourcesRead,
        );
        expect(event.resourceName).toBe("file://x");
        expect(event.response).toEqual(contents);
        expect(typeof event.duration).toBe("number");
        expect(event.duration).toBeGreaterThanOrEqual(0);
        expect(event.isError).toBeFalsy();
        expect(event.sessionId).toBe(
          deriveSessionIdFromMCPSession(mcpSessionId, projectId),
        );
        const params = asRecord(event.parameters, "event parameters");
        const req = asRecord(params.request, "request envelope");
        expect(req.method).toBe("resources/read");
        expect(req.params).toEqual({ uri: "file://x" });
        const extra = asRecord(params.extra, "extra");
        expect(extra.sessionId).toBe(mcpSessionId);
      }),
    );
  }, 20000);

  it("captures prompts/get without context stripping and resources/list without a resourceName", async () => {
    await withHarness("proj_unit_prompts", {}, (h) =>
      Effect.gen(function* () {
        // prompts/get: arguments forward untouched — the tools/call context
        // strip must not leak onto prompt arguments, even for a `context` key.
        const get = request("1", "prompts/get", {
          name: "my-prompt",
          arguments: { a: "1", context: "not stripped" },
        });
        yield* h.push(get);
        expect(h.forwarded).toHaveLength(1);
        expect(h.forwarded[0]).toEqual(get);
        yield* h.sendExit("1", success({ messages: [] }));
        expect(events).toHaveLength(1);
        expect(events[0].eventType).toBe(
          PublishEventRequestEventTypeEnum.mcpPromptsGet,
        );
        expect(events[0].resourceName).toBe("my-prompt");
        expect(events[0].userIntent).toBeUndefined();
        const params = asRecord(events[0].parameters, "event parameters");
        const req = asRecord(params.request, "request envelope");
        expect(req.method).toBe("prompts/get");
        const original = asRecord(req.params, "original params");
        expect(original.arguments).toEqual({
          a: "1",
          context: "not stripped",
        });

        // resources/list: no primary resource — and no "Unknown Tool Name"
        // fallback, which is initialize/tools-call parity only.
        yield* h.push(request("2", "resources/list", {}));
        yield* h.sendExit("2", success({ resources: [] }));
        expect(events).toHaveLength(2);
        expect(events[1].eventType).toBe(
          PublishEventRequestEventTypeEnum.mcpResourcesList,
        );
        expect(events[1].resourceName).toBeUndefined();
        expect(events[1].response).toEqual({ resources: [] });
      }),
    );
  }, 20000);

  it("maps completion/complete refs onto resourceName: prompt name, then resource uri", async () => {
    await withHarness("proj_unit_complete", {}, (h) =>
      Effect.gen(function* () {
        yield* h.push(
          request("1", "completion/complete", {
            ref: { type: "ref/prompt", name: "my-prompt" },
            argument: { name: "a", value: "x" },
          }),
        );
        yield* h.sendExit("1", success({ completion: { values: [] } }));

        yield* h.push(
          request("2", "completion/complete", {
            ref: { type: "ref/resource", uri: "file://y" },
            argument: { name: "a", value: "x" },
          }),
        );
        yield* h.sendExit("2", success({ completion: { values: [] } }));

        expect(events).toHaveLength(2);
        expect(events[0].eventType).toBe(
          PublishEventRequestEventTypeEnum.mcpCompletionComplete,
        );
        expect(events[0].resourceName).toBe("my-prompt");
        expect(events[1].eventType).toBe(
          PublishEventRequestEventTypeEnum.mcpCompletionComplete,
        );
        expect(events[1].resourceName).toBe("file://y");
      }),
    );
  }, 20000);

  it("maps an encoded Failure on resources/read onto ErrorData", async () => {
    await withHarness("proj_unit_res_err", {}, (h) =>
      Effect.gen(function* () {
        yield* h.push(request("1", "resources/read", { uri: "file://x" }));
        yield* h.sendExit("1", {
          _tag: "Failure",
          cause: [{ _tag: "Fail", error: { message: "nope" } }],
        });
        expect(events).toHaveLength(1);
        expect(events[0].eventType).toBe(
          PublishEventRequestEventTypeEnum.mcpResourcesRead,
        );
        expect(events[0].isError).toBe(true);
        expect(events[0].error).toBeDefined();
        expect(events[0].error?.message).toContain("nope");
      }),
    );
  }, 20000);

  it("derives the session from the mcp-session-id header and shares one generated session otherwise", async () => {
    const projectId = "proj_unit_sess";
    const mcpSessionId = "unit-sess-3f9c07";
    await withHarness(projectId, {}, (h) =>
      Effect.gen(function* () {
        yield* h.push(
          request("1", "tools/call", { name: "t", arguments: {} }, [
            ["mcp-session-id", mcpSessionId],
          ]),
        );
        yield* h.sendExit("1", success({ content: [] }));
        yield* h.push(request("2", "tools/call", { name: "t", arguments: {} }));
        yield* h.sendExit("2", success({ content: [] }));
        yield* h.push(request("3", "tools/list", {}));
        yield* h.sendExit("3", success({ tools: [] }));
        expect(events).toHaveLength(3);

        // Header-bearing request: deterministic derivation from the MCP
        // session id + projectId, and the raw id surfaces in extra.
        const derived = deriveSessionIdFromMCPSession(mcpSessionId, projectId);
        expect(events[0].sessionId).toBe(derived);
        const params = asRecord(events[0].parameters, "event parameters");
        const extra = asRecord(params.extra, "extra");
        expect(extra.sessionId).toBe(mcpSessionId);

        // Headerless requests on one tracking share one generated session,
        // distinct from the derived one.
        expect(typeof events[1].sessionId).toBe("string");
        expect(events[1].sessionId).toBe(events[2].sessionId);
        expect(events[1].sessionId).not.toBe(derived);
      }),
    );
  }, 20000);
});
