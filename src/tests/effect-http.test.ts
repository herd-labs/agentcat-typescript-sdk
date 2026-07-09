/**
 * Acceptance suite for the `agentcat/effect` HTTP path (`layerHttp`), per
 * docs/plans/effect-mcp-support.md §7. Drives a real Effect McpServer through
 * `HttpRouter.toWebHandler` and asserts the events captured off the queue.
 *
 * Session-id hygiene: identity dedup runs through a module-global LRU keyed
 * by the DERIVED ses_ id, shared across every test in a worker. Each test
 * therefore uses run-unique projectIds and randomUUID mcp-session-ids so its
 * derived ids can never collide with another test (or a previous run).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { randomUUID } from "node:crypto";
import { Context, Effect, Layer, Option, Predicate, Schema } from "effect";
import { McpServer, Tool, Toolkit } from "effect/unstable/ai";
import { HttpRouter } from "effect/unstable/http";
import { layerHttp, type AgentCatEffectOptions } from "../effect/index.js";
import { publishCustomEvent } from "../effect/customEvent.js";
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

/** Service a host app would provide via auth middleware. */
class CurrentUser extends Context.Service<
  CurrentUser,
  { readonly id: string; readonly name: string }
>()("effect-http-test/CurrentUser") {}

const EchoTool = Tool.make("echo", {
  description: "Echo a message back",
  parameters: Schema.Struct({ message: Schema.String }),
  success: Schema.String,
});

const echoToolkit = Toolkit.make(EchoTool);

interface TestServer {
  readonly received: Array<Record<string, unknown>>;
  readonly post: (body: object, sessionId?: string) => Promise<Response>;
  readonly dispose: () => Promise<void>;
}

/**
 * Builds one independent web handler over `layerHttp` with the echo toolkit.
 * `user` (when given) is provided as a `CurrentUser` service by host
 * middleware; the getter indirection lets a test swap the identity between
 * requests through the same handler.
 */
const makeServer = (
  projectId: string,
  options?: AgentCatEffectOptions,
  user?: () => { id: string; name: string },
): TestServer => {
  const received: Array<Record<string, unknown>> = [];
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
        { name: "http-suite-server", version: "2.0.0", path: "/mcp" },
        projectId,
        options,
      ),
    ),
  );
  const { handler, dispose } = user
    ? HttpRouter.toWebHandler(serverLayer, {
        middleware: (effect) =>
          Effect.provideService(effect, CurrentUser, {
            get id() {
              return user().id;
            },
            get name() {
              return user().name;
            },
          }),
      })
    : HttpRouter.toWebHandler(serverLayer);
  const post = (body: object, sessionId?: string) =>
    handler(
      new Request("http://localhost/mcp", {
        method: "POST",
        headers: {
          "content-type": "application/json",
          accept: "application/json, text/event-stream",
          ...(sessionId ? { "mcp-session-id": sessionId } : {}),
        },
        body: JSON.stringify(body),
      }),
    );
  return { received, post, dispose };
};

const initialize = async (
  server: TestServer,
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
  await res.text();
  const ack = await server.post(
    { jsonrpc: "2.0", method: "notifications/initialized" },
    minted ?? undefined,
  );
  await ack.text();
  return minted ?? "";
};

const callEcho = async (
  server: TestServer,
  id: number,
  args: Record<string, unknown>,
  sessionId?: string,
): Promise<unknown[]> => {
  const res = await server.post(
    {
      jsonrpc: "2.0",
      id,
      method: "tools/call",
      params: { name: "echo", arguments: args },
    },
    sessionId,
  );
  return parseJsonRpcBody(res);
};

// ---------------------------------------------------------------------------
// Suite
// ---------------------------------------------------------------------------

describe("effect layerHttp", () => {
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

  it("initialize mints an mcp-session-id and emits one event on its derived session", async () => {
    const projectId = `proj_http_init_${randomUUID()}`;
    const server = makeServer(projectId);
    try {
      const minted = await initialize(server, {
        name: "init-client",
        version: "3.1.4",
      });

      const inits = byType("mcp:initialize");
      expect(inits).toHaveLength(1);
      const init = inits[0];

      // Session identity: the ses_ id is derived deterministically from the
      // minted HTTP session, not generated.
      expect(init.sessionId).toBe(
        deriveSessionIdFromMCPSession(minted, projectId),
      );
      expect(init.sessionId?.startsWith("ses_")).toBe(true);

      // clientInfo from the initialize payload lands on the event.
      expect(init.clientName).toBe("init-client");
      expect(init.clientVersion).toBe("3.1.4");
      expect(init.serverName).toBe("http-suite-server");
      expect(init.serverVersion).toBe("2.0.0");
      expect(init.projectId).toBe(projectId);
      expect(init.resourceName).toBe("Unknown Tool Name");
      expect(init.duration).toBeTypeOf("number");

      // The observed request envelope references the minted MCP session.
      if (
        Predicate.isObject(init.parameters) &&
        Predicate.isObject(init.parameters.extra)
      ) {
        expect(init.parameters.extra.sessionId).toBe(minted);
      } else {
        expect.unreachable("initialize event without parameters.extra");
      }
    } finally {
      await server.dispose();
    }
  }, 20000);

  it("keeps clientInfo per session: interleaved sessions report their own client", async () => {
    const projectId = `proj_http_multi_${randomUUID()}`;
    const server = makeServer(projectId);
    try {
      const sessionA = await initialize(server, {
        name: "client-a",
        version: "1.0.0",
      });
      // Session B initializes AFTER A: with a single mutable clientInfo slot
      // (the Phase 1 regression) B's info would bleed into A's later calls.
      const sessionB = await initialize(server, {
        name: "client-b",
        version: "2.0.0",
      });

      expect(sessionB).not.toBe(sessionA);
      const derivedA = deriveSessionIdFromMCPSession(sessionA, projectId);
      const derivedB = deriveSessionIdFromMCPSession(sessionB, projectId);
      expect(derivedA).not.toBe(derivedB);

      const inits = byType("mcp:initialize");
      expect(inits.map((e) => e.sessionId).sort()).toEqual(
        [derivedA, derivedB].sort(),
      );

      await callEcho(server, 2, { message: "from a" }, sessionA);
      await callEcho(server, 2, { message: "from b" }, sessionB);

      const calls = byType("mcp:tools/call");
      expect(calls).toHaveLength(2);
      const callA = calls.find((e) => e.sessionId === derivedA);
      const callB = calls.find((e) => e.sessionId === derivedB);
      expect(callA?.clientName).toBe("client-a");
      expect(callA?.clientVersion).toBe("1.0.0");
      expect(callB?.clientName).toBe("client-b");
      expect(callB?.clientVersion).toBe("2.0.0");
    } finally {
      await server.dispose();
    }
  }, 20000);

  it("derives identical session ids across replicas sharing only the mcp-session-id header", async () => {
    const projectId = `proj_http_replica_${randomUUID()}`;
    const replicaOne = makeServer(projectId);
    const replicaTwo = makeServer(projectId);
    try {
      // A load balancer flips a live session to a replica that never saw its
      // initialize: only the header travels. The effect protocol itself
      // rejects the unknown session (404 "Mcp-Session-Id does not exist"),
      // but the analytics contract is what matters here: BOTH replicas map
      // the header onto the SAME deterministic ses_ id, with no shared state.
      const fabricated = randomUUID();
      await callEcho(replicaOne, 7, { message: "one" }, fabricated);
      await callEcho(replicaTwo, 7, { message: "two" }, fabricated);

      const expected = deriveSessionIdFromMCPSession(fabricated, projectId);
      const calls = byType("mcp:tools/call");
      expect(calls).toHaveLength(2);
      for (const call of calls) {
        expect(call.sessionId).toBe(expected);
        // No initialize was observed on either replica: no clientInfo exists.
        expect(call.clientName).toBeUndefined();
        expect(call.projectId).toBe(projectId);
      }
    } finally {
      await replicaOne.dispose();
      await replicaTwo.dispose();
    }
  }, 20000);

  it("publishes identify once per identity change and stamps the actor on session events", async () => {
    const projectId = `proj_http_identify_${randomUUID()}`;
    let user = { id: `alice-${randomUUID()}`, name: "Alice" };
    const aliceId = user.id;
    const server = makeServer(
      projectId,
      {
        identify: Effect.map(Effect.serviceOption(CurrentUser), (current) =>
          Option.isSome(current)
            ? { userId: current.value.id, userName: current.value.name }
            : null,
        ),
      },
      () => user,
    );
    try {
      const minted = await initialize(server, {
        name: "identify-client",
        version: "1.0.0",
      });
      const derived = deriveSessionIdFromMCPSession(minted, projectId);

      // The initialize request already resolved the identity in its request
      // fiber (middleware-provided service), bound to the minted session.
      expect(byType("agentcat:identify")).toHaveLength(1);
      const identify = byType("agentcat:identify")[0];
      expect(identify.sessionId).toBe(derived);
      expect(identify.identifyActorGivenId).toBe(aliceId);
      expect(identify.identifyActorName).toBe("Alice");

      // Same identity on the next request: publish-on-change stays silent,
      // but the tools/call event carries the actor.
      await callEcho(server, 2, { message: "first" }, minted);
      expect(byType("agentcat:identify")).toHaveLength(1);
      const firstCall = byType("mcp:tools/call")[0];
      expect(firstCall.sessionId).toBe(derived);
      expect(firstCall.identifyActorGivenId).toBe(aliceId);
      expect(server.received[0]).toEqual({ message: "first" });

      // The host's auth now resolves a different user on the same session.
      user = { id: `bob-${randomUUID()}`, name: "Bob" };
      await callEcho(server, 3, { message: "second" }, minted);
      const identifies = byType("agentcat:identify");
      expect(identifies).toHaveLength(2);
      expect(identifies[1].identifyActorGivenId).toBe(user.id);

      const secondCall = byType("mcp:tools/call")[1];
      expect(secondCall.identifyActorGivenId).toBe(user.id);
      expect(secondCall.identifyActorName).toBe("Bob");
    } finally {
      await server.dispose();
    }
  }, 20000);

  it("resolves eventTags/eventProperties Effects onto tools/call events", async () => {
    const projectId = `proj_http_tags_${randomUUID()}`;
    const server = makeServer(projectId, {
      eventTags: Effect.succeed({ team: "qa", run: "7" }),
      eventProperties: Effect.succeed({ attempt: 1, region: "eu" }),
    });
    try {
      const minted = await initialize(server, {
        name: "tags-client",
        version: "1.0.0",
      });
      const body = await callEcho(server, 2, { message: "tagged" }, minted);
      expect(firstResult(body)).toBeDefined();

      const call = byType("mcp:tools/call")[0];
      expect(call).toBeDefined();
      expect(call.tags).toEqual({ team: "qa", run: "7" });
      expect(call.properties).toEqual({ attempt: 1, region: "eu" });
    } finally {
      await server.dispose();
    }
  }, 20000);

  it("swallows failing resolvers: the call succeeds and the event carries no tags", async () => {
    const projectId = `proj_http_tagfail_${randomUUID()}`;
    const server = makeServer(projectId, {
      eventTags: Effect.fail(new Error("tags resolver exploded")),
      eventProperties: Effect.fail(new Error("properties resolver exploded")),
    });
    try {
      const minted = await initialize(server, {
        name: "tagfail-client",
        version: "1.0.0",
      });
      const body = await callEcho(
        server,
        2,
        { message: "still works" },
        minted,
      );
      // The user-visible call is unaffected by the broken resolver.
      const result = firstResult(body);
      expect(result).toBeDefined();
      expect(result?.isError).toBeFalsy();
      expect(server.received[0]).toEqual({ message: "still works" });

      const call = byType("mcp:tools/call")[0];
      expect(call).toBeDefined();
      expect(call.isError).toBeFalsy();
      expect(call.tags).toBeUndefined();
      expect(call.properties).toBeUndefined();
    } finally {
      await server.dispose();
    }
  }, 20000);

  it("round-trips context over HTTP: injected on tools/list, stripped before the handler", async () => {
    const projectId = `proj_http_context_${randomUUID()}`;
    const server = makeServer(projectId);
    try {
      const session = await initialize(server, {
        name: "context-client",
        version: "1.0.0",
      });

      // Client-visible tools/list carries the injected `context` parameter
      // through JSON-RPC serialization.
      const listRes = await server.post(
        { jsonrpc: "2.0", id: 2, method: "tools/list", params: {} },
        session,
      );
      const listResult = firstResult(await parseJsonRpcBody(listRes));
      expect(listResult).toBeDefined();
      const tools = Array.isArray(listResult?.tools) ? listResult.tools : [];
      expect(tools.length).toBeGreaterThan(0);
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
        { message: "hi", context: "compare staging configs" },
        session,
      );
      const callResult = firstResult(body);
      expect(callResult).toBeDefined();
      expect(callResult?.isError).toBeFalsy();
      expect(server.received[0]).toEqual({ message: "hi" });

      const callEvent = byType("mcp:tools/call")[0];
      expect(callEvent.userIntent).toBe("compare staging configs");
      expect(callEvent.resourceName).toBe("echo");

      // The original (unstripped) wire payload is preserved on the event.
      if (
        Predicate.isObject(callEvent.parameters) &&
        Predicate.isObject(callEvent.parameters.request) &&
        Predicate.isObject(callEvent.parameters.request.params) &&
        Predicate.isObject(callEvent.parameters.request.params.arguments)
      ) {
        expect(callEvent.parameters.request.params.arguments.context).toBe(
          "compare staging configs",
        );
      } else {
        expect.unreachable("tools/call event without request params");
      }
    } finally {
      await server.dispose();
    }
  }, 20000);

  it("publishCustomEvent outside a request fiber uses the fallback session or fails", async () => {
    const projectId = `proj_http_custom_${randomUUID()}`;
    const fabricated = randomUUID();

    // With a fallback the event is queued on the derived session directly.
    await Effect.runPromise(
      publishCustomEvent(
        { message: "standalone publish" },
        { sessionId: fabricated, projectId },
      ),
    );
    const customs = byType("agentcat:custom");
    expect(customs).toHaveLength(1);
    expect(customs[0].sessionId).toBe(
      deriveSessionIdFromMCPSession(fabricated, projectId),
    );
    expect(customs[0].projectId).toBe(projectId);
    expect(customs[0].userIntent).toBe("standalone publish");

    // Without a fallback (and no MCP request context) the Effect fails.
    const error = await Effect.runPromise(
      Effect.flip(publishCustomEvent({ message: "orphan" })),
    );
    expect(error).toBeInstanceOf(Error);
    expect(error.message).toContain("fallback");
    // The failing publish queued nothing.
    expect(byType("agentcat:custom")).toHaveLength(1);
  }, 20000);
});
