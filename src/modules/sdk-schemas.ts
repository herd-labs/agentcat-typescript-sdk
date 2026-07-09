import { createRequire } from "module";

/**
 * The MCP SDK request schemas used to (re-)register request handlers.
 * Typed as unknown: they flow straight into `server.setRequestHandler`,
 * which introspects them itself.
 */
export interface SdkRequestSchemas {
  CallToolRequestSchema: unknown;
  InitializeRequestSchema: unknown;
  ListToolsRequestSchema: unknown;
}

type SdkRequire = (specifier: string) => unknown;

let cached: SdkRequestSchemas | null = null;

/**
 * Lazily loads the MCP SDK request schemas.
 *
 * `@modelcontextprotocol/sdk` is an optional peer dependency (the
 * `agentcat/effect` entry does not need it), so the root entry must not
 * resolve it at module scope — that would break `import "agentcat"` in
 * installs without the SDK. Loading happens on the first `track()` setup
 * path instead; a server object to track implies the SDK is installed.
 * Throws when the SDK cannot be resolved; callers run inside the existing
 * setup try/catch blocks.
 */
export function getSdkRequestSchemas(): SdkRequestSchemas {
  if (cached) {
    return cached;
  }
  // Avoid a free `require` reference: esbuild rewrites it to a shim in ESM
  // bundles, and that shim throws for dynamic package requires. In CJS,
  // Function sees native require; in ESM it returns undefined and we use
  // createRequire(import.meta.url).
  const nodeRequire = getNativeRequire() ?? createRequire(import.meta.url);
  const loaded = nodeRequire("@modelcontextprotocol/sdk/types.js");
  if (
    typeof loaded !== "object" ||
    loaded === null ||
    !("CallToolRequestSchema" in loaded) ||
    !("InitializeRequestSchema" in loaded) ||
    !("ListToolsRequestSchema" in loaded)
  ) {
    throw new Error(
      "@modelcontextprotocol/sdk/types.js did not provide the expected request schemas",
    );
  }
  cached = {
    CallToolRequestSchema: loaded.CallToolRequestSchema,
    InitializeRequestSchema: loaded.InitializeRequestSchema,
    ListToolsRequestSchema: loaded.ListToolsRequestSchema,
  };
  return cached;
}

function getNativeRequire(): SdkRequire | undefined {
  try {
    const value = Function(
      "return typeof require === 'function' ? require : undefined",
    )() as unknown;
    return typeof value === "function" ? (value as SdkRequire) : undefined;
  } catch {
    return undefined;
  }
}
