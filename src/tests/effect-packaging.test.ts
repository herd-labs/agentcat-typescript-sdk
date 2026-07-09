import { describe, test, expect, beforeAll, afterAll } from "vitest";
import { execFileSync, spawnSync } from "node:child_process";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const sdkRoot = join(__dirname, "..", "..");
const packLockDir = join(tmpdir(), "agentcat-sdk-pack.lock");

const waitSync = (ms: number): void => {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
};

const withPackLock = <T>(fn: () => T): T => {
  const deadline = Date.now() + 120_000;
  while (true) {
    try {
      mkdirSync(packLockDir);
      break;
    } catch (error) {
      const code =
        typeof error === "object" && error !== null && "code" in error
          ? error.code
          : undefined;
      if (code !== "EEXIST" || Date.now() > deadline) {
        throw error;
      }
      waitSync(100);
    }
  }

  try {
    return fn();
  } finally {
    rmSync(packLockDir, { recursive: true, force: true });
  }
};

interface PackageManifest {
  name: string;
  main: string;
  module: string;
  types: string;
  exports: {
    ".": {
      types: string;
      import: string;
      require: string;
    };
    "./effect": {
      import: {
        types: string;
        default: string;
      };
    };
  };
  devDependencies: Record<string, string>;
}

const manifest: PackageManifest = JSON.parse(
  readFileSync(join(sdkRoot, "package.json"), "utf8"),
);
const pkgName = manifest.name;
// Pin the same effect build the repo tests against.
const effectVersion = manifest.devDependencies.effect;
const sdkVersion = manifest.devDependencies["@modelcontextprotocol/sdk"];
const typescriptVersion = manifest.devDependencies.typescript;

// Matches bare external import/require specifiers in the built bundles.
const externalSpecifiers = (source: string): Set<string> => {
  const pattern =
    /from\s*["']([^"']+)["']|require\(["']([^"']+)["']\)|import\s*\(\s*["']([^"']+)["']/g;
  const found = new Set<string>();
  let match: RegExpExecArray | null;
  while ((match = pattern.exec(source))) {
    const specifier = match[1] || match[2] || match[3];
    if (
      specifier &&
      !specifier.startsWith(".") &&
      !specifier.startsWith("node:")
    ) {
      found.add(specifier);
    }
  }
  return found;
};

// Guards the packaging contract of the `agentcat/effect` subpath entry
// (docs/plans/effect-mcp-support.md, decision 8):
// - the root entry must never resolve `effect` (optional peer),
// - the effect entry must never resolve `@modelcontextprotocol/sdk`,
// - the effect entry is importable once (and only once) `effect` is present.
describe.sequential("agentcat/effect packaging", () => {
  let workDir: string;
  let tarballPath: string;
  let installedDist: string;
  let installedPackageRoot: string;

  beforeAll(() => {
    // pnpm pack runs the prepack build, which cleans dist. Serialize with the
    // esm-consumer smoke test so parallel Vitest workers do not race tsup.
    workDir = mkdtempSync(join(tmpdir(), "agentcat-effect-pack-"));
    const packOutput = withPackLock(() =>
      execFileSync("pnpm", ["pack", "--pack-destination", workDir], {
        cwd: sdkRoot,
        encoding: "utf8",
      }),
    );
    const lastLine = packOutput.trim().split("\n").pop();
    if (!lastLine || !lastLine.endsWith(".tgz")) {
      throw new Error(
        `pnpm pack did not produce a tarball, got: ${packOutput}`,
      );
    }
    tarballPath = lastLine;

    writeFileSync(
      join(workDir, "package.json"),
      JSON.stringify({
        name: "agentcat-effect-consumer",
        private: true,
        type: "module",
      }),
    );
    execFileSync("pnpm", ["add", tarballPath], {
      cwd: workDir,
      stdio: "inherit",
    });
    installedPackageRoot = join(workDir, "node_modules", pkgName);
    installedDist = join(installedPackageRoot, "dist");
  }, 180_000);

  afterAll(() => {
    if (workDir) rmSync(workDir, { recursive: true, force: true });
  });

  test("package manifest entries point at emitted files", () => {
    const targets = [
      manifest.main,
      manifest.module,
      manifest.types,
      manifest.exports["."].types,
      manifest.exports["."].import,
      manifest.exports["."].require,
      manifest.exports["./effect"].import.types,
      manifest.exports["./effect"].import.default,
    ];
    for (const target of targets) {
      const relative = target.startsWith("./") ? target.slice(2) : target;
      expect(
        existsSync(join(installedPackageRoot, relative)),
        `${target} should exist in the packed package`,
      ).toBe(true);
    }
  });

  test("root bundles never reference effect", () => {
    for (const file of ["index.mjs", "index.cjs"]) {
      const specifiers = externalSpecifiers(
        readFileSync(join(installedDist, file), "utf8"),
      );
      for (const specifier of specifiers) {
        expect(
          specifier === "effect" || specifier.startsWith("effect/"),
          `${file} imports ${specifier}`,
        ).toBe(false);
      }
    }
  });

  test("effect bundle never references @modelcontextprotocol/sdk", () => {
    const specifiers = externalSpecifiers(
      readFileSync(join(installedDist, "effect", "index.mjs"), "utf8"),
    );
    for (const specifier of specifiers) {
      expect(
        specifier.startsWith("@modelcontextprotocol/sdk"),
        `effect entry imports ${specifier}`,
      ).toBe(false);
    }
  });

  test("effect declarations never reference @modelcontextprotocol/sdk", () => {
    const declarations = readFileSync(
      join(installedDist, "effect", "index.d.ts"),
      "utf8",
    );
    expect(declarations).not.toContain("@modelcontextprotocol/sdk");
  });

  test("root entry imports and requires without optional peers installed", () => {
    const esm = spawnSync(
      "node",
      [
        "--input-type=module",
        "-e",
        `import * as m from '${pkgName}'; if (typeof m.track !== 'function') { console.error('track missing'); process.exit(2); } console.log('ok');`,
      ],
      { cwd: workDir, encoding: "utf8" },
    );
    expect(esm.stderr).toBe("");
    expect(esm.status).toBe(0);
    expect(esm.stdout.trim()).toBe("ok");

    const cjs = spawnSync(
      "node",
      [
        "-e",
        `const m = require('${pkgName}'); if (typeof m.track !== 'function') { console.error('track missing'); process.exit(2); } console.log('ok');`,
      ],
      { cwd: workDir, encoding: "utf8" },
    );
    expect(cjs.stderr).toBe("");
    expect(cjs.status).toBe(0);
    expect(cjs.stdout.trim()).toBe("ok");
  });

  test("ESM root track sets up official SDK low-level handlers", () => {
    execFileSync("pnpm", ["add", `@modelcontextprotocol/sdk@${sdkVersion}`], {
      cwd: workDir,
      stdio: "inherit",
    });

    const result = spawnSync(
      "node",
      [
        "--input-type=module",
        "-e",
        `import * as agentcat from '${pkgName}';
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
const server = new Server({ name: 'pkg-low-level', version: '1.0.0' }, { capabilities: { tools: {} } });
server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: [{ name: 'echo', description: 'Echo', inputSchema: { type: 'object', properties: { text: { type: 'string' } }, required: ['text'] } }] }));
server.setRequestHandler(CallToolRequestSchema, async () => ({ content: [{ type: 'text', text: 'ok' }] }));
agentcat.track(server, 'proj_packaging', { disableDiagnostics: true });
const listHandler = server._requestHandlers.get('tools/list');
const callHandler = server._requestHandlers.get('tools/call');
if (typeof listHandler !== 'function' || typeof callHandler !== 'function') { console.error('handlers missing'); process.exit(2); }
const listed = await listHandler({ method: 'tools/list', params: {} }, {});
if (!listed.tools.some((tool) => tool.name === 'get_more_tools')) { console.error('get_more_tools missing'); process.exit(3); }
const more = await callHandler({ method: 'tools/call', params: { name: 'get_more_tools', arguments: { context: 'need specialized tool' } } }, {});
if (more?.content?.[0]?.text?.includes('full tool list') !== true) { console.error('get_more_tools response invalid'); process.exit(4); }
console.log('ok');`,
      ],
      { cwd: workDir, encoding: "utf8" },
    );
    expect(result.stderr).toBe("");
    expect(result.status).toBe(0);
    expect(result.stdout.trim()).toBe("ok");
  }, 180_000);

  test("effect declarations typecheck without the official SDK installed", () => {
    const typeWorkDir = mkdtempSync(join(tmpdir(), "agentcat-effect-types-"));
    try {
      writeFileSync(
        join(typeWorkDir, "package.json"),
        JSON.stringify({
          name: "agentcat-effect-types-consumer",
          private: true,
          type: "module",
        }),
      );
      execFileSync(
        "pnpm",
        [
          "add",
          tarballPath,
          `effect@${effectVersion}`,
          `typescript@${typescriptVersion}`,
        ],
        {
          cwd: typeWorkDir,
          stdio: "inherit",
        },
      );
      writeFileSync(
        join(typeWorkDir, "tsconfig.json"),
        JSON.stringify({
          compilerOptions: {
            strict: true,
            target: "ES2022",
            module: "ESNext",
            moduleResolution: "bundler",
            skipLibCheck: true,
            noEmit: true,
          },
          include: ["effect-typecheck.ts"],
        }),
      );
      writeFileSync(
        join(typeWorkDir, "effect-typecheck.ts"),
        `import { Effect } from "effect";
import { layerStdio, type AgentCatEffectOptions, type UserIdentity } from "${pkgName}/effect";
const identity: UserIdentity = { userId: "user_1" };
const options: AgentCatEffectOptions = {
  identify: Effect.succeed(identity),
  eventTags: () => ({ env: "typecheck" }),
};
layerStdio({ name: "typecheck-server", version: "1.0.0" }, "proj_typecheck", options);
`,
      );
      execFileSync("pnpm", ["exec", "tsc", "--noEmit"], {
        cwd: typeWorkDir,
        stdio: "inherit",
      });
    } finally {
      rmSync(typeWorkDir, { recursive: true, force: true });
    }
  }, 180_000);

  test("effect entry fails cleanly without effect, works once installed", () => {
    const importEffectEntry = () =>
      spawnSync(
        "node",
        [
          "--input-type=module",
          "-e",
          `import * as m from '${pkgName}/effect'; const fns = ['layer', 'layerStdio', 'layerHttp', 'publishCustomEvent']; const missing = fns.filter((f) => typeof m[f] !== 'function'); if (missing.length > 0) { console.error('missing: ' + missing.join(',')); process.exit(2); } console.log('ok');`,
        ],
        { cwd: workDir, encoding: "utf8" },
      );

    // Without the optional peer: module resolution must fail on 'effect'
    // itself (proving effect is external, not bundled).
    const without = importEffectEntry();
    expect(without.status).not.toBe(0);
    expect(without.stderr).toContain("effect");

    execFileSync("pnpm", ["add", `effect@${effectVersion}`], {
      cwd: workDir,
      stdio: "inherit",
    });

    const withEffect = importEffectEntry();
    expect(withEffect.stderr).toBe("");
    expect(withEffect.status).toBe(0);
    expect(withEffect.stdout.trim()).toBe("ok");
  }, 180_000);
});
