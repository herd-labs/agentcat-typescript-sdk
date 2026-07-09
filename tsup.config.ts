import { defineConfig } from "tsup";

export default defineConfig([
  {
    entry: ["src/index.ts"],
    format: ["cjs", "esm"],
    dts: true,
    splitting: false,
    sourcemap: true,
    clean: true,
    outExtension({ format }) {
      return {
        js: format === "esm" ? ".mjs" : ".cjs",
      };
    },
  },
  // ESM-only entry: effect v4 is pure ESM, and the effect integration is a
  // separate subpath so the root entry never resolves effect.
  {
    entry: { "effect/index": "src/effect/index.ts" },
    format: ["esm"],
    dts: true,
    splitting: false,
    sourcemap: true,
    clean: false,
    // effect is a peer — never bundle it so consumers (and bun link) share one copy
    external: [/^effect($|\/)/],
    outExtension() {
      return { js: ".mjs" };
    },
  },
]);
