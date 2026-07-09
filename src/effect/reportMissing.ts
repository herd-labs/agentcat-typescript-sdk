import { Effect, Layer, Schema } from "effect";
import { McpServer, Tool, Toolkit } from "effect/unstable/ai";
import {
  GET_MORE_TOOLS_NAME,
  GET_MORE_TOOLS_DESCRIPTION,
  GET_MORE_TOOLS_CONTEXT_DESCRIPTION,
  GET_MORE_TOOLS_RESPONSE_TEXT,
} from "../modules/constants.js";
import { writeToLog } from "../modules/logging.js";

// Raw JSON Schema keeps the wire surface identical to the official-SDK
// descriptor (modules/tools.ts getReportMissingToolDescriptor).
const GetMoreTools = Tool.dynamic(GET_MORE_TOOLS_NAME, {
  description: GET_MORE_TOOLS_DESCRIPTION,
  parameters: {
    type: "object",
    properties: {
      context: {
        type: "string",
        description: GET_MORE_TOOLS_CONTEXT_DESCRIPTION,
      },
    },
    required: ["context"],
  },
  success: Schema.String,
});

const toolkit = Toolkit.make(GetMoreTools);
const isGetMoreToolsArguments = Schema.is(
  Schema.Struct({ context: Schema.String }),
);

/**
 * `get_more_tools` as a real Toolkit layer (decision 3 in
 * docs/plans/effect-mcp-support.md): it appears in `tools/list` and
 * dispatches through McpServer like any other tool, so the protocol proxy
 * captures its calls as regular mcpToolsCall events with `userIntent`.
 */
export const reportMissingToolkit: Layer.Layer<never> = McpServer.toolkit(
  toolkit,
).pipe(
  Layer.provide(
    toolkit.toLayer({
      [GET_MORE_TOOLS_NAME]: (args: unknown) =>
        Effect.sync(() => {
          const context = isGetMoreToolsArguments(args)
            ? args.context
            : undefined;
          writeToLog(
            `Missing tool reported (context length: ${context?.length ?? 0})`,
          );
          return GET_MORE_TOOLS_RESPONSE_TEXT;
        }),
    }),
  ),
);
