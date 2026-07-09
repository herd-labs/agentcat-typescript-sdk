import { DEFAULT_CONTEXT_PARAMETER_DESCRIPTION } from "./constants";
import { writeToLog } from "./logging.js";

/**
 * Loose JSON Schema shape for a tool's inputSchema. Fields are validated
 * individually at the point of use; anything unrecognized is preserved by the
 * deep copy.
 */
export interface ContextParameterSchema {
  type?: unknown;
  properties?: Record<string, unknown>;
  required?: unknown;
  additionalProperties?: unknown;
  oneOf?: unknown;
  allOf?: unknown;
  anyOf?: unknown;
}

/**
 * Minimal structural shape shared by every tool representation this module
 * operates on: SDK RegisteredTools, ListToolsResult entries, and wire-encoded
 * Effect MCP tools.
 */
export interface ContextParameterTool {
  name?: unknown;
  inputSchema?: ContextParameterSchema;
}

const cloneContextParameterSchema = (
  schema: ContextParameterSchema,
): ContextParameterSchema => ({
  ...schema,
  properties: schema.properties ? { ...schema.properties } : undefined,
  required: Array.isArray(schema.required)
    ? [...schema.required]
    : schema.required,
});

/**
 * Adds a context parameter to a tool's JSON Schema.
 * This function is called AFTER the MCP SDK has converted Zod schemas to JSON Schema,
 * so we only need to handle JSON Schema format.
 *
 * Skips injection (with warning) for:
 * - Tools that already have a 'context' parameter
 * - Complex schemas (oneOf/allOf/anyOf) that can't safely have properties added
 *
 * When adding context to a schema with additionalProperties: false, that
 * constraint is removed because the newly injected parameter must be accepted.
 */
export function addContextParameterToTool(
  tool: ContextParameterTool,
  customContextDescription?: string,
): ContextParameterTool {
  // Create a shallow copy of the tool to avoid modifying the original
  const modifiedTool = { ...tool };
  const toolName = tool.name || "unknown";
  const schema = modifiedTool.inputSchema;

  // Check if tool already has context parameter - skip to avoid collision
  if (schema?.properties?.context) {
    writeToLog(
      `WARN: Tool "${toolName}" already has 'context' parameter. Skipping context injection.`,
    );
    return modifiedTool;
  }

  // Skip complex schemas that can't safely have properties added at root level
  if (schema?.oneOf || schema?.allOf || schema?.anyOf) {
    writeToLog(
      `WARN: Tool "${toolName}" has complex schema (oneOf/allOf/anyOf). Skipping context injection.`,
    );
    return modifiedTool;
  }

  const contextDescription =
    customContextDescription || DEFAULT_CONTEXT_PARAMETER_DESCRIPTION;

  const sourceSchema = modifiedTool.inputSchema ?? {
    type: "object",
    properties: {},
    required: [],
  };
  const copiedSchema = cloneContextParameterSchema(sourceSchema);
  modifiedTool.inputSchema = copiedSchema;

  // Ensure properties object exists
  if (!copiedSchema.properties) {
    copiedSchema.properties = {};
  }

  // Handle additionalProperties: false - must remove this constraint since we're adding context
  // The MCP SDK adds this constraint when converting Zod schemas to JSON Schema
  if (copiedSchema.additionalProperties === false) {
    delete copiedSchema.additionalProperties;
  }

  // Add context property
  copiedSchema.properties.context = {
    type: "string",
    description: contextDescription,
  };

  // Add context to required array
  if (Array.isArray(copiedSchema.required)) {
    if (!copiedSchema.required.includes("context")) {
      copiedSchema.required.push("context");
    }
  } else {
    copiedSchema.required = ["context"];
  }

  return modifiedTool;
}

export function addContextParameterToTools(
  tools: ContextParameterTool[],
  customContextDescription?: string,
): ContextParameterTool[] {
  return tools.map((tool) => {
    // Skip get_more_tools - it has its own special context parameter
    if (tool.name === "get_more_tools") {
      return tool;
    }
    return addContextParameterToTool(tool, customContextDescription);
  });
}
