import type { Schema } from "effect";
import { Data, Effect, Option, pipe, Record, Result } from "effect";
import type { CustomEventData, UnredactedEvent } from "../types.js";
import { AGENTCAT_CUSTOM_EVENT_TYPE } from "../modules/constants.js";
import { validateTags } from "../modules/validation.js";
import { eventQueue, publishEvent } from "../modules/eventQueue.js";
import { deriveSessionIdFromMCPSession } from "../modules/session.js";
import { writeToLog } from "../modules/logging.js";
import {
  CurrentAgentCatRequest,
  resolveSession,
  withSessionScope,
} from "./session.js";

/**
 * Explicit session for `publishCustomEvent` calls made outside an MCP request
 * fiber — mirrors the session-id-string path of the root entry's
 * publishCustomEvent.
 */
export interface PublishCustomEventFallback {
  readonly sessionId: string;
  readonly projectId: string;
}

export class PublishCustomEventError extends Data.TaggedError(
  "PublishCustomEventError",
)<{
  readonly message: string;
  readonly cause?: Schema.Defect["Type"];
}> {}

/**
 * Publishes a custom `agentcat:custom` event.
 *
 * Inside an MCP request fiber of a server composed with the agentcat layer
 * combinators, the session (and project) resolve automatically from the
 * request context. Elsewhere, pass `fallback` with an explicit session id and
 * project id; the session id is deterministically derived, exactly like the
 * root entry's string path. Fails with a `PublishCustomEventError` when neither is available.
 */
export const publishCustomEvent = (
  eventData: CustomEventData,
  fallback?: PublishCustomEventFallback,
): Effect.Effect<void, PublishCustomEventError> =>
  Effect.flatMap(Effect.serviceOption(CurrentAgentCatRequest), (current) =>
    Effect.suspend(() => {
      try {
        if (Option.isSome(current)) {
          const { tracking, clientId, mcpSessionId } = current.value;
          const session = resolveSession(tracking, mcpSessionId, clientId);
          const event = buildCustomEvent(
            eventData,
            session.sessionId,
            undefined,
          );
          withSessionScope(tracking, session, () =>
            publishEvent(tracking.ctx, event),
          );
          writeToLog(
            `Published custom event for session ${session.sessionId} with type 'agentcat:custom'`,
          );
          return Effect.void;
        }
        if (fallback) {
          const sessionId = deriveSessionIdFromMCPSession(
            fallback.sessionId,
            fallback.projectId,
          );
          const event = buildCustomEvent(
            eventData,
            sessionId,
            fallback.projectId,
          );
          eventQueue.add(event);
          writeToLog(
            `Published custom event for session ${sessionId} with type 'agentcat:custom'`,
          );
          return Effect.void;
        }
        return Effect.fail(
          new PublishCustomEventError({
            message:
              "publishCustomEvent requires an AgentCat-tracked MCP request context or an explicit fallback session",
          }),
        );
      } catch (error) {
        return Effect.fail(
          new PublishCustomEventError({
            message: error instanceof Error ? error.message : String(error),
            cause: error,
          }),
        );
      }
    }),
  );

const buildCustomEvent = (
  eventData: CustomEventData,
  sessionId: string,
  projectId: string | undefined,
): UnredactedEvent => {
  const optionalEventFields = pipe(
    {
      projectId,
      resourceName: eventData?.resourceName,
      parameters: eventData?.parameters,
      response: eventData?.response,
      userIntent: eventData?.message,
      duration: eventData?.duration,
      isError: eventData?.isError,
      error: eventData?.error,
    },
    Record.filterMap((value) =>
      value === null
        ? Result.succeed(value)
        : Result.fromNullishOr(value, () => undefined),
    ),
  );
  const event: UnredactedEvent = {
    sessionId,
    eventType: AGENTCAT_CUSTOM_EVENT_TYPE,
    timestamp: new Date(),
    ...optionalEventFields,
  };
  if (eventData?.tags) {
    event.tags = validateTags(eventData.tags);
  }
  if (eventData?.properties && Object.keys(eventData.properties).length > 0) {
    event.properties = eventData.properties;
  }
  return event;
};
