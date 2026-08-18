/**
 * PiAdapter — session adapter for the pi (pi-coding-agent) provider.
 *
 * Each thread maps to one long-lived `pi --mode rpc` child process. pi speaks
 * a JSONL protocol over stdio: commands are JSON lines on stdin (each with an
 * optional `id` for request/response correlation), and responses plus agent
 * events stream back on stdout as JSON lines.
 *
 * A t3code turn runs from `sendTurn` (the `prompt` command) until pi emits
 * `agent_settled`. Sending another message while a turn is in flight is a
 * steer (`steer` command), mirroring the other adapters' semantics. pi does
 * not gate individual tool calls on approvals — it runs with the tools the
 * user configured — so this adapter never opens approval requests. Extension
 * UI dialog requests are auto-cancelled and surfaced as warnings.
 *
 * @module provider/Layers/PiAdapter
 */
import {
  EventId,
  type PiSettings,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderTurnStartResult,
  RuntimeItemId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import { causeErrorTag } from "@t3tools/shared/observability";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import {
  PI_BUILTIN_SLASH_COMMANDS,
  PI_DRIVER_KIND,
  PI_REQUEST_TIMEOUT_MS,
  PI_RESUME_VERSION,
  isRecord,
  parsePiModelPattern,
  parsePiSlashCommandInvocation,
  piJsonLines,
  piModelSlug,
  piResultText,
  piToolItemType,
  piToolTitle,
} from "../PiSupport.ts";
import type { PiAdapterShape } from "../Services/PiAdapter.ts";

const PROVIDER = PI_DRIVER_KIND;
const STDERR_TAIL_LIMIT = 4_000;

const PiJsonRecord = Schema.Record(Schema.String, Schema.Unknown);
const encodePiJsonLine = Schema.encodeSync(Schema.fromJsonString(PiJsonRecord));
const decodePiJsonLine = Schema.decodeOption(Schema.fromJsonString(PiJsonRecord));
const isProviderAdapterRequestError = Schema.is(ProviderAdapterRequestError);
const isProviderAdapterProcessError = Schema.is(ProviderAdapterProcessError);
const isProviderAdapterSessionClosedError = Schema.is(ProviderAdapterSessionClosedError);
const isProviderAdapterValidationError = Schema.is(ProviderAdapterValidationError);

type PiRpcRecord = Record<string, unknown>;

interface PiSessionContext {
  readonly threadId: ThreadId;
  readonly scope: Scope.Closeable;
  process: ChildProcessSpawner.ChildProcessHandle | undefined;
  session: ProviderSession;
  /** Serialized stdin writer: lines are offered here and drained into the process once. */
  readonly stdinQueue: Queue.Queue<string>;
  readonly pendingRequests: Map<
    string,
    Deferred.Deferred<PiRpcRecord, ProviderAdapterRequestError>
  >;
  turns: Array<{ id: TurnId; items: Array<unknown> }>;
  activeTurnId: TurnId | undefined;
  promptsInFlight: number;
  readonly interruptedTurnIds: Set<TurnId>;
  currentModelId: string | undefined;
  sessionFile: string | undefined;
  stopped: boolean;
  assistantItemCounter: number;
  currentAssistantItemId: string | undefined;
  compactionItemCounter: number;
  activeCompactionItemId: string | undefined;
  /** toolCallId → display title, carried from tool start through updates/end. */
  readonly toolTitleById: Map<string, string>;
  stderrTail: string;
}

function parsePiResume(raw: unknown): { sessionFile: string } | undefined {
  if (!isRecord(raw)) return undefined;
  if (raw.schemaVersion !== PI_RESUME_VERSION) return undefined;
  if (typeof raw.sessionFile !== "string" || raw.sessionFile.trim().length === 0) return undefined;
  return { sessionFile: raw.sessionFile.trim() };
}

export interface PiAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: ProviderInstanceId;
}

export function makePiAdapter(piSettings: PiSettings, options?: PiAdapterLiveOptions) {
  return Effect.gen(function* () {
    const boundInstanceId = options?.instanceId ?? ProviderInstanceId.make("pi");
    const fileSystem = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const childProcessSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const serverConfig = yield* Effect.service(ServerConfig);
    const crypto = yield* Crypto.Crypto;

    const sessions = new Map<ThreadId, PiSessionContext>();
    const threadLocksRef = yield* SynchronizedRef.make(new Map<string, Semaphore.Semaphore>());
    const runtimeEventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate pi runtime identifier.",
            cause,
          }),
      ),
    );
    const nextEventId = Effect.map(randomUUIDv4, (id) => EventId.make(id));
    const makeEventStamp = () => Effect.all({ eventId: nextEventId, createdAt: nowIso });

    const offerRuntimeEvent = (event: ProviderRuntimeEvent) =>
      PubSub.publish(runtimeEventPubSub, event).pipe(Effect.asVoid);

    const getThreadSemaphore = (threadId: string) =>
      SynchronizedRef.modifyEffect(threadLocksRef, (current) => {
        const existing: Option.Option<Semaphore.Semaphore> = Option.fromNullishOr(
          current.get(threadId),
        );
        return Option.match(existing, {
          onNone: () =>
            Semaphore.make(1).pipe(
              Effect.map((semaphore) => {
                const next = new Map(current);
                next.set(threadId, semaphore);
                return [semaphore, next] as const;
              }),
            ),
          onSome: (semaphore) => Effect.succeed([semaphore, current] as const),
        });
      });

    const withThreadLock = <A, E, R>(threadId: string, effect: Effect.Effect<A, E, R>) =>
      Effect.flatMap(getThreadSemaphore(threadId), (semaphore) => semaphore.withPermit(effect));

    const appendStderrTail = (ctx: PiSessionContext, chunk: string): void => {
      ctx.stderrTail = `${ctx.stderrTail}${chunk}`.slice(-STDERR_TAIL_LIMIT);
    };

    const failPendingRequests = (ctx: PiSessionContext, reason: string) =>
      Effect.forEach(
        Array.from(ctx.pendingRequests.values()),
        (deferred) =>
          Deferred.fail(
            deferred,
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "session/closed",
              detail: reason,
            }),
          ).pipe(Effect.ignore),
        { discard: true },
      );

    const writeLine = (ctx: PiSessionContext, record: PiRpcRecord) =>
      Effect.gen(function* () {
        if (ctx.stopped) {
          return yield* new ProviderAdapterSessionClosedError({
            provider: PROVIDER,
            threadId: ctx.threadId,
          });
        }
        const line = `${encodePiJsonLine(record)}\n`;
        // Offer into the long-lived stdin drain. Writing via a one-shot
        // `Stream.run(..., child.stdin)` would END the child's stdin (the
        // spawner's `endOnDone` default), which pi treats as EOF and stops
        // answering subsequent RPC commands.
        yield* Queue.offer(ctx.stdinQueue, line);
      });

    const piRequest = (ctx: PiSessionContext, command: PiRpcRecord) =>
      Effect.gen(function* () {
        if (ctx.stopped) {
          return yield* new ProviderAdapterSessionClosedError({
            provider: PROVIDER,
            threadId: ctx.threadId,
          });
        }
        const method = typeof command.type === "string" ? command.type : "unknown";
        const id = `req-${yield* randomUUIDv4}`;
        const deferred = yield* Deferred.make<PiRpcRecord, ProviderAdapterRequestError>();
        ctx.pendingRequests.set(id, deferred);
        const writeOutcome = yield* writeLine(ctx, { ...command, id }).pipe(Effect.result);
        if (Result.isFailure(writeOutcome)) {
          ctx.pendingRequests.delete(id);
          return yield* writeOutcome.failure;
        }
        const response = yield* Deferred.await(deferred).pipe(
          Effect.timeout(PI_REQUEST_TIMEOUT_MS),
          Effect.catchTag(
            "TimeoutError",
            () =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method,
                detail: `Timed out after ${PI_REQUEST_TIMEOUT_MS}ms waiting for pi response.`,
              }),
          ),
        );
        ctx.pendingRequests.delete(id);
        if (response.success === false) {
          const detail =
            typeof response.error === "string" && response.error.trim().length > 0
              ? response.error.trim()
              : "pi reported a command failure.";
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method,
            detail,
          });
        }
        return response;
      });

    const settleTurn = (
      ctx: PiSessionContext,
      turnId: TurnId,
      options?: {
        readonly state?: "completed" | "interrupted" | "failed";
        readonly errorMessage?: string;
      },
    ) =>
      Effect.gen(function* () {
        const liveCtx = sessions.get(ctx.threadId);
        if (!liveCtx || liveCtx.stopped) {
          return;
        }
        if (liveCtx.activeTurnId !== turnId) {
          return;
        }
        const state =
          options?.state ?? (liveCtx.interruptedTurnIds.has(turnId) ? "interrupted" : "completed");
        yield* offerRuntimeEvent({
          type: "turn.completed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          payload: {
            state,
            ...(options?.errorMessage ? { errorMessage: options.errorMessage } : {}),
          },
        });
        liveCtx.activeTurnId = undefined;
        liveCtx.promptsInFlight = 0;
        liveCtx.currentAssistantItemId = undefined;
        liveCtx.session = {
          ...liveCtx.session,
          activeTurnId: undefined,
          updatedAt: yield* nowIso,
        };
      });

    const nonNegativeInt = (value: unknown): number | undefined => {
      if (typeof value !== "number" || !Number.isFinite(value)) {
        return undefined;
      }
      return Math.max(0, Math.round(value));
    };

    const emitPiTokenUsage = (ctx: PiSessionContext, data: unknown) =>
      Effect.gen(function* () {
        if (!isRecord(data)) {
          return;
        }
        const tokens = isRecord(data.tokens) ? (data.tokens as Record<string, unknown>) : {};
        const contextUsage = isRecord(data.contextUsage)
          ? (data.contextUsage as Record<string, unknown>)
          : {};
        const usedTokens = nonNegativeInt(tokens.total) ?? 0;
        const maxTokens = nonNegativeInt(contextUsage.contextWindow);
        const inputTokens = nonNegativeInt(tokens.input);
        const cachedInputTokens = nonNegativeInt(tokens.cacheRead);
        const outputTokens = nonNegativeInt(tokens.output);
        yield* offerRuntimeEvent({
          type: "thread.token-usage.updated",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: {
            usage: {
              usedTokens,
              totalProcessedTokens: usedTokens,
              ...(maxTokens !== undefined && maxTokens > 0 ? { maxTokens } : {}),
              ...(inputTokens !== undefined ? { inputTokens } : {}),
              ...(cachedInputTokens !== undefined ? { cachedInputTokens } : {}),
              ...(outputTokens !== undefined ? { outputTokens } : {}),
            },
          },
        });
      });

    const refreshPiState = (ctx: PiSessionContext) =>
      Effect.gen(function* () {
        const state = yield* piRequest(ctx, { type: "get_state" });
        if (!isRecord(state.data)) {
          return;
        }
        const data = state.data as Record<string, unknown>;
        if (typeof data.sessionFile === "string" && data.sessionFile.trim().length > 0) {
          ctx.sessionFile = data.sessionFile.trim();
        }
        const modelId = piModelSlug(isRecord(data.model) ? data.model : undefined);
        if (modelId) {
          ctx.currentModelId = modelId;
          ctx.session = { ...ctx.session, model: modelId, updatedAt: yield* nowIso };
        }
      });

    const handleAgentSettled = (ctx: PiSessionContext) =>
      Effect.gen(function* () {
        const turnId = ctx.activeTurnId;
        if (turnId !== undefined) {
          const existingTurnRecord = ctx.turns.find((turn) => turn.id === turnId);
          ctx.turns = existingTurnRecord
            ? ctx.turns.map((turn) =>
                turn.id === turnId ? { ...turn, items: [...turn.items, { settled: true }] } : turn,
              )
            : ctx.turns;
          yield* settleTurn(ctx, turnId);
        }
        // Refresh token usage best-effort.
        const stats = yield* piRequest(ctx, { type: "get_session_stats" }).pipe(Effect.result);
        if (Result.isSuccess(stats)) {
          yield* emitPiTokenUsage(ctx, stats.success.data);
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to settle pi turn", { errorTag: causeErrorTag(cause) }),
        ),
      );

    const handleExtensionUiRequest = (ctx: PiSessionContext, record: PiRpcRecord) =>
      Effect.gen(function* () {
        const method = typeof record.method === "string" ? record.method : "unknown";
        const id = typeof record.id === "string" ? record.id : undefined;
        const title = typeof record.title === "string" ? record.title : method;
        if (method === "notify") {
          const message = typeof record.message === "string" ? record.message : title;
          yield* offerRuntimeEvent({
            type: "runtime.warning",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            payload: { message: `pi extension: ${message}` },
          });
          return;
        }
        if (id === undefined) {
          return;
        }
        switch (method) {
          case "select":
          case "confirm":
          case "input":
          case "editor":
            yield* writeLine(ctx, { type: "extension_ui_response", id, cancelled: true }).pipe(
              Effect.catchCause(() => Effect.void),
              Effect.asVoid,
            );
            yield* offerRuntimeEvent({
              type: "runtime.warning",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              payload: {
                message: `pi extension requested '${method}' ("${title}"); request auto-cancelled.`,
              },
            });
            return;
          default:
          // Fire-and-forget status methods (setStatus, setWidget, ...): ignore.
        }
      });

    const mapPiEvent = (ctx: PiSessionContext, record: PiRpcRecord) =>
      Effect.gen(function* () {
        const type = typeof record.type === "string" ? record.type : undefined;
        if (type === undefined) {
          return;
        }
        const turnId = ctx.activeTurnId;

        switch (type) {
          case "message_start": {
            const message = isRecord(record.message) ? record.message : undefined;
            if (message?.role === "assistant") {
              const itemId = `assistant-${ctx.assistantItemCounter++}`;
              ctx.currentAssistantItemId = itemId;
              yield* offerRuntimeEvent({
                type: "item.started",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: ctx.threadId,
                ...(turnId !== undefined ? { turnId } : {}),
                itemId: RuntimeItemId.make(itemId),
                payload: { itemType: "assistant_message", status: "inProgress" },
                raw: { source: "pi.rpc.event", method: "message_start", payload: record },
              });
            }
            return;
          }
          case "message_end": {
            const message = isRecord(record.message) ? record.message : undefined;
            if (message?.role === "assistant" && ctx.currentAssistantItemId !== undefined) {
              const itemId = ctx.currentAssistantItemId;
              ctx.currentAssistantItemId = undefined;
              yield* offerRuntimeEvent({
                type: "item.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: ctx.threadId,
                ...(turnId !== undefined ? { turnId } : {}),
                itemId: RuntimeItemId.make(itemId),
                payload: { itemType: "assistant_message", status: "completed" },
                raw: { source: "pi.rpc.event", method: "message_end", payload: record },
              });
            }
            return;
          }
          case "message_update": {
            const assistantMessageEvent = isRecord(record.assistantMessageEvent)
              ? record.assistantMessageEvent
              : undefined;
            const deltaType =
              assistantMessageEvent && typeof assistantMessageEvent.type === "string"
                ? assistantMessageEvent.type
                : undefined;
            const delta =
              assistantMessageEvent && typeof assistantMessageEvent.delta === "string"
                ? assistantMessageEvent.delta
                : undefined;
            if (delta === undefined || delta.length === 0) {
              return;
            }
            if (deltaType === "text_delta" || deltaType === "thinking_delta") {
              yield* offerRuntimeEvent({
                type: "content.delta",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: ctx.threadId,
                ...(turnId !== undefined ? { turnId } : {}),
                ...(ctx.currentAssistantItemId !== undefined
                  ? { itemId: RuntimeItemId.make(ctx.currentAssistantItemId) }
                  : {}),
                payload: {
                  streamKind: deltaType === "text_delta" ? "assistant_text" : "reasoning_text",
                  delta,
                },
                raw: { source: "pi.rpc.event", method: "message_update", payload: record },
              });
            }
            return;
          }
          case "tool_execution_start": {
            const toolCallId =
              typeof record.toolCallId === "string" ? record.toolCallId : undefined;
            if (toolCallId === undefined) {
              return;
            }
            const toolName = typeof record.toolName === "string" ? record.toolName : undefined;
            const toolTitle = piToolTitle(toolName, record.args);
            ctx.toolTitleById.set(toolCallId, toolTitle);
            const toolArgs = isRecord(record.args) ? record.args : {};
            yield* offerRuntimeEvent({
              type: "item.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              ...(turnId !== undefined ? { turnId } : {}),
              itemId: RuntimeItemId.make(toolCallId),
              payload: {
                itemType: piToolItemType(toolName),
                status: "inProgress",
                title: toolTitle,
                // `data.command` is what the work-log extraction uses to
                // populate the command preview / expanded body.
                ...(toolName === "bash" && typeof toolArgs.command === "string"
                  ? { data: { command: toolArgs.command } }
                  : {}),
              },
              raw: { source: "pi.rpc.event", method: "tool_execution_start", payload: record },
            });
            return;
          }
          case "tool_execution_update": {
            const toolCallId =
              typeof record.toolCallId === "string" ? record.toolCallId : undefined;
            if (toolCallId === undefined) {
              return;
            }
            const toolName = typeof record.toolName === "string" ? record.toolName : undefined;
            const title = ctx.toolTitleById.get(toolCallId) ?? piToolTitle(toolName, record.args);
            const detail = piResultText(record.partialResult);
            yield* offerRuntimeEvent({
              type: "item.updated",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              ...(turnId !== undefined ? { turnId } : {}),
              itemId: RuntimeItemId.make(toolCallId),
              payload: {
                itemType: piToolItemType(toolName),
                status: "inProgress",
                title,
                ...(detail ? { detail } : {}),
              },
              raw: { source: "pi.rpc.event", method: "tool_execution_update", payload: record },
            });
            return;
          }
          case "tool_execution_end": {
            const toolCallId =
              typeof record.toolCallId === "string" ? record.toolCallId : undefined;
            if (toolCallId === undefined) {
              return;
            }
            const toolName = typeof record.toolName === "string" ? record.toolName : undefined;
            const isError = record.isError === true;
            // Successful bash output feeds the expanded tool body; other
            // results (file contents) stay out to avoid noise.
            const detail = toolName === "bash" || isError ? piResultText(record.result) : undefined;
            const title = ctx.toolTitleById.get(toolCallId) ?? piToolTitle(toolName, undefined);
            yield* offerRuntimeEvent({
              type: "item.completed",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              ...(turnId !== undefined ? { turnId } : {}),
              itemId: RuntimeItemId.make(toolCallId),
              payload: {
                itemType: piToolItemType(toolName),
                status: isError ? "failed" : "completed",
                title,
                ...(detail ? { detail } : {}),
              },
              raw: { source: "pi.rpc.event", method: "tool_execution_end", payload: record },
            });
            return;
          }
          case "compaction_start": {
            const itemId = `compaction-${ctx.compactionItemCounter++}`;
            ctx.activeCompactionItemId = itemId;
            yield* offerRuntimeEvent({
              type: "item.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              ...(turnId !== undefined ? { turnId } : {}),
              itemId: RuntimeItemId.make(itemId),
              payload: {
                itemType: "context_compaction",
                status: "inProgress",
                title: "Compacting context",
              },
              raw: { source: "pi.rpc.event", method: "compaction_start", payload: record },
            });
            return;
          }
          case "compaction_end": {
            if (ctx.activeCompactionItemId !== undefined) {
              const itemId = ctx.activeCompactionItemId;
              ctx.activeCompactionItemId = undefined;
              yield* offerRuntimeEvent({
                type: "item.completed",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: ctx.threadId,
                ...(turnId !== undefined ? { turnId } : {}),
                itemId: RuntimeItemId.make(itemId),
                payload: { itemType: "context_compaction", status: "completed" },
                raw: { source: "pi.rpc.event", method: "compaction_end", payload: record },
              });
            }
            return;
          }
          case "auto_retry_start":
          case "auto_retry_end": {
            yield* offerRuntimeEvent({
              type: "runtime.warning",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              ...(turnId !== undefined ? { turnId } : {}),
              payload: {
                message:
                  type === "auto_retry_start"
                    ? "pi is retrying after a transient provider error."
                    : "pi finished retrying a transient provider error.",
              },
              raw: { source: "pi.rpc.event", method: type, payload: record },
            });
            return;
          }
          case "extension_error": {
            const message =
              typeof record.error === "string"
                ? record.error
                : typeof record.message === "string"
                  ? record.message
                  : "pi extension error.";
            yield* offerRuntimeEvent({
              type: "runtime.error",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: ctx.threadId,
              ...(turnId !== undefined ? { turnId } : {}),
              payload: { message, class: "provider_error" },
              raw: { source: "pi.rpc.event", method: "extension_error", payload: record },
            });
            return;
          }
          case "agent_settled": {
            yield* handleAgentSettled(ctx);
            return;
          }
          default:
          // agent_start, agent_end, turn_start, turn_end, queue_update,
          // bash_execution_update, summarization_* : not surfaced directly.
        }
      });

    const handleStdoutLine = (ctx: PiSessionContext, line: string) =>
      Effect.gen(function* () {
        const parsed = decodePiJsonLine(line);
        if (Option.isNone(parsed)) {
          return;
        }
        const record = parsed.value;
        if (record.type === "response") {
          const id = typeof record.id === "string" ? record.id : undefined;
          if (id !== undefined) {
            const pending = ctx.pendingRequests.get(id);
            if (pending) {
              ctx.pendingRequests.delete(id);
              yield* Deferred.succeed(pending, record).pipe(Effect.asVoid);
            }
          }
          return;
        }
        if (record.type === "extension_ui_request") {
          yield* handleExtensionUiRequest(ctx, record);
          return;
        }
        yield* mapPiEvent(ctx, record);
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to process pi RPC line", {
            errorTag: causeErrorTag(cause),
          }),
        ),
      );

    const startStdoutReader = (
      ctx: PiSessionContext,
      process: ChildProcessSpawner.ChildProcessHandle,
    ) =>
      piJsonLines(process).pipe(
        Stream.runForEach((line) => handleStdoutLine(ctx, line)),
        Effect.catchCause((cause) =>
          Effect.logWarning("pi stdout stream failed", {
            errorTag: causeErrorTag(cause),
            stderrTail: ctx.stderrTail.slice(-1_000),
          }),
        ),
      );

    const startStderrCollector = (
      ctx: PiSessionContext,
      process: ChildProcessSpawner.ChildProcessHandle,
    ) =>
      process.stderr.pipe(
        Stream.decodeText(),
        Stream.runForEach((chunk) => Effect.sync(() => appendStderrTail(ctx, chunk))),
        Effect.catchCause(() => Effect.void),
      );

    const handleProcessExit = (ctx: PiSessionContext, code: number, detail?: string) =>
      Effect.gen(function* () {
        if (ctx.stopped) {
          return;
        }
        ctx.stopped = true;
        const reason = detail ?? `pi process exited with code ${code}.`;
        yield* failPendingRequests(ctx, reason);
        const turnId = ctx.activeTurnId;
        if (turnId !== undefined) {
          yield* settleTurn(ctx, turnId, { state: "failed", errorMessage: reason });
        }
        yield* offerRuntimeEvent({
          type: "session.state.changed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { state: "error", reason },
        });
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: {
            exitKind: "error",
            reason,
            recoverable: true,
          },
        });
        yield* Scope.close(ctx.scope, Exit.void).pipe(Effect.ignore);
        sessions.delete(ctx.threadId);
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("Failed to handle pi process exit", { errorTag: causeErrorTag(cause) }),
        ),
      );

    const startExitWatcher = (
      ctx: PiSessionContext,
      process: ChildProcessSpawner.ChildProcessHandle,
    ) =>
      process.exitCode.pipe(
        Effect.match({
          onFailure: (error) => handleProcessExit(ctx, -1, String(error)),
          onSuccess: (code) => handleProcessExit(ctx, Number(code)),
        }),
      );

    const applyModelSelection = (ctx: PiSessionContext, requestedModel: string | undefined) =>
      Effect.gen(function* () {
        const pattern = parsePiModelPattern(requestedModel);
        if (pattern === undefined) {
          return ctx.currentModelId;
        }
        const requestedSlug =
          pattern.provider === undefined
            ? pattern.modelId
            : `${pattern.provider}/${pattern.modelId}`;
        if (requestedSlug === ctx.currentModelId) {
          return ctx.currentModelId;
        }
        const setModel = yield* piRequest(ctx, {
          type: "set_model",
          ...(pattern.provider !== undefined ? { provider: pattern.provider } : {}),
          modelId: pattern.modelId,
        }).pipe(Effect.result);
        if (Result.isFailure(setModel)) {
          return yield* setModel.failure;
        }
        if (pattern.thinking !== undefined) {
          yield* piRequest(ctx, { type: "set_thinking_level", level: pattern.thinking }).pipe(
            Effect.catchCause(() => Effect.void),
            Effect.asVoid,
          );
        }
        const dataModel = isRecord(setModel.success.data)
          ? (setModel.success.data as Record<string, unknown>)
          : undefined;
        const nextModelId =
          (dataModel ? piModelSlug(dataModel.model ?? dataModel) : undefined) ?? requestedSlug;
        const normalizedModelId = nextModelId.trim();
        if (normalizedModelId.length === 0) {
          return ctx.currentModelId;
        }
        ctx.currentModelId = normalizedModelId;
        ctx.session = { ...ctx.session, model: normalizedModelId, updatedAt: yield* nowIso };
        return normalizedModelId;
      });

    const stopSessionInternal = (ctx: PiSessionContext) =>
      Effect.gen(function* () {
        if (ctx.stopped) return;
        ctx.stopped = true;
        yield* failPendingRequests(ctx, "pi session stopped.");
        if (ctx.process) {
          yield* ctx.process.kill().pipe(
            Effect.catchCause(() => Effect.void),
            Effect.asVoid,
          );
        }
        yield* Scope.close(ctx.scope, Exit.void).pipe(Effect.ignore);
        sessions.delete(ctx.threadId);
        yield* offerRuntimeEvent({
          type: "session.exited",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          payload: { exitKind: "graceful" },
        });
      });

    const buildSpawnArgs = (resumeSessionFile: string | undefined): ReadonlyArray<string> => {
      const args: string[] = ["--mode", "rpc"];
      const provider = piSettings.provider.trim();
      const model = piSettings.model.trim();
      if (provider) {
        args.push("--provider", provider);
      }
      if (model) {
        args.push("--model", model);
      }
      if (piSettings.thinkingLevel && piSettings.thinkingLevel !== "off") {
        args.push("--thinking", piSettings.thinkingLevel);
      }
      if (resumeSessionFile) {
        args.push("--session", resumeSessionFile);
      }
      return args;
    };

    const startSessionInternal = (input: ProviderSessionStartInput) =>
      Effect.gen(function* () {
        if (input.provider !== undefined && input.provider !== PROVIDER) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          });
        }
        if (!input.cwd?.trim()) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: "cwd is required and must be non-empty.",
          });
        }

        const cwd = path.resolve(input.cwd.trim());
        const existing = sessions.get(input.threadId);
        if (existing && !existing.stopped) {
          yield* stopSessionInternal(existing);
        }

        const started = yield* Effect.gen(function* () {
          const sessionScope = yield* Scope.make();
          const outcome = yield* Effect.exit(
            Effect.gen(function* () {
              const resumeSessionFile = parsePiResume(input.resumeCursor)?.sessionFile;
              const processEnv = options?.environment ?? process.env;

              const ctx: PiSessionContext = {
                threadId: input.threadId,
                scope: sessionScope,
                process: undefined,
                stdinQueue: yield* Queue.unbounded<string>(),
                session: {
                  provider: PROVIDER,
                  providerInstanceId: boundInstanceId,
                  status: "connecting",
                  runtimeMode: input.runtimeMode,
                  cwd,
                  threadId: input.threadId,
                  createdAt: yield* nowIso,
                  updatedAt: yield* nowIso,
                },
                pendingRequests: new Map(),
                turns: [],
                activeTurnId: undefined,
                promptsInFlight: 0,
                interruptedTurnIds: new Set(),
                currentModelId: undefined,
                sessionFile: resumeSessionFile,
                stopped: false,
                assistantItemCounter: 0,
                currentAssistantItemId: undefined,
                compactionItemCounter: 0,
                activeCompactionItemId: undefined,
                toolTitleById: new Map(),
                stderrTail: "",
              };

              const command = piSettings.binaryPath || "pi";
              const spawnCommand = yield* resolveSpawnCommand(
                command,
                buildSpawnArgs(resumeSessionFile),
                { env: processEnv },
              );
              const child = yield* childProcessSpawner
                .spawn(
                  ChildProcess.make(spawnCommand.command, spawnCommand.args, {
                    cwd,
                    env: processEnv,
                    shell: spawnCommand.shell,
                  }),
                )
                .pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapterProcessError({
                        provider: PROVIDER,
                        threadId: input.threadId,
                        detail: `Failed to spawn pi process: ${String(cause)}`,
                        cause,
                      }),
                  ),
                );
              ctx.process = child;

              // One long-lived stdin drain: per-request writes offer into
              // the queue so the child's stdin is never closed mid-session.
              yield* Stream.run(
                Stream.encodeText(Stream.fromQueue(ctx.stdinQueue)),
                child.stdin,
              ).pipe(Effect.forkIn(sessionScope));
              yield* startStdoutReader(ctx, child).pipe(Effect.forkIn(sessionScope));
              yield* startStderrCollector(ctx, child).pipe(Effect.forkIn(sessionScope));
              yield* startExitWatcher(ctx, child).pipe(Effect.forkIn(sessionScope));

              // Readiness handshake: get_state doubles as the startup probe
              // and gives us the session file plus current model.
              const stateResponse = yield* piRequest(ctx, { type: "get_state" }).pipe(
                Effect.timeout(PI_REQUEST_TIMEOUT_MS),
                Effect.catchTag(
                  "TimeoutError",
                  () =>
                    new ProviderAdapterRequestError({
                      provider: PROVIDER,
                      method: "get_state",
                      detail: "Timed out waiting for pi to become ready.",
                    }),
                ),
              );
              if (isRecord(stateResponse.data)) {
                const data = stateResponse.data as Record<string, unknown>;
                if (typeof data.sessionFile === "string" && data.sessionFile.trim().length > 0) {
                  ctx.sessionFile = data.sessionFile.trim();
                }
                const model = data.model;
                ctx.currentModelId = piModelSlug(isRecord(model) ? model : undefined);
              }

              const modelSelection =
                input.modelSelection?.instanceId === boundInstanceId
                  ? input.modelSelection
                  : undefined;
              if (modelSelection?.model) {
                yield* applyModelSelection(ctx, modelSelection.model);
              }

              const now = yield* nowIso;
              ctx.session = {
                ...ctx.session,
                status: "ready",
                ...(ctx.currentModelId ? { model: ctx.currentModelId } : {}),
                resumeCursor:
                  ctx.sessionFile !== undefined
                    ? { schemaVersion: PI_RESUME_VERSION, sessionFile: ctx.sessionFile }
                    : undefined,
                updatedAt: now,
              };

              return ctx;
            }).pipe(Effect.provideService(Scope.Scope, sessionScope)),
          );
          if (Exit.isFailure(outcome)) {
            yield* Scope.close(sessionScope, Exit.void).pipe(Effect.ignore);
            const error = Cause.squash(outcome.cause);
            if (
              isProviderAdapterRequestError(error) ||
              isProviderAdapterProcessError(error) ||
              isProviderAdapterSessionClosedError(error) ||
              isProviderAdapterValidationError(error)
            ) {
              return yield* error;
            }
            return yield* new ProviderAdapterProcessError({
              provider: PROVIDER,
              threadId: input.threadId,
              detail: `Failed to start pi session: ${String(error)}`,
              cause: outcome.cause,
            });
          }
          return outcome.value;
        });

        const ctx = started;
        sessions.set(input.threadId, ctx);

        yield* offerRuntimeEvent({
          type: "session.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: {
            resume: {
              schemaVersion: PI_RESUME_VERSION,
              sessionFile: ctx.sessionFile ?? null,
            },
          },
        });
        yield* offerRuntimeEvent({
          type: "session.state.changed",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: { state: "ready", reason: "pi RPC session ready" },
        });
        yield* offerRuntimeEvent({
          type: "thread.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: input.threadId,
          payload: {},
        });

        return ctx.session;
      });

    const startSession: PiAdapterShape["startSession"] = (input) =>
      withThreadLock(input.threadId, startSessionInternal(input));

    const restartPiSession = (ctx: PiSessionContext) =>
      Effect.gen(function* () {
        const sessionFile = ctx.sessionFile;
        const cwd = ctx.session.cwd;
        const runtimeMode = ctx.session.runtimeMode;
        yield* stopSessionInternal(ctx);
        yield* startSessionInternal({
          threadId: ctx.threadId,
          cwd,
          runtimeMode,
          resumeCursor:
            sessionFile !== undefined
              ? { schemaVersion: PI_RESUME_VERSION, sessionFile }
              : undefined,
        });
      });

    const runPiSlashCommandTurn = (
      ctx: PiSessionContext,
      invocation: { readonly name: string; readonly args: string },
    ) =>
      Effect.gen(function* () {
        // A command runs against the idle session. If a turn is in flight,
        // abort it first and settle it locally so the command turn owns the
        // session state.
        if (ctx.activeTurnId !== undefined) {
          const previousTurnId = ctx.activeTurnId;
          yield* piRequest(ctx, { type: "abort" }).pipe(Effect.result);
          yield* settleTurn(ctx, previousTurnId, { state: "interrupted" });
        }

        const turnId = TurnId.make(yield* randomUUIDv4);
        ctx.activeTurnId = turnId;
        ctx.promptsInFlight += 1;
        ctx.session = { ...ctx.session, activeTurnId: turnId, updatedAt: yield* nowIso };
        ctx.turns = [...ctx.turns, { id: turnId, items: [{ command: `/${invocation.name}` }] }];
        yield* offerRuntimeEvent({
          type: "turn.started",
          ...(yield* makeEventStamp()),
          provider: PROVIDER,
          threadId: ctx.threadId,
          turnId,
          payload: {
            ...(ctx.session.model ? { model: ctx.session.model } : {}),
          },
        });

        const execution = Effect.gen(function* () {
          switch (invocation.name) {
            case "reload": {
              const rpcReload = yield* piRequest(ctx, { type: "reload" }).pipe(Effect.result);
              if (Result.isFailure(rpcReload)) {
                // pi releases without an RPC `reload` command handle /reload
                // in the TUI layer only. Restart the RPC session instead:
                // the fresh process re-reads extensions, skills, prompts,
                // themes, and context files, and the resume cursor keeps the
                // conversation history.
                yield* settleTurn(ctx, turnId, { state: "completed" });
                yield* offerRuntimeEvent({
                  type: "runtime.warning",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: ctx.threadId,
                  turnId,
                  payload: {
                    message:
                      "Reloading pi extensions, skills, prompts, themes, and context files...",
                  },
                });
                yield* restartPiSession(ctx);
                return { settled: true };
              }
              yield* refreshPiState(ctx);
              yield* offerRuntimeEvent({
                type: "runtime.warning",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId,
                payload: {
                  message: "pi reloaded extensions, skills, prompts, themes, and context files.",
                },
              });
              return { settled: false };
            }
            case "compact": {
              yield* piRequest(ctx, { type: "compact" });
              yield* offerRuntimeEvent({
                type: "runtime.warning",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId,
                payload: { message: "pi compacted the session context." },
              });
              return { settled: false };
            }
            case "new": {
              yield* piRequest(ctx, { type: "new_session" });
              yield* refreshPiState(ctx);
              yield* offerRuntimeEvent({
                type: "runtime.warning",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId,
                payload: { message: "pi started a new session." },
              });
              return { settled: false };
            }
            case "clone": {
              yield* piRequest(ctx, { type: "clone" });
              yield* refreshPiState(ctx);
              yield* offerRuntimeEvent({
                type: "runtime.warning",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId,
                payload: { message: "pi cloned the session at the current position." },
              });
              return { settled: false };
            }
            case "session": {
              const stats = yield* piRequest(ctx, { type: "get_session_stats" });
              yield* emitPiTokenUsage(ctx, stats.data);
              yield* offerRuntimeEvent({
                type: "runtime.warning",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId,
                payload: { message: "Session stats refreshed." },
              });
              return { settled: false };
            }
            case "name": {
              if (invocation.args.length === 0) {
                yield* offerRuntimeEvent({
                  type: "runtime.error",
                  ...(yield* makeEventStamp()),
                  provider: PROVIDER,
                  threadId: ctx.threadId,
                  turnId,
                  payload: {
                    message: "/name requires a session name, e.g. `/name migration-notes`.",
                    class: "validation_error",
                  },
                });
                return { settled: false };
              }
              yield* piRequest(ctx, { type: "set_session_name", name: invocation.args });
              yield* offerRuntimeEvent({
                type: "runtime.warning",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId,
                payload: { message: `Session renamed to '${invocation.args}'.` },
              });
              return { settled: false };
            }
            case "export": {
              const exported = yield* piRequest(ctx, {
                type: "export_html",
                ...(invocation.args.length > 0 ? { outputPath: invocation.args } : {}),
              });
              const exportPath =
                isRecord(exported.data) && typeof exported.data.path === "string"
                  ? exported.data.path
                  : undefined;
              yield* offerRuntimeEvent({
                type: "runtime.warning",
                ...(yield* makeEventStamp()),
                provider: PROVIDER,
                threadId: ctx.threadId,
                turnId,
                payload: {
                  message:
                    exportPath !== undefined
                      ? `Session exported to ${exportPath}.`
                      : "Session exported.",
                },
              });
              return { settled: false };
            }
            default:
              return { settled: false };
          }
        });

        const outcome = yield* execution.pipe(Effect.result);
        if (Result.isFailure(outcome)) {
          // Command failures (e.g. `/compact` on a session that is too small
          // to compact) surface as a visible thread message instead of
          // failing the whole sendTurn call.
          const error = outcome.failure;
          const errorMessage = isProviderAdapterRequestError(error)
            ? error.detail
            : (error.message ?? String(error));
          yield* offerRuntimeEvent({
            type: "runtime.error",
            ...(yield* makeEventStamp()),
            provider: PROVIDER,
            threadId: ctx.threadId,
            turnId,
            payload: { message: errorMessage, class: "provider_error" },
          });
          yield* settleTurn(ctx, turnId, { state: "failed", errorMessage });
          return {
            threadId: ctx.threadId,
            turnId,
            ...(ctx.sessionFile !== undefined
              ? {
                  resumeCursor: {
                    schemaVersion: PI_RESUME_VERSION,
                    sessionFile: ctx.sessionFile,
                  },
                }
              : {}),
          } satisfies ProviderTurnStartResult;
        }
        if (!outcome.success.settled) {
          yield* settleTurn(ctx, turnId, { state: "completed" });
        }
        const liveCtx = sessions.get(ctx.threadId);
        return {
          threadId: ctx.threadId,
          turnId,
          ...(liveCtx?.sessionFile !== undefined
            ? {
                resumeCursor: {
                  schemaVersion: PI_RESUME_VERSION,
                  sessionFile: liveCtx.sessionFile,
                },
              }
            : {}),
        } satisfies ProviderTurnStartResult;
      });

    const sendTurn: PiAdapterShape["sendTurn"] = (input) =>
      withThreadLock(
        input.threadId,
        Effect.gen(function* () {
          const ctx = sessions.get(input.threadId);
          if (!ctx) {
            return yield* new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId: input.threadId,
            });
          }
          if (ctx.stopped) {
            return yield* new ProviderAdapterSessionClosedError({
              provider: PROVIDER,
              threadId: input.threadId,
            });
          }

          const text = input.input?.trim();
          const attachments = input.attachments ?? [];
          if ((!text || text.length === 0) && attachments.length === 0) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "sendTurn",
              issue: "Turn requires non-empty text or attachments.",
            });
          }

          // pi built-in slash commands map to dedicated RPC commands (reload,
          // compact, ...) rather than a model prompt.
          const slashInvocation = parsePiSlashCommandInvocation(text);
          if (
            slashInvocation !== undefined &&
            PI_BUILTIN_SLASH_COMMANDS.some((command) => command.name === slashInvocation.name)
          ) {
            return yield* runPiSlashCommandTurn(ctx, slashInvocation);
          }

          const images = yield* Effect.forEach(attachments, (attachment) =>
            Effect.gen(function* () {
              const attachmentPath = resolveAttachmentPath({
                attachmentsDir: serverConfig.attachmentsDir,
                attachment,
              });
              if (!attachmentPath) {
                return yield* new ProviderAdapterRequestError({
                  provider: PROVIDER,
                  method: "session/prompt",
                  detail: `Invalid attachment id '${attachment.id}'.`,
                });
              }
              const bytes = yield* fileSystem.readFile(attachmentPath).pipe(
                Effect.mapError(
                  (cause) =>
                    new ProviderAdapterRequestError({
                      provider: PROVIDER,
                      method: "session/prompt",
                      detail: String(cause),
                      cause,
                    }),
                ),
              );
              return {
                type: "image",
                data: Buffer.from(bytes).toString("base64"),
                ...(attachment.mimeType ? { mimeType: attachment.mimeType } : {}),
              };
            }),
          );

          if (input.modelSelection?.instanceId === boundInstanceId && input.modelSelection.model) {
            yield* applyModelSelection(ctx, input.modelSelection.model);
          }

          const steer = ctx.promptsInFlight > 0;
          let turnId: TurnId;
          if (steer && ctx.activeTurnId !== undefined) {
            turnId = ctx.activeTurnId;
          } else {
            turnId = TurnId.make(yield* randomUUIDv4);
            ctx.activeTurnId = turnId;
            ctx.session = { ...ctx.session, activeTurnId: turnId, updatedAt: yield* nowIso };
            const existingTurnRecord = ctx.turns.find((turn) => turn.id === turnId);
            ctx.turns = existingTurnRecord
              ? ctx.turns
              : [
                  ...ctx.turns,
                  {
                    id: turnId,
                    items: [
                      {
                        prompt: text,
                        ...(images.length > 0 ? { imageCount: images.length } : {}),
                      },
                    ],
                  },
                ];
            yield* offerRuntimeEvent({
              type: "turn.started",
              ...(yield* makeEventStamp()),
              provider: PROVIDER,
              threadId: input.threadId,
              turnId,
              payload: ctx.session.model ? { model: ctx.session.model } : {},
            });
          }
          ctx.promptsInFlight += 1;

          const command = steer
            ? { type: "steer", message: text ?? "", ...(images.length > 0 ? { images } : {}) }
            : { type: "prompt", message: text ?? "", ...(images.length > 0 ? { images } : {}) };
          yield* piRequest(ctx, command);

          return {
            threadId: input.threadId,
            turnId,
            ...(ctx.sessionFile !== undefined
              ? {
                  resumeCursor: {
                    schemaVersion: PI_RESUME_VERSION,
                    sessionFile: ctx.sessionFile,
                  },
                }
              : {}),
          } satisfies ProviderTurnStartResult;
        }),
      );

    const interruptTurn: PiAdapterShape["interruptTurn"] = (threadId, turnId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = sessions.get(threadId);
          if (!ctx) {
            return yield* new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId,
            });
          }
          if (ctx.stopped) {
            return yield* new ProviderAdapterSessionClosedError({
              provider: PROVIDER,
              threadId,
            });
          }
          const targetTurnId = turnId ?? ctx.activeTurnId;
          if (targetTurnId === undefined) {
            return;
          }
          ctx.interruptedTurnIds.add(targetTurnId);
          const aborted = yield* piRequest(ctx, { type: "abort" }).pipe(Effect.result);
          if (Result.isFailure(aborted)) {
            yield* Effect.logWarning("Failed to send pi abort command", {
              errorTag: aborted.failure._tag,
            });
            yield* settleTurn(ctx, targetTurnId, { state: "interrupted" });
            return;
          }
          // `agent_settled` finalizes the turn with the interrupted state.
        }),
      );

    const respondToRequest: PiAdapterShape["respondToRequest"] = (
      _threadId,
      _requestId,
      _decision,
    ) =>
      Effect.fail(
        new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "respondToRequest",
          issue:
            "pi does not issue approval requests — tools run with the permissions the user configured in pi.",
        }),
      );

    const respondToUserInput: PiAdapterShape["respondToUserInput"] = (
      _threadId,
      _requestId,
      _answers,
    ) =>
      Effect.fail(
        new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "respondToUserInput",
          issue: "pi does not issue structured user-input requests.",
        }),
      );

    const stopSession: PiAdapterShape["stopSession"] = (threadId) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = sessions.get(threadId);
          if (ctx) {
            yield* stopSessionInternal(ctx);
          }
        }),
      );

    const listSessions: PiAdapterShape["listSessions"] = () =>
      Effect.sync(() => Array.from(sessions.values()).map((ctx) => ctx.session));

    const hasSession: PiAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => {
        const ctx = sessions.get(threadId);
        return ctx !== undefined && !ctx.stopped;
      });

    const readThread: PiAdapterShape["readThread"] = (threadId) =>
      Effect.sync(() => {
        const ctx = sessions.get(threadId);
        return { threadId, turns: ctx?.turns ?? [] };
      });

    const rollbackThread: PiAdapterShape["rollbackThread"] = (threadId, numTurns) =>
      withThreadLock(
        threadId,
        Effect.gen(function* () {
          const ctx = sessions.get(threadId);
          if (!ctx) {
            return yield* new ProviderAdapterSessionNotFoundError({
              provider: PROVIDER,
              threadId,
            });
          }
          if (ctx.stopped) {
            return yield* new ProviderAdapterSessionClosedError({
              provider: PROVIDER,
              threadId,
            });
          }
          if (ctx.activeTurnId !== undefined) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "rollbackThread",
              issue: "Cannot roll back a thread while a turn is in flight.",
            });
          }
          const forkResponse = yield* piRequest(ctx, { type: "get_fork_messages" });
          const data = isRecord(forkResponse.data) ? forkResponse.data : {};
          const messages = Array.isArray(data.messages) ? data.messages : [];
          const userMessages = messages.filter(
            (message): message is Record<string, unknown> =>
              isRecord(message) && typeof message.entryId === "string",
          );
          if (userMessages.length === 0) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "rollbackThread",
              issue: "Thread has no forkable messages.",
            });
          }
          const targetIndex = Math.max(0, userMessages.length - numTurns);
          const target = userMessages[targetIndex];
          if (target === undefined) {
            return yield* new ProviderAdapterValidationError({
              provider: PROVIDER,
              operation: "rollbackThread",
              issue: "Resolved fork target is unavailable.",
            });
          }
          const forked = yield* piRequest(ctx, {
            type: "fork",
            entryId: target.entryId,
          }).pipe(Effect.result);
          if (Result.isFailure(forked)) {
            return yield* forked.failure;
          }
          if (isRecord(forked.success.data) && forked.success.data.cancelled === true) {
            return yield* new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "fork",
              detail: "pi extension cancelled the fork.",
            });
          }
          // The fork rewinds the session to the chosen user message; trim the
          // local turn records to match.
          ctx.turns = ctx.turns.slice(0, Math.max(0, ctx.turns.length - numTurns));
          return { threadId, turns: ctx.turns };
        }),
      );

    const stopAll: PiAdapterShape["stopAll"] = () =>
      Effect.gen(function* () {
        const contexts = Array.from(sessions.values());
        yield* Effect.forEach(
          contexts,
          (ctx) => withThreadLock(ctx.threadId, stopSessionInternal(ctx)),
          { discard: true },
        );
      });

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "in-session",
      },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents: Stream.fromPubSub(runtimeEventPubSub),
    } satisfies PiAdapterShape;
  });
}
