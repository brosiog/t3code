import {
  EventId,
  ProviderDriverKind,
  RuntimeItemId,
  RuntimeRequestId,
  TurnId,
  type PiSettings,
  type ProviderInstanceId,
  type ProviderRuntimeEvent,
  type ProviderSession,
  type ThreadId,
  type ModelSelection,
} from "@t3tools/contracts";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Predicate from "effect/Predicate";
import * as PubSub from "effect/PubSub";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/unstable/process";

import { resolveAttachmentPath } from "../../attachmentStore.ts";
import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { makePiRpc, type PiRpc, type PiRecord } from "../piRpc.ts";
import { buildRuntimeInstructions } from "../RuntimeInstructions.ts";
import type {
  ProviderAdapterShape,
  ProviderThreadTurnSnapshot,
} from "../Services/ProviderAdapter.ts";

import type { EventNdjsonLogger } from "./EventNdjsonLogger.ts";

const PROVIDER = ProviderDriverKind.make("pi");
function contentText(content: unknown, type: "text" | "thinking"): string {
  return Array.isArray(content)
    ? content
        .flatMap((block) => {
          if (!Predicate.isObject(block) || block.type !== type) return [];
          const text = type === "text" ? block.text : block.thinking;
          return typeof text === "string" ? [text] : [];
        })
        .join("\n")
    : "";
}

type EventInput<T = ProviderRuntimeEvent> = T extends ProviderRuntimeEvent
  ? Omit<T, "eventId" | "provider" | "providerInstanceId" | "threadId" | "createdAt">
  : never;
interface Session {
  session: ProviderSession;
  scope: Scope.Closeable;
  rpc: PiRpc;
  lock: Semaphore.Semaphore;
  activeTurn: TurnId | undefined;
  assistantItem: RuntimeItemId | undefined;
  reasoningItem: RuntimeItemId | undefined;
  pending: Map<string, string>;
  turns: ProviderThreadTurnSnapshot[];
  failed: string | undefined;
  interrupted: boolean;
  stopped: boolean;
  inputTokens: number;
  outputTokens: number;
  cost: number;
  maxTokens: number | undefined;
}

export const applyPiModelSelection = (rpc: PiRpc, selection: ModelSelection | undefined) =>
  Effect.gen(function* () {
    if (!selection) return;
    const separator = selection.model.indexOf("/");
    if (separator < 1)
      return yield* new ProviderAdapterValidationError({
        provider: "pi",
        operation: "setModel",
        issue: "Pi model IDs must use provider/model format.",
      });
    const model = yield* rpc.request("set_model", {
      provider: selection.model.slice(0, separator),
      modelId: selection.model.slice(separator + 1),
    });
    const level = getModelSelectionStringOptionValue(selection, "reasoningEffort");
    if (level) yield* rpc.request("set_thinking_level", { level });
    return model;
  });

export const makePiAdapter = Effect.fn("makePiAdapter")(function* (
  settings: PiSettings,
  options: {
    instanceId: ProviderInstanceId;
    environment: NodeJS.ProcessEnv;
    nativeEventLogger?: EventNdjsonLogger;
  },
) {
  const crypto = yield* Crypto.Crypto;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fileSystem = yield* FileSystem.FileSystem;
  const config = yield* ServerConfig;
  const sessions = new Map<ThreadId, Session>();
  const events = yield* Effect.acquireRelease(
    PubSub.unbounded<ProviderRuntimeEvent>(),
    PubSub.shutdown,
  );
  const uuid = crypto.randomUUIDv4.pipe(
    Effect.mapError(
      (cause) =>
        new ProviderAdapterRequestError({
          provider: "pi",
          method: "uuid",
          detail: cause.message,
          cause,
        }),
    ),
  );
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const emit = (ctx: Session, event: EventInput) =>
    Effect.gen(function* () {
      yield* PubSub.publish(events, {
        ...event,
        eventId: EventId.make(yield* uuid),
        provider: PROVIDER,
        providerInstanceId: options.instanceId,
        threadId: ctx.session.threadId,
        createdAt: yield* now,
        ...(ctx.activeTurn ? { turnId: ctx.activeTurn } : {}),
      });
    }).pipe(Effect.orDie);
  const get = (threadId: ThreadId) =>
    Effect.suspend(() => {
      const ctx = sessions.get(threadId);
      return ctx && !ctx.stopped
        ? Effect.succeed(ctx)
        : Effect.fail(new ProviderAdapterSessionNotFoundError({ provider: "pi", threadId }));
    });
  const unsupported = (operation: string) =>
    Effect.fail(
      new ProviderAdapterValidationError({
        provider: "pi",
        operation,
        issue: `Pi does not support ${operation} through this adapter.`,
      }),
    );
  const cancelPending = (ctx: Session) =>
    Effect.gen(function* () {
      for (const [id, method] of ctx.pending) {
        const requestId = RuntimeRequestId.make(id);
        if (method === "confirm")
          yield* emit(ctx, {
            type: "request.resolved",
            requestId,
            payload: { requestType: "permission_approval", decision: "cancel" },
          });
        else yield* emit(ctx, { type: "user-input.resolved", requestId, payload: { answers: {} } });
      }
      ctx.pending.clear();
    });
  const settle = (ctx: Session) =>
    Effect.gen(function* () {
      if (!ctx.activeTurn) return;
      yield* cancelPending(ctx);
      yield* emit(ctx, {
        type: "turn.completed",
        payload: {
          state: ctx.interrupted ? "interrupted" : ctx.failed ? "failed" : "completed",
          ...(ctx.failed ? { errorMessage: ctx.failed } : {}),
          totalCostUsd: ctx.cost,
          tokenUsage: {
            usageScope: "main_agent",
            usageStatus: "complete",
            hasSubagents: false,
            inputTokens: ctx.inputTokens,
            outputTokens: ctx.outputTokens,
          },
        },
      });
      ctx.activeTurn = undefined;
      ctx.assistantItem = undefined;
      ctx.reasoningItem = undefined;
      ctx.session = { ...ctx.session, status: "ready", updatedAt: yield* now };
      const { activeTurnId: _active, ...idle } = ctx.session;
      ctx.session = idle;
      yield* emit(ctx, { type: "session.state.changed", payload: { state: "ready" } });
    });
  const handle = (ctx: Session, record: PiRecord): Effect.Effect<void> =>
    Effect.gen(function* () {
      const raw = { source: "pi.rpc" as const, payload: record };
      switch (record.type) {
        case "agent_settled":
          yield* settle(ctx);
          break;
        case "message_start":
          if (Predicate.isObject(record.message) && record.message.role === "assistant") {
            ctx.failed = undefined;
            ctx.assistantItem = RuntimeItemId.make(yield* uuid);
            ctx.reasoningItem = undefined;
            yield* emit(ctx, {
              type: "item.started",
              itemId: ctx.assistantItem,
              raw,
              payload: { itemType: "assistant_message", status: "inProgress" },
            });
          }
          break;
        case "message_update": {
          const update = record.assistantMessageEvent;
          if (!Predicate.isObject(update) || typeof update.delta !== "string") break;
          const thinking = update.type === "thinking_delta";
          if (!thinking && update.type !== "text_delta") break;
          if (thinking && !ctx.reasoningItem) {
            ctx.reasoningItem = RuntimeItemId.make(yield* uuid);
            yield* emit(ctx, {
              type: "item.started",
              itemId: ctx.reasoningItem,
              payload: { itemType: "reasoning", status: "inProgress" },
            });
          }
          const itemId = thinking ? ctx.reasoningItem : ctx.assistantItem;
          yield* emit(ctx, {
            type: "content.delta",
            ...(itemId ? { itemId } : {}),
            raw,
            payload: {
              streamKind: thinking ? "reasoning_text" : "assistant_text",
              delta: update.delta,
              ...(typeof update.contentIndex === "number"
                ? { contentIndex: update.contentIndex }
                : {}),
            },
          });
          break;
        }
        case "message_end": {
          const message = record.message;
          if (!Predicate.isObject(message) || message.role !== "assistant") break;
          if (message.stopReason === "error")
            ctx.failed =
              typeof message.errorMessage === "string"
                ? message.errorMessage
                : "Pi model request failed";
          if (message.stopReason === "aborted") ctx.interrupted = true;
          const turn = ctx.turns.find((turn) => turn.id === ctx.activeTurn);
          if (turn)
            ctx.turns = ctx.turns.map((entry) =>
              entry === turn ? { ...entry, items: [...entry.items, message] } : entry,
            );
          const usage = message.usage;
          if (Predicate.isObject(usage)) {
            const input = [usage.input, usage.cacheRead, usage.cacheWrite].reduce<number>(
              (sum, value) => sum + (typeof value === "number" ? value : 0),
              0,
            );
            ctx.inputTokens += input;
            ctx.outputTokens += typeof usage.output === "number" ? usage.output : 0;
            if (Predicate.isObject(usage.cost) && typeof usage.cost.total === "number")
              ctx.cost += usage.cost.total;
            yield* emit(ctx, {
              type: "thread.token-usage.updated",
              payload: {
                usage: {
                  usedTokens: input + (typeof usage.output === "number" ? usage.output : 0),
                  ...(ctx.maxTokens ? { maxTokens: ctx.maxTokens } : {}),
                  compactsAutomatically: true,
                },
              },
            });
          }
          if (ctx.reasoningItem)
            yield* emit(ctx, {
              type: "item.completed",
              itemId: ctx.reasoningItem,
              raw,
              payload: {
                itemType: "reasoning",
                status: "completed",
                ...(contentText(message.content, "thinking").trim()
                  ? { detail: contentText(message.content, "thinking") }
                  : {}),
              },
            });
          if (ctx.assistantItem)
            yield* emit(ctx, {
              type: "item.completed",
              itemId: ctx.assistantItem,
              raw,
              payload: {
                itemType: "assistant_message",
                status: ctx.failed ? "failed" : "completed",
                ...(contentText(message.content, "text").trim()
                  ? { detail: contentText(message.content, "text") }
                  : {}),
              },
            });
          break;
        }
        case "tool_execution_start":
        case "tool_execution_update":
        case "tool_execution_end": {
          if (typeof record.toolCallId !== "string") break;
          const itemType =
            record.toolName === "bash"
              ? "command_execution"
              : record.toolName === "edit" || record.toolName === "write"
                ? "file_change"
                : "dynamic_tool_call";
          yield* emit(ctx, {
            type:
              record.type === "tool_execution_start"
                ? "item.started"
                : record.type === "tool_execution_end"
                  ? "item.completed"
                  : "item.updated",
            itemId: RuntimeItemId.make(record.toolCallId),
            raw,
            payload: {
              itemType,
              status:
                record.type === "tool_execution_end"
                  ? record.isError
                    ? "failed"
                    : "completed"
                  : "inProgress",
              title: typeof record.toolName === "string" ? record.toolName : "Tool",
              data: record.result ?? record.partialResult ?? record.args,
              ...(Predicate.isObject(record.args) && typeof record.args.command === "string"
                ? { detail: record.args.command }
                : {}),
            },
          });
          break;
        }
        case "compaction_end":
        case "auto_compaction_end": {
          const result = record.result;
          if (Predicate.isObject(result) && record.aborted !== true) {
            yield* emit(ctx, {
              type: "thread.state.changed",
              raw,
              payload: {
                state: "compacted",
                ...(typeof result.tokensBefore === "number"
                  ? { beforeTokens: result.tokensBefore }
                  : {}),
                ...(typeof result.estimatedTokensAfter === "number"
                  ? { afterTokens: result.estimatedTokensAfter }
                  : {}),
              },
            });
          } else if (typeof record.errorMessage === "string") {
            yield* emit(ctx, {
              type: "runtime.warning",
              raw,
              payload: { message: record.errorMessage },
            });
          }
          break;
        }
        case "extension_ui_request": {
          if (typeof record.id !== "string" || typeof record.method !== "string") break;
          const requestId = RuntimeRequestId.make(record.id);
          const title =
            typeof record.title === "string" && record.title.trim() ? record.title : "Pi input";
          if (record.method === "confirm") {
            ctx.pending.set(record.id, record.method);
            yield* emit(ctx, {
              type: "request.opened",
              requestId,
              raw,
              payload: {
                requestType: "permission_approval",
                detail: title,
                options: [
                  { decision: "accept", label: "Confirm" },
                  { decision: "decline", label: "Decline" },
                ],
              },
            });
          } else if (["select", "input", "editor"].includes(record.method)) {
            ctx.pending.set(record.id, record.method);
            yield* emit(ctx, {
              type: "user-input.requested",
              requestId,
              raw,
              payload: {
                questions: [
                  {
                    id: record.id,
                    header: "Pi",
                    question: title,
                    allowCustomAnswer: record.method !== "select",
                    multiSelect: false,
                    options: Array.isArray(record.options)
                      ? record.options.flatMap((option) =>
                          typeof option === "string"
                            ? [{ label: option, description: "", value: option }]
                            : [],
                        )
                      : [],
                  },
                ],
              },
            });
          } else if (record.method === "notify" && typeof record.message === "string") {
            yield* emit(ctx, {
              type: "runtime.warning",
              raw,
              payload: { message: record.message },
            });
          }
          break;
        }
        case "auto_retry_end":
          if (record.success === false && typeof record.finalError === "string")
            ctx.failed = record.finalError;
          break;
      }
    }).pipe(Effect.orDie);

  const stopSession = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const ctx = sessions.get(threadId);
      if (!ctx) return;
      ctx.stopped = true;
      ctx.interrupted = true;
      yield* settle(ctx);
      yield* cancelPending(ctx);
      sessions.delete(threadId);
      yield* Scope.close(ctx.scope, Exit.void);
      yield* emit(ctx, {
        type: "session.exited",
        payload: { exitKind: "graceful", recoverable: true },
      });
    });
  const stopAll = () => Effect.forEach([...sessions.keys()], stopSession, { discard: true });
  yield* Effect.addFinalizer(stopAll);

  const adapter: ProviderAdapterShape<ProviderAdapterError> = {
    provider: PROVIDER,
    capabilities: { sessionModelSwitch: "in-session", supportsConversationRollback: false },
    startSession: (input) =>
      Effect.gen(function* () {
        if (input.runtimeMode !== "full-access")
          return yield* new ProviderAdapterValidationError({
            provider: "pi",
            operation: "startSession",
            issue:
              "Pi runs tools with full access. Select Full access; configure approval extensions in Pi when needed.",
          });
        yield* stopSession(input.threadId);
        const scope = yield* Scope.make();
        const createdAt = yield* now;
        const lock = yield* Semaphore.make(1);
        let ctx: Session | undefined;
        const start = Effect.gen(function* () {
          const args = ["--append-system-prompt", buildRuntimeInstructions({ harness: "Pi" })];
          if (
            Predicate.isObject(input.resumeCursor) &&
            typeof input.resumeCursor.sessionFile === "string"
          ) {
            args.push("--session", input.resumeCursor.sessionFile);
          }
          const rpc = yield* makePiRpc({
            binaryPath: settings.binaryPath,
            cwd: input.cwd ?? config.cwd,
            environment: options.environment,
            args,
            onEvent: (record) =>
              ctx
                ? options.nativeEventLogger
                  ? options.nativeEventLogger
                      .write(record, input.threadId)
                      .pipe(Effect.andThen(handle(ctx, record)))
                  : handle(ctx, record)
                : Effect.void,
            onExit: (detail) => {
              const live = ctx;
              return live && !live.stopped
                ? Effect.gen(function* () {
                    live.failed = detail;
                    yield* settle(live);
                    live.session = { ...live.session, status: "error", lastError: detail };
                    yield* emit(live, {
                      type: "session.exited",
                      payload: { exitKind: "error", reason: detail, recoverable: true },
                    });
                  })
                : Effect.void;
            },
          }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
          yield* applyPiModelSelection(rpc, input.modelSelection);
          const state = yield* rpc.request("get_state");
          if (!Predicate.isObject(state))
            return yield* new ProviderAdapterRequestError({
              provider: "pi",
              method: "get_state",
              detail: "Invalid Pi session state",
            });
          const model = Predicate.isObject(state.model) ? state.model : undefined;
          const session: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: options.instanceId,
            threadId: input.threadId,
            status: "ready",
            runtimeMode: input.runtimeMode,
            cwd: input.cwd ?? config.cwd,
            createdAt,
            updatedAt: createdAt,
            ...(model ? { model: `${model.provider}/${model.id}` } : {}),
            ...(typeof state.sessionFile === "string"
              ? { resumeCursor: { sessionFile: state.sessionFile } }
              : {}),
          };
          ctx = {
            session,
            scope,
            rpc,
            lock,
            activeTurn: undefined,
            assistantItem: undefined,
            reasoningItem: undefined,
            pending: new Map(),
            turns: [],
            failed: undefined,
            interrupted: false,
            stopped: false,
            inputTokens: 0,
            outputTokens: 0,
            cost: 0,
            maxTokens:
              model && typeof model.contextWindow === "number" ? model.contextWindow : undefined,
          };
          sessions.set(input.threadId, ctx);
          yield* emit(ctx, { type: "session.started", payload: { resume: session.resumeCursor } });
          yield* emit(ctx, { type: "session.state.changed", payload: { state: "ready" } });
          return session;
        }).pipe(Effect.provideService(Scope.Scope, scope));
        return yield* start.pipe(Effect.onError(() => Scope.close(scope, Exit.void)));
      }),
    sendTurn: (input) =>
      Effect.gen(function* () {
        const ctx = yield* get(input.threadId);
        return yield* ctx.lock.withPermit(
          Effect.gen(function* () {
            if (input.interactionMode === "plan") return yield* unsupported("plan mode");
            const images: PiRecord[] = [];
            let message = input.input ?? "";
            for (const attachment of input.attachments ?? []) {
              const path = resolveAttachmentPath({
                attachmentsDir: config.attachmentsDir,
                attachment,
              });
              if (!path) return yield* unsupported(`attachment type '${attachment.type}'`);
              if (attachment.type === "image") {
                const bytes = yield* fileSystem.readFile(path).pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapterRequestError({
                        provider: "pi",
                        method: "attachment",
                        detail: cause.message,
                        cause,
                      }),
                  ),
                );
                images.push({
                  type: "image",
                  data: Buffer.from(bytes).toString("base64"),
                  mimeType: attachment.mimeType,
                });
              } else message += `\n\nAttached file (${attachment.name}): ${path}`;
            }
            if (!message.trim() && !images.length) return yield* unsupported("empty prompts");
            const steering = ctx.activeTurn !== undefined;
            if (!steering) {
              const selectedModel = yield* applyPiModelSelection(ctx.rpc, input.modelSelection);
              if (
                Predicate.isObject(selectedModel) &&
                typeof selectedModel.contextWindow === "number"
              ) {
                ctx.maxTokens = selectedModel.contextWindow;
              }
              if (input.modelSelection)
                ctx.session = { ...ctx.session, model: input.modelSelection.model };
              ctx.activeTurn = TurnId.make(yield* uuid);
              ctx.failed = undefined;
              ctx.interrupted = false;
              ctx.inputTokens = 0;
              ctx.outputTokens = 0;
              ctx.cost = 0;
              ctx.session = {
                ...ctx.session,
                status: "running",
                activeTurnId: ctx.activeTurn,
                updatedAt: yield* now,
              };
              ctx.turns.push({ id: ctx.activeTurn, items: [] });
              yield* emit(ctx, { type: "turn.started", payload: {} });
              yield* emit(ctx, { type: "session.state.changed", payload: { state: "running" } });
            }
            const turnId = ctx.activeTurn!;
            const response = yield* ctx.rpc
              .request("prompt", {
                message,
                ...(images.length ? { images } : {}),
                ...(steering ? { streamingBehavior: "steer" } : {}),
              })
              .pipe(
                Effect.onError((cause) =>
                  Effect.gen(function* () {
                    if (steering) return;
                    ctx.failed = String(cause);
                    yield* settle(ctx);
                  }),
                ),
              );
            if (!steering && Predicate.isObject(response) && response.disposition === "handled")
              yield* settle(ctx);
            return { threadId: input.threadId, turnId, resumeCursor: ctx.session.resumeCursor };
          }),
        );
      }),
    compaction: {
      type: "native",
      start: (threadId) =>
        Effect.gen(function* () {
          const ctx = yield* get(threadId);
          yield* ctx.lock.withPermit(ctx.rpc.request("compact").pipe(Effect.asVoid));
        }),
    },
    interruptTurn: (threadId) =>
      Effect.gen(function* () {
        const ctx = yield* get(threadId);
        ctx.interrupted = true;
        for (const id of ctx.pending.keys())
          yield* ctx.rpc.notify({ type: "extension_ui_response", id, cancelled: true });
        yield* cancelPending(ctx);
        yield* ctx.rpc.request("abort");
      }),
    respondToRequest: (threadId, requestId, decision) =>
      Effect.gen(function* () {
        const ctx = yield* get(threadId);
        if (ctx.pending.get(requestId) !== "confirm")
          return yield* unsupported("unknown approval request");
        yield* ctx.rpc.notify({
          type: "extension_ui_response",
          id: requestId,
          confirmed: ["accept", "acceptForSession", "acceptAlways"].includes(decision),
          ...(decision === "cancel" ? { cancelled: true } : {}),
        });
        ctx.pending.delete(requestId);
        yield* emit(ctx, {
          type: "request.resolved",
          requestId: RuntimeRequestId.make(requestId),
          payload: { requestType: "permission_approval", decision },
        });
      }),
    respondToUserInput: (threadId, requestId, answers) =>
      Effect.gen(function* () {
        const ctx = yield* get(threadId);
        if (!ctx.pending.has(requestId)) return yield* unsupported("unknown input request");
        const answer = answers[requestId];
        const selected = Predicate.isObject(answer) ? answer.answers : answer;
        const value =
          typeof selected === "string"
            ? selected
            : Array.isArray(selected)
              ? selected[0]
              : undefined;
        yield* ctx.rpc.notify({
          type: "extension_ui_response",
          id: requestId,
          ...(typeof value === "string" ? { value } : { cancelled: true }),
        });
        ctx.pending.delete(requestId);
        yield* emit(ctx, {
          type: "user-input.resolved",
          requestId: RuntimeRequestId.make(requestId),
          payload: { answers },
        });
      }),
    stopSession,
    stopAll,
    listSessions: () => Effect.sync(() => [...sessions.values()].map((ctx) => ctx.session)),
    hasSession: (threadId) =>
      Effect.sync(() => {
        const ctx = sessions.get(threadId);
        return ctx !== undefined && !ctx.stopped && ctx.session.status !== "error";
      }),
    readThread: (threadId) =>
      get(threadId).pipe(Effect.map((ctx) => ({ threadId, turns: ctx.turns }))),
    rollbackThread: () => unsupported("conversation rollback"),
    streamEvents: Stream.fromPubSub(events),
  };
  return adapter;
});
