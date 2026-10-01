import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  ApprovalRequestId,
  PiSettings,
  ProviderInstanceId,
  ProviderRuntimeEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../../config.ts";
import { makePiAdapter } from "./PiAdapter.ts";
import { makePiRecordSplitter } from "../piRpc.ts";

const testLayer = ServerConfig.layerTest(process.cwd(), { prefix: "t3-pi-adapter-" }).pipe(
  Layer.provideMerge(NodeServices.layer),
);
const threadId = ThreadId.make("pi-thread");
const decodeSettings = Schema.decodeSync(PiSettings);
const decodeEvent = Schema.decodeUnknownEffect(ProviderRuntimeEvent);
const modelSelection = {
  instanceId: ProviderInstanceId.make("pi"),
  model: "test/model/with-slash",
  options: [{ id: "reasoningEffort", value: "high" }],
};

const setup = Effect.fn("PiAdapterTest.setup")(function* (scenario = "normal", instanceId = "pi") {
  const fs = yield* FileSystem.FileSystem;
  const dir = yield* fs.makeTempDirectoryScoped();
  const fixture = `${dir}/pi`;
  yield* fs.copyFile(`${process.cwd()}/apps/server/src/provider/testUtils/piFixture.mjs`, fixture);
  yield* fs.chmod(fixture, 0o755);
  const log = `${dir}/commands.jsonl`;
  const adapter = yield* makePiAdapter(decodeSettings({ binaryPath: fixture }), {
    instanceId: ProviderInstanceId.make(instanceId),
    environment: { ...process.env, PI_FIXTURE_SCENARIO: scenario, PI_FIXTURE_LOG: log },
  });
  const queue = yield* Queue.unbounded<ProviderRuntimeEvent>();
  const subscription = yield* Stream.toPull(adapter.streamEvents);
  yield* Effect.gen(function* () {
    while (true) {
      const batch = yield* subscription;
      for (const event of batch) {
        yield* decodeEvent(event);
        yield* Queue.offer(queue, event);
      }
    }
  }).pipe(Effect.orDie, Effect.forkScoped);
  const seen: ProviderRuntimeEvent[] = [];
  const until = (type: ProviderRuntimeEvent["type"]) =>
    Effect.gen(function* () {
      while (true) {
        const event = yield* Queue.take(queue);
        seen.push(event);
        if (event.type === type) return event;
      }
    });
  return { adapter, log, until, seen };
});

it("splits LF records across chunks while preserving Unicode separators and CRLF", () => {
  const split = makePiRecordSplitter();
  expect(split('{"delta":"hello\u2028')).toEqual([]);
  expect(split('world\u2029🌎"}\r\n{"type":"agent_')).toEqual([
    '{"delta":"hello\u2028world\u2029🌎"}',
  ]);
  expect(split('settled"}\n')).toEqual(['{"type":"agent_settled"}']);
});

it.layer(testLayer)("Pi adapter", (it) => {
  it.effect("does not replay completion when a finished session is immediately resumed", () =>
    Effect.gen(function* () {
      const { adapter, until } = yield* setup();
      const session = yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      const first = yield* adapter.sendTurn({ threadId, input: "First" });
      expect((yield* until("turn.completed")).turnId).toBe(first.turnId);
      yield* adapter.stopSession(threadId);
      yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        resumeCursor: session.resumeCursor,
      });
      const second = yield* adapter.sendTurn({ threadId, input: "Second" });
      expect((yield* until("turn.completed")).turnId).toBe(second.turnId);
    }).pipe(Effect.scoped),
  );
  it.effect("cancels an active run without closing its session", () =>
    Effect.gen(function* () {
      const { adapter, until } = yield* setup("retry");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* adapter.sendTurn({ threadId, input: "Wait" });
      yield* until("runtime.warning");
      yield* adapter.interruptTurn(threadId);
      expect((yield* until("turn.completed")).payload).toMatchObject({ state: "interrupted" });
      expect(yield* adapter.hasSession(threadId)).toBe(true);
    }).pipe(Effect.scoped),
  );

  it.effect("isolates instances and their process lifetimes", () =>
    Effect.gen(function* () {
      const first = yield* setup("normal", "pi-first");
      const second = yield* setup("normal", "pi-second");
      yield* first.adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* second.adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* first.adapter.stopAll();
      expect(yield* first.adapter.hasSession(threadId)).toBe(false);
      yield* second.adapter.sendTurn({ threadId, input: "Still running" });
      const completed = yield* second.until("turn.completed");
      expect(completed.providerInstanceId).toBe("pi-second");
    }).pipe(Effect.scoped),
  );
  it.effect("streams text, reasoning and tools with authoritative completion and usage", () =>
    Effect.gen(function* () {
      const { adapter, until, seen } = yield* setup();
      const session = yield* adapter.startSession({
        threadId,
        runtimeMode: "full-access",
        modelSelection,
      });
      expect(session.resumeCursor).toEqual({ sessionFile: "/tmp/pi-fixture-session.jsonl" });
      yield* adapter.sendTurn({ threadId, input: "Hello", modelSelection });
      const completed = yield* until("turn.completed");
      expect(completed.payload).toMatchObject({
        state: "completed",
        totalCostUsd: 0.01,
        tokenUsage: { inputTokens: 35, outputTokens: 4 },
      });
      expect(
        seen.filter((event) => event.type === "content.delta").map((event) => event.payload),
      ).toEqual([
        { streamKind: "reasoning_text", delta: "Thinking", contentIndex: 0 },
        { streamKind: "assistant_text", delta: "Hello \u2028 world \u2029 🌎", contentIndex: 1 },
      ]);
      expect(
        seen.find((event) => event.type === "item.completed" && event.itemId === "tool-1")?.payload,
      ).toMatchObject({ status: "completed" });
      expect((yield* adapter.readThread(threadId)).turns[0]?.items).toHaveLength(1);
    }).pipe(Effect.scoped),
  );

  it.effect("sends image bytes and MIME types and translates native compaction", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const config = yield* ServerConfig;
      const id = "pi-image";
      const bytes = new Uint8Array([137, 80, 78, 71]);
      yield* fs.writeFile(`${config.attachmentsDir}/${id}.png`, bytes);
      const { adapter, until, log } = yield* setup();
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* adapter.sendTurn({
        threadId,
        input: "Read image",
        attachments: [
          { type: "image", id, name: "image.png", mimeType: "image/png", sizeBytes: bytes.length },
        ],
      });
      yield* until("turn.completed");
      const commands = (yield* fs.readFileString(log))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line));
      expect(commands.find((record) => record.type === "prompt").images).toEqual([
        { type: "image", data: Buffer.from(bytes).toString("base64"), mimeType: "image/png" },
      ]);
      if (adapter.compaction?.type !== "native") throw new Error("Expected native compaction");
      yield* adapter.compaction.start(threadId);
      expect((yield* until("thread.state.changed")).payload).toEqual({
        state: "compacted",
        beforeTokens: 1000,
        afterTokens: 100,
      });
    }).pipe(Effect.scoped),
  );

  it.effect("keeps a retrying run active until agent_settled and steers the same T3 turn", () =>
    Effect.gen(function* () {
      const { adapter, until, seen } = yield* setup("retry");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      const first = yield* adapter.sendTurn({ threadId, input: "Retry" });
      yield* until("runtime.warning");
      expect(seen.some((event) => event.type === "turn.completed")).toBe(false);
      const second = yield* adapter.sendTurn({ threadId, input: "Steer" });
      expect(second.turnId).toBe(first.turnId);
      yield* until("turn.completed");
      expect(seen.filter((event) => event.type === "turn.completed")).toHaveLength(1);
    }).pipe(Effect.scoped),
  );

  it.effect("resolves Pi extension approvals and selection dialogs", () =>
    Effect.gen(function* () {
      const { adapter, until, log } = yield* setup("dialogs");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* adapter.sendTurn({ threadId, input: "Ask" });
      yield* until("request.opened");
      yield* adapter.respondToRequest(threadId, ApprovalRequestId.make("confirm-1"), "accept");
      yield* until("user-input.requested");
      yield* adapter.respondToUserInput(threadId, ApprovalRequestId.make("select-1"), {
        "select-1": ["B"],
      });
      yield* until("turn.completed");
      const fs = yield* FileSystem.FileSystem;
      expect(yield* fs.readFileString(log)).toContain('"value":"B"');
    }).pipe(Effect.scoped),
  );

  it.effect("rejects unsupported approval modes and reports failed prompts", () =>
    Effect.gen(function* () {
      const { adapter, until } = yield* setup("reject");
      const denied = yield* adapter
        .startSession({ threadId, runtimeMode: "approval-required" })
        .pipe(Effect.result);
      expect(denied._tag).toBe("Failure");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      const rejected = yield* adapter.sendTurn({ threadId, input: "Reject" }).pipe(Effect.result);
      expect(rejected._tag).toBe("Failure");
      expect((yield* until("turn.completed")).payload).toMatchObject({ state: "failed" });
    }).pipe(Effect.scoped),
  );

  it.effect("settles a run when the process exits unexpectedly", () =>
    Effect.gen(function* () {
      const { adapter, until } = yield* setup("crash");
      yield* adapter.startSession({ threadId, runtimeMode: "full-access" });
      yield* adapter.sendTurn({ threadId, input: "Crash" }).pipe(Effect.ignore);
      expect((yield* until("turn.completed")).payload).toMatchObject({ state: "failed" });
      expect((yield* until("session.exited")).payload).toMatchObject({ exitKind: "error" });
    }).pipe(Effect.scoped),
  );
});
