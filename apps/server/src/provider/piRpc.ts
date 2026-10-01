import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { ProviderAdapterRequestError } from "./Errors.ts";

export const PiRecord = Schema.Record(Schema.String, Schema.Unknown);
export type PiRecord = typeof PiRecord.Type;
const decodeRecord = Schema.decodeUnknownEffect(Schema.fromJsonString(PiRecord));
const encodeRecord = Schema.encodeEffect(Schema.fromJsonString(PiRecord));
const isRequestError = Schema.is(ProviderAdapterRequestError);

/** Pi's framing is LF-only: Unicode separators are valid inside JSON strings. */
export function makePiRecordSplitter() {
  let pending = "";
  return (chunk: string): string[] => {
    pending += chunk;
    const lines = pending.split("\n");
    pending = lines.pop() ?? "";
    return lines.map((line) => line.replace(/\r$/, "")).filter((line) => line.trim());
  };
}

export const makePiRpc = Effect.fn("makePiRpc")(function* (input: {
  binaryPath: string;
  cwd: string;
  environment: NodeJS.ProcessEnv;
  args?: readonly string[];
  onEvent: (record: PiRecord) => Effect.Effect<void>;
  onExit: (detail: string) => Effect.Effect<void>;
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const outgoing = yield* Queue.unbounded<Uint8Array>();
  const pending = new Map<string, Deferred.Deferred<unknown, ProviderAdapterRequestError>>();
  let nextId = 0;
  let closed = false;
  let stderr = "";
  const failure = (method: string, detail: string) =>
    new ProviderAdapterRequestError({ provider: "pi", method, detail });
  const child = yield* spawner
    .spawn(
      ChildProcess.make(input.binaryPath, ["--mode", "rpc", ...(input.args ?? [])], {
        cwd: input.cwd,
        env: input.environment,
        extendEnv: false,
        stdin: "pipe",
        forceKillAfter: "2 seconds",
      }),
    )
    .pipe(Effect.mapError((cause) => failure("spawn", cause.message)));

  const terminate = Effect.fn("PiRpc.terminate")(function* (detail: string) {
    if (closed) return;
    closed = true;
    yield* Effect.forEach(
      pending.values(),
      (response) => Deferred.fail(response, failure("transport", detail)),
      { discard: true },
    );
    pending.clear();
    yield* input.onExit(detail);
  });
  yield* Effect.addFinalizer(() => terminate("Pi session closed"));
  yield* Stream.fromQueue(outgoing).pipe(
    Stream.run(child.stdin),
    Effect.catch((cause) => terminate(cause.message)),
    Effect.forkScoped,
  );
  yield* child.stderr.pipe(
    Stream.decodeText(),
    Stream.runForEach((chunk) =>
      Effect.sync(() => {
        stderr = (stderr + chunk).slice(-4096);
      }),
    ),
    Effect.ignore,
    Effect.forkScoped,
  );
  const split = makePiRecordSplitter();
  yield* child.stdout.pipe(
    Stream.decodeText(),
    Stream.mapEffect((chunk) =>
      Effect.try({ try: () => split(chunk), catch: (cause) => failure("parse", String(cause)) }),
    ),
    Stream.flatMap(Stream.fromIterable),
    Stream.runForEach((line) =>
      decodeRecord(line).pipe(
        Effect.mapError((cause) => failure("parse", cause.message)),
        Effect.flatMap((record) =>
          Effect.gen(function* () {
            if (record.type !== "response") {
              yield* input.onEvent(record);
              return;
            }
            const response = typeof record.id === "string" ? pending.get(record.id) : undefined;
            if (!response) return;
            if (record.success === true) yield* Deferred.succeed(response, record.data);
            else
              yield* Deferred.fail(response, failure(String(record.command), String(record.error)));
          }),
        ),
      ),
    ),
    Effect.catch((cause) =>
      terminate(cause.message).pipe(Effect.andThen(child.kill()), Effect.ignore),
    ),
    Effect.andThen(terminate("Pi RPC output closed")),
    Effect.forkScoped,
  );
  yield* child.exitCode.pipe(
    Effect.flatMap((code) => terminate(`Pi exited (${code})${stderr ? `: ${stderr.trim()}` : ""}`)),
    Effect.catch((cause) => terminate(cause.message)),
    Effect.forkScoped,
  );

  const notify = (record: PiRecord) =>
    Effect.gen(function* () {
      if (closed) return yield* failure(String(record.type), "Pi process is closed");
      const encoded = yield* encodeRecord(record).pipe(
        Effect.mapError((cause) => failure("encode", cause.message)),
      );
      yield* Queue.offer(outgoing, new TextEncoder().encode(`${encoded}\n`));
    });
  const request = Effect.fn("PiRpc.request")(function* (type: string, fields: PiRecord = {}) {
    const id = `t3-${++nextId}`;
    const response = yield* Deferred.make<unknown, ProviderAdapterRequestError>();
    pending.set(id, response);
    return yield* notify({ ...fields, id, type }).pipe(
      Effect.andThen(Deferred.await(response)),
      Effect.timeout(
        type === "compact" ? "5 minutes" : type === "prompt" ? "1 minute" : "15 seconds",
      ),
      Effect.mapError((cause) =>
        isRequestError(cause) ? cause : failure(type, "Pi RPC request timed out"),
      ),
      Effect.ensuring(
        Effect.sync(() => {
          pending.delete(id);
        }),
      ),
    );
  });
  return { request, notify };
});
export type PiRpc = Effect.Success<ReturnType<typeof makePiRpc>>;
