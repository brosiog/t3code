import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { PiSettings, ProviderInstanceId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import { makePiTextGeneration } from "./PiTextGeneration.ts";

const decodeSettings = Schema.decodeSync(PiSettings);
const modelSelection = {
  instanceId: ProviderInstanceId.make("pi"),
  model: "test/model/with-slash",
};
const setup = Effect.fn("PiTextGenerationTest.setup")(function* (output: string) {
  const fs = yield* FileSystem.FileSystem;
  const dir = yield* fs.makeTempDirectoryScoped();
  const binaryPath = `${dir}/pi`;
  yield* fs.copyFile(
    `${process.cwd()}/apps/server/src/provider/testUtils/piFixture.mjs`,
    binaryPath,
  );
  yield* fs.chmod(binaryPath, 0o755);
  return yield* makePiTextGeneration(decodeSettings({ binaryPath }), {
    ...process.env,
    PI_FIXTURE_OUTPUT: output,
  });
});

it.layer(NodeServices.layer)("Pi text generation", (it) => {
  it.effect("parses fenced structured output for commit messages", () =>
    Effect.gen(function* () {
      const generation = yield* setup(
        '```json\n{"subject":"feat: pi integration","body":"Connect Pi"}\n```',
      );
      const result = yield* generation.generateCommitMessage({
        cwd: process.cwd(),
        branch: "pi",
        stagedSummary: "Pi",
        stagedPatch: "diff",
        modelSelection,
      });
      expect(result).toEqual({ subject: "feat: pi integration", body: "Connect Pi" });
    }).pipe(Effect.scoped),
  );

  it.effect("rejects malformed structured responses", () =>
    Effect.gen(function* () {
      const generation = yield* setup('{"wrong":"shape"}');
      const result = yield* generation
        .generateThreadTitle({ cwd: process.cwd(), message: "Hello", modelSelection })
        .pipe(Effect.result);
      expect(result._tag).toBe("Failure");
    }).pipe(Effect.scoped),
  );
});
