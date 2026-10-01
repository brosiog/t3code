import { type PiSettings, type ModelSelection, TextGenerationError } from "@t3tools/contracts";
import { extractJsonObject } from "@t3tools/shared/schemaJson";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import { ChildProcessSpawner } from "effect/unstable/process";
import { makePiRpc } from "../provider/piRpc.ts";
import { applyPiModelSelection } from "../provider/Layers/PiAdapter.ts";
import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";

export const makePiTextGeneration = Effect.fn("makePiTextGeneration")(function* (
  settings: PiSettings,
  environment: NodeJS.ProcessEnv,
) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const runPiJson = <S extends Schema.Top>(input: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const done = yield* Deferred.make<void, TextGenerationError>();
      let output = "";
      let error: string | undefined;
      const rpc = yield* makePiRpc({
        binaryPath: settings.binaryPath,
        cwd: input.cwd,
        environment,
        args: [
          "--no-session",
          "--no-tools",
          "--no-extensions",
          "--no-skills",
          "--no-prompt-templates",
        ],
        onEvent: (record) =>
          Effect.gen(function* () {
            if (
              record.type === "message_end" &&
              Predicate.isObject(record.message) &&
              record.message.role === "assistant"
            ) {
              const message = record.message;
              error = undefined;
              if (message.stopReason === "error" || message.stopReason === "aborted")
                error = String(message.errorMessage ?? message.stopReason);
              if (Array.isArray(message.content))
                output = message.content
                  .flatMap((block) =>
                    Predicate.isObject(block) &&
                    block.type === "text" &&
                    typeof block.text === "string"
                      ? [block.text]
                      : [],
                  )
                  .join("\n");
            }
            if (record.type === "agent_settled") {
              if (error)
                yield* Deferred.fail(
                  done,
                  new TextGenerationError({ operation: input.operation, detail: error }),
                );
              else yield* Deferred.succeed(done, undefined);
            }
          }),
        onExit: (detail) =>
          Deferred.fail(done, new TextGenerationError({ operation: input.operation, detail })).pipe(
            Effect.asVoid,
          ),
      }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
      yield* applyPiModelSelection(rpc, input.modelSelection);
      yield* rpc.request("prompt", { message: input.prompt });
      yield* Deferred.await(done);
      const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(input.outputSchemaJson));
      return yield* decodeOutput(extractJsonObject(output));
    }).pipe(
      Effect.scoped,
      Effect.timeout("3 minutes"),
      Effect.mapError(
        (cause) =>
          new TextGenerationError({ operation: input.operation, detail: cause.message, cause }),
      ),
    );

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("PiTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const generated = yield* runPiJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("PiTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const generated = yield* runPiJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("PiTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const generated = yield* runPiJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("PiTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        linkedContext: input.linkedContext,
        attachments: input.attachments,
      });

      const generated = yield* runPiJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });

      return {
        title: sanitizeThreadTitle(generated.title),
        ...(generated.needsRefinement ? { needsRefinement: true } : {}),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
