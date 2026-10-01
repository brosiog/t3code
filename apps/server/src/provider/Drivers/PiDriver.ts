import {
  PiSettings,
  ProviderDriverKind,
  type ServerProvider,
  type ServerProviderModel,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import { compareSemverVersions } from "@t3tools/shared/semver";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { makePiTextGeneration } from "../../textGeneration/PiTextGeneration.ts";
import { ProviderDriverError } from "../Errors.ts";
import { ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { makePiAdapter } from "../Layers/PiAdapter.ts";
import { makeManagedServerProvider } from "../makeManagedServerProvider.ts";
import { makePiRpc } from "../piRpc.ts";
import { defaultProviderContinuationIdentity, type ProviderDriver } from "../ProviderDriver.ts";
import { mergeProviderInstanceEnvironment } from "../ProviderInstanceEnvironment.ts";
import {
  enrichProviderSnapshotWithVersionAdvisory,
  makeCachedProviderMaintenanceResolution,
  makePackageManagedProviderMaintenanceResolver,
  resolveProviderMaintenanceCapabilitiesEffect,
} from "../providerMaintenance.ts";
import {
  parseGenericCliVersion,
  providerModelsFromSettings,
  spawnAndCollect,
} from "../providerSnapshot.ts";
import {
  haveProviderSnapshotSettingsChanged,
  makeProviderSnapshotSettingsSource,
} from "../providerUpdateSettings.ts";

const DRIVER = ProviderDriverKind.make("pi");
export const piMaintenanceResolver = makePackageManagedProviderMaintenanceResolver({
  provider: DRIVER,
  npmPackageName: "@earendil-works/pi-coding-agent",
  nativeUpdate: null,
});
const decodeSettings = Schema.decodeSync(PiSettings);
const emptyCapabilities = createModelCapabilities({ optionDescriptors: [] });
const PiModels = Schema.Struct({
  models: Schema.Array(
    Schema.Struct({
      provider: Schema.String,
      id: Schema.String,
      name: Schema.String,
      reasoning: Schema.Boolean,
      contextWindow: Schema.Number,
    }),
  ),
});
const decodeModels = Schema.decodeUnknownEffect(PiModels);

export function piModelsToServerModels(
  models: (typeof PiModels.Type)["models"],
): ServerProviderModel[] {
  return models.map((model) => ({
    slug: `${model.provider}/${model.id}`,
    name: model.name,
    subProvider: model.provider,
    isCustom: false,
    capabilities: model.reasoning
      ? createModelCapabilities({
          optionDescriptors: [
            {
              id: "reasoningEffort",
              label: "Reasoning",
              type: "select",
              options: ["off", "minimal", "low", "medium", "high"].map((id) => ({ id, label: id })),
            },
          ],
        })
      : emptyCapabilities,
  }));
}

export type PiDriverEnv =
  | HttpClient.HttpClient
  | Crypto.Crypto
  | FileSystem.FileSystem
  | Path.Path
  | ChildProcessSpawner.ChildProcessSpawner
  | ServerConfig
  | ServerSettingsService
  | ProviderEventLoggers
  | BackgroundPolicy.BackgroundPolicy;

export const PiDriver: ProviderDriver<PiSettings, PiDriverEnv> = {
  driverKind: DRIVER,
  metadata: { displayName: "Pi", supportsMultipleInstances: true },
  configSchema: PiSettings,
  defaultConfig: () => decodeSettings({}),
  create: (input) =>
    Effect.gen(function* () {
      const httpClient = yield* HttpClient.HttpClient;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const serverConfig = yield* ServerConfig;
      const serverSettings = yield* ServerSettingsService;
      const eventLoggers = yield* ProviderEventLoggers;
      const environment = mergeProviderInstanceEnvironment(input.environment);
      const settings = { ...input.config, enabled: input.enabled };
      const continuationIdentity = defaultProviderContinuationIdentity({
        driverKind: DRIVER,
        instanceId: input.instanceId,
      });
      const adapter = yield* makePiAdapter(settings, {
        instanceId: input.instanceId,
        environment,
        ...(eventLoggers.native ? { nativeEventLogger: eventLoggers.native } : {}),
      });
      const textGeneration = yield* makePiTextGeneration(settings, environment);
      const resolveMaintenance = yield* makeCachedProviderMaintenanceResolution(
        resolveProviderMaintenanceCapabilitiesEffect(piMaintenanceResolver, {
          binaryPath: settings.binaryPath,
          env: environment,
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
        ),
      );
      const base = (checkedAt: string): ServerProvider => ({
        instanceId: input.instanceId,
        driver: DRIVER,
        displayName: input.displayName ?? "Pi",
        ...(input.accentColor ? { accentColor: input.accentColor } : {}),
        continuation: { groupKey: continuationIdentity.continuationKey },
        enabled: input.enabled,
        installed: false,
        version: null,
        status: input.enabled ? "warning" : "disabled",
        auth: { status: "unknown" },
        checkedAt,
        supportsConversationRollback: false,
        showInteractionModeToggle: false,
        supportsTextGeneration: true,
        reportsContextWindow: true,
        message: input.enabled ? "Checking Pi CLI..." : "Pi is disabled in T3 Code settings.",
        models: providerModelsFromSettings([], settings.customModels, emptyCapabilities),
        slashCommands: [],
        skills: [],
        setup: { canInstall: false, canAuthenticate: false },
      });
      const probe = (cwd: string) =>
        Effect.gen(function* () {
          const snapshot = base(yield* DateTime.now.pipe(Effect.map(DateTime.formatIso)));
          if (!input.enabled) return snapshot;
          const result = yield* spawnAndCollect(
            settings.binaryPath,
            ChildProcess.make(settings.binaryPath, ["--version"], {
              cwd,
              env: environment,
              extendEnv: false,
            }),
          );
          if (result.code !== 0)
            return {
              ...snapshot,
              status: "error" as const,
              message: result.stderr || "Pi version probe failed",
            };
          const version = parseGenericCliVersion(result.stdout);
          if (!version || compareSemverVersions(version, "0.87.1") < 0)
            return {
              ...snapshot,
              installed: true,
              version,
              status: "error" as const,
              message:
                "Pi 0.87.1 or newer is required for reliable RPC run completion. Update Pi and refresh this provider.",
            };
          const rpc = yield* makePiRpc({
            binaryPath: settings.binaryPath,
            cwd,
            environment,
            args: ["--no-session"],
            onEvent: () => Effect.void,
            onExit: () => Effect.void,
          });
          const inventory = yield* rpc
            .request("get_available_models")
            .pipe(Effect.flatMap(decodeModels));
          const models = piModelsToServerModels(inventory.models);
          const commands = yield* rpc.request("get_commands");
          const entries =
            Predicate.isObject(commands) && Array.isArray(commands.commands)
              ? commands.commands
              : [];
          return {
            ...snapshot,
            installed: true,
            version,
            status: models.length ? ("ready" as const) : ("warning" as const),
            auth: {
              status: models.length ? ("authenticated" as const) : ("unauthenticated" as const),
            },
            message: models.length
              ? "Pi is ready. Tools run with full access."
              : "Run pi and /login, or configure provider API keys.",
            models: providerModelsFromSettings(models, settings.customModels, emptyCapabilities),
            skills: entries.flatMap((entry) =>
              Predicate.isObject(entry) &&
              entry.source === "skill" &&
              typeof entry.name === "string" &&
              Predicate.isObject(entry.sourceInfo) &&
              typeof entry.sourceInfo.path === "string"
                ? [
                    {
                      name: entry.name.replace(/^skill:/, ""),
                      path: entry.sourceInfo.path,
                      enabled: true,
                      ...(typeof entry.description === "string"
                        ? { description: entry.description }
                        : {}),
                    },
                  ]
                : [],
            ),
            slashCommands: [
              ...entries.flatMap((entry) =>
                Predicate.isObject(entry) && typeof entry.name === "string"
                  ? [
                      {
                        name: entry.name,
                        ...(typeof entry.description === "string"
                          ? { description: entry.description }
                          : {}),
                      },
                    ]
                  : [],
              ),
              {
                name: "compact",
                description: "Compact the conversation using Pi's native compaction.",
              },
            ],
          };
        }).pipe(
          Effect.scoped,
          Effect.timeout("20 seconds"),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.catch((cause) =>
            DateTime.now.pipe(
              Effect.map((date) => ({
                ...base(DateTime.formatIso(date)),
                status: "error" as const,
                message: cause.message,
              })),
            ),
          ),
        );
      const source = makeProviderSnapshotSettingsSource(settings, serverSettings);
      const snapshot = yield* makeManagedServerProvider({
        resolveMaintenance,
        getSettings: source.getSettings,
        streamSettings: source.streamSettings,
        haveSettingsChanged: haveProviderSnapshotSettingsChanged,
        initialSnapshot: () =>
          DateTime.now.pipe(Effect.map((date) => base(DateTime.formatIso(date)))),
        checkProvider: probe(serverConfig.cwd),
        enrichSnapshot: ({ settings, snapshot, publishSnapshot }) =>
          resolveMaintenance().pipe(
            Effect.flatMap((maintenance) =>
              enrichProviderSnapshotWithVersionAdvisory(snapshot, maintenance, {
                enableProviderUpdateChecks: settings.enableProviderUpdateChecks,
              }),
            ),
            Effect.provideService(HttpClient.HttpClient, httpClient),
            Effect.flatMap(publishSnapshot),
          ),
        refreshOnInterval: false,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderDriverError({
              driver: DRIVER,
              instanceId: input.instanceId,
              detail: cause.message,
              cause,
            }),
        ),
      );
      return {
        instanceId: input.instanceId,
        driverKind: DRIVER,
        continuationIdentity,
        displayName: input.displayName,
        accentColor: input.accentColor,
        enabled: input.enabled,
        snapshot,
        snapshotForCwd: (cwd) => probe(cwd),
        adapter,
        textGeneration,
      };
    }),
};
