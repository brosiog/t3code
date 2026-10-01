import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS, ProviderInstanceId, ServerProvider } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as BackgroundPolicy from "../../background/BackgroundPolicy.ts";
import { ServerConfig } from "../../config.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { NoOpProviderEventLoggers, ProviderEventLoggers } from "../Layers/ProviderEventLoggers.ts";
import { deriveProviderInstanceConfigMap } from "../Layers/ProviderInstanceRegistryHydration.ts";
import { HttpClient } from "effect/unstable/http";
import { PiDriver, piMaintenanceResolver } from "./PiDriver.ts";
const decodeSnapshot = Schema.decodeUnknownEffect(ServerProvider);

const layer = ServerConfig.layerTest(process.cwd(), { prefix: "t3-pi-driver-" }).pipe(
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(
    Layer.succeed(
      HttpClient.HttpClient,
      HttpClient.make(() => Effect.die("No network in Pi driver tests")),
    ),
  ),
  Layer.provideMerge(ServerSettingsService.layerTest()),
  Layer.provideMerge(
    Layer.mock(BackgroundPolicy.BackgroundPolicy)({
      shouldRunScopeWork: () => Effect.succeed(false),
    }),
  ),
  Layer.provideMerge(Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers)),
);

it("hydrates Pi as an opt-in built-in provider", () => {
  const map = deriveProviderInstanceConfigMap(DEFAULT_SERVER_SETTINGS);
  expect(map[ProviderInstanceId.make("pi")]).toMatchObject({
    driver: "pi",
    config: { enabled: false, binaryPath: "pi" },
  });
});

it.layer(layer)("Pi driver", (it) => {
  it.effect("discovers authenticated models and commands with instance identity", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const dir = yield* fs.makeTempDirectoryScoped();
      const binaryPath = `${dir}/pi`;
      yield* fs.copyFile(
        `${process.cwd()}/apps/server/src/provider/testUtils/piFixture.mjs`,
        binaryPath,
      );
      yield* fs.chmod(binaryPath, 0o755);
      const instance = yield* PiDriver.create({
        instanceId: ProviderInstanceId.make("pi-work"),
        displayName: "Work Pi",
        enabled: true,
        environment: [],
        config: { ...PiDriver.defaultConfig(), binaryPath },
      });
      const snapshot = yield* instance.snapshot.refresh;
      yield* decodeSnapshot(snapshot);
      expect(snapshot).toMatchObject({
        instanceId: "pi-work",
        driver: "pi",
        displayName: "Work Pi",
        version: "0.87.1",
        installed: true,
        status: "ready",
        auth: { status: "authenticated" },
        supportsConversationRollback: false,
      });
      expect(snapshot.models[0]?.slug).toBe("test/model/with-slash");
      expect(snapshot.slashCommands[0]?.name).toBe("skill:test");
      expect(snapshot.slashCommands).toContainEqual({
        name: "compact",
        description: "Compact the conversation using Pi's native compaction.",
      });
    }).pipe(Effect.scoped),
  );

  it.effect("never spawns Pi when disabled", () =>
    Effect.gen(function* () {
      const instance = yield* PiDriver.create({
        instanceId: ProviderInstanceId.make("pi-disabled"),
        displayName: undefined,
        enabled: false,
        environment: [],
        config: PiDriver.defaultConfig(),
      });
      expect((yield* instance.snapshot.refresh).status).toBe("disabled");
    }).pipe(
      Effect.provideService(
        ChildProcessSpawner.ChildProcessSpawner,
        ChildProcessSpawner.make(() => Effect.die("Disabled Pi must not spawn")),
      ),
      Effect.scoped,
    ),
  );
});

it.layer(NodeServices.layer)("Pi maintenance", (it) => {
  it.effect("updates only the npm prefix that owns Pi", () =>
    Effect.gen(function* () {
      const capabilities = yield* piMaintenanceResolver.resolve({
        binaryPath: "/t3/pi/bin/pi",
        resolvedCommandPath: "/t3/pi/bin/pi",
        realCommandPath: "/t3/pi/lib/node_modules/@earendil-works/pi-coding-agent/dist/cli.js",
        env: {},
        platform: "darwin",
      });
      expect(capabilities.update?.args).toContain("/t3/pi");
      expect(capabilities.update?.args).toContain("@earendil-works/pi-coding-agent@latest");
      const wrapper = yield* piMaintenanceResolver.resolve({
        binaryPath: "/t3/pi-wrapper",
        resolvedCommandPath: "/t3/pi-wrapper",
        realCommandPath: "/t3/pi-wrapper",
        env: {},
        platform: "darwin",
      });
      expect(wrapper.update).toBeNull();
    }),
  );
});
