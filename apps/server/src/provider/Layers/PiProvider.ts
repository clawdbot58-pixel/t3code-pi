/**
 * PiProvider — status snapshot for the pi (pi-coding-agent) driver.
 *
 * Probes `pi --version` for install/health state and discovers the models
 * and slash commands the local pi installation has configured via a
 * short-lived RPC session (`get_available_models` / `get_commands`). Falls
 * back to the user's custom model list when discovery fails.
 *
 * @module provider/Layers/PiProvider
 */
import {
  type PiSettings,
  type ServerProviderModel,
  type ServerProviderSlashCommand,
} from "@t3tools/contracts";
import { createModelCapabilities } from "@t3tools/shared/model";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { ChildProcessSpawner } from "effect/unstable/process";

import {
  PI_DRIVER_KIND,
  PI_BUILTIN_SLASH_COMMANDS,
  PI_MODEL_DISCOVERY_TIMEOUT_MS,
  PI_VERSION_PROBE_TIMEOUT_MS,
  piModelSlug,
  probePiRpcState,
  runPiVersionCommand,
} from "../PiSupport.ts";
import {
  buildServerProvider,
  isCommandMissingCause,
  parseGenericCliVersion,
  providerModelsFromSettings,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";

const PI_PRESENTATION = {
  displayName: "Pi",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: false,
} as const;

const EMPTY_CAPABILITIES = createModelCapabilities({ optionDescriptors: [] });

const PI_BUILT_IN_MODELS: ReadonlyArray<ServerProviderModel> = [];

export function buildInitialPiProviderSnapshot(
  piSettings: PiSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = piModelsFromSettings(piSettings.customModels);

    if (!piSettings.enabled) {
      return buildServerProvider({
        presentation: PI_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Pi is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking pi CLI availability...",
      },
    });
  });
}

function piModelsFromSettings(
  customModels: ReadonlyArray<string> | undefined,
  builtInModels: ReadonlyArray<ServerProviderModel> = PI_BUILT_IN_MODELS,
): ReadonlyArray<ServerProviderModel> {
  return providerModelsFromSettings(builtInModels, customModels ?? [], EMPTY_CAPABILITIES);
}

function piModelsFromDiscoveredModels(
  discoveredModels: ReadonlyArray<Record<string, unknown>>,
): ReadonlyArray<ServerProviderModel> {
  const seen = new Set<string>();
  const models: ServerProviderModel[] = [];
  for (const discovered of discoveredModels) {
    const slug = piModelSlug(discovered);
    if (!slug || seen.has(slug)) {
      continue;
    }
    seen.add(slug);
    const name =
      typeof discovered.name === "string" && discovered.name.trim().length > 0
        ? discovered.name.trim()
        : slug;
    models.push({ slug, name, isCustom: false, capabilities: EMPTY_CAPABILITIES });
  }
  return models;
}

function piCommandsFromDiscoveredCommands(
  discoveredCommands: ReadonlyArray<Record<string, unknown>>,
): ReadonlyArray<ServerProviderSlashCommand> {
  const seen = new Set<string>();
  const commands: ServerProviderSlashCommand[] = [];
  for (const builtin of PI_BUILTIN_SLASH_COMMANDS) {
    commands.push({
      name: builtin.name,
      description: builtin.description,
      ...(builtin.inputHint ? { input: { hint: builtin.inputHint } } : {}),
    });
    seen.add(builtin.name);
  }
  for (const discovered of discoveredCommands) {
    const name = typeof discovered.name === "string" ? discovered.name.trim() : "";
    if (!name || seen.has(name)) {
      continue;
    }
    seen.add(name);
    const description =
      typeof discovered.description === "string" && discovered.description.trim().length > 0
        ? discovered.description.trim()
        : undefined;
    commands.push({ name, ...(description ? { description } : {}) });
  }
  return commands;
}

export const checkPiProviderStatus = Effect.fn("checkPiProviderStatus")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<ServerProviderDraft, never, ChildProcessSpawner.ChildProcessSpawner> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = piModelsFromSettings(piSettings.customModels);

  if (!piSettings.enabled) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Pi is disabled in T3 Code settings.",
      },
    });
  }

  const versionProbe = yield* runPiVersionCommand(piSettings, environment).pipe(
    Effect.timeoutOption(PI_VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  if (Result.isFailure(versionProbe)) {
    const error = versionProbe.failure;
    yield* Effect.logWarning("Pi CLI health check failed.", {
      errorTag: error._tag,
    });
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: !isCommandMissingCause(error),
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: isCommandMissingCause(error)
          ? "Pi CLI (`pi`) is not installed or not on PATH."
          : "Failed to execute pi CLI health check.",
      },
    });
  }

  if (Option.isNone(versionProbe.success)) {
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi CLI is installed but timed out while running `pi --version`.",
      },
    });
  }

  const versionOutput = versionProbe.success.value;
  const version = parseGenericCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  if (versionOutput.code !== 0) {
    yield* Effect.logWarning("Pi CLI version probe exited with a non-zero status.", {
      exitCode: versionOutput.code,
      stdoutLength: versionOutput.stdout.length,
      stderrLength: versionOutput.stderr.length,
    });
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "error",
        auth: { status: "unknown" },
        message: "Pi CLI is installed but failed to run.",
      },
    });
  }

  const discovered = yield* probePiRpcState(piSettings, environment).pipe(
    Effect.timeoutOption(PI_MODEL_DISCOVERY_TIMEOUT_MS),
  );
  if (Option.isNone(discovered)) {
    yield* Effect.logWarning(
      `Pi model discovery failed or timed out after ${PI_MODEL_DISCOVERY_TIMEOUT_MS}ms.`,
    );
    return buildServerProvider({
      presentation: PI_PRESENTATION,
      enabled: piSettings.enabled,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: true,
        version,
        status: "ready",
        auth: { status: "unknown" },
      },
    });
  }
  const discoveredState = discovered.value;
  const models =
    discoveredState.models.length > 0
      ? piModelsFromSettings(
          piSettings.customModels,
          piModelsFromDiscoveredModels(discoveredState.models),
        )
      : fallbackModels;
  const slashCommands = piCommandsFromDiscoveredCommands(discoveredState.commands);

  return buildServerProvider({
    presentation: PI_PRESENTATION,
    enabled: piSettings.enabled,
    checkedAt,
    models,
    slashCommands,
    probe: {
      installed: true,
      version,
      status: "ready",
      auth: { status: "unknown" },
    },
  });
});

export { PI_DRIVER_KIND };
