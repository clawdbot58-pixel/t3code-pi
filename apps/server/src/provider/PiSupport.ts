/**
 * PiSupport — shared helpers for the pi (pi-coding-agent) provider.
 *
 * pi is driven over its JSONL RPC mode (`pi --mode rpc`): commands are sent
 * as JSON lines on stdin, and responses plus agent events stream back on
 * stdout as JSON lines. This module hosts the pieces shared between the
 * status probe (PiProvider), the session adapter (PiAdapter), and text
 * generation (PiTextGeneration): the driver kind, model pattern parsing,
 * tool-name → canonical item mapping, and the version/model probes.
 *
 * @module provider/PiSupport
 */
import {
  ProviderDriverKind,
  type PiSettings,
  type ToolLifecycleItemType,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import type * as PlatformError from "effect/PlatformError";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import { spawnAndCollect } from "./providerSnapshot.ts";

export const PI_DRIVER_KIND = ProviderDriverKind.make("pi");
export const PI_RESUME_VERSION = 1 as const;
export const PI_VERSION_PROBE_TIMEOUT_MS = 4_000;
export const PI_MODEL_DISCOVERY_TIMEOUT_MS = 20_000;
export const PI_REQUEST_TIMEOUT_MS = 30_000;

export interface PiSlashCommandDefinition {
  readonly name: string;
  readonly description: string;
  readonly inputHint?: string;
}

/**
 * pi built-in slash commands with a dedicated RPC equivalent. These are
 * surfaced as composer suggestions and executed by the adapter (not sent to
 * the model). TUI-only commands (settings, quit, hotkeys, ...) are excluded.
 */
export const PI_BUILTIN_SLASH_COMMANDS: ReadonlyArray<PiSlashCommandDefinition> = [
  {
    name: "reload",
    description: "Reload keybindings, extensions, skills, prompts, themes, and context files",
  },
  { name: "compact", description: "Manually compact the session context" },
  { name: "new", description: "Start a new session" },
  { name: "clone", description: "Duplicate the current session at the current position" },
  { name: "session", description: "Show session info and stats" },
  { name: "name", description: "Set session display name", inputHint: "<name>" },
  { name: "export", description: "Export session as HTML", inputHint: "<output-path>" },
];

export interface PiSlashCommandInvocation {
  readonly name: string;
  readonly args: string;
}

/** Parse a `/command args` prompt into its invocation, if it is one. */
export function parsePiSlashCommandInvocation(
  text: string | null | undefined,
): PiSlashCommandInvocation | undefined {
  const trimmed = text?.trim();
  if (!trimmed || !trimmed.startsWith("/")) {
    return undefined;
  }
  const spaceIndex = trimmed.indexOf(" ");
  const name = (spaceIndex === -1 ? trimmed : trimmed.slice(0, spaceIndex)).slice(1).trim();
  if (!name) {
    return undefined;
  }
  const args = spaceIndex === -1 ? "" : trimmed.slice(spaceIndex + 1).trim();
  return { name, args };
}

const PiJsonRecord = Schema.Record(Schema.String, Schema.Unknown);
const encodePiJsonLine = Schema.encodeSync(Schema.fromJsonString(PiJsonRecord));
const decodePiJsonLine = Schema.decodeOption(Schema.fromJsonString(PiJsonRecord));

export interface PiModelPattern {
  /** Provider segment of a `provider/id` pattern, when present. */
  readonly provider: string | undefined;
  /** Model id (pattern or exact). */
  readonly modelId: string;
  /** Optional `:thinking` suffix. */
  readonly thinking: string | undefined;
}

/**
 * Parse a pi model pattern of the form `provider/id[:thinking]`. A plain id
 * without a slash yields an undefined provider (pi resolves it against the
 * configured default provider).
 */
export function parsePiModelPattern(
  pattern: string | null | undefined,
): PiModelPattern | undefined {
  const trimmed = pattern?.trim();
  if (!trimmed) {
    return undefined;
  }
  const colonIndex = trimmed.lastIndexOf(":");
  const thinking =
    colonIndex > 0 && colonIndex > trimmed.lastIndexOf("/")
      ? trimmed.slice(colonIndex + 1).trim()
      : undefined;
  const body = thinking === undefined ? trimmed : trimmed.slice(0, colonIndex);
  const slashIndex = body.lastIndexOf("/");
  if (slashIndex <= 0 || slashIndex === body.length - 1) {
    return { provider: undefined, modelId: body, thinking: thinking || undefined };
  }
  return {
    provider: body.slice(0, slashIndex).trim() || undefined,
    modelId: body.slice(slashIndex + 1).trim(),
    thinking: thinking || undefined,
  };
}

/** Canonical slug for a pi model as exposed in the provider snapshot. */
export function piModelSlug(
  model: { provider?: unknown; id?: unknown } | null | undefined,
): string | undefined {
  if (!model) {
    return undefined;
  }
  const provider = typeof model.provider === "string" ? model.provider.trim() : "";
  const id = typeof model.id === "string" ? model.id.trim() : "";
  if (!id) {
    return undefined;
  }
  return provider ? `${provider}/${id}` : id;
}

/** Map a pi tool name to a canonical runtime item type. */
export function piToolItemType(toolName: string | null | undefined): ToolLifecycleItemType {
  switch (toolName) {
    case "bash":
      return "command_execution";
    case "edit":
    case "write":
      return "file_change";
    case "web_search":
    case "web_fetch":
      return "web_search";
    default:
      return "dynamic_tool_call";
  }
}

/** Best-effort human title for a pi tool invocation. */
export function piToolTitle(toolName: string | null | undefined, args: unknown): string {
  if (typeof args !== "object" || args === null) {
    return toolName ?? "tool";
  }
  const record = args as Record<string, unknown>;
  const first = [record.command, record.path, record.file_path, record.pattern, record.query].find(
    (value): value is string => typeof value === "string" && value.trim().length > 0,
  );
  const target = first?.trim();
  if (toolName === "bash") {
    return target ?? "bash";
  }
  if (target) {
    return target;
  }
  return toolName ?? "tool";
}

/** Extract text content from a pi tool result / partial result payload. */
export function piResultText(result: unknown): string | undefined {
  if (typeof result === "string") {
    return result;
  }
  if (typeof result !== "object" || result === null) {
    return undefined;
  }
  const content = (result as Record<string, unknown>).content;
  if (!Array.isArray(content)) {
    return undefined;
  }
  const text = content
    .map((part) => {
      if (typeof part === "object" && part !== null) {
        const textValue = (part as Record<string, unknown>).text;
        if (typeof textValue === "string") {
          return textValue;
        }
      }
      return "";
    })
    .join("\n")
    .trim();
  return text.length > 0 ? text : undefined;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Decode a pi process stdout stream into trimmed JSONL lines. */
export function piJsonLines(
  processHandle: ChildProcessSpawner.ChildProcessHandle,
): Stream.Stream<string, PlatformError.PlatformError> {
  return processHandle.stdout.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.map((line) => line.replace(/\r$/, "")),
    Stream.filter((line) => line.trim().length > 0),
  );
}

/**
 * Run `pi --version` and collect the process output. Used for the install
 * health probe in the status snapshot.
 */
export const runPiVersionCommand = Effect.fn("runPiVersionCommand")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const binaryPath = piSettings.binaryPath || "pi";
  const spawnCommand = yield* resolveSpawnCommand(binaryPath, ["--version"], { env: environment });
  const command = ChildProcess.make(spawnCommand.command, spawnCommand.args, {
    env: environment,
    shell: spawnCommand.shell,
  });
  return yield* spawnAndCollect(binaryPath, command);
});

export interface PiRpcProbeState {
  readonly models: ReadonlyArray<Record<string, unknown>>;
  readonly commands: ReadonlyArray<Record<string, unknown>>;
}

/**
 * One-shot RPC probe: spawn `pi --mode rpc`, ask for the configured model
 * list and command list, and let the scoped child process die when the
 * effect completes. Resolves to empty collections on any failure or timeout
 * so the caller can fall back to the configured custom models.
 */
export const probePiRpcState = Effect.fn("probePiRpcState")(function* (
  piSettings: PiSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<PiRpcProbeState, never, ChildProcessSpawner.ChildProcessSpawner> {
  return yield* Effect.gen(function* () {
    const spawnCommand = yield* resolveSpawnCommand(
      piSettings.binaryPath || "pi",
      ["--mode", "rpc"],
      { env: environment },
    );
    const child = yield* ChildProcessSpawner.ChildProcessSpawner.pipe(
      Effect.flatMap((spawner) =>
        spawner.spawn(
          ChildProcess.make(spawnCommand.command, spawnCommand.args, {
            env: environment,
            shell: spawnCommand.shell,
          }),
        ),
      ),
    );

    const modelsRequestId = "t3-model-discovery";
    const commandsRequestId = "t3-command-discovery";
    yield* Stream.run(
      Stream.encodeText(
        Stream.make(
          `${encodePiJsonLine({ id: modelsRequestId, type: "get_available_models" })}\n` +
            `${encodePiJsonLine({ id: commandsRequestId, type: "get_commands" })}\n`,
        ),
      ),
      child.stdin,
    );

    const responses = yield* piJsonLines(child).pipe(
      Stream.map((line) => decodePiJsonLine(line)),
      Stream.filter(
        (parsed) =>
          Option.isSome(parsed) &&
          parsed.value.type === "response" &&
          (parsed.value.id === modelsRequestId || parsed.value.id === commandsRequestId),
      ),
      Stream.map((parsed) => Option.getOrNull(parsed)),
      Stream.take(2),
      Stream.runFold(
        () => [] as ReadonlyArray<Record<string, unknown>>,
        (acc, response) => (response === null ? acc : [...acc, response]),
      ),
    );

    let models: ReadonlyArray<Record<string, unknown>> = [];
    let commands: ReadonlyArray<Record<string, unknown>> = [];
    for (const response of responses) {
      if (!isRecord(response.data)) {
        continue;
      }
      const data = response.data as Record<string, unknown>;
      if (response.id === modelsRequestId && Array.isArray(data.models)) {
        models = data.models.filter(
          (model): model is Record<string, unknown> =>
            isRecord(model) && piModelSlug(model) !== undefined,
        );
      }
      if (response.id === commandsRequestId && Array.isArray(data.commands)) {
        commands = data.commands.filter((command): command is Record<string, unknown> =>
          isRecord(command),
        );
      }
    }
    return { models, commands };
  }).pipe(
    Effect.scoped,
    Effect.catchCause(() => Effect.succeed({ models: [], commands: [] })),
  );
});
