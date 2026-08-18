# Pi

This guide covers the pi provider in this fork. Pi ([`@earendil-works/pi-coding-agent`](https://github.com/earendil-works/pi-coding-agent))
is a standalone CLI coding agent. Unlike the subscription-based providers, pi brings its own LLM
configuration, so there is no separate `login` step.

## Install pi

Install the pi CLI on the machine running the T3 Code server (the same machine that runs the other
provider CLIs):

```bash
npm install -g @earendil-works/pi-coding-agent
```

Make sure `pi` is on the `PATH` of the shell that starts T3 Code, or set an explicit **Binary path**
in the provider settings.

## Add the provider

1. Open **Settings** → **Providers** and add a **Pi** instance (or use the default one).
2. Optionally set:
   - **Binary path** — path to the `pi` binary if it is not on `PATH`.
   - **LLM provider** — pi provider name (`anthropic`, `openai`, `google`, ...). Leave blank to use
     pi's own default.
   - **Model** — default model pattern or id. Supports `provider/id` and an optional `:thinking`
     suffix (for example `anthropic/claude-sonnet-4`). Leave blank to use pi's own default.
   - **Thinking level** — reasoning effort for models that support thinking: `off`, `minimal`,
     `low`, `medium`, `high`, `xhigh`, or `max`. Defaults to `off`.

Pi authenticates with its own configuration (API keys in pi's config, environment, or system
keychain), so no login command is needed here.

## What this fork adds

- A built-in provider driver (`PiDriver`) that spawns one `pi --mode rpc` process per active
  thread, so sessions stream into T3 Code like any other provider.
- Pi-backed text generation for commit messages, PR content, branch names, and thread titles.
- Provider status via `pi --version` (shown in Settings).
- A pi entry in the provider picker with its icon across web, desktop, and mobile.

## Extension notifications

Display-only pi extension notifications (`ui.notify` with kind `info`, such as the
[`@hk_net/pi-timestamp`](https://github.com/hknet/pi-extensions/tree/main/packages/pi-timestamp)
extension's `Sent HH:MM:SS` / `Done at HH:MM:SS` lines) are suppressed in the T3 Code work log,
because the UI renders message timestamps natively. They still appear in the pi terminal UI.
Real extension warnings and errors (kind `warning` / `error`) still surface as work-log entries.
