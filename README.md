# henmen

Delegate broad code investigation to a [Codex](https://developers.openai.com/codex/cli) worker, so the
calling agent's context stays small.

A coding agent burns most of its context window reading. Reading a 4,000-line file to find one function,
scanning a directory to learn its layout, paging through a test log — the agent needs the conclusion, but
pays for every line on the way to it. henmen hands that reading to a separate Codex thread and returns
only a structured summary.

```console
$ henmen delegate "Find where retries are implemented and who calls them" \
    --model gpt-5.6-luna --effort max --mode inspect
{"session_id":"01M2HZ314J9TAEN0MSACB27CAS","response":{"status":"done","summary":"...","evidence":["src/retry.rs:120-145"],"changed_files":[],"tests":[],"open_questions":[]}}
```

## Status

Early. The delegation half works end to end against a real `codex app-server`; the rest is not built yet.

| | |
|---|---|
| `delegate` / `resume` | works |
| session persistence | works |
| graceful shutdown on SIGINT / SIGTERM | works |
| read curation (a Claude Code hook that intercepts large reads) | not implemented |

## Requirements

- Rust 1.90 or newer (2024 edition)
- The `codex` CLI on `PATH`, already logged in (`codex login`)

## Install

```bash
cargo install --path .
```

## Usage

```bash
henmen delegate <TASK> --model <MODEL> --effort <EFFORT> --mode <inspect|edit> [--cwd <PATH>]
henmen resume <SESSION_ID> <TASK> --model <MODEL> --effort <EFFORT> --mode <inspect|edit> [--cwd <PATH>]
```

`--mode inspect` runs the worker read-only; `--mode edit` lets it write inside the workspace. A session
can be resumed in a higher mode than it started in, so a thread can investigate under `inspect` and then
implement under `edit` while keeping everything it learned:

```bash
henmen delegate "Find out why the retry path drops the last error" \
    --model gpt-5.6-luna --effort max --mode inspect
# -> session_id 01M2HZ...

henmen resume 01M2HZ... "Now fix it" \
    --model gpt-5.6-luna --effort max --mode edit
```

`--model` and `--effort` have no defaults on purpose: which model slugs exist, and which reasoning efforts
each one accepts, is decided by Codex rather than by henmen. Hardcoding a slug here would silently rot.

## Output

stdout carries one JSON object and nothing else, so a caller can parse it without stripping prose.
Diagnostics go to stderr, and the exit code reports whether the command itself succeeded.

```json
{
  "session_id": "01M2HZ314J9TAEN0MSACB27CAS",
  "response": {
    "status": "done",
    "summary": "",
    "evidence": [],
    "changed_files": [],
    "tests": [],
    "open_questions": []
  }
}
```

`status` is `done` or `blocked`. `blocked` is how a read-only worker reports that it was asked to write —
Codex returns a normally completed turn in that case, so the status field is the only signal.

The response shape is enforced on the Codex side: henmen generates a JSON Schema from the Rust type and
passes it as the turn's `outputSchema`, so the worker's final message is already structured.

## Sessions

One session is one JSON file, written before the turn starts so that an interrupted run can still be
traced back to its thread.

```
$XDG_STATE_HOME/henmen/sessions/    # or ~/.local/state/henmen/sessions/
```

## Shutdown

SIGINT and SIGTERM are caught and forwarded to the Codex child, which stops it cleanly and leaves the
thread resumable. Killing henmen with SIGKILL does not: the underlying Codex process is orphaned, keeps
running, and holds the thread's writer lock until it finishes on its own.

## Development

```bash
cargo test                 # unit tests
cargo test -- --ignored    # also runs tests against a real codex app-server
```

## License

Either of [Apache License 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this
work by you shall be dual licensed as above, without any additional terms or conditions.
