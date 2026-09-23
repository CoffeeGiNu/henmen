# henmen

Delegate code investigation and well-scoped edits to a worker agent, keeping the calling agent's context small.

Supported backends: [Codex]

## install

```bash
cargo install --path .
```

## usage

```bash
henmen models
henmen delegate <TASK> --model <MODEL> --effort <EFFORT> --mode <inspect|edit> [--cwd <PATH>] [--timeout-minutes <MINUTES>]
henmen resume <SESSION_ID> <TASK> --model <MODEL> --effort <EFFORT> --mode <inspect|edit> [--cwd <PATH>] [--timeout-minutes <MINUTES>]
```

## License

Either of [Apache License 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
