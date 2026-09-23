# henmen

Delegate broad code investigation to a worker agent, so the calling agent's context stays small.

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
