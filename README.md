# henmen

Delegate broad code investigation to a Codex worker, so the calling agent's context stays small.

## install

```bash
cargo install --path .
```

## usage

```bash
henmen delegate <TASK> --model <MODEL> --effort <EFFORT> --mode <inspect|edit> [--cwd <PATH>]
henmen resume <SESSION_ID> <TASK> --model <MODEL> --effort <EFFORT> --mode <inspect|edit> [--cwd <PATH>]
```

## License

Either of [Apache License 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT), at your option.
