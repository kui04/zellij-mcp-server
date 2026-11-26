# AGENTS.md

## Environment and checks

- Run toolchain commands inside the `devenv` shell (or via `direnv exec . <cmd>` when outside it).
- `rustfmt.toml` enables unstable formatting options (`imports_granularity`, `group_imports`) that
  stable `rustfmt` rejects. Use the `devenv` nightly `rustfmt` (`cargo fmt --all`) or
  `cargo +nightly fmt --all`.
- Run `cargo test --all-features` rather than plain `cargo test`: `tests/orchestrator_tests.rs` is
  gated behind `--features test-util`, and `tests/zellij_e2e_tests.rs` spawns a real `zellij`
  (`>= 0.46.1`) binary.
- Match the pre-commit and CI checks before finishing: `cargo fmt --all -- --check`,
  `cargo clippy --all-targets --all-features -- -D warnings`, and
  `prettier --print-width 100 --prose-wrap always --check` on Markdown files.

## Commits

- Write commit messages in the conventional commits format, all lowercase, in english. When the body
  exceeds one line, use an unordered list. Keep the whole project in english.

## Documentation

- Do not cite untracked, deleted, or temporary files (such as scratchpads, `*.tmp.*`, or deleted
  implementation plans) in comments, docstrings, or commit messages. Write self-contained rationale
  instead.
- Update documentation, code comments, and references when code changes, and remove obsolete
  references promptly.
