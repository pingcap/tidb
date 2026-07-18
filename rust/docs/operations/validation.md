# Rust Rewrite Validation

Run these commands from `rust/` when accepting a wave or a structural change.
Use 12 jobs for every Cargo build/test command.

## Canonical entrypoint

Use `scripts/rewrite-gate.sh` instead of reconstructing validation commands in
each agent prompt. It pins one shared target directory, enforces 12 Cargo jobs,
and makes the expensive workspace gate an explicit operation:

```sh
# Reads checked TSV snapshots only; this does not invoke Cargo.
scripts/rewrite-gate.sh status

# Feature-agent loop: one package, one test target, optional test-name filter.
scripts/rewrite-gate.sh leaf <package> <test-target> [<filter>]

# Evidence/queue acceptance without a workspace test or workspace Clippy run.
scripts/rewrite-gate.sh static

# Steward-only large-batch acceptance.
scripts/rewrite-gate.sh integrate
```

`integrate` is deliberately not a per-leaf or per-agent command. Run it only
after all lanes in a substantial source-family batch are frozen. The `status`
path remains usable while another lane is compiling because it reads the
checked inventories directly.

```sh
cargo fmt --all
cargo metadata --locked --no-deps --format-version 1
cargo test --locked -j 12 --workspace -q
cargo clippy --locked -j 12 --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
cargo run --locked -j 12 -p difftest --bin go_source_ledger -- --check
cargo run --locked -j 12 -p difftest --bin go_test_ledger -- --check
cargo run --locked -j 12 -p difftest --bin domain_queue -- --check
cargo run --locked -j 12 -p difftest --bin parser_translation_manifest -- --check-fragments
cargo run --locked -j 12 -p difftest --bin parser_translation_manifest -- --check
cargo run --locked -j 12 -p difftest --bin integration_parser_inventory -- --check
cargo run --locked -j 12 -p difftest --bin integration_parser_golden -- --check
cargo run --locked -j 12 -p difftest --bin integration_parser_queue -- --check
cargo run --locked -j 12 -p difftest --bin integration_plan_inventory -- --check
```

The parser evidence package must also remain independent of expression and
execution. This command succeeds only when the search prints nothing:

```sh
test -z "$(cargo tree -p difftest-parser-tests | rg 'tidb-(expr|exec)' || true)"
```

The parser oracle is checked data, so normal Rust tests never require a Go
subprocess. A parser feature may change its reviewed counts only after its
source-derived selector/corpus passes and every changed outcome is explained.
Both ledgers' `UNTRIAGED` and `PARTIAL` totals are obligations, not coverage.
Use the production ledger's target-crate queue and the test ledger's default
exact-source queue for the next parallel assignments; the optional test
package view is only a backlog summary:

```sh
cargo run --locked -j 12 -p difftest --bin go_source_ledger -- --queue <target-crate>
cargo run --locked -j 12 -p difftest --bin go_test_ledger -- --queue <ring>
cargo run --locked -j 12 -p difftest --bin go_test_ledger -- --queue <ring> package
```

## Three-speed development loop

Feature agents should not run the workspace gate for every leaf. The leaf lane
runs the smallest meaningful crate/test target. The static lane accepts a
merged evidence/queue batch without linking the workspace. The integrate lane
belongs to the evidence/workspace steward and runs once after a large batch is
frozen. Keep one persistent `CARGO_TARGET_DIR` per checkout so focused Cargo
checks reuse build artifacts; do not create a new wave target for every agent.

The integrate lane is the only place that should perform the expensive batch:

```sh
export CARGO_BUILD_JOBS=12
export CARGO_TARGET_DIR="$HOME/.cache/tidb-rust-target"
cargo fmt --all -- --check
cargo test --offline --locked -j12 --workspace -q
cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings
cargo run --offline --locked -j12 -p difftest --bin go_source_ledger -- --check
cargo run --offline --locked -j12 -p difftest --bin go_test_ledger -- --check
cargo run --offline --locked -j12 -p difftest --bin domain_queue -- --check
```

Run the full parser/plan inventory extensions when those rings or shared
routing files changed. This batching policy preserves parallel implementation
while removing repeated linker work and disk pressure; it does not weaken the
acceptance gate.
