# `rust/scripts`

Operational tooling for the Rust workspace.

## Running tests

Run scoped tests from `rust/` with 12 build jobs:

```bash
cargo test --offline --locked --release -j12 -p tidb-session --lib <test_filter>
```

Go test cases and their fixtures are the correctness reference. Do not add
Rust source-shape, call-count, file-size, or historical test-count gates.

## When the machine gets slow, it is usually disk

Builds and tests thrash long before the disk reports itself full. Free space in
this order, cheapest first:

```bash
go clean -cache                          # 16-36GB of gorun/goeval capture cruft
rm -rf rust/target/debug/incremental     # ~53GB, keeps every compiled dependency
git worktree list                        # agent worktrees run 4-11GB EACH
```

`cargo clean` frees the same space as the second line but costs a full
workspace rebuild — reach for it last. Remove an agent worktree as soon as its
work is cherry-picked rather than batching the cleanup.
