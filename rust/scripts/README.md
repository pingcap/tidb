# `rust/scripts`

Operational tooling for the Rust workspace. Two of these are gates worth
reaching for *before* a full build, because they answer in a second what
`cargo test --workspace` takes tens of minutes to tell you.

## Fast gates

| script | what it answers | cost |
|---|---|---|
| `check-source-size.sh` | has any source file grown past its recorded bound, or shrunk enough to retire its entry? | ~1s, no build |
| `test-coverage-inventory.py` | how many Go tests still have no Rust counterpart, per package? | seconds with `--cache` |

### `check-source-size.sh` — the source-size ratchet

Bounds live in `source_size_bounds.txt`, one `path max-lines` per line. The
rules, and the reasoning behind each:

* **A listed file may not exceed its bound.** Growing one is allowed only by
  raising the bound *in the same commit*, with the reason as a comment above
  the entry. That turns silent drift into a reviewed decision — `driver.rs`
  was split at 7,793 lines and regrew past 7,400 before anyone noticed.
* **A listed file at or under the 2,200-line soft limit must have its entry
  removed.** The table only ratchets down, so it doubles as the structure
  campaign's work queue.
* **An unlisted file over the soft limit fails outright.** Split it into
  sibling modules; adding an entry is not the fix.

Size itself is not the sin — a transcreated data catalog is legitimately long.
Growth nobody *decided* is.

The `source_size_ratchet` test in `difftests/result-tests` shells out to this
script, so `cargo test`/`cargo nextest` still gate it. The script owns the
logic; the test is a three-line wrapper.

## Running the test suite

Prefer **`cargo nextest run`** over `cargo test` — it runs test binaries in
parallel and is substantially faster on a many-core machine:

```bash
cargo nextest run --workspace --no-fail-fast   # full gate
cargo nextest run -p tidb-session              # while iterating
cargo test --doc                               # doc-tests (nextest skips them)
```

Install with `curl -sSfL https://get.nexte.st/latest/mac | tar zxf - -C ~/.cargo/bin`.

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
