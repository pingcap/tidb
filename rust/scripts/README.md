# `rust/scripts`

Operational tooling for the Rust workspace. Two of these are gates worth
reaching for *before* a full build, because they answer in a second what
`cargo test --workspace` takes tens of minutes to tell you.

## Fast gates

| script | what it answers | cost |
|---|---|---|
| `check-source-size.sh` | has any source file grown past its recorded bound, or shrunk enough to retire its entry? | ~1s, no build |
| `test-coverage-inventory.py` | how many Go tests still have no Rust counterpart, per package? | seconds with `--cache` |
| `run-doctests.sh` | do the examples in `///` comments still compile, still pass, and still exist? | ~4s warm, needs the libs built |

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

### `run-doctests.sh` — the doctest gate

**`cargo nextest` cannot run doctests at all.** The doctest harness is
rustdoc's, not libtest's, so there is nothing for nextest to execute. Since
the workspace gate is a nextest run, every example inside a `///` comment
went unchecked for the whole life of this project — and
`cargo test -p tidb-executor --doc` was failing the entire time without
anyone seeing it. The example that failed was a SQL statement indented four
spaces in a `//!` block, which markdown makes a code block and rustdoc then
compiles *as Rust*; grepping for ` ``` ` fences never would have found it.

The script guards two failure modes, because checking only the first leaves
the second invisible:

* **A doctest ran and failed** — cargo's own exit status.
* **A doctest stopped existing** — the `EXPECTED_DOCTESTS` count. Retagging a
  fence ` ```text `, or wrapping an example in ` ```ignore `, deletes the
  check while leaving the example looking exactly as verified as before. That
  reports `0 passed; 0 failed` and stays green. Pinning the count makes it
  red. Changing the count is allowed in the same commit that changes the
  examples — the point is that it becomes a reviewed decision, not a silent
  one. An `ignore`d doctest fails outright: an example that claims to be
  checked and is not is worse than no example.

`doctest_gate` in `difftests/result-tests` shells out to this script, same
shape as the ratchet above, so the normal workspace run enforces it. Shelling
out to `cargo` from inside a test was verified rather than assumed — cargo
releases the build-directory lock before running test binaries, so the nested
`cargo test --doc` acquires it instead of deadlocking on its parent, and
`--doc` runs only doctests so it never re-enters the wrapper.

## Running the test suite

Prefer **`cargo nextest run`** over `cargo test` — it runs test binaries in
parallel and is substantially faster on a many-core machine:

```bash
cargo nextest run --workspace --no-fail-fast   # full gate (doctests included,
                                               #   via the doctest_gate test)
cargo nextest run -p tidb-session              # while iterating
./scripts/run-doctests.sh                      # doctests alone, ~4s warm
```

Doctests no longer need a step anyone has to remember: a step a human has to
remember is a step that gets skipped, which is precisely how the failure above
survived. `cargo test --doc` by itself remains fine for a quick look, but it
does not check that the examples still *exist* — use the script for that.

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
