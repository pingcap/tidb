# Go-to-Rust Lockdown Agent Collaboration Contract

This document is the complete handoff for an agent contributing a source-file
lockdown. It deliberately does not depend on a Codex task, conversation,
memory, machine-specific path, existing checkout, or another agent's local
state. The Git remotes and checked-in repository files are the coordination
surface.

## Outcome

Each returned branch closes one complete Go source file against its Rust
landing module. Go is the source of truth. A successful branch accounts for
every production declaration, every control-flow rule, and every original
test/support declaration associated with that source. It contains executable
drift and symbol gates, boundary tests, and mutation evidence. Finding that
Rust was already correct is a successful completeness lockdown; no oracle or
ratchet movement is required.

A source-file lockdown is not a claim that the enclosing Go package is fully
transcreated. Repository-wide package completion remains governed by
`AGENTS.md` non-negotiable 6.

## Current campaign state

Use the newest accepted commit that is present on both `origin` and `ngaut`.
At this revision the accepted chain ends at:

    163559e78020c57547e437089be1f28c3552f7a9

The following source files are already locked and must not be reopened:

| Rust crate | Owning Go source | Final commit |
| --- | --- | --- |
| `tidb-datatype` | `pkg/types/vector.go` | `ff766675780d0d2089245b0b5ac02ebdc1bd3fe6` |
| `tidb-datatype` | `pkg/types/vector_functions.go` | `e5c619d62a21f26372bf11e7a717d873405db3d2` |
| `tidb-datatype` | `pkg/types/json_binary_functions.go` | `32d0096e93a1b530ad34b093045c2febe83220c5` |
| `tidb-datatype` | `pkg/types/time.go` | `163559e78020c57547e437089be1f28c3552f7a9` |

The following crates are reserved by active owners and must not be claimed by
another agent:

| Rust crate | Active boundary |
| --- | --- |
| `tidb-expr` | existing `L1cast` expression/cast owner |
| `tidb-executor` | existing `L6driver` executor owner |
| `tidb-server` | `pkg/server/internal/parse/parse.go` handshake and attribute-policy landing |
| `tidb-protocol` | same `parse.go` lockdown; `StmtFetchCmd` lands in prepared-statement decoding |

The highest-ranked currently eligible independent surfaces are therefore
rank 18 (`pkg/meta/meta_test.go::TestMeta`, landing in `tidb-meta`) and rank 19
(`pkg/statistics/histogram_test.go` merge tests, landing in `tidb-stats`). The
ranking source is `rust/docs/operations/test-coverage-gaps.md`. Its rows are a
dispatch queue, not proof of missing behavior: re-read the Go test and resolve
the exact owning production source before claiming a crate. Falsifying a stale
ranked row is a successful finding; continue with the complete source-file
lockdown only when that source is not already locked.

## One-owner-per-crate protocol

One active unit owns one entire Rust crate. Two units must never run in the
same crate concurrently, even when their Rust files appear disjoint. Before
editing:

1. Fetch both remotes and list campaign branches.
2. Read this document at the newest accepted common tip.
3. Reject any target whose crate is listed as active or already locked.
4. Choose exactly one eligible crate and resolve exactly one owning Go source
   file.
5. Create a dedicated branch named
   `codex/task325-<rust-crate>-<go-basename>-lockdown`.
6. Push that branch, still pointing at the accepted base if necessary, to both
   remotes as the ownership announcement before changing Rust code.

Use these repository-independent commands, replacing angle-bracket values:

    git fetch origin --prune
    git fetch ngaut --prune
    git ls-remote --heads origin 'codex/task325-*'
    git ls-remote --heads ngaut 'codex/task325-*'
    git worktree add -b <branch> <worktree> <accepted-sha>
    git -C <worktree> push origin HEAD:refs/heads/<branch>
    git -C <worktree> push ngaut HEAD:refs/heads/<branch>

Every worktree uses a target directory that no other worktree uses:

    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> \
      cargo test --offline --locked -j12 -p <crate> --all-targets

Never inspect or build from an older shared checkout. If another agent has
claimed every eligible crate, dispatch nothing and wait. Do not manufacture
parallelism by taking a second file in an owned crate.

## Source ownership and inventory

Read any package `doc.go` before implementation. Then read the complete Go
production file, its build/platform/generated variants and inputs, every
original test and support artifact that exercises it, direct consumers, and
the current Rust landing module. Do not infer ownership from a ranked test
filename: follow the called production symbols to their defining Go file.

Check in an inventory beside the Rust landing module. It must:

- pin the Go source SHA-256, byte count, line count, and ordered declaration
  identities;
- classify every Go production declaration exactly once as `PORTED`,
  `DECLINED`, or `UNREACHABLE`;
- classify every syntactic control-flow locus exactly once, using stable source
  keys and naming the boundary test that observes it;
- classify every source-owned original test/support declaration exactly once;
- name a concrete Rust symbol for every `PORTED` row and compile-anchor that
  symbol so deletion or rename fails;
- quote source or measured probe evidence for every `DECLINED` row;
- provide a reachability proof for every `UNREACHABLE` row;
- reject duplicate, missing, empty, unknown, or pending classifications;
- fail when the Go file drifts or a `PORTED` symbol disappears.

`TODO`, `WIP`, `UNTRIAGED`, silent omission, family-level summaries that hide
individual branches, and an unclassified Go declaration are failed lockdowns.
Where one Go file legitimately lands in multiple Rust modules, keep one atomic
inventory and one crate owner; name every landing symbol rather than shrinking
the Go boundary.

## Behavioral work and falsification

Start with direct Go evidence for boundary cases. Tests must pin rules, not
copy a recorded answer without exercising the branch. For each mismatch:

1. Add the smallest Go-derived Rust regression.
2. Run it before the fix and record the exact failure.
3. Fix behavior at the owning layer, preserving Go value, diagnostic,
   mutation, ordering, overflow, malformed-input, and side-effect semantics.
4. Run the same test after the fix and record the pass.
5. Search sibling paths for the same failure class.

If a probe disproves the brief, record the falsification and do not invent a
code change. If Go intentionally panics or relies on unsafe representation and
safe Rust refuses it, classification still requires a measured boundary and a
specific source-backed reason; “Rust is safer” alone is not evidence.

Mutation-probe every independent rule family in a disposable worktree at an
immutable provisional commit. Mutate one rule at a time and require the
intended fully qualified test, source-drift gate, or compile-time symbol gate
to fail. Restore the mutation explicitly and verify the disposable worktree is
clean before the next probe. A mutation that passes is evidence that the test
is inadequate, not evidence that the implementation is correct.

## Validation and returned-SHA contract

Use the WIP profile while iterating and the Ready profile before returning a
SHA. Follow `.agents/skills/tidb-verify-profile/SKILL.md`. Before Go package
tests, follow `.agents/skills/tidb-failpoint-test-runner/SKILL.md`. Decide
`make bazel_prepare` with `.agents/skills/tidb-bazel-prepare-gate/SKILL.md`;
Rust-only changes normally do not trigger it.

At minimum, a returned SHA must have:

- the exact relevant Go tests or disposable Go probes passing;
- the affected Rust crate's full `--all-targets` tests passing;
- affected-crate clippy passing with warnings denied;
- inventory drift and symbol gates passing;
- every mutation killed with its test identity recorded;
- `cargo fmt --all -- --check` and `git diff --check` passing;
- the full Rust workspace passing in a new clean detached worktree with a new
  exclusive target directory;
- `make -j12 lint` passing from repository root;
- source hashes, inventory counts, and ratchet constants verified by direct
  grep rather than inferred from a broad gate;
- a clean final worktree.

Cargo commands run serially within a target directory. Do not run
`make bazel_lint_changed` unless explicitly requested. Do not use RealTiKV
unless the behavior depends on real TiKV.

Commit only a complete source-file lockdown. Push the identical final SHA to
both remotes without force, then verify both refs directly:

    git push origin HEAD:refs/heads/<branch>
    git push ngaut HEAD:refs/heads/<branch>
    git ls-remote origin refs/heads/<branch>
    git ls-remote ngaut refs/heads/<branch>

The handoff must state:

- branch and full SHA;
- Rust crate and complete owning Go source file;
- changed files;
- counts of production declarations, control-flow rules, and test/support
  declarations;
- every measured divergence and every falsified premise;
- mutation count and whether every mutation was killed;
- ratchet constants before and after, explicitly saying when none moved;
- exact validation commands and results;
- correctness, compatibility, and performance risks;
- what was not verified and why.

Only after both remote refs match may the unit remove its worktrees and exact
exclusive target/cache paths. Build artifacts are regenerable; source and the
final branch must already be recoverable from both remotes. Never remove a
broad directory, another agent's worktree, or another unit's target.

## Integration rules

The coordinator independently gates each returned remote SHA in a clean
worktree. A green contributor gate is evidence, not a substitute for this
independent gate. The coordinator rejects any branch that:

- started from a stale or unverified base without declaring it;
- overlaps an active crate owner;
- reopens an already locked Go source;
- omits any declaration, branch, or source-owned test/support artifact;
- uses `DECLINED` or `UNREACHABLE` without evidence;
- lacks a drift/symbol gate or has a surviving mutation;
- moves unrelated ratchets or shared files without source-backed need;
- cannot reproduce its claimed validation from the pushed SHA.

Write-disjoint crate branches may be integrated in completion order. After an
integration changes the accepted tip, later agents must base new units on that
new dual-remote tip; already active units report their older base explicitly
and are rebased or integrated only by the coordinator after conflict review.
