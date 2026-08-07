# Go-to-Rust Lockdown Agent Collaboration Contract

This document is the complete handoff for an agent contributing a source-file
lockdown. It deliberately does not depend on a Codex task, conversation,
memory, machine-specific path, existing checkout, Cargo or Go cache, or another
agent's local state. A fresh clone plus the dispatch envelope below is enough.
The Git remotes and checked-in repository files are the coordination surface.

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

## Required dispatch envelope

Mutable campaign state does not belong in this document because it becomes
stale. The coordinator supplies exactly one dispatch envelope for each unit:

```text
accepted_sha: <full commit present on both origin and ngaut>
accepted_ref: <same full branch ref on both remotes, naming accepted_sha>
rust_crate: <exactly one crate owned by this unit>
owning_go_source: <one complete production Go source file>
branch: codex/task325-<rust-crate>-<go-basename>-lockdown
already_locked_sources: <complete list at accepted_sha>
reserved_crates: <all currently active crate owners>
ranked_evidence: <optional queue row; never parity proof>
```

The agent verifies the envelope from both remotes before editing. Blank,
ambiguous, stale, or conflicting ownership data means stop and return a
falsification receipt; do not choose a substitute target. A ranked test row is
only dispatch evidence. Re-read the Go test, follow its production calls to
the defining Go file, and falsify the row if the behavior is already owned or
the premise is wrong.

## Fresh-host bootstrap

Do not depend on a pre-existing checkout, unpushed commit, local patch, cache,
probe output, absolute path, or another agent's worktree. Starting from an
empty host, only Git credentials for both remotes and the repository's normal
Go/Rust build toolchains are required:

```sh
git clone git@github.com:pingcap/tidb.git <repository>
git -C <repository> remote add ngaut git@github.com:ngaut/tidb.git
git -C <repository> fetch origin --prune
git -C <repository> fetch ngaut --prune
git -C <repository> ls-remote origin <accepted-ref>
git -C <repository> ls-remote ngaut <accepted-ref>
git -C <repository> cat-file -e <accepted-sha>^{commit}
```

If the clone already defines `ngaut`, verify its URL instead of adding it.
Both `ls-remote` checks must print the full `accepted_sha` for the dispatched
`accepted_ref`, and `cat-file` must resolve it as a commit. Never recover inputs
from a coordinator's machine. Generate probes and inventories from the
checked-in Go source in the unit's own worktree, and check every durable input
or generator into the branch.

## One-owner-per-crate protocol

One active unit owns one entire Rust crate. Two units must never run in the
same crate concurrently, even when their Rust files appear disjoint. Before
editing:

1. Fetch both remotes and list campaign branches.
2. Check out the exact dispatched `accepted_sha` and read this document there.
3. Reject any target whose crate is reserved or whose source is already locked
   in the dispatch envelope or checked-in inventories.
4. Choose exactly one eligible crate and resolve exactly one owning Go source
   file.
5. Create a dedicated branch named
   `codex/task325-<rust-crate>-<go-basename>-lockdown`.
6. Push that branch, still pointing at the accepted base if necessary, to both
   remotes as the ownership announcement before changing Rust code.

Use these repository-independent commands, replacing angle-bracket values.
The worktree and Cargo target paths must be newly allocated and exclusive to
this unit:

    git fetch origin --prune
    git fetch ngaut --prune
    git ls-remote --heads origin 'codex/task325-*'
    git ls-remote --heads ngaut 'codex/task325-*'
    git worktree add -b <branch> <worktree> <accepted-sha>
    git -C <worktree> push origin HEAD:refs/heads/<branch>
    git -C <worktree> push ngaut HEAD:refs/heads/<branch>

Every worktree uses a target directory that no other worktree uses:

    cd <worktree>/rust
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=<exclusive-target> \
      cargo test --locked -j12 -p <crate> --all-targets

Do not add `--offline`: a fresh agent is not assumed to have a dependency
cache. `Cargo.lock` remains authoritative through `--locked`. Never inspect or
build from an older shared checkout. If another agent has claimed every
eligible crate, dispatch nothing and wait. Do not manufacture parallelism by
taking a second file in an owned crate.

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

Return this receipt with every branch, including falsifications and no-code
lockdowns:

```text
outcome: COMPLETE | FALSIFIED | BLOCKED
branch: <branch>
sha: <full final SHA, or accepted_sha when no commit was required>
accepted_base: <full accepted_sha>
rust_crate: <crate>
owning_go_source: <path>
inventory: <production declarations>/<branches>/<test-support declarations>
classifications: <PORTED>/<DECLINED>/<UNREACHABLE>
divergences: <measured list, or NONE>
mutations: <killed>/<attempted, with every test identity>
ratchets_before_after: <directly grepped values; say UNCHANGED plainly>
validation: <exact commands and results>
risks: <correctness/compatibility/performance>
not_verified: <items and reasons, or NOTHING>
origin_ref: <ls-remote output>
ngaut_ref: <ls-remote output>
cleanup: <exact worktree and exclusive target reclaimed>
```

`COMPLETE` means the whole dispatched Go source is classified and gated. It
does not mean the enclosing Go package is fully transcreated. `FALSIFIED` is a
successful result when measurement disproves the brief. `BLOCKED` is reserved
for an external condition the unit cannot resolve without broadening scope.

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
