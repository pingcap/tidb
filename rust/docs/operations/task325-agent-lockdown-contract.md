# Task 325 Agent Lockdown Contract

This document is the self-contained handoff for agents working on the Go-to-Rust
lockdown campaign. It intentionally contains no developer-specific paths,
preinstalled cache locations, or assumptions about a main checkout. Go source at
the accepted integration commit is the authority.

## Completion boundary

A package-completion claim must cover one complete upstream Go package, including
production sources, generated and platform variants, tests, support files,
fixtures, build metadata, integration decisions, and validation receipts. A
single-source lockdown may be accepted as seed evidence, but it must say plainly
that it is not whole-package completion.

A lockdown owns one named Go source and one Rust crate/module. It accounts for
every declaration, field, function, branch outcome, loop, short-circuit outcome,
closure, and directly owning test/support obligation. Every obligation has
exactly one verdict:

- `PORTED`: names a real Rust symbol and a compiled behavioral boundary test.
- `DECLINED`: quotes the relevant Go rule and records a measured architectural,
  safety, or dependency boundary. A decline is not a parity claim.
- `UNREACHABLE`: includes structural proof that the Rust entry surface cannot
  construct or reach the state.

An unclassified, duplicated, silently omitted, or placeholder obligation fails
the lockdown. `TODO` is not a verdict. If every obligation is declined, the
receipt is useful falsification evidence, not an implementation-completeness
claim.

Completeness is the deliverable. Moving a differential oracle is not required.
An unchanged ratchet is a successful result when the source boundary is fully
closed.

## Ownership and dispatch

Only one active unit may own a Rust crate at a time. If all crates with eligible
work are owned, dispatch nothing. Waiting is correct; a second concurrent owner
is not.

Do not reopen an already locked Go source or Rust file. Extend its checked-in
inventory or receipt when evidence is incomplete. Use a divergence-driven unit
only for a genuinely new surface not owned by a lockdown.

Before editing, publish a reservation branch from the accepted integration tip.
The branch name must identify the task, crate, and source. The reservation does
not authorize advancing the integration branch.

## Checkout isolation

Never use a pre-existing main checkout as source evidence. Resolve the accepted
tip from the remote integration branch, create a fresh worktree at that exact
commit, and use a worktree-exclusive Cargo target directory.

Example, with repository-neutral placeholders:

```bash
git fetch origin hparser-integration
accepted_tip=$(git rev-parse origin/hparser-integration)
unit_worktree=$(mktemp -d "${TMPDIR:-/tmp}/task325-unit.XXXXXX")
unit_target=$(mktemp -d "${TMPDIR:-/tmp}/task325-target.XXXXXX")
git worktree add -b codex/task325-<crate>-<source>-lockdown \
  "$unit_worktree" "$accepted_tip"
export CARGO_TARGET_DIR="$unit_target"
export CARGO_BUILD_JOBS=12
```

The agent must verify that both configured remotes expose the same accepted
integration SHA before beginning. If they differ, stop and report the divergence.
Do not guess which remote is authoritative.

## Inventory and gates

Keep the inventory and machine-readable receipt beside the owning Rust module or
its integration test. Pin every owning Go artifact by repository-relative path,
byte length, line count, and SHA-256. The generator must use syntax-aware Go AST
obligations; prose function lists are insufficient.

The checked-in gate must fail when any of the following changes:

- an owning Go artifact path, hash, size, line count, or zero-count class;
- an AST obligation ID, source location, kind, owner, or source quote;
- the exact one-verdict census;
- a `PORTED` Rust symbol or compiled anchor;
- decline or unreachability evidence;
- a mutation plan path, target hash, result, or receipt hash;
- a directly owning Go test/support artifact.

Test ownership is semantic, not filename-only. Include a test or support artifact
when it directly owns the source type, exported symbol, failpoint, exact type
label, or source-specific behavior. Generic consumer tests are not direct owners,
but the receipt must state the ownership rule so omissions are reviewable.

## Parity and mutation proof

Port Go semantics, not recorded answers. Preserve nil versus empty values,
integer widths and overflow, error identity and ordering, JSON field/default
rules, string and case-folding behavior, clone/aliasing behavior, concurrency,
and side-effect order wherever the Go source makes them observable.

Mutation-probe every reachable rule with boundary cases. Each mutation must alter
one semantic rule, make the named boundary test fail, then be restored and make
the test pass. A surviving mutation is a finding that the test is too weak; fix
the test and rerun it. Record every attempt, including initially surviving probes,
in the receipt.

Falsification is success. If a stale brief, partial branch, assumed port, or test
owner census is wrong, record the measured contradiction and correct the receipt.
Do not manufacture implementation work to satisfy the brief.

## Validation

Use the repository Ready profile. Commands may be adapted to the owning crate and
source, but the final unit must include:

```bash
# Source-owned Go oracle tests, with repository failpoint handling when required.
go test -run '^<OwningTest>$' ./<owning/go/package>

# Rust source, inventory, mutation, and crate gates.
cd rust
cargo test --offline --locked -j12 -p <crate> <scoped-test-filter>
cargo clippy --offline --locked -j12 -p <crate> --all-targets -- -D warnings
cargo fmt --all -- --check
cd ..
git diff --check <accepted-tip>..HEAD
make -j12 lint
```

Then create a second clean detached worktree at the exact final unit SHA, with a
different exclusive target directory, and run:

```bash
cd rust
cargo test --offline --locked -j12 --workspace
```

Do not add `--all-targets` to the full workspace completion gate: it executes
existing benchmark binaries and can turn the gate into an unrelated hours-long
benchmark sweep. Crate-level `--all-targets` remains appropriate when required by
the owning surface.

Directly inspect the four checked-in ratchet constants before handoff. A unit must
not weaken them to make tests pass. At the time this contract was written, the
accepted values were query `0`, catalog `100`, table `1`, and integration `78`;
the integration tip itself remains the current authority.

## Handoff and integration

The unit pushes its exact final SHA to the same task branch on both remotes and
verifies both refs with `git ls-remote`. It returns:

1. exact SHA, branch, crate, owning Go source, and accepted parent SHA;
2. artifact and obligation census by verdict;
3. production semantics added or corrected;
4. every mutation result, including strengthened surviving probes;
5. exact validation commands and results;
6. ratchet constants and whether any oracle moved;
7. explicit declines, unreachability proofs, and unverified external behavior;
8. cleanup confirmation for its worktrees, targets, caches, and temporary probes.

Only the coordinator advances `hparser-integration`. The coordinator independently
gates the returned SHA in a clean worktree, applies it onto the current accepted
tip without force-pushing, gates the combined SHA again, pushes that exact SHA to
`hparser-integration` on both remotes, verifies both refs, and reclaims the
integration artifacts.

Never merge or rebase an entire stale task branch when its base contains unrelated
history. Transplant only the audited source-specific commits. Resolve shared
`Cargo.lock` changes by regenerating the lockfile from the current accepted tip;
never choose an old whole-file side.

