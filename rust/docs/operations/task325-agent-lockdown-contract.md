# Task 325 package-lockdown contract

This is the self-contained dispatch and integration contract for the Go-to-Rust
campaign. Go at the coordinator-named accepted `hparser-integration` commit is
the source of truth. No developer-specific checkout, cache, credential, or
environment path is part of a unit brief.

## Atomic claim

The minimum new dispatch and completion unit is one complete Go package. It
includes every direct production, test, build-tag, platform, generated, build,
fixture, support, and generator-input artifact. Its raw AST inventory is split
into exhaustive per-Go-file ledgers, but those ledgers are one atomic package
claim. A package may map to multiple write-disjoint Rust crates; one package
owner owns the whole mapping, and no mapped crate may have another concurrent
owner.

Historical single-file lockdowns remain useful, content-addressed seed
evidence. A package unit absorbs or extends them without rewriting their
historical receipts. A file receipt is never promoted to a package-completion
claim, and new work is not dispatched as another partial file lockdown.

Every Go AST obligation has exactly one verdict:

- `PORTED` names a declared production Rust symbol and an exact behavioral test
  body that calls its qualified identity.
- `DECLINED` binds an exact Go quote and a checker-executed, content-addressed
  measured probe. It is an explicit implementation gap, never parity.
- `UNREACHABLE` binds an exact Go quote and a content-addressed structural proof
  with at least two distinct boundary cases and proof steps.

No TODO, placeholder, duplicate, omitted, or `UNCLASSIFIED` obligation closes
an inventory. `inventory_complete` means the census is exact.
`implementation_complete` is true only when at least one production Rust symbol
is ported and there are zero `DECLINED` obligations. A zero-PORTED package is
`falsification`; a mixed PORTED/DECLINED package is `classified-gaps`, not a
transcreated package. Falsification and an unchanged oracle are successful,
honest outcomes.

## Ownership and isolation

The coordinator dispatches from an exact SHA verified identical on `origin` and
`ngaut`. The owner creates a fresh isolated worktree at that SHA and a local
`codex/...` branch. Never use the divergent main checkout as evidence. The
owner does not publish a task branch. The only remote code branch in this
campaign is `hparser-integration`, and only the coordinator may update it.

One owner per crate is absolute. If any crate required by a package is already
reserved, do not dispatch overlapping work. Waiting is correct. A package that
maps to several crates is dispatched only when the coordinator can reserve all
of them for the one package owner.

All changed, staged, or untracked paths below a mapped Rust crate since the
accepted source commit are part of the receipt-owned file set, including
`Cargo.toml`, helpers, production files, and tests. There is no
"integration-only" bypass. `rust/Cargo.lock`, outside individual crate roots,
is coordinator batch state. A dedicated receipt directory may live below a
mapped crate: only its exact spec and checker-declared proof graph are exempt
from production ownership, and extra Rust/Go source or unreferenced proof
artifacts fail the census.

## Owner lane: compare and edit only

The coordinator seeds `package.toml`, the artifact manifest, and per-file raw
ledgers before handoff. The package owner then:

1. compares every Go artifact and obligation against the mapped Rust
   production code and tests;
2. implements every reachable Go rule in native production Rust;
3. edits only mapped Rust files, tests, ledgers, symbol/rule matrices, evidence
   plans, and content-addressed proof inputs in scope;
4. returns a clean local descendant SHA plus the artifact/obligation census,
   implementation changes, explicit gaps, structural proofs, and risks.

The owner runs no Cargo command, Go package test, Clippy, `make`, full-workspace
gate, executable probe, or mutation. The owner does not push any remote ref.
This keeps compilation and shared dependency state in one warmed lane and makes
the owner brief independent of coordinator-local state.

## Coordinator lane: execute, fix, integrate, push

The coordinator is the sole executable-evidence and integration owner. It:

1. generates/seeds package inventories from the accepted source commit;
2. reviews the returned local SHA and feeds every integration or compile fix
   under a mapped crate back into the same package-owned receipt;
3. executes measured probes and every semantic mutation through the fixed
   declarative runner in `rust/scripts/go-package-lockdown.py`;
4. runs scoped Go/Cargo compilation, formatting, Clippy, lint, and the clean
   full-workspace gate once in a warmed integration lane rather than once per
   file or owner;
5. batches only write-disjoint crate packages, resolves shared `Cargo.lock`
   from the current integration tip, and creates the combined candidate;
6. pushes the exact gated candidate only to `hparser-integration` on both
   remotes, verifies both refs, and reclaims worktrees and targets.

The fixed evidence runner constructs commands; evidence never supplies an
executable or shell fragment. Cargo evidence runs from `rust/` as:

```text
cargo test --offline --locked -j12 --quiet -p <mapped-crate> \
  --test <target> <exact-test> -- --exact --nocapture
```

Go evidence applies the repository failpoint decision to the accepted source
tree. A package using failpoints runs from the repository root as one
cleanup-owning fixed command:

```text
./tools/check/failpoint-go-test.sh <pinned-package> \
  -run ^<ExactTest>$ -count=1 -v
```

A package without failpoints runs directly as:

```text
go test ./<pinned-package> -run ^<ExactTest>$ -count=1 -v
```

Raw logs are independently content-addressed. Run and verification may contain
nondeterministic timing/build lines, so the checker compares normalized exact
named-test PASS/FAIL observations and exit/outcome, not raw-log equality. A
compile-only failure is not a killed mutation because it proves the named test
did not execute. Measured probes emit one canonical runtime JSON record with
every case ID, input, observed value, and conclusion. The checker compares it
to separately content-addressed expected values; a hash marker, missing case,
or hardcoded expected JSON literal in the named test is rejected. Each mutation proves baseline PASS,
mutated FAIL or records SURVIVED, and restored PASS. Source bytes are restored
in `finally` after survivors, failures, and runner errors.

## Required evidence

`package.toml` pins the Go package, accepted source commit, complete mapped
crate set, extra artifacts, and receipt-owned Rust paths. The package manifest
`source_commit` must equal the coordinator-supplied full SHA passed as required
`--accepted-source-commit` on every frontend invocation; HEAD cannot silently
authorize itself. The package manifest
must match both the current tracked census and exact blobs at `source_commit`.
Nested exclusions require direct tracked `.go` files and an exact distinct
directory proof; `testdata` and arbitrary subtrees cannot be excluded. Every
repository input referenced by `go:generate` is manifested, or generation fails
closed when static resolution is impossible. Checker schema v2 accepts only an
exact literal basename that resolves to a pinned regular file in the claimed
package. Globs, directories, `all:`, paths, and directives in excluded nested
packages fail closed rather than approximating cmd/go embed resolution.

Every ledger row binds the full owning Go source blob hash as well as its AST
node hash. Straight-line declaration/body drift invalidates preserved verdicts.
`DECLINED`, `UNREACHABLE`, and dynamic-fixture evidence use exact
content-addressed JSON artifacts bound to `source_commit` and at least two
distinct boundary cases. Dynamic fixture evidence binds the exact
source/line/access expression and either an exact manifested resolved set or an
explicit no-artifact conclusion; arbitrary `measured:` prose fails.
The generated helper-call manifest inventories every `go/ast.CallExpr` in each
direct `*_test.go`. Every exact helper call set must be joined to either a
mechanically detected direct fixture access, a content-addressed structural
no-fixture proof, or a measured fixture-resolution plan. Helper wrappers such
as `LoadTestSuiteData` and `GenerateOutputIfNeeded` cannot be omitted merely
because the direct file API sits in another function.

The symbol registry names a tracked receipt-owned definition under
`rust/crates/<crate>/src/**`. The final Rust identifier must be an actual
`fn`, `struct`, `enum`, `trait`, `type`, `const`, `static`, or `mod`
declaration at the full claimed module/type/impl identity, not an unrelated
same-leaf declaration. The separate compile anchor names an exact Cargo target
and fully module-qualified `#[test]` identity whose body executably uses the
`mapped_crate_name::...` identity. Test-local `crate::` decoys, bare path
bindings, comments, strings, and unrelated test bodies do not satisfy the
gate. Each rule mutation binds that production definition path and registered
target/test tuple. Mutation sources are likewise
receipt-owned production `src/**/*.rs` files, never tests or support files.

Each PORTED semantic rule has at least two distinct boundary cases and a
one-rule mutation. Every execution has an immutable content-addressed attempt
plan binding its baseline commit, production source, operator, constructed
command, and exact test. Historical survivors remain owned and countable after
a production fix. The current rule plan must have a current-source verified
KILLED attempt; retired historical plans may end `SURVIVED` without being
rewritten. Attempt rows form a contiguous content-addressed sequence/hash
chain. Each append binds the prior history head plus the committed receipt or
history-checkpoint content hash; the committed sequence is an exact prefix, so
history cannot be deleted, reordered, or rewritten during the survivor-to-fix
transition.

## Coordinator validation and handoff

The coordinator selects the repository Ready profile and runs the smallest
scoped Go/Cargo gates that prove the package, then:

```bash
cd rust
cargo fmt --all -- --check
cargo clippy --offline --locked -j12 -p <crate> --all-targets -- -D warnings
cd ..
make -j12 lint
git diff --check <accepted-tip>..HEAD
```

From a second clean detached worktree at the exact combined SHA and a distinct
target directory:

```bash
cd rust
cargo test --offline --locked -j12 --workspace
```

Do not add `--all-targets` to the full workspace gate. Inspect current ratchet
constants directly at the candidate; never weaken them to pass. The final
coordinator report names the exact accepted and final SHAs, package and mapped
crates, artifact/obligation verdict counts, implementation-completeness truth,
every historical/current mutation outcome, exact commands, oracle movement (or
plainly none), dual-remote ref verification, and cleanup.
