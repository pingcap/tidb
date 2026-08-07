# Lock down the complete `pkg/util/intset` Go package in Rust

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan implements the package-level Go-to-Rust transcreation rule in root `AGENTS.md` and the persistent LOCKDOWN contract. All source evidence comes from worktree `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-344-wave8` at official integration base `56d06365eae71a692e538986d84003565f880103`; the divergent main checkout is not an evidence source.

## Purpose / Big Picture

After this unit is complete, a reviewer can prove that Rust `tidb_util::intset::FastIntSet` accounts for the entire direct Go package `pkg/util/intset`, not only the methods already present in `intset.rs`. The checked-in inventory will bind all four package artifacts and every Go AST obligation to exactly one `PORTED`, `DECLINED`, or `UNREACHABLE` classification. A source edit, missing inventory row, missing Rust owner, or boundary regression will make a named gate fail.

The observable behavior is exact parity for construction, the `[0, 64)` bitmap-to-sparse transition, negative and large values, iteration and `Next` ordering, copy representation, set algebra, shifting, inclusive ranges, small bitmap extraction, panic/error text, and string range coalescing. A zero-divergence result is a successful lockdown; oracle ratchet movement is not required.

## Progress

- [x] (2026-08-07) Recovered task #325's package-level decision and excluded already locked packages/files at official tip `66ef34195`.
- [x] (2026-08-07) Created isolated branch/worktree `codex/lockdown-344-wave8` with exclusive Cargo target directory `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-344-wave8/tgt`.
- [x] (2026-08-07) Read all four direct Go package artifacts and all 761 lines of the existing Rust owner; confirmed there is no package `doc.go`, nested `AGENTS.md`, or existing intset inventory.
- [x] (2026-08-07) Ran the Go baseline: all six source tests passed. The generic Go AST tool reported 534 obligations, and the existing Rust intset filter passed six intset tests.
- [x] (2026-08-07) Fast-forwarded to official tip `56d06365eae7`; the four source artifact hashes and 534-obligation census remained unchanged after the histogram publication chain.
- [x] (2026-08-07) Measured extreme boundaries directly in Go: `Shift` wraps overflowing `int` addition, `MinInt` takes the source fast path, and `MaxInt` is stored but collides with the `Next`/`ForEach` sentinel.
- [x] (2026-08-07) Built the four-artifact manifest, zero-class ratchets, deterministic 534-row classification, Python drift gate, and compiled Rust owner/evidence gate.
- [x] (2026-08-07) Audited every production owner and original test/support/benchmark subtree; 446 obligations are `PORTED`, while 88 Go benchmark/reference-model/runtime-harness obligations are explicitly `DECLINED`.
- [x] (2026-08-07) Added eight named source-boundary tests and fixed the two measured divergences: wrapping `Shift` arithmetic and the `MaxInt` `Next`/`ForEach` sentinel contract.
- [ ] Commit the restored implementation, run boundary mutation probes for every behavior cluster, and record killed/surviving probes with exact named tests.
- [ ] Run WIP then coordinator Ready/full-workspace gates, verify direct ratchets, and publish according to the current official-branch delivery policy.

## Surprises & Discoveries

- Observation: task #325's earlier note said this package had three artifacts, but the real tip has four.
  Evidence: `git ls-files -- pkg/util/intset` returns `BUILD.bazel`, `fast_int_set.go`, `fast_int_set_test.go`, and `fast_int_set_bench_test.go`.
- Observation: the source package is much larger than six top-level tests suggest.
  Evidence: `go_package_lockdown_inventory` reports 534 obligations: 182 production declaration/function/control-flow obligations, six tests, six benchmarks, twelve test helpers, 65 test rows, 49 assertion calls, and the remaining test control-flow/support obligations. The first implementation pass incorrectly summed the production categories as 183; the generator's 446/88 status ratchet rejected that hand count before writing an inventory.
- Observation: the current Rust port already carries the main production surface and all six top-level Go test themes, but it has no content-addressed completeness proof.
  Evidence: `cargo nextest run -p tidb-util -E 'test(/intset/)'` runs all six `intset::tests` successfully, while `rg --files rust/crates/tidb-util/src` finds no intset inventory.
- Observation: macOS `shasum` is unusable in this environment because the configured `C.UTF-8` locale is unavailable.
  Evidence: `shasum -a 256 ...` exits 9 in Perl locale initialization; `openssl dgst -sha256` returns all four hashes and is the deterministic replacement.
- Observation: Go stores `MaxInt` and reports it through `Len`, `Has`, and `SortedArray`, but `Next(MaxInt)` returns `(MaxInt, false)`, `ForEach` skips it, and `String` renders `()`.
  Evidence: `/tmp/tidb_intset_probe.go` printed `max-int len=1 has=true next=(9223372036854775807,false) sorted=[9223372036854775807] each=[] string=()`.
- Observation: Go `Shift` uses machine-int wraparound. `1 + MaxInt` and `(MaxInt-1) + 2` both become `MinInt`; the small-only `Shift(MinInt)` fast path returns the input unchanged because the source negation and `uint32` conversion also wrap.
  Evidence: the same probe printed all three exact shifted arrays before any Rust production edit.

## Decision Log

- Decision: use the complete direct Go package `pkg/util/intset` as the atomic unit rather than ranked `pkg/expression/builtin_cast.go` as a partial file unit.
  Rationale: root `AGENTS.md` non-negotiable 6 forbids integrating or reporting a partial upstream Go package. Task #325 explicitly selected `pkg/util/intset` as the first unlocked leaf package for this reason, and it remains unlocked.
  Date/Author: 2026-08-07 / Codex
- Decision: reuse the checked-in generic `rust/difftests/tools/go_package_lockdown_inventory` AST generator and add an intset-specific checker rather than hand-maintain source line numbers.
  Rationale: content-addressed AST identities detect missing or changed functions, branches, loops, short-circuits, tests, rows, assertions, helpers, and benchmarks. Line-number inventories drift under harmless formatting and can omit an item from the initial baseline.
  Date/Author: 2026-08-07 / Codex
- Decision: keep Go test-only reference models, runtime stack-name seeding, and Go benchmark comparisons explicit in the inventory even when they are not production Rust APIs.
  Rationale: root package atomicity includes original test/support and benchmark artifacts. Such items may be `PORTED` to a Rust test owner or `DECLINED` with a measured test-harness/runtime reason, but may not disappear from the receipt.
  Date/Author: 2026-08-07 / Codex

## Outcomes & Retrospective

No completion outcome is claimed yet. Baseline execution is green, but completeness remains unproven until the inventory regenerates exactly, every `PORTED` owner compiles, missing boundaries are tested, and mutations demonstrate that the tests observe the rules they name.

## Context and Orientation

The Go package has one production file, two test files, and one Bazel build artifact. `pkg/util/intset/fast_int_set.go` wraps `golang.org/x/tools/container/intsets.Sparse` with a `uint64` cache for values 0 through 63. `fast_int_set_test.go` contains six top-level tests plus a map-backed reference model and random-test helpers. `fast_int_set_bench_test.go` contains six Go-runtime performance benchmarks. `BUILD.bazel` is part of the package artifact boundary even though Cargo, not Bazel, builds the Rust crate.

The Rust owner is `rust/crates/tidb-util/src/intset.rs`. It uses `BTreeSet<i64>` for the observable ordered sparse-set behavior and already implements the named Go methods. New inventory metadata belongs beside that file as `rust/crates/tidb-util/src/intset.inventory.tsv`. A package checker belongs under `rust/scripts/` and must run the generic Go AST tool, compare its exact content-addressed rows with the checked inventory, and reject artifact or zero-class drift. Rust tests in `intset.rs` compile-gate every public owner and verify every evidence test named by a `PORTED` row.

The package has no Go failpoint use. No integration-test result files, RealTiKV fixtures, generated Go code, platform variants, build tags, `go:generate`, `go:embed`, or tracked `testdata` are present; the manifest records each zero explicitly so later additions cannot be silently ignored.

## Plan of Work

First, generate the raw 534-row AST set from the generic Go tool. Add the build artifact and artifact-role/hash metadata around it, then classify each row. Production functions and control flow map to concrete `FastIntSet` Rust owners and named boundary evidence. Test rows/assertions map to the exact source-backed Rust test that executes them. Test-only reference helpers may map to the Rust reference-model test. Runtime-only random seeding and Go benchmark execution may be declined only with specific evidence that distinguishes harness mechanics from production semantics.

Second, compare every source method to Rust, not only the six test names. Strengthen or add named tests for the bitmap cutoff at 63/64, first large insertion and large-state persistence after removal/clear/copy, negative iteration versus `Next`, exact MaxInt sentinel, mixed-representation equality and set algebra, positive/negative shift boundaries and integer overflow behavior, inclusive full-bitmap range, invalid range panic text, exact `GetSmallUInt64` error text, and negative/pair/range string formatting. Any production difference is fixed before the row is marked `PORTED`.

Third, add two independent gates. The script gate regenerates artifacts and AST obligations from Go and compares exact hashes/identities/classifications. The Rust gate parses the checked TSV, asserts category/status totals, compiles public owners through typed function references, and verifies every named Rust test evidence still exists. Neither substring comments nor a manually maintained row count can satisfy both gates.

Fourth, commit a clean baseline before mutation probing. Save each mutated Rust source to an explicit temporary copy outside the repository, mutate one boundary rule at a time, require its named test to fail, restore bytes from the saved copy, verify byte equality, and rerun the test. A surviving mutation is a test finding and must be addressed or recorded before completion.

Finally, run the `tidb-util` and differential WIP gates. The Bazel prepare decision gate is required before claiming readiness, but the current diff changes only Rust, TSV, JSON, Python, and Markdown; it adds or modifies no Go or Bazel file and therefore does not trigger `make bazel_prepare`. The coordinator then reruns full workspace tests, fmt, clippy, `make lint`, direct inventory/ratchet checks, and non-force publication.

## Concrete Steps

Run all commands from `/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-344-wave8` unless a command explicitly changes to `rust/`.

Regenerate and check the Go side with:

    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 go test -v ./pkg/util/intset
    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 go run ./rust/difftests/tools/go_package_lockdown_inventory --root . --package pkg/util/intset
    PATH=/tmp/tidb-lockdown-341-go.8LuDjg/go/bin:$PATH GOTOOLCHAIN=go1.26.0 python3 rust/scripts/pkg-intset-lockdown.py

Run Rust WIP checks from `rust/` with:

    CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-344-wave8/tgt cargo nextest run -p tidb-util -j12 -E 'test(/intset/)'
    CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-344-wave8/tgt cargo nextest run -p tidb-util -j12 --no-fail-fast
    CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-344-wave8/tgt cargo nextest run -p difftest-result-tests -j12 --no-fail-fast
    CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-344-wave8/tgt cargo fmt -p tidb-util -- --check
    CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-lockdown-344-wave8/tgt cargo clippy -p tidb-util --all-targets -j12

Run repository checks from the worktree root only when the applicable profile requires them:

    make lint
    git diff --check

Never run `make bazel_lint_changed`; the user did not request that expensive optional sweep.

## Validation and Acceptance

Acceptance requires all four artifacts and all 534 AST obligations to regenerate exactly; every row must have one allowed status and non-empty evidence. Every `PORTED` production owner must compile and every `PORTED` source test/assertion/row must resolve to a named Rust evidence test. Every original Go top-level test must pass, every intset Rust test must pass, and deliberate mutations must fail their intended named tests before byte-for-byte restoration.

Behavioral acceptance is stronger than matching names. It includes all cutoff, ordering, representation, algebra, shift, range, error, panic, and formatting boundaries named above. A change to the Go source or `BUILD.bazel` must fail the package gate even when current Rust tests still pass. Removing or renaming a Rust owner or evidence test must fail independently.

## Idempotence and Recovery

Go inventory generation and check mode are deterministic and read-only. Cargo commands use only the worktree-exclusive `tgt/`. Generated inventory updates are performed by the checked package script, reviewed as a diff, and never hand-edited to hide a missing row.

Mutation probes start from a committed tree. Each touched source is copied to a unique directory under `/tmp`, restored with `cp` from that saved copy rather than Git checkout or stash, compared byte-for-byte, and followed by `git diff --check` plus the same named test. The untracked `tgt/` directory is never staged.

## Artifacts and Notes

Baseline artifact hashes at `56d06365ea` are unchanged from `66ef34195`:

    926298d33e0083e10af40a51224f60a43505930d717598b2ca2c8421acd98615  pkg/util/intset/BUILD.bazel
    f6d209b6d683a08076254e0e34c5456b4aad91214d2632322907375042fca0fb  pkg/util/intset/fast_int_set.go
    303615a2ea0e21f5abc19e1471ab1047f3a119796da6c47bb5b7036be1ce27e8  pkg/util/intset/fast_int_set_test.go
    652c2926fd3fe55bffa05b35277bd4e5e5d959e337a2a9a330933e465c8ef665  pkg/util/intset/fast_int_set_bench_test.go

The baseline Go run reports six passing tests. The generic AST categories total 534: production has one declaration, two fields, one constant, 28 functions, 100 branches, 12 loops, 34 short-circuits, and four closures; test/support has six tests, six benchmarks, twelve helpers, 65 table rows, 49 assertions, and the remaining test control-flow/support obligations.

The baseline Rust filter selected twelve tests because the regular expression also matched `disjointset::int_set`; all six `intset::tests` passed. Completion commands will use exact test names or a module-qualified filter where evidence must be isolated.

## Interfaces and Dependencies

No third-party dependency change is expected. Production remains `tidb_util::intset::FastIntSet` backed by Rust standard-library `BTreeSet<i64>`. Public method signatures remain stable unless a measured Go semantic cannot be represented safely; any such case is recorded before changing an interface. Inventory hashing uses the existing `sha2` dev dependency already used by neighboring `tidb-util` lockdown modules. The checker reuses the existing standard-library-only Go AST tool and Python standard library.

Security extension review: this unit adds no network, authentication, persistence, deployment, IAM, or secret surface, so SECURITY-01 through SECURITY-09 and SECURITY-11 through SECURITY-15 are not applicable. SECURITY-10 remains satisfied because no dependency or supply-chain source changes are planned and existing lockfiles remain authoritative.

Revision note: created on 2026-08-07 after recovering task #325's package decision, reading the current source at official tip, and measuring the four artifacts, 534 obligations, Go baseline, and Rust baseline.
