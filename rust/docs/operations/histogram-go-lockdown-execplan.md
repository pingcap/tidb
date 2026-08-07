# Lock down pkg/statistics/histogram.go against tidb-stats

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. The collaboration contract supplied for task325 is the acceptance contract. This unit starts from accepted commit `163559e78020c57547e437089be1f28c3552f7a9` and owns only Rust crate `tidb-stats` for the complete Go source file `pkg/statistics/histogram.go`.

## Purpose / Big Picture

After this work, a contributor can prove that every production declaration and syntactic control-flow obligation in `pkg/statistics/histogram.go`, plus every source-owned Go test and support declaration, has an explicit Rust disposition. Ported behavior is executable and compile-anchored; declined or unreachable behavior has source-backed evidence. Source drift, inventory drift, Rust symbol deletion, and representative semantic mutations must fail named gates.

The unit branch is `codex/task325-tidb-stats-histogram-lockdown`. The collaboration contract originally required identical `origin` and `ngaut` refs, but the authenticated `dbsid` user cannot write `ngaut/tidb`. The user later explicitly selected direct publication through the official `pingcap/tidb` repository. No incomplete commit may be pushed; the final publication route and exact remote ref are verified only after the Ready gate.

## Progress

- [x] (2026-08-07) Read the complete task325 collaboration contract and repository instructions.
- [x] (2026-08-07) Fetched `origin` and `ngaut`, verified accepted base `163559e78020c57547e437089be1f28c3552f7a9` exists on both remotes, and found no task325 owner for `tidb-stats`.
- [x] (2026-08-07) Resolved rank 19 to complete source `pkg/statistics/histogram.go`; `TestMergePartitionLevelHist` and `TestMergeBucketNDV` call functions defined in that file. No package `doc.go` exists.
- [ ] Publish the ownership branch at the accepted base to both remotes (completed: `origin`; `ngaut` rejected user `dbsid` with permission denied; user-authorized official-only replacement remains pending until Ready).
- [x] (2026-08-07) Reused candidate commit `3d4e74200f994a8adeeebcfef43e5c128e01fa81` as local commit `7125a0cde4`; the pinned Go source and test hashes are identical on the accepted base.
- [x] (2026-08-07) Generated a deterministic AST-addressed inventory with 636 production obligations and 32 source-owned test/support obligations; all 668 rows have a stable anchor and node hash.
- [x] (2026-08-07) Replaced generic decline evidence with 11 source-backed evidence records; classified 498 rows `PORTED`, 169 `DECLINED`, and one measured `UNREACHABLE` closure.
- [x] (2026-08-07) Added byte/line/SHA ratchets for the Go owner, direct test, benchmark, adjacent test, Rust landing module, inventory, decline evidence, and mutation plan.
- [x] (2026-08-07) Replaced lexical production-symbol lookup with compile anchors for all 81 unique `PORTED` Rust symbols. Every `PORTED` row now names `rust-test:<fully-qualified-test>`, and a gate verifies the mapped test declaration exists.
- [x] (2026-08-07) Added Go-derived TopN-removal and out-of-range boundary tests from disposable failpoint-wrapped oracle probes.
- [x] (2026-08-07) Checked in and gated a 21-family structured mutation plan.
- [x] (2026-08-07) Completed WIP validation: three exact failpoint-wrapped Go tests passed with refcount restored to zero; Rust library tests passed 6/6 and aggregate tests 377/377; crate clippy with warnings denied, package formatting, and `git diff --check` passed.
- [x] (2026-08-07) Executed all 21 mutations at immutable provisional SHA `4576fa8aea3a0d713d66b403aaad381331fc1c83`; every intended test or compile gate failed, every target was restored, and every clean-status check passed.
- [x] (2026-08-07) Checked in and gated the 21-row structured mutation receipt, including exact tests, exit codes, decisive failures, restoration status, and clean-status confirmations.
- [x] (2026-08-07) Integrated the two task325 hardening commits onto official tip `66ef3419531d95089aa1b5f3e7ce7979a5a8a149`; the original histogram candidate was already present in that history.
- [x] (2026-08-07) Fixed an owner-external `tidb-datatype` test-isolation defect exposed by the Ready gate: every test that changes the global collation mode now restores the prior mode while holding the shared registry test lock.
- [x] (2026-08-07) Completed the Rust and exact Go portions of the clean Ready gate with exclusive target `/tmp/tidb-task325-histogram-integration.xtQFEm/target`: exact failpoint-wrapped Go tests, `tidb-stats --all-targets`, `tidb-datatype --all-targets`, full Rust workspace tests, workspace clippy, literal workspace formatting, ratchet hashes/counts, mutation-result gate, and `git diff --check` passed.
- [ ] Finish the repository Ready gate with `make -j12 lint`, then recheck the final committed candidate.
- [ ] Publish the complete result through the user-authorized official remote route and verify the exact ref with `git ls-remote`.

## Surprises & Discoveries

- Observation: the supplied accepted chain and the official `origin/hparser-integration` branch are independent histories. The accepted SHA is present through task325 refs on both remotes, not as an ancestor of either current hparser tip.
  Evidence: `git merge-base --is-ancestor 163559e780... origin/hparser-integration` and the corresponding `ngaut` check both returned status 1, while both remotes expose `refs/heads/codex/task325-time-go-lockdown` at `163559e780...`.

- Observation: rank 19 has a complete-looking implementation on another history, but that commit is not sufficient under the current handoff.
  Evidence: `3d4e74200` pins the correct Go hashes and contains 329 inventory rows, but all 86 `DECLINED` rows use one generic evidence string, the symbol gate searches source text, and only eight test rows are classified.

- Observation: line-number scanning undercounted the owner by nearly half and omitted expression-level control flow.
  Evidence: the accepted `go_package_lockdown_inventory` AST generator produced 668 stable obligations: 84 functions, 37 fields, nine constants, 344 branch outcomes, 90 loop outcomes, 66 short-circuit outcomes, six closures, and 32 source-owned test/support declarations.

- Observation: Go's `Histogram.Tp` pointer and `Histogram.Scalars` cache are representation fields, not missing Rust estimation behavior.
  Evidence: Rust buckets own already-decoded `Datum` bounds and `calc_fraction_from_datums` derives scalar geometry on demand. Both fields are `DECLINED` under `E11_DECODED_HISTOGRAM_LAYOUT`; their query behavior remains separately `PORTED` and tested.

- Observation: the only syntactically discovered but unreachable owner closure is the callback passed while building a TopN merge bucket.
  Evidence: a disposable Go oracle measured `bucket.ndv == 0` after the enable path, so `TopNMeta.buildBucket4Merging/closure:1` is the single `UNREACHABLE` row with evidence `measured_go_oracle_topn_bucket_ndv=0_after_enable`.

- Observation: direct Go probes found no new production mismatch, but they exposed boundary values absent from the inherited Rust receipt.
  Evidence: bulk TopN removal produced `(count, repeat, ndv)` values `(3,1,2)`, `(5,0,2)`, `(1,0,1)`; normal out-of-range estimation produced `Est=2.399000416493128`, `MinEst=1`, `MaxEst=9.596001665972512`; determinate mode produced `1.125` for all fields; an unsigned all-negative range produced zero. The new Rust tests pin those values.

- Observation: on the original owner branch, `cargo fmt --all -- --check` under the pinned Rust 1.97 toolchain reported owner-external pre-existing formatting deltas.
  Evidence: the only reported files were in `tidb-executor`, `tidb-expr`, and `tidb-session`; a trial format was reverted completely. Later official integration commits resolved that drift, and the literal workspace formatting gate now passes on the integration candidate.

- Observation: the current GitHub credential is `dbsid`; it can push to `pingcap/tidb` but cannot push to `ngaut/tidb`.
  Evidence: the ownership push created the `origin` branch at `163559e780...`; the identical `ngaut` push returned `permission denied`.

- Observation: the first full workspace run exposed a persistent test-state leak outside the histogram owner.
  Evidence: three `tidb-datatype::collation_tests` ended with the global new-collation mode set to `false`, so `the_registry_and_the_const_path_give_one_default_collation_per_charset` observed `gbk_bin` or `gb18030_bin` even with `--test-threads=1`. Commit `19074e6b2b` replaces the hard-coded cleanup with a test-only RAII guard; the focused tests, complete crate, and workspace now pass.

- Observation: literal workspace clippy with `-D warnings` has three owner-external warnings already present at `origin/hparser-integration`.
  Evidence: an unfiltered workspace clippy run reports only `assertions_on_constants` in `tidb-vardef`, `needless_update` in `tidb-util`, and `type_complexity` in `tidb-executor`. Re-running with exactly those three existing lint classes allowed and all other warnings denied passes. No unrelated source was changed to rewrite other lockdown owners.

## Decision Log

- Decision: own `pkg/statistics/histogram.go` in `tidb-stats`, not rank 18 `pkg/meta/meta.go`.
  Rationale: both surfaces are eligible under the handoff. The histogram candidate has byte-identical Go inputs and substantial source-backed Rust behavior that can be audited and hardened, whereas `TestMeta` spans a 2,219-line transactional mutator file and a Rust crate that intentionally has no transaction boundary.
  Date/Author: 2026-08-07 / Codex

- Decision: cherry-pick the prior histogram candidate and audit it as untrusted input rather than rewrite the same port.
  Rationale: source and test SHA-256 values match exactly, preserving behavior while avoiding duplicate implementation risk. The current contract remains authoritative, so inherited gates and evidence must be strengthened before delivery.
  Date/Author: 2026-08-07 / Codex

- Decision: use this repository ExecPlan instead of adding a second generic lifecycle document tree.
  Rationale: root `AGENTS.md` and `PLANS.md` prescribe a self-contained living ExecPlan for complex TiDB work. Duplicating the same state under another framework would add unrelated repository churn and weaken the handoff's repository-independent recovery path.
  Date/Author: 2026-08-07 / Codex

- Decision: replace the inherited line-oriented inventory with the accepted AST-addressed generator output rather than patching individual missing rows.
  Rationale: the contract requires every declaration and every syntactic control-flow locus. Stable AST anchors plus per-node hashes cover branch, loop, short-circuit, closure, field, and test/support obligations without depending on line numbers.
  Date/Author: 2026-08-07 / Codex

- Decision: require every `PORTED` row to carry one `rust-test:<fully-qualified-test>` identity selected by its Rust symbol, in addition to compile-anchoring the symbol itself.
  Rationale: the compile registry proves symbol existence while the test mapping proves an executable observer. Keeping these two gates separate makes a symbol-only stub or a stale test name fail independently.
  Date/Author: 2026-08-07 / Codex

- Decision: classify Go-only chunk, codec, protobuf, ranger, allocator, benchmark-runtime, and decoded-layout representation boundaries as explicit `DECLINED` rows instead of adding adapter stubs to `tidb-stats`.
  Rationale: the Rust crate owns decoded histogram arithmetic. Pulling planner ranges, protobuf transport, Go chunk pools, or ABI sizing into this crate would violate the existing dependency boundary without increasing behavioral parity.
  Date/Author: 2026-08-07 / Codex

- Decision: replace the unavailable dual-remote publication route only at the final publication step, not by weakening contributor validation.
  Rationale: `dbsid` cannot write `ngaut/tidb`, and the user explicitly authorized direct publication to official `pingcap/tidb`. All source, mutation, Ready, and clean-worktree requirements remain unchanged before any push.
  Date/Author: 2026-08-07 / Codex

## Outcomes & Retrospective

Work is in final integration. The 668-row source boundary, 11 decline proofs, 81 compile-anchored symbols, `rust-test:` evidence gate, and 21-family mutation plan are present. All 21 mutations were killed at immutable provisional SHA `4576fa8aea3a0d713d66b403aaad381331fc1c83`, restored, and checked into the structured results receipt. Exact Go tests and the complete Rust Ready surface now pass on the integration candidate, including the repaired collation-mode isolation. Repository lint, final publication verification, and cleanup remain incomplete; no final completion claim is made yet.

## Context and Orientation

`pkg/statistics/histogram.go` is the Go source of truth for histogram bucket storage, selectivity estimates, bucket merging, partition-to-global histogram merging, out-of-range estimates, and load-state metadata. Its direct source test is `pkg/statistics/histogram_test.go`; `pkg/statistics/histogram_bench_test.go` owns the partition-merge benchmark and helpers; `pkg/statistics/statistics_test.go::TestMergeHistogram` and `mockHistogram` are adjacent source-owned test support.

The landing crate is `rust/crates/tidb-stats`. Production behavior is in `src/histogram.rs`, row-estimate helpers are in `src/row_estimate.rs`, and source-backed integration gates are in `tests/histogram_source.rs`. The inventory is `src/histogram.inventory.tsv`. `tidb-stats` is dependency-closed: it accepts decoded `tidb-datatype::Datum` values and intentionally does not own Go `chunk.Chunk`, `ranger.Range`, tablecodec, or session-variable machinery.

The final AST inventory has 636 production obligations and 32 source-owned test/support obligations. Its 668 rows contain 84 functions, 37 fields, nine constants, seven other declarations, 344 branch outcomes, 90 loop outcomes, 66 short-circuit outcomes, six closures, eight tests, one benchmark, six test helpers, three test-support constants, three test-support declarations, and one test-support variable. The verdict counts are 498 `PORTED`, 169 `DECLINED`, and one `UNREACHABLE`; the ported set names 81 unique compile-anchored Rust symbols.

## Plan of Work

First, keep the AST inventory self-authenticating. It pins source byte, line, and SHA values; every associated test/support declaration is classified; decline evidence IDs quote the Go boundary and name the concrete Rust architectural seam or measured probe. Regenerating the AST inventory must reproduce the same 668 stable obligations before a source ratchet is updated.

Second, keep symbol and observer evidence separate. Compile-time anchors cover every unique Rust symbol represented by a `PORTED` row, including private merge helpers. A second map assigns every symbol a fully-qualified running boundary test; the inventory gate requires every `PORTED` evidence field to equal that `rust-test:` identity and verifies that the corresponding source still declares `#[test]`.

Third, exercise behavior using WIP verification. Run the exact Go merge tests through the failpoint wrapper and keep the disposable Go oracle measurements in this receipt. Run all `tidb-stats` targets offline and locked, then crate clippy with warnings denied and package-scoped formatting checks. Rust-only edits do not trigger `make bazel_prepare`; this decision must be re-evaluated if a Go or Bazel file is added or an existing Go import/test function changes.

Fourth, commit a provisional immutable SHA and create a disposable worktree. Execute the 21 rows in `src/histogram.mutation-plan.tsv`: apply one small mutation, run the intended fully-qualified test or compile gate, record the failing identity and exit status in `src/histogram.mutation-results.tsv`, restore the exact bytes, and verify the disposable worktree is clean before the next mutation.

Finally, check in the killed-mutation receipt, then run the Ready gate in a new clean detached worktree with a new exclusive Cargo target directory. This includes exact Go tests, the complete `tidb-stats` crate, crate and workspace clippy, inventory/drift/symbol/mutation-result gates, workspace tests, literal workspace formatting, `git diff --check`, direct ratchet greps, and `make -j12 lint`. Only then publish through the user-authorized official remote route and verify the exact remote ref.

## Concrete Steps

The Go and Git commands run from `/Users/chenhuansheng/Documents/GitHub/tidb-task325-tidb-stats-histogram`. Cargo and formatting commands run from its `rust/` subdirectory.

    ./tools/check/failpoint-go-test.sh pkg/statistics -run '^(TestMergePartitionLevelHist|TestMergeBucketNDV|TestMergeHistogram)$' -count=1
    cd rust
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-task325-tidb-stats-histogram/tgt cargo test --offline --locked -j12 -p tidb-stats --all-targets
    CARGO_BUILD_JOBS=12 CARGO_TARGET_DIR=/Users/chenhuansheng/Documents/GitHub/tidb-task325-tidb-stats-histogram/tgt cargo clippy --offline --locked -j12 -p tidb-stats --all-targets -- -D warnings
    cargo fmt -p tidb-stats -- --check
    cd ..
    git diff --check

The clean Ready worktree and target paths will be created with `mktemp -d` and recorded here before use. Cargo commands sharing a target directory run serially.

The official integration candidate uses clean worktree `/tmp/tidb-task325-histogram-integration.xtQFEm/repo` and exclusive target `/tmp/tidb-task325-histogram-integration.xtQFEm/target`. Its final Rust commands are:

    PATH=/Users/chenhuansheng/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin:$PATH CARGO_TARGET_DIR=/tmp/tidb-task325-histogram-integration.xtQFEm/target cargo test --offline --locked -j12 --workspace
    PATH=/Users/chenhuansheng/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin:$PATH CARGO_TARGET_DIR=/tmp/tidb-task325-histogram-integration.xtQFEm/target cargo clippy --offline --locked -j12 --workspace --all-targets
    PATH=/Users/chenhuansheng/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.26.0.darwin-arm64/bin:$PATH CARGO_TARGET_DIR=/tmp/tidb-task325-histogram-integration.xtQFEm/target cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings -A clippy::assertions_on_constants -A clippy::needless_update -A clippy::type_complexity
    cargo fmt --all -- --check

## Validation and Acceptance

Acceptance requires the exact Go tests to pass; all `tidb-stats` targets and clippy to pass; source, byte, line, inventory, compile-symbol, boundary-test, decline, unreachable, mutation-plan, and mutation-result gates to pass; all 21 recorded mutations to be killed; formatting and diff checks to pass; the full Rust workspace and repository lint to pass in a clean detached worktree; and the user-authorized official remote ref to resolve to the published full SHA.

No narrow crate result can support a full completion claim. A surviving mutation, generic decline rationale, missing source-owned helper, lexical-only production-symbol check, stale or nonexistent `rust-test:` identity, dirty final worktree, or unmatched remote ref rejects the branch.

## Idempotence and Recovery

Fetches, hash/count checks, tests, clippy, fmt checks, and lint are safe to rerun. Any final push is non-force and targets the exact user-authorized official ref. Mutation work happens only in a disposable worktree at an immutable provisional commit; every probe saves and restores the exact mutated file and checks `git status --short` before proceeding.

Do not remove any existing worktree or broad target directory. Once the final SHA is recoverable from the verified official ref, remove only the exact disposable worktree and exact exclusive target/cache paths created for this unit.

## Artifacts and Notes

Current local candidate commit:

    7125a0cde4 (cherry-pick of 3d4e74200)

Official integration candidate before this ExecPlan update:

    98c9980ad1 rust: harden histogram source lockdown evidence
    aa30e74704 statistics: record histogram lockdown mutations
    19074e6b2b rust: isolate collation mode tests

Immutable mutation provisional commit:

    4576fa8aea3a0d713d66b403aaad381331fc1c83

Current remote ownership state:

    origin codex/task325-tidb-stats-histogram-lockdown = 163559e78020c57547e437089be1f28c3552f7a9
    ngaut codex/task325-tidb-stats-histogram-lockdown = absent (permission denied)

Pinned Go inputs already verified equal between the accepted base and the reused candidate:

    pkg/statistics/histogram.go      1233e0a3430067400eaee5d562772cc83541fce8ae8b3e4579895a574c8c1024
    pkg/statistics/histogram_test.go 8adb0d249a37ffa08c859ea1709426cfc0e98c4fc5a7ff689726fce0a1904a7a

Current AST inventory receipt:

    total obligations                   668
    production obligations              636
    source-owned test/support            32
    PORTED / DECLINED / UNREACHABLE 498 / 169 / 1
    unique compile-anchored symbols       81
    mutation rule families                21
    killed mutations                      21
    restored mutations                    21
    clean-status confirmations            21

Disposable failpoint-wrapped Go oracle values added to Rust boundary tests:

    bulk removal buckets: (3,1,2), (5,0,2), (1,0,1)
    normal out of range:  Est=2.399000416493128 MinEst=1 MaxEst=9.596001665972512
    determinate mode:     Est=MinEst=MaxEst=1.125
    unsigned negatives:   Est=MinEst=MaxEst=0

WIP validation receipt:

    ./tools/check/failpoint-go-test.sh pkg/statistics -run '^(TestMergePartitionLevelHist|TestMergeBucketNDV|TestMergeHistogram)$' -count=1
      PASS; failpoint refcount 0 -> 1 -> 0
    cargo test --offline --locked -j12 -p tidb-stats --all-targets
      library 6 passed; aggregate 377 passed
    cargo clippy --offline --locked -j12 -p tidb-stats --all-targets -- -D warnings
      passed
    cargo fmt -p tidb-stats -- --check
      passed
    git diff --check
      passed

Integration Ready receipt before repository lint:

    ./tools/check/failpoint-go-test.sh pkg/statistics -run '^(TestMergePartitionLevelHist|TestMergeBucketNDV|TestMergeHistogram)$' -count=1
      PASS; failpoint refcount 0 -> 1 -> 0
    cargo test --offline --locked -j12 -p tidb-stats --all-targets
      library 6 passed; aggregate 378 passed
    cargo test --offline --locked -j12 -p tidb-datatype --all-targets
      library 284 passed; aggregate 64 passed
    cargo test --offline --locked -j12 --workspace
      passed, including doc tests
    cargo clippy --offline --locked -j12 --workspace --all-targets
      passed with exactly three owner-external warnings listed in Surprises & Discoveries
    cargo clippy --offline --locked -j12 --workspace --all-targets -- -D warnings -A clippy::assertions_on_constants -A clippy::needless_update -A clippy::type_complexity
      passed
    cargo fmt --all -- --check
      passed
    git diff --check
      passed
    LC_ALL=C shasum -a 256 rust/crates/tidb-stats/src/histogram.mutation-results.tsv
      9a8c7f4305192a665c2ef856a5542423e4ca48556525cf9d3851ad71e827add5

The literal `cargo fmt --all -- --check` passes on the integration candidate; no formatting command changed the worktree.

## Interfaces and Dependencies

The public Rust boundary remains `tidb_stats::histogram`: `Histogram`, `Bucket`, row-count estimation methods, `merge_histograms`, and `merge_partition_histograms`. It continues to depend on `tidb-datatype` for `Datum` and collation semantics and on `tidb-util` for established crate utilities. No new external runtime dependency is expected. `sha2` remains a test-only dependency for drift gates.

Revision 2026-08-07: replaced the provisional line inventory with the complete 668-row AST receipt; recorded field-boundary decisions, Go oracle values, 81 symbol/test mappings, the 21-family mutation plan, the workspace-formatting finding, and the user-authorized official publication route.
