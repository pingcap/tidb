# Close the whole-chunk spill artifact

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root. This plan must be maintained according to it.

## Purpose / Big Picture

TiDB hash aggregation, sort, and TopN spill complete columnar batches to `DataInDiskByChunks` and later read those batches back. After this work, the accepted `pkg/util/chunk/chunk_in_disk.go` production artifact and its direct `chunk_in_disk_test.go` contract have explicit Rust behavior and receipt evidence: rows and auxiliary chunk state survive both allocating Get and caller-owned Fill, exact disk images remain compatible, short reads cannot deserialize partial data, quota and cleanup accounting stay balanced, and direct executor consumers still compile and pass their spill tests.

This is one artifact inside the larger incomplete `pkg/util/chunk` package. It must not be described as whole-package completion.

## Progress

- [x] (2026-08-10 03:55Z) Read the accepted production/test sources at `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`, the current Rust storage stack, all direct Rust consumers, and the 132 UNCLASSIFIED artifact obligations.
- [x] (2026-08-10 04:31Z) Added and ran the seven-case public source-shaped storage contract, including one combined deterministic replacement for the accepted random write/read failpoint. It passed against the existing implementation after correcting test-oracle setup mistakes; no production semantic gap was exposed.
- [x] (2026-08-10 05:22Z) Classified all 102 production and 30 direct-test obligations: 113 PORTED, 10 evidence-backed DECLINED Go-only failure-injection/runtime mechanics, and 9 structurally UNREACHABLE valid-input or fixed-positive-loop branches.
- [x] (2026-08-10 05:38Z) Ran and independently verified the measured deterministic failure-adaptation probe and all six semantic mutations. Every mutation passed at baseline, failed its named public assertion, restored the exact production bytes, passed after restoration, and independently verified.
- [x] (2026-08-10 05:50Z) Completed Ready validation, confirmed both remote tips were still the checkpoint base, and pushed exact checkpoint `8192aed2afd26949f1efe8b57d6750c3ad8cc957` to `origin/hparser-integration` and `ngaut/hparser-integration` without force.

## Surprises & Discoveries

- Observation: the current Rust serializer and deserializer already replay real Go byte fixtures, including auxiliary state and duplicate whole-column aliases.
  Evidence: `chunk_in_disk.rs::serialized_bytes_match_go`, `deserialize_reads_the_go_image_back`, and `chunk_identity_tests::codec_and_spill_images_repeat_duplicate_alias_slots` pass at baseline `6c93b76a71db29931e24f30507085eb8d6345453`.

- Observation: accepted `FillChunk` leaves an existing destination selection unchanged when the serialized image has no selection because `deserializeChunkData` only touches `chk.sel` when `selSize != 0`.
  Evidence: accepted `chunk_in_disk.go::deserializeChunkData`; current Rust carries the same branch deliberately.

- Observation: `injectChunkInDiskRandomError` is failpoint-only randomized test machinery. Rust already proves the user-visible failure class through a real create-file error in `hash_agg_spill_tests::test_random_fail`.
  Evidence: the accepted helper is called only at Add/read entry and contains only failpoint, random error, and sleep operations; Rust has no production caller or hidden random delay.

- Observation: the public artifact contract passes without any production edit.
  Evidence: `cargo test --offline --locked -j12 -p tidb-chunk --test chunk_in_disk_contract -- --nocapture` ran seven tests successfully, including a real create-file failure and a checksum-boundary truncated read; strict all-target Clippy also exited zero.

- Observation: a short spill read must be made to cross the checksum writer's flushed-file boundary; truncating a tiny image leaves all bytes in the live checksum cache and therefore does not model a truncated file read.
  Evidence: the exact-read contract uses a multi-block payload and truncates the flushed file before `fill_chunk`, which then returns an error without mutating the destination.

- Observation: the first exact-read mutant discarded the I/O error and then attempted to deserialize zero-filled bytes, aborting on a bogus huge allocation before Rust could report a named failed assertion.
  Evidence: the operator was replaced with a safe semantic mutant that turns premature EOF into `Ok(total)`. The public exact-read assertion then fails normally, and official run/verify evidence records that named failure.

## Decision Log

- Decision: preserve the native Rust `SpillStorage` authority instead of restoring Go's mutable process-global temporary-directory switch.
  Rationale: startup owns path, quota, encryption, and directory leasing; every live spill consumer already receives that immutable authority. Reintroducing a global would weaken security and lifecycle ownership.
  Date/Author: 2026-08-10 / Codex.

- Decision: classify Go capacity/growth branches by their observable byte/value behavior rather than recreating Go slice allocation geometry.
  Rationale: the goal explicitly excludes standard-library allocation mechanics unless TiDB behavior depends on them. Small, large, fresh, and reused buffers are covered by semantic boundary tests and exact file images.
  Date/Author: 2026-08-10 / Codex.

- Decision: decline randomized failpoint/sleep mechanics with a deterministic public filesystem-failure probe while porting all normal and real I/O error propagation.
  Rationale: the observable obligation is prompt error propagation and cleanup, not Go's random-number generator or goroutine timing.
  Date/Author: 2026-08-10 / Codex.

## Outcomes & Retrospective

The observable artifact contract and its receipt are closed without a production change. All 132 obligations have final verdicts (113 PORTED, 10 DECLINED, 9 UNREACHABLE), the deterministic failure adaptation is OBSERVED and independently VERIFIED, and six rules are guarded by independently KILLED mutations. The incremental checker advances to obligation `Oe3d590766a60c595` in the next unrelated artifact.

The full `tidb-chunk` crate, strict all-target Clippy, the hash-aggregation/sort/TopN spill consumers, direct-dependent all-target checks, and workspace all-target compilation pass. `make -j12 lint` exits zero; it still prints the repository's known internal-package diagnostic for `rust/difftests/gobinaryrow` and BSD `find -n` portability diagnostic. The source-size ratchet still reports the unchanged baseline `tidb-executor/src/kv_table.rs` overage (2,785 lines versus 2,200).

Checkpoint `8192aed2afd26949f1efe8b57d6750c3ad8cc957` was pushed without force to both authorized `hparser-integration` remotes after confirming both tips were still its base. This closes only the accepted whole-chunk spill artifact; the complete `pkg/util/chunk` package remains in progress.

## Context and Orientation

The accepted authority is `pkg/util/chunk/chunk_in_disk.go` and its direct test `chunk_in_disk_test.go` at source commit `665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f`. `DataInDiskByChunks` writes one native-endian image per Chunk into a checksum-framed, optionally AES-CTR encrypted temporary file. An offset table provides random chunk access. The image stores virtual rows, capacity, required rows, selection bytes, and each column's logical length, null bitmap, packed data, and variable offsets.

The Rust implementation is `rust/crates/tidb-chunk/src/chunk_in_disk.rs`; `DiskFileReaderWriter` in `chunk_util.rs` owns the checksum/encryption stack. Live consumers are hash aggregation in `tidb-executor/src/hash_agg/spill.rs`, sorted partitions in `sort_partition.rs`, and TopN runs in `topn_spill.rs`.

The package receipt lives at `rust/crates/tidb-chunk/tests/pkg_util_chunk_lockdown`. `ledgers/chunk_in_disk.go.tsv` contains 102 production obligations and `ledgers/chunk_in_disk_test.go.tsv` contains 30 direct-test obligations. Both begin this plan entirely UNCLASSIFIED.

## Plan of Work

Add `rust/crates/tidb-chunk/tests/chunk_in_disk_contract.rs`. The test owns isolated `SpillStorage` directories and calls only public APIs. It must cover two source-shaped chunks containing strings, NULLs, integers, and binary JSON; different capacity, required-row, virtual-row, and selection state; allocating Get and caller-owned Fill; a no-selection image refilled into a destination with an existing selection; a zero-column virtual chunk; a large image crossing checksum/cache boundaries; truncated-file rejection before destination mutation; empty-input and create-file error atomicity; quota accounting; idempotent close; and file deletion.

Do not add production conditionals merely for the tests. If the new contract exposes a semantic mismatch, first record a deterministic failing test, then fix the owning storage abstraction and broaden the regression to the whole failure class.

Register the new test and this ExecPlan in `package.toml`. Add exact compile-anchor symbols and semantic rules. Classify normal format, lifecycle, error, and accounting paths as PORTED. Use a measured probe for the Go-only randomized failpoint/sleep obligations and structural proof for direct-test zero-iteration branches whose accepted callers bind fixed positive chunk, row, and selection counts. Bind every PORTED rule to an independently killed mutation and retain all generated logs even though root `.gitignore` ignores `*.log`.

Finally run direct consumer tests/checks, Ready validation, a live remote fetch/merge loop, and non-force pushes to `origin/hparser-integration` and `ngaut/hparser-integration`.

## Concrete Steps

From repository root, WIP validation is:

    cd rust
    cargo fmt --all -- --check
    cargo test --offline --locked -j12 -p tidb-chunk --test chunk_in_disk_contract -- --nocapture
    cargo clippy --offline --locked -j12 -p tidb-chunk --all-targets -- -D warnings

The official incremental receipt check is:

    python3 rust/scripts/go-package-lockdown.py --root . check --spec rust/crates/tidb-chunk/tests/pkg_util_chunk_lockdown/package.toml --accepted-source-commit 665fc02e2be48a7199d5ffeb5d3d6bec1dfed04f

After this artifact closes, that checker must advance past both `chunk_in_disk` ledgers and stop only at the next unrelated incomplete artifact.

Ready validation additionally includes the full `tidb-chunk` crate, strict Clippy, exact hash-aggregation/sort/TopN spill tests, direct-dependent checks, workspace all-target compilation, and `make -j12 lint`.

The exact spill-consumer tests used by this checkpoint are:

    cargo test --offline --locked -j12 -p tidb-executor hash_agg_spill_tests --lib
    cargo test --offline --locked -j12 -p tidb-executor sort::tests::test_unparallel_sort_spill_disk --lib -- --exact
    cargo test --offline --locked -j12 -p tidb-executor topn::spill_tests::test_generate_topn_results_when_spill_only_once --lib -- --exact

## Validation and Acceptance

The public contract passes only when Get and Fill reproduce all physical rows and auxiliary state, no-selection Fill preserves the accepted destination-selection rule, zero-column virtual rows round-trip, the real file exists while live and is absent after close, truncated reads return an error without mutating the destination, real create-file failure leaves counters and trackers unchanged, and quota failure remains accounted until close then releases exactly to zero.

Every receipt mutation must pass at baseline, fail for its intended semantic assertion after the one mutation, restore the exact source bytes, pass again, and independently verify. Production source hashes before and after evidence collection must match.

## Idempotence and Recovery

All tests use uniquely named temporary directories and RAII cleanup. Mutation execution is safe to rerun only with a new attempt identifier; the official runner always restores source bytes in a `finally` path. If a mutation or merge fails, confirm the three storage sources match their committed hashes before continuing. Remote races are resolved only by another fetch, normal merge, and affected-gate rerun; never force push.

## Artifacts and Notes

Initial ledger counts:

    chunk_in_disk.go.tsv       UNCLASSIFIED 102
    chunk_in_disk_test.go.tsv  UNCLASSIFIED 30

Final ledger counts:

    PORTED       113
    DECLINED      10
    UNREACHABLE    9
    UNCLASSIFIED   0

Initial shipped base and both remote tips:

    6c93b76a71db29931e24f30507085eb8d6345453

## Interfaces and Dependencies

No new crate dependency is required. The production interface remains:

    pub struct DataInDiskByChunks;

    impl DataInDiskByChunks {
        pub fn new(field_types: Vec<FieldType>, file_name_prefix_for_test: &str, storage: Arc<SpillStorage>) -> Self;
        pub fn add(&mut self, chunk: &Chunk) -> Result<(), DiskError>;
        pub fn get_chunk(&mut self, chunk_index: usize) -> Result<Chunk, DiskError>;
        pub fn fill_chunk(&mut self, chunk_index: usize, destination: &mut Chunk) -> Result<(), DiskError>;
        pub fn close(&mut self);
    }

Revision note (2026-08-10 03:55Z): created the artifact plan after auditing accepted source, current Rust format/storage code, direct consumers, and exact ledger inventory.

Revision note (2026-08-10 04:25Z): recorded the green six-case public contract and strict Clippy result. No production code changed because the contract exposed no semantic mismatch.

Revision note (2026-08-10 04:31Z): added the combined deterministic write/read failure adaptation used by the measured probe; a multi-block payload is required so the read crosses the checksum writer's live cache into the truncated file.

Revision note (2026-08-10 05:38Z): closed all 132 obligations, recorded the verified measured probe and six independently killed mutations, and recorded the green full-crate and direct spill-consumer gates.

Revision note (2026-08-10 05:50Z): recorded the Ready profile, known baseline-only diagnostics, and exact checkpoint pushed to both `hparser-integration` remotes.
