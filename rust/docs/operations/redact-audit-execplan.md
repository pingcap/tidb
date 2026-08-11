# Complete `pkg/util/redact` as one atomic Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB uses `pkg/util/redact` to suppress log values, unwrap marker-delimited diagnostic text, and render stream-backup task configuration without exposing cloud credentials. The Rust SQL node contains the scalar and marker helpers, but it explicitly omits `TaskInfoRedacted`; therefore a Rust caller cannot safely log the same `kvproto` task type and the package cannot be claimed complete.

After this plan is complete, Rust will expose the whole direct Go package at one pinned source revision. Its tests will reproduce the unchanged Go package tests plus the existing BR integration test that checks exact compact protobuf text for S3, GCS, and Azure and proves the input task is not mutated. One semantic receipt, one commit, and one linear push will represent the complete Go package.

## Progress

- [x] (2026-08-11 11:39Z) Fixed the three-file direct Go inventory at source commit `ae70341a13841576092042b0f2e87dfdf6b675db`; confirmed there is no `doc.go` and no package failpoint use.
- [x] (2026-08-11 11:39Z) Ran the unchanged direct Go tests; all three passed.
- [x] (2026-08-11 11:39Z) Ran BR `TestRedactBackend` through the failpoint wrapper; it passed and failpoints were disabled afterward.
- [x] (2026-08-11 11:39Z) Ran the existing Rust redact tests; all four passed, while the production module still declares the package incomplete.
- [x] (2026-08-11 11:39Z) Added the BR-derived Rust regression test; before the fix it failed with unresolved `tidb_proto` and `TaskInfoRedacted` errors.
- [x] (2026-08-11 11:59Z) Added dependency-closed `kvproto` inputs, wire-contract coverage, exact gogo compact text, and nonmutating `TaskInfoRedacted`; ten focused redact tests pass.
- [x] (2026-08-11 11:59Z) Replaced the poisonable mode mutex with atomic state and aligned invalid-mode assertions, Scanner limits, invalid UTF-8, file output, and uppercase key behavior.
- [x] (2026-08-11 12:14Z) Added the semantic receipt; the WIP gate accepted one package and ran two unique commands.
- [x] (2026-08-11 12:14Z) Completed pre-sync Ready validation, Bazel decision gate, generated-tool cleanup, and staged diff self-review.
- [x] (2026-08-11 12:23Z) Committed the one-package change and rebased its single commit onto fetched remote commit `939fa0c76aeb26975396dec1fefff79c033f454f`.
- [x] (2026-08-11 12:23Z) Repeated the Ready profile after that rebase: both Go oracles, the semantic gate, complete owning-crate tests and doctests, formatting, all-target clippy, and repository lint passed.
- [ ] Fetch once more, rebase and repeat validation if the remote advanced, then push linearly and verify the remote SHA.

## Surprises & Discoveries

- Observation: the module's incompleteness comment incorrectly says `TaskInfoRedacted` has no upstream test.
  Evidence: `br/pkg/streamhelper/advancer_test.go::TestRedactBackend` checks exact output for all three credential-bearing backends and checks the original task after each rendering.

- Observation: Go redacts S3 `sse_kms_key_id` even when the input field is empty, so the redacted output contains a field absent from the original text.
  Evidence: the S3 assertion expects `sse_kms_key_id:"[REDACTED]"` while the original assertion omits that zero-valued field.

- Observation: Azure replaces the entire customer-key message, not only its two strings.
  Evidence: the redacted assertion contains only `encryption_key`; the original contains both `encryption_key` and `encryption_key_sha256`.

- Observation: the existing Rust process-wide mode uses `Mutex<String>`, which can become poisoned after a panic although Go's `atomic.String` has no equivalent poisoned state.
  Evidence: both `init_redact` and `need_redact` call `lock().unwrap()`.

- Observation: pinned gogo compact text duplicates every nonzero BR/encryption enum name and sorts map keys.
  Evidence: a Go probe printed `compression_type:ZSTDZSTD`, `cipher_type:AES256_CTRAES256_CTR`, and CloudDynamic attrs in `a`, `z` order; `text_gogo.go` writes through twice when its enum registry lookup misses.

- Observation: Go silently truncates de-redaction at a 65536-byte Scanner token and converts every invalid UTF-8 byte to one replacement rune.
  Evidence: probes produced both lines for 65535 bytes, no output for 65536 bytes, and `61efbfbdefbfbd62` for input bytes `61fffe62`.

- Observation: `DeRedactFile` opens and truncates its output before scanning its already-open input, so identical paths produce an empty file.
  Evidence: source order is `os.Open(input)`, `os.OpenFile(output, O_TRUNC...)`, then `DeRedact`; Rust now uses that same order and has an identical-path regression.

- Observation: the all-fields oracle confirms every credential replacement and all preserved fields in source declaration order.
  Evidence: exact Go outputs cover all 17 S3 fields, all six GCS fields, all Azure fields, all Bucket fields and map ordering, and all nested Azure/GCP/AWS KMS fields.

- Observation: the unboxed generated KMS oneof made `MasterKey` at least 384 bytes and failed all-target clippy.
  Evidence: `clippy::large-enum-variant` identified `MasterKeyKms` versus the 24-byte file variant; configuring prost to box only `.encryptionpb.MasterKey.backend.kms` preserves wire bytes and removes the generated memory-layout warning.

## Decision Log

- Decision: Use a dependency-closed projection of the pinned `kvproto` BR task protocol in the existing `tidb-proto` prost generation chain.
  Rationale: `TaskInfoRedacted` accepts a real protocol type. A handwritten utility-only mirror would invent a parallel API and could omit oneof branches or security configuration; generating from checked-in protocol input preserves field numbers, cardinality, and native workspace boundaries.
  Date/Author: 2026-08-11 / Codex

- Decision: Keep the production owner in `tidb-util::redact` and depend on `tidb-proto` rather than create another crate.
  Rationale: the missing Go API belongs to `pkg/util/redact`; `tidb-proto` already owns generated wire contracts, while redaction and compact rendering are utility behavior.
  Date/Author: 2026-08-11 / Codex

- Decision: Test exact compact text and post-call input equality for S3, GCS, and Azure, then add source-derived coverage for every noncredential storage branch and security shape.
  Rationale: the BR test is the behavioral oracle for credential scrubbing. The production switch intentionally leaves Noop, Local, CloudDynamic, and HDFS unchanged, and package completeness must retain those variants and unrelated task fields even though the Go integration test does not enumerate them.
  Date/Author: 2026-08-11 / Codex

- Decision: Represent the process-wide redact mode with `AtomicU8` constants.
  Rationale: the state has only empty, OFF, and ON values. An atomic integer preserves Go's lock-free, nonpoisoning semantics without storing borrowed strings or adding failure paths.
  Date/Author: 2026-08-11 / Codex

## Outcomes & Retrospective

Implementation and post-sync Ready validation are complete, but publication is not. The initial four-test partial module now has generated dependency-closed protocol types, exact BR compact text, source-derived Scanner/file/encoding behavior, a nonpoisoning atomic mode, eleven focused utility tests, and an independent protocol wire test. Both Go oracles, the semantic gate, full owning-crate tests, formatting, all-target clippy, repository lint, and the Bazel decision gate pass on the rebased tree. One final fetch, any validation required by a new remote advance, linear publication, and remote-SHA verification remain before the package can be reported complete.

## Context and Orientation

The accepted Go package consists only of `pkg/util/redact/BUILD.bazel`, `pkg/util/redact/redact.go`, and `pkg/util/redact/redact_test.go` at `ae70341a13841576092042b0f2e87dfdf6b675db`. The production file contains mode-based `String`/`Stringer`, line-oriented `DeRedact`/`DeRedactFile`, global redact state, `Value`, upper-case hexadecimal `Key`, `WriteRedact`, and `TaskInfoRedacted`. There is no package `doc.go`.

`TaskInfoRedacted.String` accepts `kvproto/pkg/brpb.StreamBackupTaskInfo`. It shallow-copies the task and storage wrapper, copies only the selected S3, GCS, or Azure configuration, replaces credential fields, and calls gogo protobuf `CompactTextString`. Nil task info renders `nil`. The closest authoritative consumer test is `br/pkg/streamhelper/advancer_test.go::TestRedactBackend`, last changed at `a239f120bbb14b4d095f0c1bcb4cc81c0d376afb`.

The Rust owner is `rust/crates/tidb-util/src/redact.rs`, exported by `rust/crates/tidb-util/src/lib.rs`. Protocol generation belongs to `rust/crates/tidb-proto`: its checked-in `.proto` inputs are compiled from `build.rs` and exported from `src/lib.rs`. The pinned Go module dependency is `github.com/pingcap/kvproto v0.0.0-20260622063236-b41e86365ce0` from `go.mod`. A semantic receipt is a TOML file consumed by `rust/scripts/semantic-package-gate.py`; it freezes direct Go package bytes and inventory and runs scoped Cargo commands.

## Plan of Work

First add the BR-derived Rust test before the implementation. It constructs the same three task values, compares the exact Go compact strings, and compares the input before and after. Run it and retain the unresolved-type or unresolved-function compiler failure as the required pre-fix evidence.

Next add checked-in protocol projections for `StreamBackupTaskInfo`, `StorageBackend`, every storage oneof branch, and the dependency-closed stream-security graph. Preserve original package names, field numbers, scalar/repeated/oneof cardinality, enum values, and map fields from kvproto. Extend `tidb-proto/build.rs` and `src/lib.rs`, then add a path dependency from `tidb-util`.

Implement `TaskInfoRedacted` in `tidb-util::redact`. Clone the task before modification so Rust callers receive the same observable nonmutation guarantee as Go. Replace exactly the Go fields: S3 access key, secret key, and KMS key ID; GCS credentials blob; Azure shared key and access signature, plus a new customer-key message containing only `[REDACTED]` as its encryption key. Render present, nondefault task fields in protobuf field order using gogo compact-text spelling and escaping. Keep all other backend and security values unchanged.

Finish the direct-source audit by converting global state to `AtomicU8`, retaining Go's invalid-mode behavior outside assertion-enabled builds, and documenting unavoidable adapter differences for UTF-8 file input and Go Scanner's token limit. Add `rust/crates/tidb-util/tests/redact.semantic.toml`, update this plan with evidence, and run its package gate.

Finally run the Ready profile for both changed owning crates, formatting, all-target clippy, and `make lint`. Inspect the actual diff through the Bazel prepare gate. Fetch the current remote branch, rebase the one-package commit if necessary, rerun all validation, push without force to `hparser-integration`, and verify the remote SHA equals the local SHA.

## Concrete Steps

Run the direct Go oracle from `pkg/util/redact`:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH /Users/chenhuansheng/.cache/codex-go1.25.10/go/bin/go test -run '^(TestRedact|TestDeRedact|TestRedactInitAndValueAndKey)$' -tags=intest,deadlock

Run the BR oracle from repository root; the wrapper must report `new_refcount=0` during cleanup:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH ./tools/check/failpoint-go-test.sh br/pkg/streamhelper -run '^TestRedactBackend$' -count=1

Run focused Rust tests and the package gate from repository root:

    cd rust && CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-proto
    cd rust && CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util --lib redact::tests
    python3 rust/scripts/semantic-package-gate.py rust/crates/tidb-util/tests/redact.semantic.toml

Run the Ready gates from `rust`, except repository lint from the root:

    cargo fmt --all --check
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-proto
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo test --offline --locked -j12 -p tidb-util
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-proto --all-targets -- -D warnings
    CARGO_TARGET_DIR=/tmp/tidb-package-audit.DnxFlT/rust/target cargo clippy --offline --locked -j12 -p tidb-util --all-targets -- -D warnings
    make lint

## Validation and Acceptance

Acceptance requires unchanged Go tests to pass and Rust to reproduce their behavior. In particular, S3, GCS, and Azure rendering must byte-for-byte equal `TestRedactBackend`; after every rendering, the original Rust task must compare equal to its pre-call clone. Nil info must render `nil`. All storage variants and all unrelated task/security fields must survive redaction unchanged, while only the credential fields listed above change.

The semantic gate must accept exactly `pkg/util/redact` at its pinned source and run focused `tidb-proto` and `tidb-util` tests. Full tests for both owning crates, format, both all-target clippy commands, and `make lint` must pass. The final diff must contain no Go source, accepted Go package, Bazel, or Go module edits. The final publication must be a single package commit based on the latest remote `hparser-integration`, pushed linearly without force, with matching local and remote SHAs.

## Idempotence and Recovery

All Go, Cargo, semantic-gate, formatting-check, clippy, and lint commands are safe to rerun. The failpoint wrapper is serialized and always disables failpoints on exit; do not invoke the controller directly. Protobuf output is generated into Cargo `OUT_DIR`, so only checked-in `.proto` inputs and build configuration are edited. If the shared Cargo target runs out of space, delete only `/tmp/tidb-package-audit.DnxFlT/rust/target/debug/incremental` with an exact-path `find ... -depth -delete`, then rerun the same command.

If `make lint` leaves ignored binaries under `tools/bin`, remove only a confirmed file or symlink with `unlink`; never recursively remove the directory. If remote advances, fetch and rebase only the one local package commit, resolve actual overlapping evidence, and repeat every post-rebase gate before pushing.

## Artifacts and Notes

Initial direct Go oracle:

    PASS
    ok github.com/pingcap/tidb/pkg/util/redact 0.853s

Initial BR oracle:

    PASS
    ok github.com/pingcap/tidb/br/pkg/streamhelper 0.958s
    [failpoint-state] new_refcount=0

Initial Rust seed:

    running 4 tests
    test result: ok. 4 passed; 0 failed; 0 ignored; 319 filtered out

Pre-fix regression evidence:

    error[E0433]: cannot find module or crate `tidb_proto` in this scope
    error[E0422]: cannot find struct, variant or union type `TaskInfoRedacted` in this scope
    error: could not compile `tidb-util` (lib test) due to 4 previous errors

Focused implementation evidence:

    test tests::br_stream_backup_task_projection_preserves_wire_contract ... ok
    test result: ok. 1 passed; 0 failed; 0 ignored; 9 filtered out
    running 11 tests
    test result: ok. 11 passed; 0 failed; 0 ignored; 319 filtered out

Pre-sync Ready evidence:

    semantic package gate: 1 packages, 2 unique commands
    tidb-proto unit tests: 10 passed; integration tests: 15 passed
    tidb-util unit tests: 329 passed; 1 helper ignored; integration tests: 22 passed
    tidb-util doctest: 1 passed; tidb-proto doctests: 0 tests
    cargo fmt --all --check: exit 0
    cargo clippy -p tidb-proto -p tidb-util --all-targets -- -D warnings: exit 0
    make lint: exit 0
    make bazel_prepare: not required; no Go, Bazel, Go module, import, or Go test-function diff

Post-sync Ready evidence on base `939fa0c76aeb26975396dec1fefff79c033f454f`:

    direct Go oracle: PASS; ok github.com/pingcap/tidb/pkg/util/redact 0.430s
    BR oracle: PASS; ok github.com/pingcap/tidb/br/pkg/streamhelper 0.695s; new_refcount=0
    semantic package gate: 1 packages, 2 unique commands
    complete tidb-proto and tidb-util unit, integration, and doctest runs: exit 0
    tidb-util library tests: 335 passed; 1 helper ignored
    cargo fmt --all --check: exit 0
    cargo clippy -p tidb-proto -p tidb-util --all-targets -- -D warnings: exit 0
    make lint: exit 0
    make bazel_prepare: not required; no Go, Bazel, Go module, import, or Go test-function diff

The accepted direct Go inventory is:

    pkg/util/redact/BUILD.bazel
    pkg/util/redact/redact.go
    pkg/util/redact/redact_test.go

## Interfaces and Dependencies

`rust/crates/tidb-proto` will export the generated `backup` and `encryptionpb` modules. `backup::StreamBackupTaskInfo` owns an optional `backup::StorageBackend`; the backend owns a prost oneof covering Noop, Local, S3, GCS, CloudDynamic, HDFS, and AzureBlobStorage. The task security oneof references `backup::CipherInfo` or `backup::MasterKeyConfig`; master-key configuration reaches the generated `encryptionpb` messages.

`rust/crates/tidb-util/src/redact.rs` will define:

    pub struct TaskInfoRedacted<'a> {
        pub info: Option<&'a tidb_proto::backup::StreamBackupTaskInfo>,
    }

and implement `std::fmt::Display` so `TaskInfoRedacted { info: None }.to_string()` is `nil` and a present task renders the redacted compact protobuf text. Existing mode, marker, file, value, key, and writer APIs remain available.

Plan revision note: created after direct inventory pinning, source review, both unchanged Go oracle runs, failpoint cleanup, existing Rust baseline, protocol dependency inventory, and implementation-option review; updated after the pre-fix failure, implementation, protocol wire test, Scanner/UTF-8/file-order probes, all-fields Go goldens, focused Rust passes, semantic gate, generated-layout clippy fix, pre-sync Ready validation, remote rebase, and post-sync Ready repetition.
