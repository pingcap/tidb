# Transcreate and certify `pkg/util/keydecoder` as one Rust package

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` current while work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

TiDB exposes decoded row and index keys in `INFORMATION_SCHEMA.DEADLOCKS.KEY_INFO`. After this package is complete, Rust decodes the same physical table keys against the statement's catalog snapshot and emits the same database, table, partition, handle, index, and value JSON fields as Go. The package is delivered atomically with its direct contract, both catalog adapters, and the live system-table consumer.

## Progress

- [x] (2026-08-12) Fixed the four-artifact Go inventory and accepted source pin `b2f2faaa95bff642920560c24e3714957bcb4c74`; current package bytes match the pin.
- [x] (2026-08-12) Confirmed there is no `doc.go`, failpoint, generated input, fixture, benchmark, fuzz target, example, platform/build-tag variant, `go:generate`, or `go:embed`; inventoried `TestDecodeKey` and `TestMain`.
- [x] (2026-08-12) Ran the original Go test normally and under race and captured source-derived JSON, nil-slice, swallowed-index-error, and partial-record-error outputs with a temporary external probe.
- [x] (2026-08-12) Implemented the direct Rust contract against `ClusterCatalog` and found the physical-partition error-ID regression through a fail-before-fix assertion.
- [x] (2026-08-12) Re-synchronized to remote commit `7e818c27f51013054d11cffde20e81098ec3f48d`, discovered its new live `DEADLOCKS.KEY_INFO` path, recorded the path returning `NULL`, and made the same test pass with Go-shaped JSON.
- [x] (2026-08-12) Moved the decoder core below session, adapted persisted and executor catalogs, retained cluster database IDs during conversion, and wired the live consumer without changing transaction code.
- [x] (2026-08-12) Completed post-redesign WIP and Ready validation, including all touched crates, repository lint, semantic/source/inventory gates, and exact clean-base reproduction of the two unrelated full-session failures.
- [ ] Synchronize with a fresh `hparser-integration` tip, repeat Ready after any rebase, commit one complete Go package, push without force, and verify all three SHAs.

## Surprises & Discoveries

- Observation: `go test -list .` cannot be used as a successful inventory command for this package.
  Evidence: `TestMain` runs goleak while `-list` skips `TestDecodeKey`, so the test's `defer view.Stop()` never stops the OpenCensus worker. The command lists `TestDecodeKey` and then fails on that worker; the Bazel target is already marked flaky. Normal and race executions both pass.

- Observation: Go handles record and index decode failures differently after catalog metadata is found.
  Evidence: a corrupt record payload returns populated database/table fields plus `cannot decode record key of table 1`; a corrupt index payload returns those fields with nil error and zero `index_id`.

- Observation: an index key whose ID is absent from a known table retains `index_id` but discards its successfully decoded values.
  Evidence: the Go probe produced `{"db_name":"test","table_name":"TableOne","db_id":1,"table_id":1,"index_id":2}` with `IndexValues == nil`.

- Observation: a corrupt record key for a partition has two IDs in one return.
  Evidence: the partial result contains logical table ID 3 while the error names physical partition ID 5. The Rust regression first failed with ID 3, then passed after preserving the original physical ID.

- Observation: remote commit `7e818c27f` added the first live Rust consumer while this package was in preparation.
  Evidence: `tidb-session` dispatches `INFORMATION_SCHEMA.DEADLOCKS`, but `DeadlockRecord::to_datum` had no `KEY_INFO` arm and returned `NULL`. A session regression using a valid record key failed with `NULL` before integration and passed with exact JSON afterward.

- Observation: the persisted catalog had the source database ID, but the executor catalog discarded it during cluster conversion.
  Evidence: `ClusterCatalog` nests `DBInfo`, while `driver::Catalog::Database` previously kept only a name. The live consumer uses `driver::Catalog`, so exact Go JSON required retaining and transferring that ID.

- Observation: unrestricted all-target Clippy has two unrelated target-branch baseline failures.
  Evidence: both the worktree and clean base fail dependency Clippy on `tidb-protocol/src/binary_params.rs` (`double_must_use`). With `--no-deps`, both fail `tidb-exec/src/real_tikv_dml.rs` (`items_after_test_module`). The final commands must keep all other warnings denied while allowing only reproduced baseline lints.

## Decision Log

- Decision: Use `b2f2faaa95bff642920560c24e3714957bcb4c74` as the complete Go package pin.
  Rationale: it is the latest package-changing ancestor of the target branch, enumerates the same four direct artifacts, and every current blob is byte-identical.
  Date/Author: 2026-08-12 / Codex

- Decision: Keep one decoder core in `tidb-executor::keydecoder` behind a small owned-metadata `KeyInfoCatalog` contract.
  Rationale: `tidb-session` already depends on `tidb-executor`, while `tidb-exec` depends in the other direction. This placement lets the live session path and persisted `ClusterCatalog` share byte semantics without a cycle or a second decoder.
  Date/Author: 2026-08-12 / Codex

- Decision: Retain database IDs in `driver::Catalog` and transfer persisted IDs in `cluster_session_catalog`.
  Rationale: callback injection could decode against a newer schema than an explicit transaction sees, and omitting `db_id` would contradict Go. Catalog metadata is already the statement snapshot, so preserving the ID there keeps both snapshot and JSON semantics.
  Date/Author: 2026-08-12 / Codex

- Decision: Preserve Go's `(partial DecodedKey, error)` result through a Rust failure value containing both objects.
  Rationale: an ordinary `Result<DecodedKey, Error>` would erase metadata filled before a record payload error.
  Date/Author: 2026-08-12 / Codex

- Decision: Treat Go's `TableByID`-success/`SchemaByTable`-failure branch as eliminated by both Rust catalog invariants.
  Rationale: each Rust table is nested inside exactly one database snapshot and cannot be represented as an orphan.
  Date/Author: 2026-08-12 / Codex

## Outcomes & Retrospective

Authority, inventory, Go normal/race tests, behavior probes, direct contract, both adapters, the live fail-before-fix regression, and post-redesign Ready validation are complete. Publication remains.

## Context and Orientation

The accepted package is exactly `pkg/util/keydecoder/BUILD.bazel`, `keydecoder.go`, `keydecoder_test.go`, and `main_test.go`. `DecodeKey` first applies Go's shallow record/index predicate, decodes the physical table ID, resolves a logical table before a partition, and fills all metadata available in one `InfoSchema` snapshot. Record keys publish an integer or common handle. Index keys publish ID, name, and values only when the index still exists.

The shared core is `rust/crates/tidb-executor/src/keydecoder.rs`. The executor-catalog adapter is beside its private database map in `driver/catalog.rs`. The persisted-catalog adapter and stable public re-export are `rust/crates/tidb-exec/src/keydecoder.rs`. The direct source contract remains in `rust/crates/tidb-exec/tests/keydecoder_source.rs`.

The live Go consumer is `pkg/executor/infoschema_reader.go`. Its Rust path is `tidb-session::dispatch` -> `deadlock_history_table_rows` -> `tidb-executor::deadlock_history::rows`. That path borrows the same transaction working catalog or autocommit shared catalog that the statement sees, decodes a nonempty key, JSON-serializes success, and returns SQL `NULL` on decode failure.

## Plan of Work

Keep the source-shaped public types and decode behavior in the lower executor crate. Implement `KeyInfoCatalog` for `driver::Catalog`, looking up logical table IDs before partition IDs and exposing current index metadata. Keep the persisted `ClusterCatalog` adapter in `tidb-exec`, where the local type permits the trait implementation without reversing dependencies.

Retain database IDs in executor catalog entries. Synthetic in-process schemas receive monotonic IDs, while `register_database_with_id` overwrites the preseeded schema identity during cluster materialization. Update the cluster session conversion to call that explicit path.

Build `DEADLOCKS.KEY_INFO` in the history row builder, matching Go's nonempty-key guard, error-to-NULL behavior, JSON field order, and string datum. Borrow the session-visible catalog through `with_catalog_mut` so explicit transactions keep their catalog image.

Bind every owner, adapter, consumer, and regression to the four-artifact source pin in the compact semantic receipt. Run focused WIP tests first, then all touched crate tests/checks, formatting, baseline-aware Clippy, repository lint, source/inventory gates, and final diff review.

## Concrete Steps

From repository root, run the Go authority:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -run '^TestDecodeKey$' -tags=intest,deadlock -count=1 ./pkg/util/keydecoder
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH go test -race -run '^TestDecodeKey$' -tags=intest,deadlock -count=1 ./pkg/util/keydecoder

From `rust`, set `CARGO_INCREMENTAL=0` and reuse `/tmp/tidb-package-audit.DnxFlT/rust/target` as `CARGO_TARGET_DIR`:

    cargo test --offline --locked -j12 -p tidb-exec --test all keydecoder_source
    cargo test --offline --locked -j12 -p tidb-session --lib deadlocks_table_exposes_package_rows_and_requires_process
    cargo test --offline --locked -j12 -p tidb-executor deadlock_history
    cargo test --offline --locked -j12 -p tidb-exec
    cargo test --offline --locked -j12 -p tidb-session
    cargo check --offline --locked -j12 -p tidb-server --all-targets
    cargo fmt --all --check

Run baseline-aware all-target Clippy for each changed crate while denying every non-baseline warning. Run the semantic gate, repository `make lint`, source pin, inventory, and atomic boundary checks from repository root. Do not use `make bazel_lint_changed`.

Run `make bazel_prepare` only if the final diff adds, removes, moves, or renames Go files; changes a Go import block or adds a top-level Go test; changes Bazel files or Go modules; or updates Bazel targets. The current Rust/Cargo/docs-only diff does not trigger it.

## Validation and Acceptance

Go `TestDecodeKey` must pass normally and under race. Rust must reproduce every direct source assertion plus exact JSON omission, nil/present `index_values`, missing-index behavior, swallowed corrupt-index errors, partial corrupt-record failures, and the physical partition error ID. Both catalog adapters must preserve database/table/index/partition metadata. A real `SELECT ... FROM information_schema.DEADLOCKS` must return the decoded JSON instead of `NULL` for a valid key and retain `NULL` on invalid or empty keys.

The focused tests, complete owning crates, server all-target check, formatting, baseline-aware Clippy, semantic gate, repository lint, source pin, and inventory/atomic checks must pass. The package is publishable only as one commit on the latest remote target, followed by a normal push and explicit equality checks for local, remote-tracking, and remote-advertised SHAs.

## Idempotence and Recovery

All tests, probes, formatting checks, semantic checks, and Git read checks are safe to rerun. Cargo uses a shared target directory with incremental compilation disabled; do not clean it wholesale. If the remote advances, rebase only this one package's work and repeat Ready. Never force push.

## Artifacts and Notes

Failpoint decision: no accepted package artifact references failpoint or testfailpoint, and its Bazel target has no failpoint dependency. Ordinary targeted Go tests are correct.

Bazel decision: the diff changes Rust source/tests/manifests/lock metadata and this plan only. It changes no Go file, import block, test function, Bazel file, Go module, or Bazel target; `make bazel_prepare` is not required unless final evidence changes.

Recorded fail-before-fix evidence:

    cargo test --offline --locked -j12 -p tidb-session --lib deadlocks_table_exposes_package_rows_and_requires_process
    left KEY_INFO: Null
    right KEY_INFO: {"db_name":"test","table_name":"t","handle_type":"int","handle_value":"1","db_id":1,"table_id":1}

Post-fix focused evidence passes four direct keydecoder cases and the live session regression for both a stored record key and a stored secondary-index key.

Ready evidence:

    cargo test --offline --locked -j12 -p tidb-executor
    652 passed, 4 existing ignored.

    cargo test --offline --locked -j12 -p tidb-exec
    695 passed, 1 existing ignored.

    cargo test --offline --locked -j12 -p tidb-session
    1026 passed, 9 existing ignored, and 2 failed. Both failures reproduce byte-for-byte on a detached clean worktree at target base 7e818c27f: literal_timestamp_defaults_print_in_the_consuming_session_zone expects SHOW CREATE to omit NULL, and the_ported_rejections_carry_tidbs_own_errno expects 1504 where the current parser returns 1064.

    cargo check --offline --locked -j12 -p tidb-server --all-targets
    pass.

    cargo clippy --offline --locked -j12 -p tidb-executor --all-targets --no-deps -- -D warnings
    cargo clippy --offline --locked -j12 -p tidb-session --all-targets --no-deps -- -D warnings
    cargo clippy --offline --locked -j12 -p tidb-server --all-targets --no-deps -- -D warnings
    cargo clippy --offline --locked -j12 -p tidb-exec --all-targets --no-deps -- -D warnings -A clippy::items-after-test-module
    all pass; only the previously reproduced tidb-exec baseline lint is allowed.

    cargo fmt --all --check
    semantic package gate: 1 package, 3 unique commands
    PATH=... GOPATH=/Users/chenhuansheng/go make -o tools/bin/revive lint
    all pass.

    go test -run '^TestDecodeKey$' -tags=intest,deadlock -count=1 ./pkg/util/keydecoder
    go test -race -run '^TestDecodeKey$' -tags=intest,deadlock -count=1 ./pkg/util/keydecoder
    both pass; race emits only the recurring macOS linker LC_DYSYMTAB warning.

Source pin, four-artifact inventory, staged diff whitespace, and Bazel prerequisite gates pass. The final diff contains no Go/Bazel/module trigger, so `make bazel_prepare` is not required.

## Interfaces and Dependencies

`tidb-executor::keydecoder` exposes `HandleType`, `DecodedKey`, `KeyDecoderError`, `KeyDecoderFailure`, `KeyInfoIndex`, `KeyInfoTable`, `KeyInfoCatalog`, and generic `decode_key`. `tidb-exec::keydecoder` re-exports that contract and implements the trait for `ClusterCatalog`. `driver::Catalog` implements the same trait and retains database IDs. `deadlock_history::rows` accepts a catalog snapshot and emits JSON for `KEY_INFO`.

The core depends on `tidb-codec`, `serde`, and `serde_json`; the persisted adapter depends on `tidb-model`. No optimizer or transaction implementation file is modified.

Plan revision note (2026-08-12): replaced the stale no-consumer design after remote `7e818c27f` added `INFORMATION_SCHEMA.DEADLOCKS`; recorded the live fail/pass regression and the lower-layer shared-core architecture.

Plan revision note (2026-08-12): recorded final record/index live coverage, complete Ready commands, and exact clean-base reproduction of two unrelated full-session failures.
