# Share TIME conversion policy across writes and casts


This living plan follows root `PLANS.md`.

## Purpose / Big Picture


K03 table conversion and X01 typed expression conversion still use the wrong shared TIME parsing entrypoint. Repair the connected parsing, numeric admission, JSON type dispatch and diagnostic consumers together. A datetime ending in 23:59:59.999999 must become TIME 24:00:00 when rounding to zero fractional digits, rather than losing the carry into a calendar day. Embedded JSON durations retain the precision specified by Go's JSON signature. Errors retain their best-effort value and caller-specific warning policy.

## Progress


- [x] Confirmed clean Cloud checkout at 95c8a23ae8aac8ddb7a5d380666e213310508c61 and refreshed Go master 7a3dacb52efe58d28db360ae8639d8838c376544.
- [x] Traced complete conversion entrypoints in Go types/time.go, datum.go and expression/builtin_cast.go plus table/column.go and executor/insert_common.go.
- [x] Six grouped baseline regressions failed; the Go oracle confirmed 21 boundary/date-policy/DST cases. Ten live SQL scenarios fail against the old server.
- [x] Repaired shared parser, numeric TIME admission and typed diagnostics; migrated table and expression consumers. Removed the wrong column-to-StrToDuration adapter.
- [x] Passed 103 distinct retained Rust tests, ten live unistore SQL scenarios, affected all-target checking, lint and locked server build.
- [ ] Self-review, update both finding registers and receipt, publish through actual hook and fresh pre-push locked build; verify remote SHA.

## Context and Orientation


Use `/workspace/tidb`, branch hparser-integration. Native `/workspace/client-rust` remains unchanged. Source `/workspace/.cloud-setup/env.sh` and set CARGO_BUILD_JOBS=1 before Cargo, from `/workspace/tidb/rust`. Go comparison lives at `/workspace/.cloud-setup/go-master`. Do not create competing checkouts.

`tidb-datatype/src/duration.rs` owns ParseDuration's grammar and datetime fallback. `convert.rs::str_to_duration` implements a different Go helper that can intentionally return a calendar time; it must retain that contract. Before this batch, `datum_convert.rs` routed column TIME through that helper and lacked contextual temporal diagnostics. `tidb-expr/src/cast.rs` reused column conversion even where Go's typed signature differs. `tidb-executor/src/driver/write_cast.rs` completes raw errors according to INSERT/UPDATE/ODKU ownership.

## Milestones and Concrete Steps


First extend the existing datatype duration/conversion and expression cast suites with grouped regressions, then run:

    cargo test --locked --no-fail-fast -p tidb-datatype -p tidb-expr --lib -- duration_batch_ --test-threads=1

Keep failures in `/workspace/.cloud-setup/duration-owner-batch/before.log`. A scoped external Go probe uses the refreshed export without editing Go repository files.

Second repair datetime fallback to parse at input precision, extract its duration, then round. Give datum TIME and expression duration parsing one flag-aware ParseDuration value owner. Preserve numeric column admission, typed error-side values, and JSON temporal/string distinctions. Retire the obsolete column-to-StrToDuration adapter after callers migrate. Do not change Go's separate StrToDuration contract.

Third group retained tests by target. Use existing server wire probes for numeric/string TIME writes and casts. Run affected all-target checks, root make lint and cargo build --locked -p tidb-server once after the batch. The actual commit hook and another fresh locked build immediately before push are mandatory.

## Validation and Acceptance


Each baseline regression must fail for its observed behavior, then pass after the repair. Retain malformed values, precision, signed numeric bounds, datetime fallback, invalid-date policy, JSON source type and SQL strict/warning/error controls. Preserve raw error codes and INSERT/UPDATE/ODKU naming. No full Go package or performance acceptance is inferred from this maintenance batch. K03/X01 remain partial until their broader obligations pass.

## Surprises & Discoveries


Go ParseDuration parses fallback DATETIME with GetFsp(input), extracts the clock, then rounds the duration. Rust instead rounds the calendar before extracting the clock, losing midnight carries. The column converter also uses StrToDuration, which Go does not use for this conversion. JSON native durations bypass target-FSP rounding in Go; malformed JSON strings return a zero duration beside an error, unlike numeric JSON which returns NULL.

## Decision Log


2026-10-07: compose the existing duration owner across K03/X01 and write diagnostics. Keep column numeric checks separate from expression Int/Real/Decimal signatures because Go separates them. Do not generalize every temporal conversion into one cast policy.

## Outcomes & Retrospective


Six baseline regressions and the live SQL probes reproduced the differences. Expression CAST coverage passes after repair. The first grouped run exposed a NULL-display assumption in the new test and a stale DST assertion in an existing test; both were corrected using the Go oracle, preserving a separate StrToDuration control. All 80 datatype tests and 23 expression tests now pass. The rebuilt server passes all ten live SQL scenarios; affected all-target checking, lint and locked build passed. Self-review verified the source hashes and limited both register updates to K03/X01. Broad finding counts remain 56 unresolved because these are wider package obligations, not a count of individual repaired behaviors. Publication gates and remote verification are recorded externally in final-handoff.json.

## Idempotence and Recovery


Preserve concurrent changes and native source. Never replay one-use edit scripts. Recover this batch from the base Git commit rather than resetting unrelated work. Keep compiler caches and use only affected targets; no cargo clean.

## Interfaces and Dependencies


Retain existing ParseDuration, StrToDuration, Datum conversion and expression evaluation entrypoints. A shared flag-aware parser can expose existing Converted<MySqlDuration> and typed parse errors. No dependency or lockfile change is intended. Current receipts and source hashes belong in `rust/docs/parity/current-audit/duration-owner-batch-validation.json`; publication results live outside the commit in the Cloud task directory.
