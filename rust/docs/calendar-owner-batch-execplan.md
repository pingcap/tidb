# Share calendar conversion values and diagnostic ownership


This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture


Repair the connected DATE/DATETIME/TIMESTAMP conversion boundary in K03 and X01. Column conversion must retain Go's values, conversion-stage warnings and final errors separately; typed expression signatures must preserve JSON-native values and validate ordinary temporal conversions. A parse warning must not disappear from INSERT or become a strict-mode failure. String ParseTime DST errors remain fatal in expression casts because Go handleInvalidTimeError does not downgrade that error class. JSON durations use the statement date, JSON numeric values remain invalid calendar sources, and native JSON calendar payloads retain their source microseconds independently of displayed precision.

## Progress


- [x] Confirmed clean Cloud checkouts, hparser-integration at 387c5d771c528c6b3f9d80216f9b7091439555a2, native master at 8b752f9638ad157931725b66ffdc57e0465432a9; refreshed Go master remains 7a3dacb52efe58d28db360ae8639d8838c376544.
- [x] Read shared Go datum/time/table/expression owners and existing Rust consumers; scoped Go oracle confirms numeric admission and typed fallback distinctions.
- [x] Eight grouped Rust regressions and eleven live SQL scenarios failed before implementation.
- [x] Repaired shared calendar datum diagnostics, migrated write callers and expression source dispatch, and removed the old calendar/DST write adapter.
- [x] Passed 125 retained Rust tests, eleven live SQL scenarios, all-target checks, lint and the locked build; self-reviewed and updated both finding registers.
- [ ] Publish through the actual hook and a fresh pre-push locked build; verify remote SHA and save the Cloud checkpoint.

## Context and Orientation


All work runs in /workspace/tidb on hparser-integration. /workspace/client-rust is unchanged. Source /workspace/.cloud-setup/env.sh and set CARGO_BUILD_JOBS=1 for Cargo from /workspace/tidb/rust. Go comparison sources are /workspace/.cloud-setup/go-master; use rg --no-ignore. Root and deeper AGENTS instructions apply. No applicable deeper Rust AGENTS or source-package doc.go was found.

The datatype owner is rust/crates/tidb-datatype/src/datum_convert.rs, with parsing in time_parse.rs and temporal type conversion in mysql_time.rs. Diagnostics distinguish warnings emitted during conversion from the error returned beside the best-effort value. rust/crates/tidb-executor/src/driver/write_cast.rs applies table zero-date policy and statement-specific naming. rust/crates/tidb-expr/src/cast.rs selects Go's source-specific expression signatures. JSON native calendars and durations are typed binary payloads, not textual date literals.

## Milestones and Plan of Work


First extend the existing datatype and cast suites with shared calendar regressions. The external Go oracle in /workspace/.cloud-setup/calendar-owner-batch records source behavior without editing Go files. Run the grouped calendar_batch_ filter before production edits and retain failures.

Next make datum calendar conversion report parser warnings and typed conversion warnings through the existing context sink. Preserve the raw error beside its value, including unsigned overflow and invalid typed timestamps. Route calendar writes through the existing contextual adapter while retaining table-owned zero-date policy. Remove the replaced legacy calendar/DST handling only after all supported callers migrate. Expression JSON signatures decode native payloads directly; ordinary Time sources use Convert before RoundFrac. Preserve the difference between Convert's DST warning and ParseTime's DST error.

Finally run retained datatype conversion/time and expression cast suites together by target, affected all-target checking and root make lint. Build the locked server and probe actual unistore SQL for truncation, strict mode, JSON calendar sources, DATE clock removal and INSERT/UPDATE/ODKU behavior. Keep regressions that represent Go contracts; correct stale assertions only with pinned source/oracle evidence. Record exact commands and hashes in the committed validation receipt.

## Concrete Steps


From /workspace/tidb/rust after environment activation:

    cargo test --locked --no-fail-fast -p tidb-datatype -p tidb-expr --lib -- calendar_batch_ --test-threads=1
    cargo check --locked --all-targets -p tidb-datatype -p tidb-expr -p tidb-executor -p tidb-server
    cargo build --locked -p tidb-server

Run make lint from /workspace/tidb. The actual executable hooks/pre-commit must run the locked server build. Immediately before each push rerun that build, push normally to pingcap/tidb hparser-integration, then verify remote SHA. Do not force push or bypass hooks.

## Validation and Acceptance


Baseline behavioral failures must pass after repair. Validate both diagnostic code/order and retained value, with JSON/string/numeric/typed temporal controls. Full package acceptance remains atomic: this batch maintains existing owners and does not accept complete pkg/types, pkg/expression, pkg/table or pkg/executor. Their generated/platform/build/test/fixture inventories remain authoritative. K03/X01 retain broader generated/ANALYZE/vector/kernel obligations until independently completed. No workload performance or multi-node claim is made.

## Surprises & Discoveries


Go ParseDatetime emits truncation warnings directly even in strict mode. Time.Convert emits a warning and succeeds for a DST gap, while ParseTime returns its adjusted value beside a DST error. A single generic event cannot decide both callers' policy. Go's timestamp datum conversion also excludes unsigned numeric input, while DATE/DATETIME support unsigned values and retain a DATE-typed zero beside unsigned overflow.

## Decision Log


2026-10-07: use the existing typed conversion diagnostic sink and contextual table adapter. Keep expression source dispatch separate where Go separates it; native JSON calendars bypass ordinary Time.Convert validation and rounding. No dependency, native-client or Go source changes are needed.

## Outcomes & Retrospective


All 98 datatype and 27 expression cases passed. The retained expression test asserting a successful string DST cast was corrected against Go handleInvalidTimeError, which preserves error 8179. One new test incorrectly assumed native JSON calendar column conversion matched the expression signature; the Go oracle confirmed it must fail through Unquote, while a JSON string succeeds. Both controls are retained. Affected all-target checking, lint, the locked build and all eleven live SQL scenarios passed. The first link exhausted output space and failed with SIGBUS; after retiring two inactive historical expression test executables, the identical build linked in 6.23 seconds. Compiler caches and the current server were retained. Publication results are recorded externally in final-handoff.json.

## Idempotence and Recovery


Preserve concurrent changes and native source. Do not replay one-use mutation helpers. Keep compiler caches; do not cargo clean. Retire only identified inactive build artifacts if needed for disk space, recording hashes and process/link checks. Durable validation lives in rust/docs/parity/current-audit/calendar-owner-batch-validation.json; external logs and publication results live in /workspace/.cloud-setup/calendar-owner-batch.

## Interfaces and Dependencies


Retain Datum.convert_to_in and convert_to_in_context, Time.convert_kind, BinaryJSON.as_time/as_duration and existing write/cast entrypoints. Extend their shared ownership rather than introducing another parser, error formatter or session clock. Native dependencies and lockfiles remain unchanged.
