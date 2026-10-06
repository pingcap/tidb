# Share JSON result types and conversion errors

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Expressions, result metadata, generated columns and expression indexes must see
one JSON value/type contract. Work starts at 093145bc8b883b29b471e4d5dc6a94d73fc38a35
on hparser-integration. Fresh Go master is
3ca96b1d5df8da123e7a650512654eedab12c861; native client remains cfafb1e.
This is connected maintenance of X01/K03, not complete package transcreation.

Go pkg/expression/builtin_json.go owns JSON_SEARCH, JSON_UNQUOTE, JSON_PRETTY,
JSON_QUOTE and JSON_TYPE result contracts; builtin.go getRetTp widens strings
by declared length. pkg/types/datum.go convertToMysqlJSON rejects binary literals
with ErrInvalidJSONCharset. Rust's JSON_SEARCH returns text, its escape check
counts characters instead of bytes, and result metadata leaves string widths
unspecified. expression_index.rs contains a second type resolver to compensate.
The table-write conversion also turns a binary-charset rejection into 1366.

## Progress


- [x] Refresh refs and trace source contracts and downstream consumers.
- [x] Add grouped failing regressions and real-wire baseline.
- [x] Fix shared result typing/value production and typed conversion error.
- [x] Remove the expression-index type override after migrating its caller.
- [x] Run grouped regressions, owner coverage, wire checks, lint and builds.
- [ ] Update both registers, commit through hook, build before push and verify.

## Milestones and Plan of Work


First extend existing JSON and write-conversion tests to reproduce JSON carrier,
escape byte-length, result metadata, expression-index and charset-code failures.
Then return the existing BinaryJSON datum from JSON_SEARCH, retain the original
walk and error order, and share authoritative result types with index admission.
Derive JSON string widths from Go's signatures; apply Go's width-based string
codes for those signatures. Remove go_result_type_code and its executor adapter
only after all references migrate. Preserve original Go-derived test vectors,
updating their old Rust string-carrier expectations to source JSON identity.
Keep a typed invalid-JSON-charset error from datum conversion through all table
write shapes. Do not merge JSON CAST and column conversion: Go gives them
intentionally different binary-literal semantics.

## Concrete Steps and Acceptance


Source /workspace/.cloud-setup/env.sh in every build/server shell; Cargo cwd is
/workspace/tidb/rust and CARGO_BUILD_JOBS=1. Run targeted json_result_batch filters
for tidb-expr and tidb-executor together before/after implementation; run retained
JSON/rewriter/expression-index cases together afterward. Existing test targets
and SQL session fixtures are sufficient; add no new harness crate.
Use the current server for TCP baseline, rebuild once at the batch boundary and
repeat the same checks. Assert native JSON results, 1210 ESCAPE rejection,
source result metadata, correct expression-index admission and 3144 writes.
Run cargo check --locked for affected targets, make lint, changed-region format
checks and git diff --check. Keep unrelated known parser failures visible.

## Decision Log


Fix the common type/error owners and their consumers in one batch. JSON_SEARCH
keeps its Go walk semantics and original cases. Binary JSON itself remains the
value owner. No full pkg/expression, pkg/types or pkg/executor acceptance and no
performance improvement is inferred. X01/K03 stay partial for broader contracts.

## Surprises & Discoveries


Go KindMysqlBit ToString returns raw bytes; it is not a decimal-string JSON
conversion bug. Preserve that existing behavior. JSON_UNQUOTE's width depends
on its argument's string cast, so an unconditional LongBlob override is wrong
for short strings as well as duplicated.

## Outcomes & Retrospective


Implementation and validation complete: four Rust regressions failed before;
ten of fourteen expanded TCP checks failed on the original server. Final
211 Rust cases and all fourteen TCP checks pass. Five broader DDL failures
match full normalized diagnostics from the historical harness-dedup receipt;
none were deleted or weakened. A temporary-borrow error in the new regression
was corrected before execution. Initial server linking exhausted disk; after
recorded removal of inactive artifacts and the failed temporary output, the
unchanged build passed. The external metadata probe initially confused internal
LongBlob251 with wire Blob252; Go dumpType confirms252. The corrected probe
passes all fourteen checks on the unchanged binary. Publication follows externally.

The TCP baseline exposed an additional connected defect: cluster CREATE TABLE
hardcoded hidden index columns to BIGINT, so string-index inserts failed with
1366 and JSON expression indexes were admitted. That metadata path now invokes
the existing expression-index builder for both validation and inferred types.

## Recovery and Artifacts


Logs and one-off scripts belong in /workspace/.cloud-setup/json-result-batch.
Durable validation is rust/docs/parity/current-audit/json-result-batch-validation.json.
Restore only affected paths if recovery is needed; preserve concurrent work.
Actual precommit and fresh immediate-prepush locked tidb-server builds are
mandatory. Push exact authorized branch normally, verify SHA, then save Cloud
checkpoint and recovery bundle. Draft saving is distinct from Publish/restore.
