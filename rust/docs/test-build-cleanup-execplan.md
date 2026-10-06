# Retire disconnected statement and error boundaries

This living ExecPlan follows root PLANS.md.

## Purpose and Context


Remove the unused statement-status model and executor/protocol error-conversion
chain. Their private tests exercised models that the server never called.
Work in /workspace/tidb on hparser-integration from bbe6da8779ecb93b847afa5187f64cc0e4120398.
Fresh Go master is b36c940a4332c866d8b0e2afde88f5e7c2fd7fed.
The live owners are tidb-session/src/stmt_ctx.rs::publish_statement_status,
tidb-executor/src/driver/errors and tidb-protocol/src/error_packet.rs.

## Progress


- [x] Trace complete disconnected chain and retained owners against live sources.
- [x] Remove six files, dead stream method and registrations; retain warning types and cap vectors.
- [x] Run grouped warning, packet, stream and driver-owner tests; lint and diff review.
- [ ] Pass actual hook build, fresh pre-push locked build and verify remote SHA.
- [ ] Preserve recovery bundle and save/read back Cloud checkpoint.

## Milestones and Plan of Work


Delete exec/src/statement_status.rs, exec/src/error_conversion.rs,
protocol/src/error_conversion.rs and their three private test files. Remove
unused root exports and ResultSetStreamError::error_kind (zero callers).
Move StatementWarning and WarningLevel unchanged into warning_publication;
keep their root exports. Extend the existing warning-handler test with the
individual append cap and oversized batch/set vectors from the retired model.
Keep live DriverError rendering, result streaming and raw ERR framing untouched.
Correct three documents that describe the retired proof as a live boundary.
The retained driver-error test exposed five old MysqlError expected values
missing the existing evaluation-origin marker. Correct those expected values
with from_evaluation(), preserving exact code/state/message assertions.

## Validation and Acceptance


Source /workspace/.cloud-setup/env.sh and use CARGO_BUILD_JOBS=1 in rust/:

    cargo test --locked -p tidb-exec -p tidb-protocol --test all -- warning_publication_source:: error_packet_source:: resultset_stream_source:: --test-threads=1
    cargo test --locked -p tidb-executor --lib -- driver::errors:: --test-threads=1

Require nonzero passing tests for every selected target. Verify moved types
unchanged and no remaining references to removed descriptors/models. Run root
make lint and git diff --check. This is removal of disconnected code, not a
behavior fix, so no invented fail-before regression. The real pre-commit hook
must run cd rust && cargo build --locked -p tidb-server. Rerun immediately
before authorized normal push; verify hparser-integration remote SHA.

## Surprises & Discoveries


The statement model's documentation named a retired Session consumer.
The protocol error table and unused result-stream category method formed the
other half of the same disconnected adapter; historical errno proof did not
validate live wire behavior. The first driver-owner run had 9 passes and one stale expectation failure:
actual code/state/message matched, but expected from_evaluation was false.
Production rendering already marks evaluation errors true; five expected
values need the same marker. Warning handlers remain a bounded Go utility;
retaining them does not claim that every warning path shares their owner.

## Decision Log


Remove the complete unused chain in one batch. Preserve warning cap/batch/set
semantics in the actual utility owner tests, with u16 wrapping checked at the
publication boundary. Keep live driver-error and protocol byte tests. No broad
finding closure or full Go package acceptance is claimed by this cleanup.

## Outcomes & Retrospective


Implementation and grouped validation complete: 31 tests, lint, continuity
and diff checks pass. Six files and 1180 net Rust lines removed.
Commit/push and checkpoint gates remain pending in this committed receipt;
external final-handoff.json records their eventual results. Native client-rust is unchanged.

## Recovery, Artifacts and Dependencies


Before-images are recoverable with git show bbe6da8779:<path>; restore only
individual files without overwriting concurrent work. Logs and inventory live
in /workspace/.cloud-setup/statement-boundary-cleanup. Durable receipt is
rust/docs/parity/current-audit/statement-boundary-cleanup-validation.json.
No dependency changes. Cloud save, Publish and fresh-task restoration are
separate. Revision: replace completed context-owner cleanup with retirement
of the disconnected statement/error chain and its private tests.
