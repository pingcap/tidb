# Share server statement attempts

This ExecPlan follows `PLANS.md` and remains a living record of implementation and validation.

## Purpose / Big Picture

Text and binary prepared execution must make identical retry decisions without duplicating transaction error classification or temporary session state changes. A statement may be attempted once more only when its first attempt reports that retry is safe. Client-visible output, cursor setup, and file transfer prohibit retry. SQL results and protocol-specific warnings must remain unchanged.

## Progress

- [x] Read server instructions, both execution paths, result writing, transaction manager contracts, and existing fallback tests. The server package has no `doc.go`.
- [x] Select a shared connection method with an explicit retry safety type and a callback receiving the original TiFlash error only during fallback.
- [x] Add focused tests covering classification, retry limits, state restoration, and protocol-specific warnings.
- [x] Extract the runner and adapt text/prepared callers without moving their warning handling.
- [x] Regenerate Bazel metadata, run focused failpoint-enabled tests and `make lint`, and review the final diff.

## Surprises & Discoveries

The transaction manager sees first-attempt errors even when retry is unsafe. A transaction retry takes precedence over TiFlash fallback, and errors from that retry are returned without further classification. Text execution passes fallback warnings into `handleStmt` (which appends them only for the last statement); prepared execution appends its fallback warning after the second attempt because statement initialization can clear warnings.

Existing `writeChunks` returns retryable only before the first successful `Next`, before column metadata is written. Cursor execution and file-transfer handling return unsafe regardless of error class.

## Decision Log

- Decision: Keep warning construction in each protocol caller and pass the fallback error into the attempt callback. Rationale: preserves warning timing, error level, parser warnings, and last-statement handling. Date: 2026-11-02.
- Decision: Use a named retry safety type at the attempt boundary; retain existing bool results inside result writers. Rationale: makes the shared runner contract explicit without changing packet-writing APIs. Date: 2026-11-02.
- Decision: Restore the prior retry flag and TiFlash engine membership with deferred cleanup on both error and panic. Rationale: temporary retry state must not leak into subsequent statements or overwrite preexisting state. Date: 2026-11-02.
- Decision: Work in the already attached clean worktree and initially keep changes uncommitted. Rationale: the refactor request authorizes edits in the active workspace; the subsequent request to file a PR authorizes committing and publishing them. Date: 2026-11-02.

- Decision: Publish this completed increment as a PR using `Issue Number: ref #72587`. Rationale: the issue tracks multiple independent server refactors, so this PR must reference rather than close the whole issue. Date: 2026-11-02.

## Outcomes & Retrospective

Both protocols now use the shared runner. Warning differences and output safety remain covered by SQL-facing tests. Temporary retry state restores its prior value on success, error, and panic. The Ready verification profile passed: metadata regeneration, focused failpoint-enabled tests, lint, and whitespace checks. A read-only independent review found no remaining correctness issues. No real-cluster test or performance benchmark was run; SQL/protocol compatibility is covered by the focused mockstore tests.

## Context and Orientation

`pkg/server/conn.go` implements `clientConn.handleQuery`, parsing one or more SQL statements and invoking `handleStmt`. `pkg/server/conn_stmt.go` implements `executePlanCacheStmt`, invoking `executePreparedStmtAndWriteResult` for binary prepared execution. Both currently classify an error through `sessiontxn.GetTxnManager(...).OnStmtErrorForNextAction` at `StmtErrAfterQuery`, optionally execute once with `RetryInfo.Retrying=true`, or execute once with TiFlash removed from `IsolationReadEngines` when a TiFlash timeout permits fallback to TiKV. TiFlash and TiKV are storage engines; fallback reruns a read against TiKV.

## Plan of Work

Add `pkg/server/conn_stmt_retry.go` containing a named retry safety type and `clientConn.runStmtWithRetry(ctx, attempt) error`. Invoke `attempt(nil)` first. On failure classify the error exactly once, honor a replacement transaction error, and stop if retry is unsafe. For `StmtActionRetryReady`, save and temporarily set the retry flag, defer restoration, and return the second attempt's error. Otherwise, only a permitted TiFlash timeout can trigger fallback: save engine membership, remove TiFlash, defer exact restoration, and return the second attempt's error. Do not loop or reclassify retry results.

Change both attempt methods to return the named safety type, converting the result writer's bool at their boundary. Replace the duplicated caller logic with callbacks. Text callbacks append an error-level fallback warning to parser warnings before calling `handleStmt`; prepared callbacks append the error after execution. Leave parsing, extension hooks, cursor ownership, file transfer, result writing, and timing initialization in place.

Add `pkg/server/conn_stmt_retry_test.go` with a session and transaction manager test double that exercise the real runner without storage startup. Test success, replacement errors, unsafe attempts, transaction priority, fallback permissions, wrapped timeout classification, failed retries, and exact flag/engine restoration including panic. Extend existing fallback coverage for binary output safety and both protocols' engine restoration.

## Concrete Steps

Run from repository root:

    make bazel_prepare
    ./tools/check/failpoint-go-test.sh pkg/server -run '^(TestRunStmtWithRetry.*|TestStmtRetryWarningHandling|TestTiFlashFallback|TestRcReadCheckTSConflict|TestRcReadCheckTSConflictExtra|TestCursorExistsFlag)$' -count=1
    make lint
    git diff --check

`make bazel_prepare` is required because Go files, tests, and imports change. The failpoint wrapper enables failpoints and disables them in cleanup. Focused tests should pass with unchanged warning output and no additional attempts after output.

## Validation and Acceptance

The runner tests must observe at most two attempts and exactly one classification for a failed first attempt, even when the second attempt fails. Unsafe attempts never run twice. During transaction retry the flag is true and afterward its original value is restored; during fallback TiFlash is absent and afterward its original presence or absence is restored. Cleanup also runs while unwinding panics. Existing SQL-facing tests must still report `Error 9012 TiFlash server timeout` and reject retries after output/cursor setup. Apply the Ready verification profile, including `make lint`, and report any environment blockers precisely.

## Idempotence and Recovery

Tests and metadata regeneration can be rerun. Failpoint changes must be disabled by the wrapper's cleanup. Review generated metadata diffs and retain only changes attributable to this refactor. Never edit generated Bazel metadata manually. No production services, remote branches, or user data are modified.

## Artifacts and Notes

Initial worktree was clean at commit `63cd22c529`. Validation used the Ready profile because this changes Go execution code.

The initial existing-test run passed (45.023 seconds):

    ./tools/check/failpoint-go-test.sh pkg/server -run '^(TestTiFlashFallback|TestRcReadCheckTSConflict|TestRcReadCheckTSConflictExtra)$' -count=1

`make bazel_prepare` completed successfully. Its final output changed only `pkg/server/BUILD.bazel` to register the two new files and the direct test dependency on storage error definitions. `make lint` completed successfully, including a second run after the warning tests were added. `git diff --check` passed.

To verify that the new safety tests detect replay, temporarily disabled the runner's safety guard and ran:

    ./tools/check/failpoint-go-test.sh pkg/server -run '^TestRunStmtWithRetry/(transaction_retry_after_output_prohibited|fallback_after_output_prohibited)$' -count=1

Both selected cases failed as expected because an unsafe attempt was repeated; the original guard was restored automatically after failpoint cleanup. This was a deliberate mutation check, not a delivered failure.

The final test command in Concrete Steps passed (22.083 seconds), covering both runner tests, all five warning scenarios, existing TiFlash transports, both RC conflict tests, and cursor status. The wrapper exited zero and reported failpoint refcount zero after cleanup. Logs are in `/tmp/tidb-server-attempts-tests.log`, `/tmp/tidb-server-attempts-lint.log`, and `/tmp/tidb-server-attempts-unsafe-mutation.log`.

Final review verified the two-attempt limit, one classification per initial error, transaction retry precedence, error equivalence for annotated timeouts, and protocol-specific warning ownership. The deliberate improvement to state cleanup restores existing retry flags and engine membership instead of forcing false or adding an originally absent engine.

## Interfaces and Dependencies

The runner lives in package `server` and uses existing `sessiontxn`, `variable`, `kv`, and TiFlash error definitions; no new external dependency is needed. An attempt callback has the form `func(tiFlashErr error) (stmtRetrySafety, error)`. A non-nil input is the first attempt's TiFlash timeout and is used solely by protocol-specific warning code. The callback owns result-set cleanup and must report unsafe once client interaction prevents repeating the statement.
