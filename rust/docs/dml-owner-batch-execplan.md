# Share multi-table DML planning metadata and consumers

Current state is maintained in [the DML read/FK continuation](dml-trigger-owner-batch-execplan.md). The matrix interpreters and render-only FK policy described as pending below have since been replaced; this plan's earlier milestones remain historical evidence.


This living ExecPlan follows PLANS.md. Update progress, discoveries, decisions and outcomes with the implementation. The user requires source fixes in connected batches with one combined baseline and grouped final validation, and forbids pushing.

## Purpose and context


Joined UPDATE and DELETE must discover table identities without executing their read source, carry all target FK plans, and authorize the same targets execution will mutate. Go's pkg/planner/core buildUpdate/buildDelete resolve these facts before executing the child. Starting Cloud HEAD is 40ee71600bff5dccce1102a97126e85ed0a870ec; reference master is 93a01d31f6da205ae4bf376825293903a6899fdb. The remote schema-sync commit 1ed2cf975c is preserved separately pending safe integration.

The existing executor multi_dml.rs mixes layout discovery with row materialization. Derived and view layout discovery can run queries; multi_dml_physical_plan supplies an empty FK spec; Session DELETE authorization separately searches all AST references. These are connected consumers of the same resolved source/target boundary under A01/E02/E03. This work does not claim whole pkg/planner/core or pkg/executor acceptance, nor closure of all eight B02 findings. Broader table policy/session/auto-ID/historical prerequisites remain explicit.

## Progress


- [x] Inspect live Go owners and all Rust planning/authorization/execution callers.
- [x] Add related regressions and capture a combined original-source baseline.
- [x] Implement metadata-only source discovery, shared target resolution and multi-table FK planning together.
- [x] Migrate callers and remove obsolete competing lookup paths.
- [x] Run grouped regressions, affected all-target checks, lint and the configured hook's mandatory locked build; inspect failures.
- [x] Update both finding registers and evidence; prepare one normal local commit with the actual hook and no push. The resulting commit/recovery/draft receipt is /workspace/.cloud-setup/dml-owner-batch/final-handoff.json, written after success.

## Surprises & Discoveries


The whole-ALTER catalog clone also rolled back MaxForeignKeyID, contrary to Go's failed-FK drop lifecycle. Preserve only this high-water mark by physical ID on failure; constraints, indexes and row changes remain rolled back. Go's strict ParseOneStmt returns 1149 after parsing more than one valid statement; malformed statements still retain their own parse error. The old 1064 cardinality expectation was inaccurate.

The supposed layout-only path invokes derived_source_relation, which calls run_query_stmt. LATERAL layout probing also runs a query. Matrix tables still use a separate interpreter and cannot be deleted without a row-identity migration. Go buildDelete pops its final SELECT visit when neither WHERE nor ORDER is present; this is not a blanket removal of every source privilege.

## Decision Log


Use the existing logical planner to discover derived/lateral metadata without a row executor. Preserve the existing matrix read implementation until its callers have a valid shared physical identity. Consolidate writable-target resolution for permission and FK producers; retain original error semantics and whole-package obligations. Do not remove useful failing expression tests discovered during prior cleanup.

## Milestones


The first milestone captured seven original runtime failures together in the existing session suites. The second migrated metadata, authorization, FK plan producers and all four physical-plan callers, then removed the obsolete target/layout paths. The third checks the whole connected change: grouped session SQL, parser and optimizer cases, affected all-target checking, lint and the configured locked-build hook. The last milestone records evidence in both registers, creates one normal local commit and refreshes the verified recovery bundle/startup draft without pushing.

## Plan of Work


Extend planner_bridge to expose immutable logical FROM metadata. Build a row-free MultiSource from resolved names and catalog metadata, using the existing join naming rules only on empty rows. Route ordinary KV DML, EXPLAIN, prepared planning and DELETE authorization through it. Build per-table FK specs from resolved assignments/deletion targets, merge aliases by physical table identity, allocate FK nodes with the DML planner's allocator, and preserve source ordering. Remove the Session AST target resolver after every caller migrates.

## Validation and acceptance


Add session regressions for side-effecting derived and lateral sources, invalid/derived DELETE targets, direct and prepared authorization, FK checks/cascades and multi-alias targets. Capture failures together before changing implementation. Reuse existing multi-table DML, FK, privilege, prepared-statement and quota tests. Run compatible filters together; no compilation per isolated issue. Run scope-appropriate cargo check --locked, make lint and formatting at the batch boundary. The actual hooks/pre-commit must run cd rust && cargo build --locked -p tidb-server before committing. Do not push or dry-run. Preserve logs and exits in /workspace/.cloud-setup/dml-owner-batch.

## Idempotence and Recovery


Preserve concurrent changes. Use only explicit file staging and normal local commits. Never reset or bypass hooks. Keep the prior verified recovery bundle until a new bundle verifies. Maintain original regression obligations even if a test exposes another defect.

## Outcomes & Retrospective


The connected source changes are implemented. The official baseline had seven runtime failures. The final grouped SQL run passes 140 cases, the selected parser run passes nine, and optimizer rule tests pass 59 (including the expected-panic invariant control). Affected all-target checking and make lint pass. The configured hooks/pre-commit passes the mandatory locked server build. The normal commit runs that hook again as required; its result and exact SHA are recorded after success in the external final handoff. Final validation JSON records actual completed gates. Parent findings remain partial, counts unchanged at 57 unresolved: EXPLAIN FK nodes do not yet replace runtime policy lookup, row vectors/handles and the matrix interpreter remain.

The grouped retry additionally exposed a real FK-ID rollback regression and strict-parser cardinality mismatch. Both are repaired without dropping the assertions. A five-case live Go parser oracle confirms the exact 1149 cardinality error and malformed trailing-input rejection. The source-less correlated LATERAL failure came from LIMIT pushed below the only column-producing projection; the conservative pushdown guard preserves the existing ColumnPruner assertion. No full Go SQL oracle, live multi-node TiKV or performance claim is made.

The first append attempt used an incorrect relative directory and its no-regression run was interrupted; it is excluded. A corrected baseline followed. Subsequent compilation/diagnostic retries are retained in the receipt, including the initial metadata-field typo. Recompilation was necessary to resolve concrete failures; none counts as acceptance by itself.

Broader parser validation completed without further compilation: 721 passed, 14 failed. All 14 also fail in the retained historical executable (719 passed, 16 failed) and appear in the earlier committed cloud-account-test receipt. None reaches the new 1149 return: they fail in unchanged grammar/restore/charset paths. This historical control is not an exact starting-HEAD rebuild; no whole-parser green claim is made. The seven new regressions and final 208 focused cases remain green.

## Aggressive removal continuation


The user explicitly requested further removal after the validated a8b3a0d7 checkpoint. Continue the same owner migration across the matrix adapter: resolve its metadata before reading any leaf, consume the shared source table definitions, remove the executable lateral schema probe and the duplicate base/view/derived metadata builders, and remove its private source-kind AST walker. Existing positional-row mutation remains until a real physical identity replacement exists. EXPLAIN needs metadata and should use the common planner instead of a matrix-specific refusal. Group regressions for probe side effects, EXPLAIN and early metadata errors before changing these paths, then run affected SQL and required gates once at completion. Record the follow-on evidence under /workspace/.cloud-setup/dml-removal-batch. No push.

The removal continuation is implemented: 175 net production lines removed from multi_dml.rs. Combined baseline: two failures and one passing metadata-error control. Grouped post-change run: 144 passed, zero failed/ignored, including the mixed matrix/storage view/USING/derived DELETE control. Required check/lint results are in the separate removal validation receipt. The normal commit is the single locked-server gate for this continuation; its actual result is recorded in /workspace/.cloud-setup/dml-removal-batch/final-handoff.json after success. Matrix mutation/correlation binding remains deliberately because its positional identities are still live.

## Shared physical read and interpreter retirement


Continue from 7957cf17dd2a25d7eeed2e7d6af18f051c9235a4 with Go master refreshed to 93a01d31f6da205ae4bf376825293903a6899fdb. The entire remaining matrix joined-DML interpreter can retire once the physical memory scan carries its snapshot row position through joins, selection, sort, limit and Apply. This position is an internal adapter identity, not a new SQL column or a claim that Go virtual tables have positional handles. Stored-table integer/common/hidden handles retain their existing path.

The implementation milestone adds the adapter identity to planner metadata and memory scan output, consumes it at the joined write boundary, and removes matrix-only materialization, nested-loop joins, per-row query rebinding, selection/sort/limit and late memory-accounting paths. Metadata-only coalesced naming remains until the resolved DML handoff owns it directly. All producers and both UPDATE/DELETE consumers migrate together. Meaningful existing regressions stay.

Validation uses one baseline of the three new matrix SQL cases, then one grouped session run covering joined DML, FK, authorization, prepared execution and memory quota plus affected all-target checks, make lint and the actual normal-commit locked-build hook. Logs go to /workspace/.cloud-setup/dml-interpreter-removal. Subquery predicates must work through the shared planner; duplicate equal rows must keep distinct identities through ORDER/LIMIT and outer joins, and unmatched sides must not delete another row. No push, no full-package acceptance claim. On failures retain the checks and repair or revert only this batch's changes.

- [x] Capture grouped original-source baseline: two unsupported-subquery failures and one passing positional/outer-join control.
- [x] Migrate physical identities and both read consumers; delete the interpreter and orphan materializers.
- [x] Validate grouped behavior and required checks, update both registers, and prepare a normal local commit. Actual hook/recovery/startup outcomes are recorded after success in the external final handoff.

The first migration run passed 187 cases and exposed two shared-path defects, retained as regression obligations: IN-subquery lowering leaked an auxiliary column into the DML output, and TopN discarded input rows beyond the requested fill. Live Go buildUpdate/buildDelete freezes the original source schema; current sortexec.TopNExec.loadChunksUntilTotalLimit explicitly stopped setting RequiredRows for issue #62135 and retains every input chunk before heap trimming. Remove the obsolete exact-fill/truncation policy rather than altering memory scans to satisfy it. An independently linked diagnostic using the already-built current libraries reproduced the TopN failure even in plain SELECT. The existing topn_keeps_candidates_beyond_the_limit_in_the_first_child_chunk regression remains and is included with the heap, logical-source and SQL suites. Also remove the now-unreferenced row_is_selected helper. These are connected prerequisites of retiring the DML interpreter, not whole-package acceptance.

The shared-read removal completes its source and behavioral milestones: 595 net production lines removed, 315 final grouped cases pass, affected all-target checking and make lint pass. No test was removed or weakened. The normal local commit must pass the actual locked-build hook; its SHA/result and recovery/draft outcomes are recorded afterward in the external final handoff. Parent counts stay57 unresolved; single-table matrix execution, final write buffering and full planner/FK ownership remain.
