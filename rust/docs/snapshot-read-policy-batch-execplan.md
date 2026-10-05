# Share snapshot read policy across point and batch readers

## Purpose / Big Picture


Make SQL point, batch-point and reused prepared reads carry Go session
replica, adaptive closest-read, busy-store and read-timeout settings. Carry policy
through existing deferred, MaxTS, ordinary and explicit-transaction snapshots to
the native client; do not add a selector, runtime or cache. This advances O13/N03
without claiming complete executor/session/client-go package acceptance. Native
read resource groups must remain independent of transaction commit groups.
Actual non-leader selection remains blocked by ClientPd's inherited leader-only
PdClient fallback; configuration handoff is not routing acceptance.

## Progress


- [x] Refresh both remotes and compare Go source ownership.
- [x] Add grouped regressions to the existing point-read fixture.
- [x] Record three SQL producer failures and an emitted-request consumer failure.
- [x] Extend the existing native resource-group regression, reproduce the shared-field defect and separate read/write ownership. Seven grouped native cases pass, including all nine emitted request assertions in that regression.
- [x] Implement shared options, planner estimates and all snapshot carriers.
- [x] Native 63046fb9b2e0faeb74f7d58bcf27bbb5bbfdee65 passed seven tests and all-target check, published after a fresh locked server build, and synchronized through the maintained patch/protobuf workflow. TiDB now uses its snapshot-only API.
- [x] Grouped validation: 63 TiDB and seven native cases pass. Both affected all-target checks, root make lint, scoped formatting and diff self-review pass.
- [x] Update both registers and durable validation receipt; prepare normal-hook/fresh-build publication. Postcommit publication and Cloud startup results are recorded in /workspace/.cloud-setup/snapshot-read-policy-batch/final-handoff.json.

## Context and Orientation


Base is 853d55127a8991f19756d79348fd17a43b4c3fb9 in /workspace/tidb on
hparser-integration. Native base was 06b4ccc2735ecf89ed57136241cb7b7d204d6c07; validated repair
63046fb9b2e0faeb74f7d58bcf27bbb5bbfdee65 is now published and synchronized.
Fresh Go master is 93a01d31f6da205ae4bf376825293903a6899fdb in
/workspace/.cloud-setup/go-master. Authority: executor/builder.go
InitSnapshotWithSessCtx/newReplicaReadAdjuster, point_get.go Open,
physicalop PointGetPlan/BatchPointGetPlan.GetAvgRowSize and isolation/base.go.
Rust's physical and prepared builders own point readers; ClusterSnapshot and
TableStorage carry reads; cluster_table_storage owns their lifetimes;
tidb-txnkv ClientTransaction delegates to the maintained native snapshot.

## Milestones and Plan of Work


First reproduce dropped settings with real SQL and existing storage fixtures,
using no-op option seams only for observing the missing production calls. Then
connect immutable policy and estimates at reader construction. Deferred opens
must retain options, MaxTS must not bypass them, and explicit statements must
replace old options without changing transaction timestamps or write ownership.
Native callbacks must use current request key counts. Go fast plans have no
accessCols and estimate zero, rather than a guessed output width.

## Validation and Recovery


Source /workspace/.cloud-setup/env.sh in each shell. Run TiDB Cargo from rust/
with CARGO_BUILD_JOBS=1 and --locked. Extend the existing point, storage and
transaction fixtures; combine filters per target. Run scoped all-target cargo
check, root make lint and git diff --check at the batch boundary. The actual
hooks/pre-commit must run cargo build --locked -p tidb-server; repeat immediately
before each authorized push and verify remote SHA. Preserve concurrent work and
use normal fast-forward publication only. Logs and before-images live under
/workspace/.cloud-setup/snapshot-read-policy-batch. Restore only this batch's
files when testing the baseline; never reset unrelated work.

## Surprises & Discoveries


The native client already owns replica adjustment and configurable read timeout.
The TiDB ClientPd adapter does not implement map_region_to_store_with_replica;
the native trait default discards selection settings. Keep T02 and O13 partial.
Current Go point initialization does not install priority/cache overrides.
The native transaction shares resource_group_name between reads and commit,
whereas Go KVTxn.SetResourceGroupName also initializes an independent snapshot
field. Repair the native owner instead of temporarily mutating transaction state.
The native baseline emitted the wrong read group on both Prewrite and Commit
and an empty group after reset; the repaired regression observes the original
write group in all three cases. Get/BatchGet with and without options and both
scanner entrypoints retain the read override. Existing source tests verify
timeout fallback, retry timeout handling and per-region replica adjustment.
The first grouped TiDB run exhausted disk during executor linking, before tests
ran. Remove inactive generated executables, preserve logs/source/recovery and
retry after space recovery; this is not a passing validation.
SQL's coprocessor settings do not configure that separate snapshot path. Both
planner point structs already retain accessCols, including the fast-plan None.

## Decision Log


Group O13/N03 point/batch configuration, estimates and native consumers as one
connected repair. Keep local/stale scope lifecycle, routing-owner consolidation,
live cluster tests and complete package acceptance explicit unless verified.

## Outcomes & Retrospective


Implementation and scoped validation complete. N03/O13/T02 remain partial. This connected maintenance batch does
not remove any original Go test or count configuration plumbing as functional
replica routing. No finding is closed or performance improvement claimed. The final grouped run
passes all 63 selected TiDB cases and seven native cases. The tidb-txnkv library
harness has zero matching cases; its two standalone emitted-request/lock-wait
cases provide validation, not the empty harness. Both repositories pass affected
all-target checking; root make lint passes after the final test correction.
TiDB publication still must execute the actual locked-build hook and repeat the
locked build immediately before push; external final-handoff.json records the
resulting commit, remote SHA and saved startup draft without a self-referential
source commit.

The expanded point suite exposed an unhinted full-table IN test requiring an
IndexLookup plan. Remove that helper/assertion and keep the existing fixture
focused on initial admission, cache reuse and correct rebound rows. Go
find_best_task.go::compareTaskCost owns unhinted plan choice; the exact Go
physical choice for this synthetic fixture was not executed. The first grouped
run passed 32 cases before that assertion; a no-fail-fast investigation passed
62 cases including all new policy checks and failed only the same assertion.
