# `pkg/ttl/ttlworker` parity receipt

Status: Completed the missing-Go-behavior fix and recorded the complete
package inventory. This receipt covers the full Go package and its current
Rust boundary; it is not a repository-wide parity claim.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Rust comparison branch: `origin/hparser-integration` at the pre-fix commit
used for this batch. No Rust `ttlworker` owner exists in that branch.

## Complete Go inventory

Before editing, every tracked artifact in `pkg/ttl/ttlworker` and its nested
`integrationtest` package was read in full: 25 artifacts and 12,187 lines.
There is no package `doc.go`, fixture or `testdata` directory, generated
source or input, platform/build-tag variant, benchmark, fuzz target, README,
or ownership artifact.

| artifact | lines | role |
| --- | ---: | --- |
| `BUILD.bazel` | 123 | Go library/test targets and dependencies |
| `config.go` | 130 | TTL worker configuration and failpoint intervals |
| `del.go` | 424 | TTL delete task execution |
| `del_test.go` | 642 | delete-task unit tests |
| `integrationtest/BUILD.bazel` | 32 | integration-test target and dependencies |
| `integrationtest/helpers_test.go` | 64 | integration test session-pool helpers |
| `integrationtest/manager_job_adapter_test.go` | 262 | manager adapter integration tests |
| `integrationtest/timer_sync_test.go` | 505 | timer synchronization integration tests |
| `job.go` | 155 | TTL job state and summary |
| `job_manager.go` | 1,431 | job scheduling, ownership, and completion |
| `job_manager_integration_test.go` | 1,974 | job-manager integration tests |
| `job_manager_test.go` | 723 | job-manager unit tests |
| `scan.go` | 436 | TTL scan task execution |
| `scan_integration_test.go` | 194 | scan cancellation integration tests |
| `scan_test.go` | 575 | scan-task unit tests |
| `session.go` | 336 | TTL session setup/restore |
| `session_integration_test.go` | 448 | session fault integration tests |
| `session_test.go` | 416 | session unit tests |
| `task_manager.go` | 831 | task ownership and scheduling |
| `task_manager_integration_test.go` | 723 | task-manager integration tests |
| `task_manager_test.go` | 311 | task-manager unit tests |
| `timer.go` | 287 | TTL timer requests |
| `timer_sync.go` | 430 | timer synchronization |
| `timer_test.go` | 592 | timer unit tests |
| `worker.go` | 143 | worker lifecycle |

## Missing-Go behavior restored

The branch delta had removed Go-master behavior from the complete package:

- `JobManager` no longer accepted an external-workload manager, recycled TTL
  tasks after the local running set completed, or elected the TTL owner through
  etcd. Those removals made the Go package silently diverge from Go master.
- The corresponding `extworkload`, `owner`, `config`, and kvproto BUILD
  dependencies had also been deleted.
- The focused Go-master tests covering option wiring, TTL-task recycling, and
  the master-role guard had been removed.

The production implementation, BUILD dependencies, and those focused tests
were restored. The test fake's method signatures were adapted to the current
branch `extworkload.Manager` interface (`InitializeGCV2(context.Context)`,
integer GC lifetime arguments, and `RegisterTTLTask`) without changing the
production contract. No Rust-only substitute was added.

## Rust boundary

`rg` found no Rust owner for `ttlworker`, `JobManager`, `TaskManager`, scan,
delete, or timer synchronization. The package coordinates Go domain,
infoschema, sessions, timer tablestore/runtime, etcd, TiKV, failpoints,
Prometheus metrics, and live testkit behavior. It remains a complete Go
orchestration boundary until those dependencies have dependency-closed Rust
owners; no speculative partial crate was introduced.

## Validation

Profile: **Ready** for this restoration batch.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh pkg/ttl/ttlworker -run 'TestCheckFinishedJob(RecyclesExternalTTLTask|DoesNotRecycleExternalTTLTaskFromMaster)$' -count=1` — passed. The package contains failpoints, so the canonical wrapper enabled and disabled them.
- `make bazel_prepare` — attempted as required because Go/Bazel artifacts and top-level tests were restored; unavailable locally (`bazel: No such file or directory`).
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — repository Ready gate (run after the batch is staged).
- `git diff --check` — passed after the batch is staged.

## Risks and unverified scope

- Correctness risk: owner election and external-workload recycling now match
  Go master; etcd/controller failure paths remain covered only by the existing
  Go tests.
- Compatibility risk: the test adapter follows this branch's changed
  `extworkload.Manager` API; no public API outside `ttlworker` was changed.
- Performance is unchanged from Go master.
- Not verified locally: Bazel generation (tool unavailable), live etcd owner
  campaigning, cross-runtime Rust interoperability, non-host platforms, and
  the full 50-shard TTL worker suite.

The rolling repository audit continues with the next unclaimed package.
