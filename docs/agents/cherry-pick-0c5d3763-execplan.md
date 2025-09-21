# Cherry-Pick `0c5d3763be` Meta Service Introduction

This ExecPlan is a living document. Keep `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` up to date as work proceeds.

Reference: `PLANS.md` at repository root; this plan is maintained according to it.

## Purpose / Big Picture

The goal is to copy upstream commit `0c5d3763be5e962a1f39ea916b516dd3df41d439` from `tidbcloud/tidb-cse` onto the current branch and preserve its user-visible behavior: TiKV-backed TiDB should use the new meta-service abstractions introduced by that commit, including the new `pkg/metaservice` package and the wiring changes around store bootstrap, InfoSync, helper access, and AutoID service registration.

Success is observable when the cherry-pick completes as a normal commit on the current branch and targeted validation shows the merged tree builds/tests with the new meta-service paths enabled.

## Progress

- [x] (2026-05-18 16:40 CST) Fetched upstream commit `0c5d3763be` from `https://github.com/tidbcloud/tidb-cse.git`.
- [x] (2026-05-18 16:42 CST) Started `git cherry-pick -x FETCH_HEAD` and identified 13 conflicted files.
- [ ] Resolve conflicts by preserving current-branch API evolution while importing the meta-service behavior from the upstream commit.
- [ ] Continue the cherry-pick and ensure the resulting index is conflict-free.
- [ ] Run `make bazel_prepare` because the cherry-pick adds new Go files and Bazel metadata.
- [ ] Run scoped validation plus required completion checks and record exact commands.

## Surprises & Discoveries

- Observation: The target branch has already evolved several interfaces beyond the source commit, especially `pkg/server/http_status.go`, `pkg/store/helper/helper.go`, and `pkg/store/mockstore/*`.
  Evidence: `git cherry-pick` conflicted in those files, and the current branch already contains newer keyspace-aware helper logic and status-server refactors.

- Observation: The current repository already replaced the old `kv.EtcdBackend` path with `kv.MetaServiceBackend` in some non-conflicted files.
  Evidence: `pkg/kv/kv.go` and `pkg/store/etcd.go` already reference `MetaServiceBackend`.

## Decision Log

- Decision: Resolve conflicts manually instead of forcing `theirs` or `ours`.
  Rationale: Several files contain legitimate concurrent evolution; blindly selecting one side would either drop the requested meta-service feature or regress current-branch behavior.
  Date/Author: 2026-05-18 / Codex

- Decision: Keep current-branch helper/status-server structure where it is clearly newer, then splice in the upstream meta-service behavior.
  Rationale: The current branch has additional functionality unrelated to the upstream commit that must not be regressed.
  Date/Author: 2026-05-18 / Codex

## Outcomes & Retrospective

Pending until the cherry-pick and validation finish.

## Context and Orientation

The upstream commit introduces `pkg/metaservice` and rewires several TiKV-facing abstractions from raw etcd access to meta-service-aware access. The current branch already contains newer work in some of the same files, so the merge must preserve:

- current branch helper keyspace-aware region/stat logic in `pkg/store/helper/helper.go` and `pkg/store/helper/helper_test.go`,
- current branch status-server and tracing additions in `pkg/server/http_status.go`,
- current branch mockstore keyspace helpers in `pkg/store/mockstore/mockstorage/storage.go` and `pkg/store/mockstore/unistore.go`.

The conflict-heavy files are:

- `cmd/tidb-server/main.go`
- `lightning/pkg/importer/import.go`
- `pkg/ddl/BUILD.bazel`
- `pkg/ddl/owner_mgr_test.go`
- `pkg/ddl/schematracker/checker.go`
- `pkg/domain/infosync/BUILD.bazel`
- `pkg/domain/infosync/info.go`
- `pkg/server/http_status.go`
- `pkg/store/driver/tikv_driver.go`
- `pkg/store/helper/BUILD.bazel`
- `pkg/store/helper/helper_test.go`
- `pkg/store/mockstore/mockstorage/storage.go`
- `pkg/store/mockstore/unistore.go`

## Plan of Work

First, resolve the mechanical BUILD/test conflicts by keeping both required dependencies where appropriate. Second, reconcile interface changes centered on `kv.MetaServiceBackend`, `tikv_driver`, mock storage, InfoSync, and the status server. Third, continue the cherry-pick, regenerate Bazel metadata, and run the smallest valid validation set plus completion checks.

## Concrete Steps

From repository root:

    git fetch https://github.com/tidbcloud/tidb-cse.git 0c5d3763be5e962a1f39ea916b516dd3df41d439
    git cherry-pick -x FETCH_HEAD

After conflicts are resolved:

    git add <resolved files>
    git cherry-pick --continue
    make bazel_prepare

Validation commands will be recorded after the merge is in a buildable state.

## Validation and Acceptance

Acceptance requires:

1. `git cherry-pick --continue` completes successfully.
2. `make bazel_prepare` succeeds and any required generated Bazel metadata is included.
3. Targeted tests or build commands covering the touched store/meta-service paths pass.
4. No conflict markers remain in the working tree.

## Idempotence and Recovery

The resolution steps are safe to rerun until `git cherry-pick --continue`. If a merge attempt becomes unsalvageable, `git cherry-pick --abort` would restore the pre-pick state, but that will only be used if the imported change proves incompatible.

## Artifacts and Notes

Initial conflict list from `git diff --name-only --diff-filter=U`:

    cmd/tidb-server/main.go
    lightning/pkg/importer/import.go
    pkg/ddl/BUILD.bazel
    pkg/ddl/owner_mgr_test.go
    pkg/ddl/schematracker/checker.go
    pkg/domain/infosync/BUILD.bazel
    pkg/domain/infosync/info.go
    pkg/server/http_status.go
    pkg/store/driver/tikv_driver.go
    pkg/store/helper/BUILD.bazel
    pkg/store/helper/helper_test.go
    pkg/store/mockstore/mockstorage/storage.go
    pkg/store/mockstore/unistore.go

## Interfaces and Dependencies

The key interfaces that must be consistent after the merge are:

    type MetaServiceBackend interface {
        GetPDAddrs() ([]string, error)
        TLSConfig() *tls.Config
        StartGCWorker() error
        MetaServiceInfo() (*metaservice.Info, error)
    }

The main files that must agree on this behavior are:

- `pkg/kv/kv.go`
- `pkg/store/driver/tikv_driver.go`
- `pkg/store/etcd.go`
- `pkg/store/mockstore/mockstorage/storage.go`
- `pkg/server/http_status.go`
- `pkg/domain/infosync/info.go`
