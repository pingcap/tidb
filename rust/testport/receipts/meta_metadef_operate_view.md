# `pkg/meta/metadef` — `OPERATE VIEW` system-schema parity receipt

Status: bounded source-parity batch; the direct package inventory is complete
for this behavior cluster, but the package is not claimed as a complete
Go-master transcreation. Comparison source: Go `origin/master` at
`78cac443a4f46c13bfe27eb247b5c80657952547` (2026-09-02).

## Complete direct inventory

All direct production, test, ownership, and build artifacts were read before
editing. The package has seven artifacts and 1,260 lines after the focused
regression. It has no package `doc.go`, fixture/testdata directory, generated
source/input, platform-specific variant, or nested package boundary.

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 29 | Go library/test metadata |
| `OWNERS` | 10 | ownership metadata |
| `db.go` | 68 | system-database classification helpers |
| `db_test.go` | 40 | database-classification tests |
| `system.go` | 164 | reserved system IDs and predicates |
| `system_tables_def.go` | 915 | system-table DDL constants |
| `system_test.go` | 34 | reserved-ID and DDL contract tests |

The package contains seven production functions and five test functions. No
benchmarks or generated/platform variants are present.

## Implemented behavior

- `CreateUserTable` and `CreateDBTable` now declare the Go-master
  `Operate_view_priv` column with the normal `ENUM('N','Y')` default.
- `CreateTablesPrivTable` now accepts the `Operate View` table privilege,
  allowing the privilege cache to load the same static privilege set as Go.
- Rust `tidb-metadef` already contains these three definitions and its source
  contract test; no duplicate Rust-only behavior was added.

The Go-master materialized-view maintenance table constants and reserved IDs
are a separate bootstrap/session behavior cluster and remain an explicit
follow-up boundary rather than being silently folded into this privilege
schema fix.

## Regression and validation

Profile: Ready for this bounded package behavior. The focused regression was
run before and after the production edit.

- Pre-fix `go test ./pkg/meta/metadef -run '^TestPrivilegeTableDefinitionsIncludeOperateView$' -count=1` failed because `CreateUserTable` lacked `Operate_view_priv`.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/meta/metadef -run '^TestPrivilegeTableDefinitionsIncludeOperateView$' -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./pkg/meta/metadef -count=1` — passed.
- `git diff --check` — passed for the package changes before commit.
- `make bazel_prepare` is required by the new top-level Go test under repository policy; the local gate is recorded with the package commit and is blocked when `bazel` is unavailable.

## Risks and boundaries

- Existing upgraded clusters still require the owning `pkg/session` upgrade
  path to add these columns; this receipt does not claim that migration.
- The privilege cache and user-attribute consumer are separate package
  boundaries and are validated in `privilege_privileges_user_attributes.md`.
