# `pkg/executor` — USER_ATTRIBUTES visibility consumer receipt

Status: bounded consumer batch; the complete direct root-package inventory was
read before editing, but this is not a complete Go-master transcreation of the
large executor package. Comparison source: Go `origin/master` at
`78cac443a4f46c13bfe27eb247b5c80657952547` (2026-09-02).

## Inventory and boundary

The direct root package inventory covers 173 artifacts and 101,740 lines,
including all production/test Go files, `BUILD.bazel`, and `OWNERS`; recursive
inspection covers its nested executor packages and fixture books. The edited
consumer is `infoschema_reader.go`; its existing BUILD target already declares
the `pkg/privilege/privileges` dependency. No generated, platform-specific, or
fixture artifact was changed in this batch. The root package has no `doc.go`.

## Implemented behavior

`memtableRetriever.setDataForUserAttributes` now constructs the privilege
package's `UserAttrFilter` from the authenticated session and active roles,
then filters each `(user, host, metadata)` row before materializing the
INFORMATION_SCHEMA result. A nil session user leaves the existing unrestricted
fallback intact. This removes the Rust/Go behavior gap at the ordinary executor
consumer rather than adding a test-only filter.

## Regression and validation

The Go-master `TestInfoSchemaUserAttributes` regression lives in the owning
privilege test suite because it exercises the SQL-visible result. It was run
before the consumer edit and failed: an ordinary account received every user
row instead of only its own row. After the edit it passed for the no-privilege,
SUPER, SELECT-on-`mysql.user`, SELECT-on-`mysql.*`, CREATE USER, and
SYSTEM_USER cases.

- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 FAILPOINT_LOG_CHANNEL=stderr ./tools/check/failpoint-go-test.sh pkg/privilege/privileges -run '^TestInfoSchemaUserAttributes$' -count=1` — passed after the edit; pre-fix failure is recorded above.
- The focused privilege package cache/filter/SEMV2 run is recorded in
  `receipts/privilege_privileges.md`.
- `make bazel_prepare` is required by the Go consumer/test changes and is
  blocked locally because the `bazel` executable is unavailable.
- Ready-wide `make lint`, Rust formatting, and `git diff --check` are run with
  the package commit and recorded in the final handoff.

## Risks and unverified surfaces

- The filter's privilege matching depends on the privilege cache and upgraded
  system-table migration; those owners remain separate receipts.
- This consumer path is validated through the SQL integration regression, not
  through a full executor-package run. Bazel analysis and platform builds were
  not available locally.
