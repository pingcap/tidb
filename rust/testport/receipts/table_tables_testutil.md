# `pkg/table/tables/testutil` parity receipt

Status: Completed the missing-Go-behavior fix and recorded the complete
package inventory. This receipt covers the package's helper and focused
regression; it is not a repository-wide parity claim.

Comparison source: Go `origin/master` at
`c6054025ed4c32ab3672a2a24ea46892714d21ec`.
Rust comparison branch: `origin/hparser-integration` at the pre-fix commit.

## Complete Go inventory

Before editing, both tracked artifacts in `pkg/table/tables/testutil` were read
in full: 94 lines. The focused regression added one test artifact, making the
post-fix package three artifacts and 154 lines. There is no package `doc.go`,
fixture or `testdata` directory, generated source or input, platform/build-tag
variant, benchmark, fuzz target, README, or ownership artifact.

| artifact | pre-fix lines | post-fix lines | role |
| --- | ---: | ---: | --- |
| `BUILD.bazel` | 19 | 31 | Go helper library and focused test target |
| `indexcheck.go` | 75 | 78 | index-key range/count helper |
| `indexcheck_test.go` | — | 45 | table-collation encoder regression |

## Missing-Go behavior restored

The branch delta hard-coded `codec.NewEncoder(collate.NewCollationEnabled())`
when constructing the minimum index key. Go master uses the table's persisted
`UseNewCollate()` mode, which is required when a task/table snapshot differs
from the process default. The fix restores that source behavior, removes the
unneeded collation BUILD dependency, and adds `newIndexEncoder` as the narrow
test seam.

`TestIndexEncoderUsesTableCollation` uses a collated string datum and two table
modes. Under the pre-fix process-default implementation both encoders were
identical and the test failed; after the fix the old/new encoded keys differ as
required by the table mode.

## Rust ownership and boundary

No Rust production owner exists for this Go test helper. Rust's
`tidb-tablecodec` owns key encoding, while executor source inventories cover
the callers; no Rust helper or test-only API should replace Go's domain/testkit
index scan utility. The helper remains a Go-only test boundary.

## Validation

Profile: **Ready** for this restoration batch.

- Pre-fix regression: `go test -tags=intest ./pkg/table/tables/testutil -run TestIndexEncoderUsesTableCollation -count=1` — failed as expected because both encoders used the process default.
- Post-fix regression: `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/table/tables/testutil -run TestIndexEncoderUsesTableCollation -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/table/tables/testutil -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make bazel_prepare` — attempted as required for the new Go test/Bazel target; unavailable locally (`bazel: No such file or directory`).
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — repository Ready gate (run after the batch is staged).
- `git diff --check` — passed after the batch is staged.

## Risks and unverified scope

- Correctness risk is reduced: persisted table collation mode now drives the
  helper's index-key encoder, with a focused fail-before/pass-after test.
- Compatibility risk is limited to test-helper behavior and BUILD metadata; no
  production DML API changed.
- Performance impact is negligible.
- Not verified locally: Bazel generation (tool unavailable), live cross-table
  snapshot modes beyond the focused seam, non-host platforms, and repository-
  wide integration suites.

The rolling repository audit continues with the next unclaimed package.
