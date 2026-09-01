# `pkg/util/admin` — Go-master parity receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). The package is
implemented by the Rust executor/session ADMIN CHECK owners; this audit found
and fixed one clustered-primary-index selection gap.

## Complete inventory

All four Go-master artifacts were read in full before editing:

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 46 | `29abe75673b3a564edce36a7434151abbc2da0ae` | `af12a97650cbb8c0139582c06c84dd4374d56bd1b4bcd5190f68923762a36433` | library and flaky short integration-test targets, with all dependencies inventoried |
| `admin.go` | 268 | `c9000d95d6c765bae2c0e5e8c636ab212e7a656b` | `9773c7c65c89ca209344e0a5576da5e5a0025a197dd6313d89f5361750688907` | count comparison, row/index consistency scan, row decoding, and error declaration |
| `admin_integration_test.go` | 57 | `41b5e36ff9cd72eee661f7bff839e754e52f3c94` | `7dcd54f2caa1ab98846f7dd6f80348403ef6d69735632e52bd72f504d94ae75b` | mock-store corruption regression for `ADMIN CHECK TABLE` |
| `main_test.go` | 41 | `fe5ca4e6a60ddf8212b63b7576ad5963c4bc2f9c` | `e0ab3d276299af17751e49e880a6b96d6d3b0974cce6d802c1106a1415039c9c` | common setup, async-commit configuration, and goleak harness |

The package totals 412 textual lines, five named production functions
(`getCount`, `CheckIndicesCount`, `CheckRecordAndIndex`, `makeRowDecoder`, and
`iterRecords`) plus five function literals used for restoration, reporting,
and row filtering. It also has the `RecordData` carrier, two count-result
constants, the standard error variable, one source regression, and one test
harness. There is no `doc.go`, README, generated output, platform-specific
source, fixture, benchmark, fuzz target, nested package, or additional build
artifact.

## Go behavior

`CheckIndicesCount` runs restricted `COUNT(*)` queries at the transaction or
snapshot timestamp with invisible indexes enabled, preserving the session
setting afterward. It returns `TblCntGreater`/`IdxCntGreater` and the first
offending index, or `ErrAdminCheckTable` with the source count message.

`CheckRecordAndIndex` scans the record key range, decodes rows through the
session schema, restores origin defaults for nullable stored values, handles
global partition handles, and checks each row's generated index key. Missing
or duplicate entries are reported through the consistency reporter; storage,
decode, and default-value failures are traced. The integration test deliberately
adds a second record key in the transaction buffer and proves `ADMIN CHECK
TABLE` rejects the committed corruption.

## Rust owner and fix

`tidb-executor::admin_check` owns the count and both-direction consistency
checks, index-entry decoding, corruption error shapes, and handle-range
results. `tidb-session::admin_check_arm` owns statement resolution, refusal
rules, empty-success output, and error mapping. The existing Rust owner tests
cover generated-column, enum/unique, null, range, missing/orphaned entry,
wrong-handle, value-mismatch, unknown-index, view, multi-table, and clustered
handle cases.

The Go planner's `buildPhysicalIndexLookUpReaders` skips a clustered PRIMARY
KEY: its encoded key is the record key and no `_i` entry exists. Rust's
`check_table` previously selected every metadata index, so a composite
clustered primary table was scanned under a nonexistent index range and
reported a false `DataInconsistent`. The fix keeps the metadata entry for
other consumers, filters `clustered_primary` from whole-table checks, and
returns success for an explicitly named clustered primary index, matching Go's
zero-reader `ADMIN CHECK INDEX` path. A focused session regression covers both
forms.

No Rust-only behavior was removed; `clustered_primary` is already the source
metadata flag used to skip primary-key entry writes.

## Validation (Ready profile)

The pre-fix focused Rust session test failed with
`DataInconsistent` on index `PRIMARY` for
`clustered_primary_key_with_two_indexes`. After the fix:

- `cargo +nightly-2026-08-22 fmt --all -- --check` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-executor --lib admin_check -- --test-threads=1` — 7 passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-session --lib tests_admin_check::clustered_primary_key_is_not_scanned_as_an_index -- --test-threads=1` — passed.
- `cargo +nightly-2026-08-22 test --offline --locked -p tidb-session --lib tests_admin_check -- --test-threads=1` — 17 passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test -tags=intest ./pkg/util/admin -count=1` — passed.
- `PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 make lint` — passed.
- `git diff --check` — passed.

No Go or Bazel file changed, so `make bazel_prepare` is not required. The
untagged Go test was also attempted and stopped at the repository's expected
`--tags=intest` guard; the canonical tagged command above is the passing
result. Full Bazel/sharded and live TiKV tests were not run.

## Risks and unverified scope

- Correctness: the focused pre-fix failure, seven executor owner tests, all 17
  session ADMIN CHECK tests, and the Go corruption integration test pass.
- Compatibility: clustered PRIMARY remains metadata-visible to planners and
  stats, while consistency scans now match Go's intentional omission.
- Performance: whole-table checks avoid a nonexistent primary-index scan; no
  secondary-index path or storage write path changed.
- Not verified locally: Bazel execution, real TiKV/partitioned live checks,
  fast-check-table worker paths, and race-enabled admin integration.
