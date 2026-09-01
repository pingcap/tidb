# `pkg/util/admin` — Go-master parity boundary receipt

Go baseline: `origin/master` at
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01). This package is the
storage/index consistency helper behind `ADMIN CHECK TABLE`; its executable
owner crosses executor, table, tablecodec, row decoding, and session layers.

## Complete inventory

All four Go-master artifacts were read in full before making the ownership
decision:

| Artifact | Lines | Git blob | SHA-256 | Disposition |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 46 | `29abe75673b3a564edce36a7434151abbc2da0ae` | `af12a97650cbb8c0139582c06c84dd4374d56bd1b4bcd5190f68923762a36433` | library/integration-test targets and cross-package deps inventoried |
| `admin.go` | 268 | `c9000d95d6c765bae2c0e5e8c636ab212e7a656b` | `9773c7c65c89ca209344e0a5576da5e5a0025a197dd6313d89f5361750688907` | count and row/index consistency production owner inventoried |
| `admin_integration_test.go` | 57 | `41b5e36ff9cd72eee661f7bff839e754e52f3c94` | `7dcd54f2caa1ab98846f7dd6f80348403ef6d69735632e52bd72f504d94ae75b` | corrupted-index SQL regression inventoried and executed |
| `main_test.go` | 41 | `fe5ca4e6a60ddf8212b63b7576ad5963c4bc2f9c` | `e0ab3d276299af17751e49e880a6b96d6d3b0974cce6d802c1106a1415039c9c` | common setup/goleak harness inventoried |

There is no `doc.go`, generated/platform variant, fixture, benchmark, fuzz
target, or nested package. The production file has 14 function/method
declarations plus the `RecordData` carrier and two count constants. The one
source test mutates a mock-store record key to prove `ADMIN CHECK TABLE`
rejects inconsistent data; `main_test.go` only configures common setup and
leak exclusions.

## Go behavior

`CheckIndicesCount` temporarily enables invisible indexes, selects a snapshot,
counts table rows and each index through restricted SQL, logs counts, and
returns the mismatch direction/index. `CheckRecordAndIndex` iterates record
keys, decodes rows with a session-aware row decoder, repairs NULL defaults,
recomputes index keys, detects duplicate/missing entries, and reports through
the consistency reporter. The internal iterator handles global-index
partition handles and uses `RowKeyPrefixFilter`; all storage and decode errors
are traced. `ErrAdminCheckTable` carries the 8003 contract.

## Rust ownership and integration decision

Rust's `tidb-executor::admin_check` already contains a substantial native
consistency checker and source-derived tests for generated columns, enum and
point-get indexes, count mismatches, and corruption. It is not a complete
root-package owner for the Go helper's restricted-SQL/session context,
invisible-index toggle, row-decoder/default-value behavior, global-index
partition path, consistency reporter, and SQL command integration. The
existing executor owner is the correct integration boundary; adding another
`tidb-util` checker would duplicate storage semantics and create Rust-only
behavior. No production source change is justified by this audit.

## Validation

Profile: **WIP**. This is an inventory and explicit boundary audit with no
code fix and no package-completion claim; `make bazel_prepare` and the Ready
lint gate are not triggered.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
go test -tags=intest,deadlock ./pkg/util/admin -run '^TestAdminCheckTableCorrupted$' -count=1
# ok
```

## Risks and unverified behavior

- Correctness: the Go corrupted-index integration regression passes, and the
  Rust executor checker has source-derived corruption tests; no duplicate
  utility owner is claimed.
- Compatibility: count direction, invisible indexes, NULL/default decoding,
  partition handles, and consistency error formatting remain cross-package
  contracts.
- Performance: no runtime code changed. A future package-complete owner must
  preserve full-index scans and snapshot semantics.
- Not verified locally: the full flaky 50-shard executor/admin suite, live
  TiKV consistency reporting, global-index partition integration, and the
  Rust server SQL command path.
