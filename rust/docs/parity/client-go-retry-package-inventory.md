# Pinned client-go config/retry package inventory

Open whole-package dependency of the snapshot/coprocessor audit. TiDB master
64e8c4c05ecbe7dfe3eca211c4fb44f97bd75c59 pins client-go/v2
v2.0.8-0.20260921040125-5f38569c8cc0. This inventory is not package acceptance.

| Artifact | Lines | SHA-256 | Rust owner / disposition |
| --- | ---: | --- | --- |
| `backoff.go` | 488 | `e98b7961006b5ddd0205749ea74aa34c96d8ab1bf8f125708e5e85a197258bc6` | tidb-txnkv/src/retry.rs and region retry owners; full source/options/metrics/cancellation reconciliation open |
| `backoff_test.go` | 289 | `2898d5d441a17dbeb4b8a58e23e154eeb5461773551cfe8f1b8950c759848e01` | tidb-txnkv/tests/region_error_recovery_source.rs; original cases/goleak acceptance remains open |
| `config.go` | 218 | `7334efcfa5da819b05b8491ff7845341ac06d72a012d0d6a53391bee8774a61b` | tidb-txnkv/src/retry.rs and region retry owners; full source/options/metrics/cancellation reconciliation open |
| `main_test.go` | 25 | `2c6ab265185c75aa5f5f51dc0be86aed1ccc229df9aba07d586b42f3c2ba1793` | tidb-txnkv/tests/region_error_recovery_source.rs; original cases/goleak acceptance remains open |

All four package artifacts are inventoried. There is no doc.go, fixture,
generated input/output, platform variant or package-local build file. The module
build and license input hashes are in client-go-txnlock-package-inventory.md.
Original TestMain uses goleak. Backoffer.Clone/Fork preserve charged sleep and
category history but omit the per-category delay functions, restarting their
exponential schedule. Snapshot split workers must inherit that state correctly.
