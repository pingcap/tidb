# Pinned client-go config package inventory

This complete package is an open dependency acceptance unit of txnsnapshot.
TiDB master 0b505ecc58b659655345b7bb85a619db02f94300 pins client-go/v2
v2.0.8-0.20260921040125-5f38569c8cc0. This inventory is not a package parity
claim. The live BatchGet consumer reuses tidb-config::tikvcfg, already published
by TiDB's StoreGlobalConfig; the duplicate vendored client configuration remains
an integration boundary to reconcile for its own consumers.

| Artifact | Lines | SHA-256 | Rust owner / disposition |
| --- | ---: | --- | --- |
| `OWNERS` | 7 | `6c5efe13722aa214714edf7fa2ca645d20aa414792a5980540133d48484c79bf` | Original review metadata; retained upstream, no runtime counterpart. |
| `client.go` | 300 | `f376928961422a3cfc16edd66bf2c24000b44f6c930002e76334ed36663d8800` | tidb-config/src/tikvcfg.rs and vendored client configuration; field/default/validation reconciliation open. |
| `config.go` | 229 | `5c2e95914310f9760a55c630842e444091e2e338078f5fe3ff727cf5e88556eb` | tidb-config/src/tikvcfg.rs published config; BatchGet flag now consumed by live tidb-txnkv. Full API/default/path reconciliation open. |
| `config_test.go` | 172 | `71651fe444eb05af3cd6018e027f76733be1549211bee436398197f6a851d787` | Existing tidb-config source tests plus snapshot flag regression; complete original-test mapping open. |
| `main_test.go` | 27 | `dbba0a6b61a885ce774947de30bc8a7cfc7c5c299c34fcd5a3ed132906ca4654` | Failpoint and goleak test lifecycle; Rust scoped test ownership, full gate open. |
| `nextgen_off.go` | 20 | `37882c6db6443f831d7b6cc812e5c3a192757b5aae9b33684f8dc7c1f890a965` | Default build variant; feature correspondence gate open. |
| `nextgen_on.go` | 20 | `3fb781d3787d50fd735c116721ece4c16d171156367f5de1ae9f6b695e8d5d6b` | nextgen build variant; feature correspondence gate open. |
| `ruv2.go` | 66 | `d3e72e1e484567f1562077cd5816567fde45bd00764de24acf8fd19db0315822` | Resource-unit weights; complete owner/dependency reconciliation open. |
| `ruv2_test.go` | 81 | `5f25a959e9bac6cb3a5be4f015f76d6ef16b67e463dcc35825d308e145720d2b` | Original resource-unit configuration tests; mapping open. |
| `security.go` | 106 | `fa5aed97334b8be350ad73dceaab0b351b20f5763b44b89dbe3ebd977a70eb65` | TLS/security config; tidb-config and native transport owners, full API/validation gate open. |
| `security_test.go` | 120 | `fa3c227f9ce4b70011527acc042ada3a23cb7ba049579bfe0e72e4c763ec9fd1` | Original security fixtures/tests; mapping open. |

All eleven root artifacts are listed. config/retry is a separate four-artifact
package inventoried in client-go-retry-package-inventory.md. The package has
no doc.go, local BUILD file, nested fixture directory or generated input/output.
The two nextgen files are mutually exclusive Go build variants. Module go.mod,
go.sum and LICENSE hashes are recorded in client-go-txnlock-package-inventory.md;
TiDB go.mod/go.sum and DEPS.bzl provide the consuming build. Original package
tests include a failpoint-aware TestMain and a goroutine leak gate.

The read-only async_batch_get_enabled accessor borrows the existing publication
lock long enough to copy one boolean. It adds no setting or second global and
avoids cloning unrelated configuration strings on every BatchGet. The
snapshot_lock_wait_source standalone Rust target changes the published TiDB
setting after transaction construction and verifies both modes, independent
synchronous initial requests, the one-batch bypass and worker join semantics.
All other source/test/build/security/nextgen and complete-package gates remain
open; see store-copr-audit-execplan.md for exact validation and limitations.
