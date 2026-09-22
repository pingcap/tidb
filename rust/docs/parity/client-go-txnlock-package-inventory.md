# Pinned client-go txnkv/txnlock package inventory

Open whole-package dependency unit for the coprocessor/snapshot audit in
rust/docs/operations/store-copr-audit-execplan.md. Pin:
client-go/v2 v2.0.8-0.20260921040125-5f38569c8cc0, selected by TiDB master
bb80c86a127b579a93c2070a7f3464ef1b609e38. This is an inventory and bounded
seed evidence; the package is not transcreated.

| Artifact | Lines | SHA-256 | Rust owner / disposition |
| --- | ---: | --- | --- |
| `lock.go` | 43 | `4157ffa6a50f11540424c847e57f2922a4b112829e493a71f769f55697734694` | tidb-txnkv/src/lock/model.rs; shared/ordinary lock decoding |
| `lock_resolver.go` | 1777 | `d7018971ec92613445bbdb5a7ebad3214bcb63f6a52e7becf9a83952eea2da91` | tidb-txnkv/src/lock/resolver.rs and pessimistic.rs; partial, complete resolver lifecycle/options/cache/metrics remain open |
| `lock_resolver_test.go` | 160 | `db4a4a1786abacef8cc3965b587bd628103e3bd541e2c55e33fa634a3c054ddc` | tidb-txnkv/tests/lock_resolver_source.rs and pessimistic_lock_source.rs; original test reconciliation remains open |
| `lock_test.go` | 63 | `7061b234c5feab7f1bba1b2479ff36517702a1c7a3e65e32261bc85dc6c40bf1` | tidb-txnkv/tests/lock_model_source.rs; original test reconciliation remains open |
| `main_test.go` | 25 | `5c53aa90d1afd98d5dd5ad42eee7897264c8b9abffda6a3a6632473748d39cfd` | Cargo harness; Go testsetup/goleak gate remains separately required |
| `test_probe.go` | 129 | `380a38d0f35589f567edd2a052d702aa7d9b3b498618f919cc8f8e46844c354d` | Go test support; existing Rust scripted client seams, full correspondence open |

All six package artifacts are listed. There is no doc.go, nested fixture,
platform/build-tag variant, generated output/input or package-local Bazel file
in this module package. test_probe.go is production-compiled test support and
remains part of the inventory. Module build/license inputs are pinned below;
TiDB go.mod/go.sum and DEPS.bzl govern the consuming build.

| Module input | SHA-256 |
| --- | --- |
| `go.mod` | `4e7e04c1183bcd6ad33ec5afb44725bd93f94a92d101127a47f476c02c7bd805` |
| `go.sum` | `4037ceea026bbaccdc199f3e70c3c641f91b35247de34ecb28e50880bef0565e` |
| `LICENSE` | `c71d239df91726fc519c6eb72d318ec65820627232b2f796219e87dcf35d0ab4` |

The current Go ignored-hint contract is lock_resolver.go's
backoffOnLockHintsInRequest before resolveLocks: only ForRead checks the exact
request hints, any matching transaction charges one BoTxnLockFast backoff, and
resolution runs afterward. Repeated ignored responses exhaust the caller's
existing backoffer. The complete package's status cache, asynchronous cleanup,
metrics, failpoints, original tests and all caller integrations remain open.

The direct-unary cop response delegate now borrows the current per-region
budget through blocking-lock status/cleanup recovery, matching
coprocessor.go:2674-2728's shared Backoffer path. This is focused caller seed
evidence; complete caller and package reconciliation remains open.
