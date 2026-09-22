# Pinned client-go util package inventory

The entire root util package is an open dependency acceptance unit of
transaction snapshots. TiDB master 0b505ecc58b659655345b7bb85a619db02f94300
pins client-go/v2 v2.0.8-0.20260921040125-5f38569c8cc0. The existing Rust
owner is rust/third_party/tikv-client-rs/src/util, with snapshot diagnostic
aggregation in transaction/snapshot_stats.rs. This inventory and individual
regressions are seed evidence, not a transcreated-package claim.

| Artifact | Lines | SHA-256 | Rust owner / disposition |
| --- | ---: | --- | --- |
| `dns.go` | 60 | `fa440043804ddf52a2fe8b10fc749daa701b373f6fd592430b823f61a6f645bf` | dns.rs and existing native fixtures; complete reconciliation open |
| `execdetails.go` | 1366 | `9c4e32e4c0357c1b002f24c098e1d0317432aec4d3069be7f9ae15b37150000e` | execdetails.rs and snapshot_stats.rs; original tests only partially reconciled |
| `execdetails_test.go` | 1075 | `1b256a55502b2ede163dddf57d982345a0024b40c066716eb7d38c23042f42f9` | execdetails.rs and snapshot_stats.rs; original tests only partially reconciled |
| `failpoint.go` | 63 | `11341b0951b5798e643da1b590fb0b9d84b67b7608d9c7310a866250cae919f0` | failpoint.rs and existing native fixtures; complete reconciliation open |
| `main_test.go` | 25 | `40d5549f5ecd71526173d7943a9808e6a168b117d370f6f793aadb1ce0daf285` | Rust test lifecycle; full original leak/build gates open |
| `misc.go` | 200 | `3548e39d38b86370ba1bf3012eb162e0d4dd35bdc0fff96a5daf38f3a3951f6f` | misc.rs and existing native fixtures; complete reconciliation open |
| `misc_test.go` | 140 | `72b530fa37b3acfa50dec945738fd368bcb043e24999be0f627e969e7e79e52d` | misc.rs and existing native fixtures; complete reconciliation open |
| `pd_interceptor.go` | 150 | `5c43afc2dcdf4199ee8f56f4826c15a5aaf39adef5c95b2f7ba4ace661f80523` | pd_interceptor.rs and existing native fixtures; complete reconciliation open |
| `point_response_stats.go` | 115 | `76ff883bd8cc3d41c0960c551cc34371d15b714d7dd9e3dc2f7767019ebf1311` | point_response_stats.rs; coverage/value/invalidity integration and original case correspondence |
| `rate_limit.go` | 73 | `37f9a143a212e8cd25edd911b33d3ad960f23ab520d2efe49b3bc43bb1ec5abe` | rate_limit.rs and existing native fixtures; complete reconciliation open |
| `rate_limit_test.go` | 70 | `0cd0b82982be5680f72d63a938d7d4d6c63826c1d961c3edfa6058fd3c4e0bd7` | rate_limit.rs and existing native fixtures; complete reconciliation open |
| `request_source.go` | 190 | `584a1879d137339adad74a92b35917c844104ed3ebe61142266530347dcd4c7c` | request_source.rs and existing native fixtures; complete reconciliation open |
| `request_source_test.go` | 95 | `5eeb8d4595fb229475320cf62b60a07a31f3ef137c174e8c6071a41409704a3c` | request_source.rs and existing native fixtures; complete reconciliation open |
| `ts_set.go` | 74 | `a3969d914b4aec481e0f8918ee48d252f9479d44f1cf02c205e1ca6d413ceb86` | ts_set.rs and existing native fixtures; complete reconciliation open |

All root files are inventoried. There is no root doc.go, generated input/output,
fixture directory or package-local build artifact. The async, codec, collectors,
intest, israce and redact subdirectories are separate Go packages, not root
source variants; intest/israce have build-tag variants that remain independent
acceptance gates. Module go.mod/go.sum/LICENSE hashes are recorded in
client-go-txnlock-package-inventory.md. TiDB go.mod/go.sum and DEPS.bzl provide
the consuming build boundary. Root main_test.go owns its original test lifecycle.

Point response counters use explicit wrapping addition and signed Go-compatible
scan counts. Zero response state is valid without coverage; missing scan details
remain sticky; invalid merges preserve prior values. The data is copied by value.
The optional live snapshot collector and vendored response interceptor share
these types. The util package's remaining production/test/build/platform
reconciliation is open; no whole-package completion is inferred from this work.
