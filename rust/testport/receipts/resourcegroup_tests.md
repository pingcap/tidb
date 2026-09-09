# `pkg/resourcegroup/tests` — complete Go-master integration-test receipt

Comparison source: Go `origin/master` at commit
`0bc44483e3e41a8ea917d4382dc202369468d200` (2026-09-01).

## Complete inventory

This separate `tests` package contains two tracked artifacts and 889 lines.
Both the nine-shard flaky Bazel target and the complete integration source
were read line by line. There are no generated results, SQL fixture files,
platform variants, benchmarks, or fuzz targets in this directory.

| Artifact | Lines | Git blob | SHA-256 | Role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 27 | `0268aa2eff7dbf3377c8a6eba8171e45ba307bba` | `695add602311ca3f85a128ebcf7b6d4e2a5c585495f15b59ea7032d5e9766cf2` | nine-shard, race-enabled integration target with failpoint/testkit deps |
| `resource_group_test.go` | 862 | `b70dba26132bd193cfd893e8ca78515bda5de352` | `9be50ebbcbdb2becc6d15dde8fd8d9a9a8defd48dd8b8444634e7cb542041de9` | DDL, runaway, flush, flood, binding-hint, conversion, and burst-limit scenarios |

The nine source tests are `TestResourceGroupBasic`,
`TestResourceGroupRunaway`, `TestResourceGroupRunawayExceedTiDBSide`,
`TestRunawayRecordFlushLoopAddAndFlush`, `TestResourceGroupRunawayFlood`,
`TestAlreadyExistsDefaultResourceGroup`, `TestNewResourceGroupFromOptions`,
`TestBindHints`, and `TestResourceGroupBurstLimit`. They exercise resource
control enable/disable gates, CREATE/ALTER/DROP and information-schema/show
output, every runaway action/watch shape, TiDB-side elapsed enforcement,
asynchronous record/watch flushing and expiry, repeated-query aggregation,
default-group races, DDL option validation, binding hint precedence, and
burst-limit transitions.

## Rust ownership and decision

The scenarios span Go DDL/domain/session/server/testkit lifecycle and the
separate runaway package. Rust parser/model/DDL conversion and RU-carrier
fragments do not provide a dependency-closed integration harness equivalent
to this package. The source tests remain authoritative evidence for the Go
behavior; no Rust-only fixture or substitute test is introduced.

## Validation and risk

Profile: **WIP** integration audit. Failpoints were enabled and disabled by
the repository wrapper.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
./tools/check/failpoint-go-test.sh pkg/resourcegroup/tests -count=1
# PASS; ok .../pkg/resourcegroup/tests 31.682s
```

No source behavior changed, so correctness, compatibility, and performance
risk are unchanged. Not verified: Bazel execution, a real TiKV cluster, and a
Rust end-to-end resource-control harness.
