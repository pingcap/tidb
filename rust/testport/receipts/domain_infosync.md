# `pkg/domain/infosync` parity receipt

Comparison source: Go `origin/master` at commit
`5e8a1a229a7591ddac49a0cd3b795587c2595ab9` (2026-09-01).

## Complete inventory

The package contains exactly twelve tracked artifacts and 3,618 Go-master
lines: nine production source files, two test files, and one Bazel build file.
All twelve artifacts were read in full before editing. There is no `doc.go`,
fixture directory, `testdata`, generated source or input, platform variant,
benchmark, fuzz target, or `OWNERS` file.

| artifact | Go-master lines | Go-master blob | SHA-256 | role |
| --- | ---: | --- | --- | --- |
| `BUILD.bazel` | 94 | `a5fbec62cf6e9e55b100d48ba7a68c8743d73456` | `29f76ea5912003d49aa5075189d4b0f5ee00c9a2f65c26ca636cc1f4a59d2533` | library and test targets |
| `error.go` | 25 | `57c6085b8569650bf46c24ce179c34b252e83be2` | `0e62c10bd9bb361908e44d41efaf9c2db04650e0a1dd8727df7d5b1285f11804` | domain error sentinel |
| `info.go` | 1,353 | `cbb3a96f2796a3440bfb13744b14bcba5c6fcb42` | `660d18fefa173cdf50e759d441f4a8f0869bad7a1b4350460b1e63ac92de930d` | global info syncer, etcd/PD/session and TiFlash integration |
| `info_test.go` | 340 | `404cb0d0fd49a46601b1ecb38e4984efd379072a` | `d26f4468f297c85aee56d009a8a7490000ba7cf052d794338121aa0ab31e3b22` | syncer, placement, TiFlash, and PD error tests |
| `label_manager.go` | 176 | `ce1de98308fede070e06dbd872a53ddbc81947c1` | `3d53dba468fa56af1b2ca90b3337b6129d5e4e98d4d3bbed257ff44d1c238d4c` | PD label-rule manager |
| `label_manager_test.go` | 46 | `d0dae9ac077b7835b0e20ac75039a6f421eaf35e` | `54e9788cef46d4246eeeae53eadacc85c0350f47de271921ba84210e514be9bd` | keyspace label filtering tests |
| `mock_info.go` | 119 | `41e1880384d5b0cd8c588ee59758466706d43943` | `1705fe6e747c80eebbbb40a05922cbd737c4d75ad8193beaec6f993b8f2bef7c` | test info-syncer implementation |
| `placement_manager.go` | 175 | `1a9d20ee1bf9bbd0be4b8e8f73c3a648b20a5866` | `3112851d773c5ac95c3ab703ff535206670402302704dc161c18c1bf7b78c24b` | PD placement-bundle manager |
| `region.go` | 120 | `d1a8547618a7dcbbcc759547f7a76196323b3a8e` | `c81435488568fe27a146a5a78b71bfcede36630b74838a6b5c03ccd568899258` | region and columnar progress helpers |
| `resource_manager_client.go` | 164 | `25348059687e5a0c1bd24f095ef523265a572d28` | `c8b666503ef678c4471f8283539a69d8dc392e113a9dd39902d24d5f6822475d` | resource-manager mock and watch bridge |
| `schedule_manager.go` | 63 | `772e0208c7a94eb6cb65877b2bce4eb3841626ee` | `3016993050776c7b7016c234590d47538fa2c45612fc30719345f6ac075b1a8f` | PD schedule configuration manager |
| `tiflash_manager.go` | 943 | `4ec80e1009aeb7c7dfeeeb108e8ba614ed4ad9cb` | `e6e45b75c29899ef6fea07ebe3ec93f09e09638c73512f845ca9b42aa85fb4d5` | TiFlash placement, progress, and mock server |

The production inventory has 214 top-level declarations (including types,
constants, variables, and functions); the Go-master test inventory has eight
top-level tests plus `TestMain`. The working tree adds one seventeen-line focused
regression test, so the post-change checkout has 3,635 lines while retaining
the exact Go-master production inventory and all source hashes above.

## Parity findings and implementation

`GlobalInfoSyncerInit` was missing Go master's variadic
`serverinfo.SyncerOption` argument and did not forward options to
`serverinfo.NewSyncer`. This prevented non-serving domains from selecting the
status-endpoint-claim policy exposed by the completed `serverinfo` package.
The signature and forwarding were restored, and
`TestGlobalInfoSyncerInitServerInfoOptions` calls
`serverinfo.WithoutStatusEndpointClaim` to guard the API and behavior seam.

The local `mockResourceManagerClient` was also missing Go master's `Get` and
`Put` metastore methods. Both methods now return empty successful responses,
keeping the mock compatible with the current PD resource-manager interface.

Go master uses a newer kvproto oneof spelling in `label_manager_test.go` than
this branch's pinned kvproto dependency. The existing `{Id: 42}` literal is
retained as the only test-only compatibility substitution; no dependency was
upgraded for a source-only syntax change.

## Native integration decision

This package is Go-native domain infrastructure coupled to etcd leases,
PD's HTTP and resource-manager clients, session management, placement rules,
and TiFlash status endpoints. Rust has no dependency-closed infosync owner or
equivalent SQL/session/domain integration. No Rust-only behavior was found to
remove and no speculative Rust facade was introduced.

## Validation and risk

Profile: **Ready**. The focused regression failed before the implementation at
compile time with `too many arguments in call to GlobalInfoSyncerInit` and
passes after the change. The complete failpoint-aware package suite passes:

    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    ./tools/check/failpoint-go-test.sh ./pkg/domain/infosync -run '^TestGlobalInfoSyncerInitServerInfoOptions$' -count=1
    PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH \
    GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 \
    ./tools/check/failpoint-go-test.sh ./pkg/domain/infosync -count=1

Shared Ready gates (`make lint`, Rust formatting, and `git diff --check`) are
run for this batch. `make bazel_prepare` is required because a new top-level
Go test was added, but is blocked locally by the missing `bazel` executable.

The compatibility risk is limited to callers that opt into the new variadic
server-info policy and to PD mock metastore calls; existing callers remain
source-compatible. The endpoint policy itself is implemented by
`pkg/domain/serverinfo`, and this package only forwards the explicit option.

## Outcome

The complete infosync inventory, Go-master comparison, focused regression, and
explicit Go-only ownership boundary are recorded. The rolling repository audit
continues.
