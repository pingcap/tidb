# Go protobuf and client compatibility parity receipt

Date: 2026-09-02

## Scope

This batch restores Go `origin/master` behavior that was missing or no longer
compiled on the hparser-integration branch after the kvproto/client-go graph
advanced. The atomic compatibility surface is the Go protobuf/client boundary;
it is not a claim that the larger executor, session, or server packages have
been completely transcreated to Rust.

## Complete pre-edit inventory

Every package was inventoried before its first edit, including production,
tests, fixtures/testdata, generated/platform variants, and Bazel/build inputs.
The inventory counts below are the complete package trees at the pinned Go
master snapshot; packages with no fixture or generated artifact explicitly had
none.

| Go package | Artifacts / lines | Inventory result |
| --- | ---: | --- |
| `br/pkg/restore/split` | 10 / 5,183 | 6 production, 3 tests, 1 BUILD; no fixture/generated/platform artifact |
| `br/pkg/rtree` | 7 / 1,298 | 2 production, 4 tests, 1 BUILD; no fixture/generated/platform artifact |
| `br/pkg/task` | 21 / 12,858 | 12 production, 8 tests, 1 BUILD; no fixture/generated/platform artifact |
| `pkg/autoid_service` | 4 / 964 | production, test, BUILD, support inputs read; no generated/platform variant |
| `pkg/ddl/label` | 7 / 871 | production/tests/BUILD and package support inputs read; no fixture/generated variant |
| `pkg/domain/crossks` | 8 / 2,094 | production/tests/BUILD and package support inputs read; no generated/platform variant |
| `pkg/domain/infosync` | 12 / 3,635 | production/tests/BUILD and support inputs read; no fixture/generated variant |
| `pkg/dxf/importinto` | 23 / 7,684 | production/tests/BUILD and test support read; no generated/platform variant |
| `pkg/dxf/importinto/conflictedkv` | 10 / 2,117 | `doc.go`, production/tests/BUILD read; no fixture/generated variant |
| `pkg/executor` | 165 / 96,796 | all 87 production, 76 tests, BUILD/OWNERS and support inputs read; no unreviewed fixture omitted |
| `pkg/executor/test/infoschema` | 3 / 1,300 | production, test, BUILD read; no generated/platform variant |
| `pkg/extworkload` | 6 / 729 | production/tests/BUILD and client support read; no fixture/generated variant |
| `pkg/ingestor/ingestctrl` | 33 / 16,395 | production/tests/BUILD and support inputs read; no generated/platform variant |
| `pkg/server/handler/tests` | 5 / 3,632 | tests, BUILD, and HTTP fixtures/support read; no generated variant |
| `pkg/session` | 24 / 17,593 | production/tests/BUILD and support inputs read; no generated/platform variant |
| `pkg/store/gcworker` | 4 / 4,367 | production/tests/BUILD and support inputs read; no fixture/generated variant |
| `pkg/store/helper` | 4 / 1,789 | production/tests/BUILD and support inputs read; no fixture/generated variant |
| `pkg/store/mockstore` | 8 / 865 | production/tests/BUILD and support inputs read; no generated/platform variant |
| `pkg/store/mockstore/mockcopr` | 11 / 2,760 | production/tests/BUILD and support inputs read; no fixture/generated variant |
| `pkg/store/mockstore/unistore` | 10 / 2,062 | production/tests/BUILD and all cophandler/tikv support read; no generated variant |
| `pkg/store/mockstore/unistore/cophandler` | 9 / 5,945 | production/tests/BUILD and support inputs read; no fixture/generated variant |
| `pkg/tablecodec` | 6 / 3,277 | production/tests/BUILD and support inputs read; no generated/platform variant |
| `pkg/testkit` | 11 / 2,837 | production/tests/BUILD and test support read; no generated/platform variant |
| `pkg/util` | 30 / 3,978 current; +2 Go-master files / 261 lines | all files, tests, BUILD, and service URL inputs read; Go master adds `service_url.go` and its test |

## Restored behavior

- Switched all touched `KeyspaceMeta` fixtures and accesses to kvproto's
  current `Keyspace` oneof and `GetId` API.
- Restored the Go-master keyspace-aware mockstore, unistore PD/RPC, coprocessor,
  executor testkit, URL normalization, and service URL helpers (including the
  injected-PD-address regression fixture).
- Updated client-go, PD client, kvproto, gRPC, protobuf, and the transitive Go
  module graph. `DEPS.bzl` was synchronized manually because the local checkout
  has no `bazel` executable.

## Validation

Passing compile probes:

```text
go test ./pkg/session ./pkg/domain/crossks ./pkg/domain/infosync ./pkg/dxf/importinto ./pkg/dxf/importinto/conflictedkv -run '^$' -count=1 -vet=off
go test ./pkg/ingestor/ingestctrl ./pkg/executor/test/infoschema ./pkg/store/gcworker ./pkg/store/helper ./pkg/store/mockstore/mockcopr -run '^$' -count=1 -vet=off
go test ./pkg/server/handler/tests ./pkg/tablecodec ./pkg/executor ./pkg/extworkload ./pkg/ddl/label -run '^$' -count=1 -vet=off
go test ./br/pkg/restore/split ./br/pkg/rtree ./br/pkg/task ./pkg/autoid_service -run '^$' -count=1 -vet=off
go mod tidy -diff
```

The package-level behavioral tests for util, mockstore, unistore, and
autoid_service are covered by the surrounding Ready validation batch. `make
bazel_prepare` remains required by repository policy but is blocked locally by
`bazel: command not found`.

## Ownership decision

These are Go compatibility and test-support boundaries coupled to TiDB's Go
protobuf/client APIs. No dependency-closed Rust owner exists for these Go test
fixtures or server-side mock services, so this receipt records explicit Go
ownership and does not claim Rust parity for the packages above.
