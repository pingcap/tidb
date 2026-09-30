# Complete protocol ownership repair

Baseline: integration `1203ee4487cb6095b0e7e73b0770743999fd6259`;
Go master `e953a09d9d5e29e60c62f42d3aacebb819af49a5`. The integration
branch was pulled with `--ff-only` and master fetched before this change.
Both were current. Native client-rust remains the already synchronized
`b2b3783`; no native source or generated artifact was edited.

## Removal and ownership

All five remaining handwritten schemas in `tidb-proto/proto` are deleted:
`pdpb.proto`, `brpb.proto`, `tikvpb.proto`, `etcdserverpb.proto` and
`mvccpb.proto`. PD/BR use the complete native generated packages. TiKV's
complete descriptor supplies every service, message and batch command;
message-to-Bytes adaptation preserves the existing transport representation
without a second field/tag list. Health feedback retains message presence.
Etcd generation takes all six schemas from master's pinned API module,
including auth, membership and internal Raft inputs. Exact source bytes,
file membership, licenses, dependency pins and original module artifacts
are recorded and checked through the existing source guards.

The handwritten `unimplemented_tikv` helper and repeated fixture forwarding
methods are removed. Generated default server methods let mocks override
only supported RPCs, following Go's embedded Unimplemented server pattern.
The PD fixture adapter shares native request/response identities. Production
PD uses the native client; its GC scope now constructs the proper oneof.

Changed files are the proto crate (build, exports, inputs, receipt and tests),
PD request construction and fixtures, transaction RPC fixtures and one stale
transport comment, source/audit scripts and their tests, Cargo manifest/lock,
Makefile's lint gate and the audit/ExecPlan documentation. The shared native
receipt now covers nine generated packages and all 139 kvproto artifacts;
the etcd receipt covers every one of its 24 module artifacts.

## Evidence

Before production edits, three new tests in `pd_wire_source` failed:
keyspace zero and watch progress re-encoded to empty bytes, and PD/BR types
had different identities from the native owner. Existing three tests passed.
The full proto suite now passes 55 tests, including the original Go global-GC
barrier compatibility cases, exact wire vectors, complete TiKV message/service
comparison and a pointer check for zero-copy batch decoding.

The after-image `protocol-contracts-after.json` is generated from actual Rust
build descriptors and master's pinned inputs. It reports **zero omissions or
contract differences** across PD, BR, TiKV and all four etcd protobuf packages.
The 71 intentional opaque TiKV fields are retained, not classified as defects.
The old 400 omissions and one presence mismatch remain recorded in the
historical `protocol-projections.json` before-image.

PD mock-RPC tests pass **45**, including absent scope versus explicit zero,
TSO lifecycle and TLS tests. Transaction integration tests pass **423**, with
**10 pre-existing ignored tests**, including batch-stream lifecycle,
forwarding, active cancellation, lock/commit and PD region routing. All targets
of the proto crate and every direct consumer compile. Original Go PD and etcd
package tests pass; the selected BR/TiKV/auth/membership/MVCC/gateway packages
compile and have no test files. No failpoints appear in these Go packages.

Exact validation commands (repository root unless stated):

```sh
# Red, before replacing the local owners; then green after replacement:
(cd rust && cargo test --locked -p tidb-proto --test pd_wire_source)
(cd rust && cargo test --locked -p tidb-proto)
(cd rust && cargo test --locked -p tidb-pd-client -p tidb-txnkv --test all)
(cd rust && cargo check --locked --all-targets -p tidb-proto -p tidb-pd-client -p tidb-txnkv -p tidb-server -p tidb-executor -p tidb-exec -p tidb-distsql -p tidb-expr -p tidb-planner -p tidb-unistore -p tidb-util)
python3 rust/scripts/sync-tipb.py
python3 rust/scripts/sync-tipb.py --etcd-api
python3 rust/scripts/check-shared-client-proto.py
python3 -m unittest discover -s rust/scripts/tests -p test_tipb_sync.py
python3 rust/scripts/audit-protocol-projections.py --go-ref origin/master
make lint
git diff --check
# In the pinned github.com/pingcap/kvproto module directory:
go test ./pkg/pdpb ./pkg/tikvpb ./pkg/brpb
# In the pinned go.etcd.io/etcd/api/v3 module directory:
go test ./authpb ./membershippb ./mvccpb ./etcdserverpb ./etcdserverpb/gw
```

The three source gates and all five source-guard unit tests passed in `make
lint`, which also passed. Bazel preparation is inapplicable: no Go, Go-module,
or Bazel inputs changed. Logs are `/private/tmp/tidb-complete-protocol-red.log`,
`tidb-complete-protocol-tests.log`, `tidb-protocol-final-rpc-tests.log`,
`tidb-protocol-all-consumers.log`, `tidb-protocol-after-audit.log`,
`tidb-protocol-go-{kvproto,etcd}-tests.log` and `tidb-protocol-lint.log`.

Publication requires the actual pre-commit locked server build and a separate
fresh locked build after commit, immediately before push. Their final results
are recorded in the task's publication evidence.

## Acceptance boundary and risks

P01 and P02's schema ownership defects are repaired. This maintenance of
already integrated contracts is **not acceptance of the entire external Go
packages**. BR's handwritten BackupSchemaVersion helper, etcd's redacted
logging helpers and HTTP gateway behavior, original-test translation and
external package/runtime integration remain package-wide review work. No
schema exposure adds a new SQL executor or implements an unimplemented RPC.
P03 PD service-mode discovery and other structural findings remain open.

Generated layouts and test-service traits change at Rust compile time; every
in-repository direct consumer was checked. Socket fixtures validate transport
behavior, but real PD/TiKV/TiFlash upgrade/interoperability tests were not run.
The zero-copy representation is preserved; sysbench/TPCC/TPCH/YCSB benchmarks
were not run and no throughput claim is made. Existing SQL/DDL/statistics and
grants baseline failures were not rerun by this protocol-only repair.
