# `pkg/ddl/schemaver` package receipt

Pinned Go source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete upstream inventory

| Artifact | Lines | Git blob |
| --- | ---: | --- |
| `pkg/ddl/schemaver/BUILD.bazel` | 58 | `8d091843ffbd35911c2b3dc4deb34bb0b33d8fb6` |
| `pkg/ddl/schemaver/mem_syncer.go` | 144 | `c47be9a4cb895d937e17efb63dcf7357b2068150` |
| `pkg/ddl/schemaver/syncer.go` | 678 | `65268db733faa579bab1c0534e911ed2615201d5` |
| `pkg/ddl/schemaver/syncer_nokit_test.go` | 268 | `950034bd6cce981138fb3b1d2b9e65f5e69034ea` |
| `pkg/ddl/schemaver/syncer_test.go` | 207 | `f296672280831d63ce09f2452b1f46a652e281de` |

There is no `doc.go`, generated source/input, fixture directory, or
platform-specific source variant in the pinned package. `BUILD.bazel` maps to
the Rust Cargo manifest and workspace lockfile; it carries no runtime branch.

## Rust carrier and integration

- `crates/tidb-schemaver/src/lib.rs`: constants, context/channel native
  equivalents, `SyncSummary`, and the complete `Syncer` interface.
- `crates/tidb-schemaver/src/mem_syncer.rs`: complete `MemSyncer`, including
  the capacity-one nonblocking global-version channel and mock session.
- `crates/tidb-schemaver/src/etcd_syncer.rs`: complete etcd syncer, session
  retry/lifetime, exact and prefix watches, MDL and non-MDL waits, general
  server-set calculation, per-job mirror, monotonic CAS writes, and cleanup.
- `crates/tidb-pd-client/src/etcd.rs` and
  `crates/tidb-server/src/serverinfo_etcd.rs`: native production binding for
  range revision, exact-key metadata, revision compare-and-put, create-if-
  absent, and exact/prefix watch responses.
- `crates/tidb-server/src/cluster_session_node/schema_sync.rs` and `ddl.rs`:
  one initialized syncer is shared by the follower report loop and DDL owner.
  The former direct PUT/session code and the separate manual owner wait were
  removed.

Prometheus observations and Go failpoint injection sites do not alter package
control flow in an ordinary build. Existing Rust logging and deterministic
fake-etcd controls are the native observability/test boundaries; no alternate
SQL or synchronization behavior is introduced for them.

## Exact test mapping

| Pinned Go test | Rust test |
| --- | --- |
| `TestNodeVersions` | `etcd_syncer::tests::test_node_versions` |
| `TestDecodeJobVersionEvent` | `etcd_syncer::tests::test_decode_job_version_event` |
| `TestSyncJobSchemaVerLoop` | `etcd_syncer::tests::test_sync_job_schema_ver_loop` |
| `TestCalculateUpdatedMap` | `etcd_syncer::tests::test_calculate_updated_map` |
| `TestGetServersForISSync` | `etcd_syncer::tests::test_get_servers_for_is_sync` |
| `TestSyncerSimple` | `etcd_syncer::tests::test_syncer_simple` |
| `TestPutKVToEtcdMono` | `etcd_syncer::tests::test_put_kv_to_etcd_mono` |

The obsolete two-test ignored gap file in `tidb-executor` was deleted. The
crate now contains exactly the seven tests in the pinned Go package; classic
and nextgen builds exercise their respective branches.

## WIP validation

- `cargo fmt --all`
- `cargo test --offline -p tidb-schemaver`
  (`7 passed; 0 failed; 0 ignored`)
- `cargo test --offline -p tidb-schemaver --features nextgen`
  (`7 passed; 0 failed; 0 ignored`; the Go-equivalent MDL-off test returns
  immediately in nextgen)
- `cargo test --offline -p tidb-pd-client --lib`
  (`24 passed; 0 failed; 1 live-PD probe ignored`)
- `cargo test --offline -p tidb-server --lib schema_sync`
  (`6 passed; 0 failed`)
- `cargo check --offline -p tidb-server`

No live PD/TiKV integration test or Ready-profile workspace lint was run in
this WIP package batch.
