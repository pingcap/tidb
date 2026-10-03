# Shared TiFlash replica metadata and polling batch

Starting integration: `80af2d654e32e71d7676cd34ece35ceccdb476c5` on
`hparser-integration`. Fresh Go master remains
`93a01d31f6da205ae4bf376825293903a6899fdb`; remote integration remains
`7b991676da79f044774caf6da4dfffe247160feb`; native master remains
`19a56ccda1e128218cd33c69709038219aced9bc` and is unchanged.

This batch addresses F01/F02/F03 together and N03's cluster HTTP consumer.
F03's recorded classic API-V1 polling contract is repaired. F01/F02 advance to
partial; N03 remains partial. The register has **60 unresolved (49 open,
eleven partial), 26 repaired, 86 tracked**. This is maintenance of existing
executable owners, not transcreation/acceptance of complete `pkg/ddl`,
`pkg/domain/infosync`, `pkg/util` or configuration/security packages.

## Source decisions and behavior

Go `pkg/ddl/table.go::onSetTableFlashReplica` preserves logical `Available`
when count or labels change, unless `ResetAvailable` explicitly requests a
restore reset. It creates a new record without the old partition-ID list.
The maintained Rust statement/parser/planner now follows that policy. Zero
count still removes the record. Physical availability updates resolve the
containing logical table, persist under its metadata key, preserve the ordered
partition list, and make the logical table ready only when every normal
partition is ready. Unknown physical IDs still refuse without writing.

Go `partition.go::removeTiFlashAvailablePartitionIDs` and
`clearTruncatePartitionTiflashStatus`, plus `table.go`'s truncate policy,
require readiness to follow physical identities. DROP removes retired IDs;
TRUNCATE PARTITION removes replaced IDs and resets logical readiness while
retaining untouched partition readiness; TRUNCATE TABLE resets all readiness
while preserving replica settings.

The poller collects normal and adding physical partition IDs as Go
`ddl_tiflash_api.go::LoadTiFlashReplicaInfo` does. This also fixes false stale
rule classification against a desired physical partition. Legacy classic
rule creation/acceleration/deletion still belongs to this poller; it has NOT
been safely migrated to durable DDL and GC. Adding-partition priority,
keyspace encoding and safe retired-rule cleanup remain F01.

Go's retained polling context is composed in the existing manager: store
refresh every five owner ticks, immediate refresh retry after failure,
1000-entry unavailable-table backoff, growth before increment with fractional
threshold truncation (1 initial, 1.5 rate, 10 maximum), and a 1000-item bounded
available-progress refresh queue. Full-replica progress is distinct from
one-replica availability. One shared map serves the worker and SQL reader;
network calls do not hold its lock. Owner loss clears progress and joined
shutdown retires the cache. The pending queue preserves source refill timing.

Production no longer constructs a separate gRPC PD client for store discovery.
It obtains PD HTTP metadata, selects exact write-node labels, retains retired
stores for source error handling, and aborts failed Up/Disconnected status
collection while skipping failures on Down/Offline/Tombstone stores. The
shared HTTP client consumes the existing `ClusterSecurity` CA and optional
client identity and selects HTTPS for PD and TiFlash requests. It uses only
the configured CA roots; verification is never disabled. Positive TLS and
untrusted-peer rejection execute against real local TLS sockets.

The SQL reader consumes cached full progress and partition averages, using the
existing shared numeric truncation owner. Its uncached fallback and Go's
on-demand/circuit-breaker retriever remain separate unaccepted I01/infosync
obligations. No complete virtual-table or HTTP/security package is claimed.

## Regressions and test cleanup

Before production edits, three regressions fail: count/label changes clear
availability, physical partition publication returns TableNotExists, and
discovery runs each tick. A broader same-owner review reproduces three more:
DROP retains retired availability IDs, TRUNCATE PARTITION retains readiness,
and TRUNCATE TABLE retains readiness. All six pass after repair. The explicit
restore-reset/zero-count case also passes.

Eight `#[ignore]` tests in
`tidb-executor/tests/tiflash_replica_test_source.rs` had empty bodies and stale
claims that no replica carrier/poller existed. They are removed. Their complete
historical source notes and unverified upstream obligations remain in
[the obligation register](tiflash-unverified-test-obligations.json). Neither
retirement nor a missing placeholder is counted as passing coverage. Both
tests with real assertions stay enabled and pass.

An isolated oracle executes unchanged backoff definitions extracted from fresh
Go master and produces fifty ticks. The Rust regression checks the full trace;
it is independent evidence for that policy, NOT execution of the original Go
DDL package or all its test/fixture obligations.

## Validation and publication

Exact commands, results, source hashes and limitations are in
[the validation receipt](tiflash-replica-batch-validation.json). Targeted
validation covers 19 poller tests, six catalog-writer cases, two real TLS
discovery cases and two retained executor source cases: **29 distinct Rust
tests**. Repeated runs do not increase this count. All-target compilation,
root lint and actual locked publication build gates are recorded there.

No full Go DDL/infosync suite, multi-node TiKV/TiFlash, live ADD PARTITION
readiness, mTLS peer validation, CN allowlist/platform matrix, new-task cloud
restore, or workload benchmark is claimed. Durable ADD PARTITION wait/rollback
phases remain F02/D01; placement/GC coordination remains F01/D08/O03. Native
deployment defaults and other configuration consumers remain N03. No measured
sysbench/TPC-C/TPC-H/YCSB improvement is claimed.

The authorized push destination remains `pingcap/tidb hparser-integration`.
Do not substitute a fork, force push, bypass hooks or reuse an old build before
pushing. The reusable cloud setup uses the existing checkouts, two Cargo jobs
and disabled incremental compilation. A blocked push must preserve these
validated local changes in the existing bundle and cloud draft.
