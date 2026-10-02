# PD request connection ownership

This is maintenance of the existing PD client, addressing TiDB finding P07.
It is not acceptance of the complete Go PD root, service-discovery or TSO
packages. The reference is Go TiDB master
93a01d31f6da205ae4bf376825293903a6899fdb and its pinned PD client
v0.0.0-20260805103528-afa43111d149. client.go GetRegion/GetAllStores/ScanRegions
retain the service connection returned by inner_client.go getServiceClient;
the request does not hold a client-wide lock over network I/O.

The native RetryClient previously held its cluster write lock during every
metadata RPC, and a read lock during TSO waits. Reconnect also held the write
lock during discovery and retired-stream joins. Four local tonic regressions
failed with bounded timeouts: metadata overlap, metadata during stalled TSO,
replacement during a retained metadata request, and metadata during discovery.

All existing Cluster request methods now construct Send + 'static futures.
These retain only the selected tonic client, cluster ID and owned arguments,
or the selected TSO context. The explicit lifetime prevents borrowing Cluster
across the await. Public call sites still await the same operations. One retry
macro constructs the future under a short read lock and awaits after dropping
that guard; retry_mut is removed. Decoding and logical command metrics remain
inside the same retry lifecycle. Each new attempt reads the current connection.

A separate reconnect mutex serializes refreshes. Connection preparation owns a
membership/security snapshot, publication briefly locks Cluster, and retirement
joins outside that lock. Same-URL healthy TSO reuse, canceled-context replacement,
failed-discovery preservation, response headers/timeouts and source results are
unchanged. Cluster itself is not cloned and remains the sole owner of manager
cancellation. Existing retry limits/service modes are not changed by this repair.

Validation from this native repository:

    cargo test --locked --lib source_pd_concurrency_
    cargo test --locked --lib pd::
    cargo test --locked --lib region_cache::test
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo fmt --all --check
    git diff --check

The four new cases fail before repair and pass afterward. The PD suite passes
151 cases, cache suite 89, and full library 1,528 with two existing ignored.
Strict library Clippy, formatting and diff checks pass. Prior all-target Clippy
exposed two unrelated redundant closures in pd/circuitbreaker_tests.rs; this
change neither suppresses those lints nor edits those tests.

Four original Go LoadKeyspaceByID cases and a retained metadata concurrency
oracle pass with Go1.25.14, -race and the source TestMain leak checks. Failpoints
were enabled in a disposable module copy and disabled on exit. The oracle uses
the actual Go client RPCs with atomic service publication in a test discovery
adapter; it is not a full PD discovery/TSO acceptance test. It covers overlapping
metadata, range/result identity, new-connection selection and old-connection
completion after publication.

Logs are /private/tmp/native-pd-concurrency-{red,green,pd,cache,all,clippy}.log
and /private/tmp/go-pd-concurrency-oracle.log. TiDB's
rust/docs/pd-request-ownership-execplan.md records exact Go commands and downstream
sync/build gates; its current-audit directory retains the Go oracle source.
Native source must be synchronized through TiDB's maintained script, including
all patches and regeneration. No live PD failover, mixed-node/TLS deployment,
other-platform run or sysbench/TPC-C/TPC-H/YCSB measurement is claimed. Removing
serialization allows independent RPCs to overlap; workload throughput is not
measured. Complete discovery/service-mode/shutdown ownership remains open.
