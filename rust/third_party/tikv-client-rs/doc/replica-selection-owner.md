# Shared replica candidate selection

TiDB master 93a01d31f6da205ae4bf376825293903a6899fdb pins client-go
v2.0.8-0.20260928031501-8edb23f6c7ee. The source owner is
internal/locate/replica_selector.go: ReplicaSelectMixedStrategy.next,
isCandidate and calculateScore. This repairs existing ports, not acceptance
of the complete internal/locate package.

The idle path previously filtered attempts == 0 before shared selection, so a
DataIsNotReady follower could not take Go's allowed second attempt. The native
candidate snapshot now carries the request busy flag and decayed store wait.
Mixed and idle selection use one predicate, five-bit score and random tie
selection. The region-cache facade exposes these existing native types for
TiDB's metadata adapter to consume the same owner. Epoch/store filtering and
request state remain in their existing owners.

The existing source_go_region_request3_TestSendReqWithReplicaSelector fixture
failed before the repair: expected follower 12, got None. After repair it
passes, including exhaustion after the second attempt and existing busy and
unknown-liveness controls. Exact commands from the native repository:

    cargo test --locked --lib source_go_region_request3_TestSendReqWithReplicaSelector
    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --lib -- -D warnings
    cargo fmt --all --check
    git diff --check

The full suite passed 1,405 tests, two ignored; Clippy passed. Red/green/full
logs are /private/tmp/native-replica-owner-{red,green,all,clippy}.log.
After publication, TiDB must synchronize through its maintained script and
replace its own candidate score/filter/tie loop. No real-cluster or benchmark
validation and no complete cache-owner migration is claimed.

The public embedding path is tikv::{MixedReplicaSelection, ReplicaCandidate,
ReplicaLiveness}; region_cache itself is private. TiDB compilation caught the
missing public facade export, which is supplied in the publication follow-up.
