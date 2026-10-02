# PD transport candidate: source-corrected lifetime and rejected readiness shortcut

This is a continuation of the living ExecPlan in `README.md`, with native
client-rust still at `6163ecfc587b248dcbf0e30c1c9d905b4bc5a665` and TiDB master
still at `93a01d31f6da205ae4bf376825293903a6899fdb`. Both implementation branches
were refreshed before the experiment. The entire pinned PD grpcutil package
remains the atomic implementation/acceptance unit. No production owner or
dependency is changed by these files.

The experiment changes the implementation direction in two concrete ways.
First, the planned unconditional background reconnection after losing a Ready
connection was incorrect for Go's default balancer. Second, exposing h2's existing
non-ACK SETTINGS flag is insufficient to implement Go's readiness contract.
The failed candidate remains isolated seed evidence; it must not be integrated.

## Source correction: reconnect according to the balancer state

Pinned grpc-go `clientconn.go::createTransport` clears the lost transport and
publishes Idle. `balancer/pickfirst/pickfirst.go::updateSubConnState` handles a
previously Ready subconnection by installing an idle picker, and `idlePicker.Pick`
calls ExitIdle on demand. Initial background dialing and retries after failed
setup are a separate path. Caller-selected resolvers and balancers also remain
part of the eventual transport option contract.

The first added Go test expected an unsolicited replacement socket and failed
after three seconds. The corrected test observes Idle after closing an established
peer, observes no new socket during a bounded wait, calls Connect, and receives
the replacement's HTTP/2 preface. It also proves setup-context cancellation does
not kill the nonblocking connection and explicit Close terminates a stalled
replacement despite another retained connection reference. Original tests and
the expanded source contracts pass with race/goleak under both PD-owned and
TiDB-selected error/zap dependencies.

The isolated Rust owner was corrected to the same Idle/demand lifecycle before
recording the final observations. Its deliberately fixed 100 ms delay after
failed setup is test scaffolding, **not** the required Go backoff implementation.

## Candidate and executable results

`h2-readiness.patch` adds only three accessors over h2 0.4.19's existing internal
SETTINGS state. It adds no parser, extra PING/RPC, capacity sentinel, or new protocol
policy. The complete published crate artifact inventory is `h2-inputs.json`;
the isolated candidate's dependency versions are locked in `h2-probe.Cargo.lock`.
The crate archive excludes some upstream fixtures/CI artifacts, so this inventory
does not claim acceptance of the complete upstream h2 repository or its tests.

`h2_owner_probe.rs.txt` owns a retained worker, shared state/handles, separate
blocking setup wait, explicit joined close, final-owner cancellation, initial
background dialing and demand-driven reconnect after an established transport
fails. It is copied into scratch by `probe_h2.py`; it is neither a workspace crate
nor a native client module. All protocol handling still uses the h2 engine.

The final candidate suite has **eight passing tests and two failing tests, with
none ignored**. Passing cases exercise stalled SETTINGS, blocking setup cleanup,
zero/one/maximum stream capacity, invalid window settings, retained handles,
active HTTP/2 response/body cancellation, idle/demand reconnection and last-owner
cleanup. Active HTTP/2 streams are transport evidence, not generated gRPC codec,
unary interceptor, TLS, or full TSO integration coverage.

The two failures preserve Go's expectations:

| Server preface | Pinned Go | h2 flag candidate |
| --- | --- | --- |
| PING, then SETTINGS | Refuses readiness | Reports ready |
| Unknown frame, then SETTINGS | Refuses readiness | Reports ready |
| Initial SETTINGS ACK | Reports ready | Continues waiting |

Go's `internal/transport/http2_client.go::readServerPreface` requires the first
parsed frame to be SETTINGS, including an ACK. h2's flag describes receipt of
initial non-ACK settings; its normal reader may process or discard earlier frames.
These are different state contracts. Passing stalled-peer and zero-stream-limit
tests therefore cannot establish parity. `candidate-observations.json` records
actual candidate outputs alongside Go expectations validated by the source tests.
The harness expects those two failures and raises on changed outcomes, compilation
failure, unexpected failures or missing observations. Harness success means the
experiment reproduced, not that the candidate is accepted.

## Reproduce and validate

From TiDB root, with the recorded published h2 and PD source trees available:

    PYTHONDONTWRITEBYTECODE=1 python3 rust/docs/parity/current-audit/pd-grpcutil-contract/probe_h2.py \
      --h2-source /Users/qiliu/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/h2-0.4.19 \
      --pd-source /Users/qiliu/go/pkg/mod/github.com/tikv/pd/client@v0.0.0-20260805103528-afa43111d149 \
      --output /private/tmp/pd-h2-contract-published

Use a new output directory on subsequent runs. The harness checks all recorded
h2 artifacts and the existing complete PD package inventory, copies inputs,
patches scratch h2, and runs these exact commands in the copied candidate:

    cargo test --locked --lib -- --test-threads=1
    cargo clippy --locked --all-targets -- -D warnings
    cargo fmt --all --check

The test command intentionally exits 101 for the two documented failures;
Clippy and formatting pass. In copied PD source, the harness runs:

    go test -mod=readonly -race ./pkg/utils/grpcutil -count=1

That command passes twice: PD's own dependency selection and TiDB master's error/
zap selection. The original TestMain goleak gate and original test cases remain
unchanged. The existing source-oracle helper enables only copied grpcutil/retry
failpoints and checks byte restoration after disabling them, even after a failed
test. No Go repository source/import/module/Bazel file changes, so bazel_prepare
is not required. No generated output is hand-edited.

TiDB publication additionally requires `git diff --check`, `make lint`, the actual
hook's `cd rust && cargo build --locked -p tidb-server`, and the same locked build
after committing immediately before the normal hparser-integration push.
Root `make lint`, candidate strict Clippy/formatting and both Go race/goleak
runs passed. Publication uses the actual hook and fresh post-commit locked build;
the resulting commit and push are recorded in the conversation. The optional
`tools/bin/goword` is absent, so no spelling-check success is claimed.

## Integration decision and remaining work

Do not publish this candidate in native client-rust, advance the TiDB native pin,
or remove existing transport paths. There is no accepted native production change
to synchronize. A viable backend must expose the actual first-server-frame
contract through its protocol owner, preserve the source's state-dependent
connection lifetime, and pass the remaining TLS/GOAWAY/backoff/option/cache/RPC
tests from `README.md`. Replacing these failures with h2's current outputs would
weaken the contract rather than fix the implementation.

The experimental grpcio 0.13.0 alternative is also not selected: its Rust client
credential builder captures static PEM; its exposed reload callback is server-only,
whereas PD's file-based TLS owner reloads client certificates for handshakes.
The isolated bundled-Core ARM build failed on an x86-only Abseil compiler option;
that is a local build observation, not proof that grpcio cannot support ARM.
The new grpc 0.9.0 transport's Hyper handshake likewise does not itself prove
server-preface readiness. These findings do not authorize a new production backend.

No package closes, and the structural register remains 85 tracked / 77 unresolved /
eight repaired. Production correctness and compatibility risks remain open;
the experimental code cannot alter runtime SQL or transaction behavior. Linux,
ThreadSanitizer, live PD/TiKV failover, TLS rotation, GOAWAY draining and sysbench/
TPC-C/TPC-H/YCSB performance were not validated. No performance improvement is claimed.


Scratch cleanup removed five superseded/failed experiment target directories,
reclaiming 892,305,408 allocated bytes. Logs and source evidence were retained;
`/private/tmp/pd-h2-cache-cleanup.json` records the exact paths. The final published
experiment's target remains available. No production artifacts or user data were
removed by this continuation.
