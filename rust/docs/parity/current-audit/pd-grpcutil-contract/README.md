# Restore PD transport ownership before migrating discovery

This living ExecPlan follows `PLANS.md` at the TiDB repository root. It covers the
entire pinned PD `pkg/utils/grpcutil` package. The current change is an executable
contract review and adapter feasibility experiment, **not a transcreation or a
production integration**. All implementation and package acceptance remain open.

## Purpose and context


A PD connection must survive cancellation of its setup context after creation,
start dialing without SQL requests driving it, and terminate outstanding
RPCs when its owner closes it. PD, keyspace and TSO clients for the same URL must
share the appropriate connection owner. Retry and circuit-breaker interception
must run in Go's order, with a copied backoffer for each unary RPC and a breaker
observation for each physical attempt. These are prerequisites to removing the
current duplicate native transport paths.

Reconnection depends on state and policy. With the pinned default `pick_first`
balancer, losing an established connection returns it to Idle; a subsequent
picker call or explicit Connect starts another attempt. This differs from the
initial background dialing/retry lifecycle. The continuation's source oracle
disproved this plan's earlier blanket requirement for autonomous reconnection
after a Ready transport fails.

Both repositories were refreshed. TiDB master is
`93a01d31f6da205ae4bf376825293903a6899fdb`, PD is
`v0.0.0-20260805103528-afa43111d149`, native client-rust is
`6163ecfc587b248dcbf0e30c1c9d905b4bc5a665`, and TiDB's starting integration commit is
`e67d717d029a8bb2f222281652452d762d1f023d`. The package has one production file,
one original test file, three constants and eleven production functions. It has
no package doc, generated source/input, build-tagged production variant, fixture
or package-local build file. Original `TestGetCallerID` has five cases;
`TestMain` runs goleak using the five shared testutil files, including Linux and
non-Linux variants. `inventory.json` records package, shared support, build,
consumer and transport dependency inputs separately.

The source oracle runs original tests unchanged, plus six contract tests. The
Rust probe deliberately uses the currently selected tonic API directly, so it
can reject an unsuitable adapter without changing native production ownership.
It is saved as text and is not part of either production build.

## Progress


- [x] Refresh both branches and the Go master reference.
- [x] Review every grpcutil declaration and locate all PD source consumers.
- [x] Reproduce native lazy-dial and HTTP/2 readiness gaps with a socket-level probe.
- [x] Exercise Go nonblocking setup, blocking readiness, metadata, retry copies,
  breaker classification/failpoint, cache races/closed reuse, dial option precedence
  and unary interceptor ordering, alongside original cases and goleak.
- [x] Finish repeatable harness, 29 PD inputs, 12 native inputs, exact backend-source
  inventory and TiDB error/zap dependency-selection validation.
- [x] Publish the contract review as `a8c256a78c` after the actual hook build
  (0.69s) and a fresh post-commit locked server build (16.04s); remote branch agrees.
- [ ] Select a transport adapter that passes the entire lifecycle acceptance set.
- [x] Prototype the h2 SETTINGS-state accessor outside both production graphs:
  eight lifecycle checks pass, but two source-contract tests fail. Preserve both
  failures; the accessor alone is not an acceptable readiness adapter.
- [x] Correct the reconnection design from Go source and a fail-before oracle:
  established default-policy connections remain Idle until demand. The corrected
  candidate follows that lifecycle; initial retry/backoff acceptance remains open.
- [ ] Implement all eleven functions and three constants as one owner, migrate
  every existing native caller, and remove their displaced policies atomically.
- [ ] Complete native/TiDB integration validation and publish the whole package.

## Surprises & Discoveries


The experiment rejects two tempting shortcuts. On tonic 0.12.3,
`Endpoint::connect_lazy` starts no TCP connection until a request is made.
`Endpoint::connect` returns even when a TCP peer reads the HTTP/2 client preface
but never supplies server SETTINGS. Go `GetClientConn` starts background dialing
without an RPC and its `WithBlock` call expires against that same stalled peer.
The paired JSON outputs contain `false,false` for tonic and `true,true` for Go.
Consequently an endpoint cache, or switching eager dialing to `connect_lazy`,
cannot establish package parity.

The source's `GetCalleeID` comment says invalid addresses panic, but its body
returns an empty string. Metadata helpers replace the outgoing map; a present
key with an empty value slice panics. The source connection cache returns a
previously closed entry. The backoffer interceptor returns the last RPC error
when its wait is canceled. Each of these observable distinctions must be kept;
none should be silently "improved" during translation.

Tonic's connection worker performs request-driven reconnection without PD's
backoff policy. Request-driven reconnection after established transport loss is
also Go's default-policy behavior, not itself a mismatch. Its endpoint executor
owns both the channel buffer worker and HTTP/2 workers. Hyper also spawns its internal connection task through that
executor. A scheme that guesses task identity from spawn order is therefore not
an acceptable connection owner. A custom TCP dialer alone cannot observe the
TLS/HTTP/2 setup performed above it. These conclusions are from the exact source
files hashed in the inventory, not a proposed new transport implementation.

## Decision Log


Decision (2026-10-01, continuation): test a narrow h2 readiness accessor rather
than infer readiness from stream capacity or parse HTTP/2 frames in a second
owner. The protocol engine already records the relevant state. grpcio 0.13.0's
public client credential builder accepts static certificates; its dynamic reload
callback is server-only, whereas PD's file-based TLS config reloads client
certificates on handshakes. Its isolated ARM build also failed in bundled
Abseil with an x86-only compiler flag. Neither experiment changes production.
The new grpc 0.9.0 transport uses the same early Hyper handshake boundary and
does not by itself settle readiness. Backend feasibility is still a prerequisite,
not a package acceptance or a reason to weaken the source lifecycle contract.

Decision (2026-10-01, source-oracle correction): remove the proposed unconditional
reconnect-after-Ready loop. `grpc/clientconn.go::createTransport` publishes Idle
on transport loss; `balancer/pickfirst/pickfirst.go` installs an idle picker when
the old state was Ready. The initial oracle incorrectly expected another socket
without demand and failed after three seconds. The corrected test observes Idle,
proves no unsolicited connection during a bounded interval, calls Connect and
observes a replacement. This is a correction to the plan and isolated candidate,
not a production-native repair or a claim about every configurable balancer.


Decision (2026-10-01): retain the entire grpcutil package as one acceptance unit.
Do not integrate only its easy metadata/interceptor functions while leaving the
underlying connection contract unmet. This follows the user requirement to fix
structural ownership and repository rule 6.

Decision (2026-10-01): use a paired, executable readiness experiment before
selecting the adapter. A wrapper around the current two tonic constructors fails
that experiment. No backend replacement, protocol parser, speculative retry
state machine, or dummy readiness RPC is introduced by this review.

Decision (2026-10-01): preserve prior native publication and dependency pin for
this audit. There is no native production change to publish or dependency update
to manufacture. This review adds to W01/P03/P06/P07 evidence without closing them.

## Context and orientation


Native `src/pd/cluster.rs::Connection` creates independent PD and keyspace
channels using `SecurityManager::connect`. It has no URL connection map.
`PdMessage::send`, `get_prev_region_with_buckets`, and `connect_member` are distinct
unary paths. `timestamp.rs` retains a concrete tonic PD client in its stream
worker. `region_cache.rs::pd_region_meta_call` executes the breaker around a
logical operation, potentially hiding multiple wire attempts. Its classifier
only recognizes a direct native gRPC status. Shared `pd/backoff.rs`,
`pd/circuitbreaker.rs`, and `pd/errs.rs` already exist and must remain the owners
of retry arithmetic, breaker state and PD-coded errors.

Go's `servicediscovery` package owns the maps/lifetimes passed to grpcutil.
Resource-manager discovery also uses direct `GetClientConn`; it must not be
silently forced into a global cache. The transport package does not own leader
selection, service-mode switching or TSO batch completion. Those remain separate
parent-package acceptance units, even when callers must be migrated here.

## Plan of work and milestones


Milestone one, this review, supplies the complete package inventory and executable
source contracts. Run `run.py` as documented below. Both paired readiness results
must be produced, original tests and source contracts must pass under race/goleak,
and copied source must be byte-identical again after failpoint cleanup. This is
review evidence, not a passing native parity test.

Milestone two must prove a candidate adapter in isolation. It must expose a
connection handle with shared identity, a setup lifetime distinct from its
connection lifetime, initial background connect/retry state, blocking readiness and
explicit close. Prove TCP, TLS and HTTP/2 failures, Idle after established transport
loss with the default policy and reconnection after demand,
server GOAWAY with existing streams, and close of unary/stream/setup tasks. Prove
backoff 1s/1.6/20%-jitter/3s, successful-reset behavior, caller-option ordering and
caller setup deadlines. Do not promote a backend solely because the two initial
readiness checks pass; the remaining cases below are required.

Milestone three implements native `pd::grpcutil` as the sole owner of the source
helpers. A per-owner URL map must atomically retain one connection and close a
losing candidate, preserve exact URL keys and reuse closed entries as Go does.
The setup timeout is three seconds, not the lifetime of the cached connection.
Go URL parsing supplies the authority passed to the backend; do not reuse
SecurityManager's whitespace/scheme normalization for this contract. TLS versus
insecure credentials and PD connection parameters follow caller dial options.
The native dial-option representation must support blocking setup and ordered
unary interception; an Endpoint-to-Endpoint closure alone cannot express both.

A unary call must run the caller interceptors, copied retry owner, then shared
circuit breaker and actual generated RPC in source order. Store the decoded
reply separately from the last nonnil RPC error. Preserve the last error's cause
and identity during canceled backoff. The breaker sees every physical RPC,
including wrapped gRPC statuses, and classifies exactly DeadlineExceeded,
Unavailable and ResourceExhausted. Stream calls do not use unary retry/breaker
interception. Context metadata must be applied to each request/stream by the
same owner while preserving unrelated context values.

Milestone four migrates all existing paths in cluster, timestamp and region_cache
and removes their displaced transport/breaker policies. Keep generated protobuf
outputs generated. Add failing native regressions before edits, then prove they
pass. Review every source declaration and caller against the inventory, preserve
all root APIs, and update the complete package receipt. Only then publish native
master, run the maintained TiDB sync script and validate TiDB adapters.

## Concrete steps and validation


From TiDB root, the isolated review harness runs with explicit source roots:

    python3 rust/docs/parity/current-audit/pd-grpcutil-contract/run.py \
      --pd-source /Users/qiliu/go/pkg/mod/github.com/tikv/pd/client@v0.0.0-20260805103528-afa43111d149 \
      --native /Users/qiliu/projects/client-rust \
      --output /private/tmp/pd-grpcutil-review-final

It copies Go source into scratch, enables only scratch grpcutil/retry failpoints,
runs `go test -mod=readonly -race ./pkg/utils/grpcutil -count=1`, and always disables
them. It resolves only its scratch module with `go list -mod=mod -deps -test`, then
runs read-only with TiDB-selected pingcap/errors and zap versions. It runs the
Rust probe as a temporary native example using `cargo run --locked`, removing the
example in a finally block. It fails if that example already exists. The output
records backend observations; a passing harness does **not** mean parity.

Before accepting the eventual production package, additionally require native
full-library tests, strict Clippy, all targets, formatting, every original case
mapping, and source-input hashes. For this review no production test rerun or
benchmark can establish a performance improvement. There is no real Go source,
import, module or Bazel edit requiring `make bazel_prepare`.

For this review's TiDB publication, run formatting/diff checks, root `make lint`,
then the actual hook and a new post-commit build:

    TERM=xterm git -c core.hooksPath=hooks commit -m 'rust: record complete PD transport contracts and backend gaps'
    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

The hook must actually run `cargo build --locked -p tidb-server`. Missing optional
`tools/bin/goword` is not a successful spelling check. Never bypass the hook or
replace the fresh build with an earlier probe build.

## Remaining acceptance and risks


TLS certificate verification/reload policy, address edge cases, HTTP/2 GOAWAY,
connection-backoff jitter/cap/reset, configurable resolver/balancer policy,
wait-for-ready/default fail-fast distinction, close while generated unary/stream work
is active, cache loser task/socket teardown, and full native wire interception
still need candidate-adapter validation. The current cache concurrency oracle
checks one retained map entry and closed reuse; it does not force every racing
caller to create a candidate or count loser sockets. The unreachable-network
failpoint uses a malformed URL so it does not conceal/leak an allocated connection.
Do not claim that this tests the successful-allocation fault-injection branch.

No production semantics, API compatibility or performance change is shipped.
The outstanding correctness risk is the existing ownership mismatch, now with
reproducible evidence. Linux execution, native ThreadSanitizer, live topology,
sysbench, TPC-C, TPC-H and YCSB were not run. Source test inputs and platform support
are inventoried, not represented as platform validation.

## Idempotence and recovery


The harness requires a new output directory, preserves original module-cache
sources, and never edits production files. A temporary example is removed even
if Cargo fails. Inspect logs after failure; do not remove unrelated work or
regenerate expected results manually. The inventory script is read-only in its
check mode. Keep the saved readiness observations separate from eventual parity
assertions so a backend improvement does not silently become a test failure to
be "fixed" back to incorrect behavior.

## Outcomes & Retrospective


The paired experiment rules out two insufficient adapter designs and makes the
next implementation's acceptance criteria concrete. The full grpcutil package,
PD root/discovery/TSO and routing migration remain open. The structural register
remains 85 tracked, 77 unresolved and eight repaired; the register is not an
exhaustive package inventory or a claim of complete TiDB parity.

Revision note (2026-10-01): added complete source-boundary review, readiness
experiment and explicit non-acceptance of production transport parity.

Validation record (2026-10-01): the documented run.py command passes with output
`/private/tmp/pd-grpcutil-review-verified`. Original Go cases, all six added
contract tests, race detection, source TestMain/goleak and byte-for-byte scratch
failpoint restoration pass under both dependency selections. Source outputs are
identical; `observations.json` is copied from that run, not manually authored.
The native temporary example is removed and native git status remains clean.
The package inventory is checked before execution, including newly added files.
Root `make lint`, `git diff --check`, `gofmt -s -d` for the oracle, Rust probe
formatting and Python syntax/provenance checks pass. The actual hook and a fresh post-commit `cargo build --locked -p tidb-server`
both passed, and `a8c256a78c4c5c72415e517c2ba9ef9e695f0749` is published on
`hparser-integration`. The optional goword executable was absent; no spelling
validation is claimed. Native master remains `6163ecf` and both working trees
were clean after publication. This evidence-only receipt update repeats both
mandatory Rust build gates before its own publication.

Disk maintenance: with no active Go/Rust compiler found, verified and removed
446 ignored TiDB incremental-cache directories whose newest files were older
than six hours (10,745,339,904 allocated bytes). Final libraries, binaries, source
and recent incremental caches remain. Free space rose from approximately 3 GiB
to 11 GiB. The deletion manifest is `/private/tmp/pd-grpcutil-cache-removed.json`.


Continuation result (2026-10-01): `probe_h2.py` reproduces eight passing lifecycle
cases and two failing Go first-frame contracts in `/private/tmp/pd-h2-contract-published`.
Both source race/goleak runs pass with the added peer-frame and Idle/demand cases.
The whole experiment and exact commands are in `h2-candidate-review.md`.
The source oracle disproved this plan's earlier unconditional reconnect assumption;
the plan and isolated owner now preserve default-policy Idle until demand.
The accessor-only readiness candidate is rejected for production; no native SHA,
dependency pin, package acceptance or structural-register count changes.
