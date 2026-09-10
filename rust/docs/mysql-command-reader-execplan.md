# Buffer MySQL command input like Go

This living ExecPlan follows root PLANS.md.

## Purpose / Big Picture


Reduce socket reads and latency for ordinary SQL commands without changing SQL,
packet limits, timeouts or batching policy. Go's BufferedReadConn keeps a 16 KiB
input buffer; Rust currently reads each command's first header byte, remaining
three header bytes and body directly from ClientStream. Use the native BufReader
at the connection's authenticated command boundary. This is a scoped transport
optimization, not a whole-package parity or overall performance completion claim.

## Progress


- [x] Read Go pkg/server/internal/util/buffered_read_conn.go and packetio.go,
  Rust protocol packet readers, ClientStream TLS ownership and command loop.
- [x] Establish clean d26fec5d07 source and build an immutable control server.
- [x] Add the connection-owned native 16 KiB input buffer after authentication.
- [x] Verify all 35 retained packet, compression, TLS, timeout and connection
  tests. Run the secure-transport global-mutating test in its own process;
  it leaves the flag enabled, contaminating both parallel and serial batches.
- [x] Pass release build, all-target server check, formatting and Ready lint.
  The protocol read probe confirms 3000 to 1000 reads for 1000 small complete
  commands, identical decoding under one-byte fragmentation, and 300 to 200
  reads for 100 commands with 128 KiB bodies. The temporary example is removed.
- [x] Compare 84000 fixed-work sysbench and 36000 TPC-C events against the
  unchanged control. Fresh Go result equality and all eleven consistency
  conditions pass. Record separate before/after/Go profiles and Ready checks.
- [x] Preserve upstream 882cc27174 and 832ed58f36. Merged release build and
  lint pass; executor condition nine passes, condition eleven retains its
  documented MergeJoin expectation failure (zero versus two).
- [x] Merged binary passes 6000 sysbench and 6000 TPC-C transactions with Go
  equality and all eleven consistency conditions. Restore auto-analyze to 1;
  verify ten owned PIDs absent and ten ports closed, retaining the fixture.
- [x] Preserve the later 4665f15924 table-probe commit; three focused planner
  tests pass. Final-merge validation limits are recorded in the receipt.
- [x] Prepare the scoped commit for normal origin/hparser-integration
  publication. Remote SHA verification is the final delivery check.

## Surprises & Discoveries


Rust read_header_or_eof deliberately separates a clean EOF from a truncated
header by reading one byte before the remaining three. Without a buffered
connection this is three read calls for an available ordinary nonempty command,
not two. On plaintext TCP these cross the socket boundary; TLS already buffers
decrypted bytes internally, so they are not three socket syscalls in TLS mode.
Go keeps those parser-level reads above BufferedReadConn.DefaultReaderSize,
which is 16 * 1024. Existing result output is already coalesced; do not assume
response writes are the same missing boundary.

The live profiles prove both Rust command paths use TLS. The local sysbench
driver's --mysql-ssl=off skips SSL setup rather than explicitly disabling TLS;
Go's profile also enters TLS below BufferedReadConn. Consequently the adapter
probe's three-to-one reads cannot be called a live socket-call improvement.

## Decision Log


Use std::io::BufReader, not a new buffer implementation or a workload threshold.
Create it after authentication and TLS upgrade, before PacketIoReader chooses
the negotiated compression decoder. This avoids reading TLS handshake bytes
ahead of the raw-to-TLS transition. The same buffer then survives all commands,
including local infile and compressed exchanges. Reach the original ClientStream
through BufReader.get_ref when applying the session's read timeout.

## Context and Orientation


rust/crates/tidb-server/src/mysql_connection.rs owns one ClientStream shared with
the output and shutdown descriptor. Authentication uses PacketReader. The command
loop creates PacketIoReader after the authentication OK and selects compression
once. rust/crates/tidb-protocol/src/packet.rs owns framing and EOF detection;
those parsers should stay unchanged. The new buffer belongs below the compression
decoder and above the authenticated plaintext/TLS stream.

## Milestones and Concrete Steps


First build the clean control from rust/ with CARGO_BUILD_JOBS=12 cargo build
--offline --locked --release -j12 -p tidb-server --bin tidb-server. Keep the binary
immutable under /private/tmp/tidb-command-reader.yRKVf8/control-server.

Next update only mysql_connection.rs: import BufReader, wrap the command stream
with capacity 16 * 1024, and follow its get_ref to set socket timeouts. Run retained
server tests for mysql_client_lifecycle_source, server_internal_packetio_source,
mysql_tls_source and require_ssl_login_source together. Build one release candidate.

Finally run the owned fixed-work harness with one/eight/32 sysbench clients and
eight TPC-C clients in alternating candidate/control order. Run Ready lint from
repository root with GOMAXPROCS=12 GOFLAGS='-p=12' make -j12 lint; run scoped Cargo
check and formatting from rust/. Exact commands and measured results belong in
the accompanying benchmark receipt. No build or profiling may overlap timing.

## Validation and Acceptance


All retained protocol/lifecycle checks must pass, including negotiated zlib/zstd,
TLS login, timeout changes and long-data commands. Fixed-work runs must have zero
errors, exact event counts, bounded fresh Go SQL equality and all eleven TPC-C
consistency conditions. Judge throughput, latency and CPU together; source-level
read-call reduction alone is not an end-to-end speedup claim.

## Idempotence and Recovery


Preserve unrelated main-checkout changes and the retained TiKV fixture. Only stop
PIDs owned by this harness; restore auto-analyze before cleanup. Preserve the
control binary and evidence. Discard rejected code using apply_patch, not reset.
Only normal, non-force pushes to origin/hparser-integration are authorized.

## Outcomes & Retrospective


The native input buffer closes one concrete Go implementation gap. Thirty-five
retained server tests pass with process isolation for the existing global-flag
test. Two alternating pairs show serial throughput +0.01%, eight-client -0.25%,
32-client +1.54%, and TPC-C elapsed +1.66% with unchanged SQL CPU. This is mixed
evidence, not an overall speedup. TLS already buffers record input; the dominant
scheduling, executor and network costs remain. Keep the alignment, but do not
promote a broad performance claim. Exact commands, binary identities, profiles
and limitations are in benchmarks/mysql-command-reader-baseline.json.

The broader optimization goal stays open. Live validation and cleanup are
complete for this increment; the final delivery checks publication. Unrelated planner parity
failures are not hidden by this increment.
