# Retain the connection owner through panic recovery

## Purpose and source contract


A panic while serving a command must attempt Go's 1105/HY000 error through the
same framed writer, then close that connection and release its resources. The
server must continue serving other clients. N06 currently records an outer Rust
catch that runs only after the session, prepared registry and writer unwind.
Consequently it cannot send the error and loses buffered/protocol state.

This is maintenance of the existing server owner, not transcreation or acceptance
of the complete Go pkg/server package. Fresh pulls leave integration at
725d442d2901a2c31450a3b0e0ceea684b68c314, Go master at
93a01d31f6da205ae4bf376825293903a6899fdb, and client-rust at
19a56ccda1e128218cd33c69709038219aced9bc. The native dependency is current.
There is no pkg/server doc.go or deeper rust AGENTS.md.

Go master pkg/server/conn.go Run recovers, calls writeError, then Close through
WithRecovery. writeError uses the live PacketIO counter, codec and buffer. It
attempts the write even after earlier response bytes; write failure is logged.
closeConn closes the transport before statements/session. dispatch's deferred
token release happens during unwind, before Run recovery. The Rust equivalent
must use ownership and Drop without creating a second raw-socket writer.

## Progress


- [x] Pull latest references and verify the source ownership defect.
- [x] Reproduce missing panic ERR with bounded existing wire fixtures.
- [x] Retain command session, packet I/O and prepared registry through recovery.
- [x] Verify sequences, buffering, codecs, cleanup and ordinary command behavior.
- [x] Reconcile audit; focused tests, all-target compilation, formatting and lint pass.
- [x] Actual commit hook and independent locked server build pass; final publication reruns the gate immediately before push.

## Design and milestones


First extend tests/panic_recovery_source.rs. The fixture must tolerate existing
handshake initialization so its injected panic occurs during the intended
command. Assert an error packet followed by closure, preserve response sequence
after partial rows and prove a later connection still works. Run against the
unchanged production code and record the observed failures before edits.

Then give mysql_connection.rs one private command-connection owner, retaining
session, prepared statements, reader, writer and close handle across a borrowed
command-loop call. Catch the loop while the owner is still intact; attempt the
error using its writer's current sequence, then retire transport before the
session and registry. Keep the existing outer guard as containment for setup or
cleanup panics; do not claim Go Run recovery for pre-handshake setup. Synchronize
logical and compressed write counters after request/file-data reads, reflecting
Go's single PacketIO sequence. Preserve the command token's inner unwind boundary.

Finally exercise ordinary/partial replies, local infile, prepared commands,
plain/TLS and zlib/zstd, and failed error writes. Use existing fixtures rather
than a new server implementation. Update N06 only at its proven scope. Broader
configuration, charset, handshake initialization and SQL package gaps remain.

## Validation and acceptance


From rust/, run the targeted panic recovery and command-token tests, affected
packet/TLS/result writer groups, and cargo check --locked -p tidb-server
--all-targets. Follow repository regression fail-before/pass-after policy. Root
make lint, targeted rustfmt --check and git diff --check are required. No Go,
Bazel or generated inputs change, so bazel_prepare is not triggered. Original
pkg/server tests require failpoint-aware setup if run; no full Go-package gate
or acceptance claim is implied by a focused source oracle.

Before publishing, commit with TERM=xterm git -c core.hooksPath=hooks commit so
the actual hook runs cargo build --locked -p tidb-server. Immediately after the
final commit, from root run:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

If the remote advances, fetch and integrate without force, inspect the new code,
repeat affected gates and the hook/fresh locked build. Verify remote HEAD and a
clean tree. Keep exact commands/results in this receipt.

## Surprises & Discoveries


The old panic fixture panics on handshake SET NAMES, so it never exercises the
command loop. The live loop has separate reader/writer logical counters; ordinary
reply helpers set sequence explicitly, which hid missing synchronization needed
by recovery before the first reply and after local-infile uploads.

## Decision Log


Retain the complete command owner across recovery instead of reconstructing an
ERR on a raw socket or catching selected SQL handlers. Reconstruction loses TLS,
compression and buffered/partial response state. A shared command boundary also
covers callbacks, prepared commands and result iteration. Rust resource Drop
replaces Go's deferred cleanup while preserving its ordering.

## Outcomes & Retrospective


N06 is repaired at the recorded existing-owner boundary. CommandConnection keeps
session, prepared registry, packet reader/writer and close handle alive across
the borrowed loop. Its recovery attempts the shared error writer before Drop
shuts down the transport; field cleanup then retires prepared/session state.
Command admission and the socket watcher unwind before the error attempt.
There is no new thread, packet copy, transport or public API in this repair.
The normal dispatch helpers and packet framing stay shared. Rust's original
outer catch remains containment for setup/cleanup panics; no pre-handshake
panic protocol parity is claimed.

Two initial regressions failed against unchanged production 725d442d29 with
EndOfStream instead of ERR, including after metadata/one row. Final five tests
pass all 25 transport/dispatch cases: query, prepared dispatch, partial lazy rows
and LOCAL INFILE, each plain/TLS and none/zlib/zstd, plus failed error write.
Each successful recovery case first runs PING, preserving proof that command
sequence resets. It keeps external session close handles alive, verifies the
transport is closed before both session drops, and serves a second connection
with one shared command permit. The failed-write test verifies the returned
Panicked message survives a BrokenPipe recovery attempt.

The expanded fixture initially expected plain EOF for TLS; Rustls reports
UnexpectedEof on raw transport close without close_notify. The assertion now
accepts that exact TLS outcome, not arbitrary read errors. The failed-write
case uses the direct server result because the existing statement-EOF watcher
can race a forced socket close and affect the separate failed-connection count.
No production behavior was altered to accommodate either fixture correction.

Thirty-seven additional admission/protocol/shutdown tests pass. The register is
86 tracked, 71 unresolved (65 open/six partial), 15 repaired. No live cluster,
full package acceptance or sysbench/TPC-C/TPC-H/YCSB performance claim. The
prior complete-server failures/hangs remain historical baseline evidence in
command-admission-ownership-execplan.md; those broad suites were not rerun.

Exact focused commands (from rust/):

    cargo test --locked -p tidb-server --test all panic_recovery_ -- --test-threads=1
    cargo test --locked -p tidb-server command_token -- --test-threads=1
    cargo test --locked -p tidb-server --test all forced_shutdown_cancels_an_inflight_com_query -- --test-threads=1
    cargo test --locked -p tidb-server --test all server_internal_packetio_source -- --test-threads=1
    cargo test --locked -p tidb-server --test all mysql_tls_source -- --test-threads=1
    cargo test --locked -p tidb-server --test all resultset_writer_source -- --test-threads=1
    cargo test --locked -p tidb-server --test all sql_node_lifecycle_source -- --test-threads=1
    cargo test --locked -p tidb-server --test all fallible_process_shutdown_source -- --test-threads=1
    cargo check --locked -p tidb-server --all-targets

Root gates: `make lint`, `git diff --check`, and:

    rustfmt --edition 2021 --check rust/crates/tidb-server/src/mysql_connection.rs rust/crates/tidb-server/tests/panic_recovery_source.rs rust/crates/tidb-server/tests/concurrent_mysql_sessions_source.rs

All pass. Existing compiler/linker/jemalloc warnings remain; there is no
warning-free-build claim. Source contract checked with
`git show origin/master:pkg/server/conn.go` (Run/writeError/closeConn) at the
pinned master. No copied replacement Go oracle or original Go package test was
used. Machine results: parity/current-audit/connection-recovery-validation.json.
The actual hook and fresh pre-push locked build are publication gates below.

## Recovery and interfaces


Keep all new ownership internal to tidb-server; use existing PacketIoReader,
PacketIoWriter, QuerySession and PreparedStatementRegistry. No dependency or
public protocol type change is needed. Tests have bounded reads and explicit
shutdown. Preserve unrelated work and restore any baseline-control files byte
for byte. Do not hand-edit generated code or widen the audit's claims.

## Publication gates


The actual `TERM=xterm git -c core.hooksPath=hooks commit` ran the mandatory
`cd rust && cargo build --locked -p tidb-server` successfully. An independent
locked server build also passed. This receipt-only completion is amended with
the same actual hook. The final publication command reruns the locked build
after that amendment and pushes only on success:

    (cd rust && cargo build --locked -p tidb-server) && git push origin HEAD:hparser-integration

Publication verification compares HEAD with `git ls-remote origin
refs/heads/hparser-integration` and checks `git status --short`. Native
client-rust is unchanged/current at 19a56cc; it needs no commit or dependency
sync for this server-only repair.
