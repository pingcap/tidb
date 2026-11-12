# TiProxy frontend continuity regression

This opt-in local harness starts two TiDB servers with separate mock stores and
the **unchanged released TiProxy v1.3.2**. Supply a TiDB built from the old
`8947a33f18` base and a TiDB built from the staged session-token changes:

```bash
TIDB_OLD_BIN=/absolute/path/to/old/tidb-server \
TIDB_NEW_BIN=/absolute/path/to/new/tidb-server \
TIPROXY_BIN=/absolute/path/to/released/v1.3.2/tiproxy \
go run ./tests/sessionmigration
```

Run from the repository root with the Go version in `go.mod`. The only imported
nonstandard package is the repository's existing MySQL driver; no TiProxy source
or dependency is added. Obtain TiProxy from the official
[v1.3.2 release](https://github.com/pingcap/tiproxy/releases/tag/v1.3.2), and verify
its release checksum if supplying a distributed binary. Alternatively, build the
unchanged tag at commit `606669fca926969cbfc527269aaf60c02e75a79e` with
`make cmd_tiproxy`; the tag's Makefile embeds its version and commit. The harness
checks its version but does not independently prove its provenance. Build both TiDB binaries with
the same architecture and normal server build configuration.

The frontend uses one dedicated `sql.Conn`, with a custom dialer that rejects a
second physical connection. It initializes a current database, time zone, user
variables, SQL `PREPARE`, and protocol binary prepared statement. It verifies the
actual server's `@@port` changes old → new → old, while every command succeeds
and all session state and both prepared statements remain usable. It updates a
user variable after the first move and checks it after rollback as well.

Static backend addresses are local TCP gates. Closing a source gate's listener
makes TiProxy's regular backend health check fail while retaining existing byte
streams to the source. TiProxy performs its ordinary unhealthy-backend migration;
the harness never kills a source session or edits TiProxy. Both servers share a
temporary ECDSA signing certificate and TLS certificate, with matching local test
users, permissions, schema, and data. This proves connection/state continuity and
token interoperability; the independent mock stores do not test distributed data
consistency or production discovery through PD.

After all old TiDB instances are gone, run the phase-two configurations with two
instances of the **new** binary (set both binary variables to the new path):

```bash
TIDB_OLD_BIN=/absolute/path/to/new/tidb-server \
TIDB_NEW_BIN=/absolute/path/to/new/tidb-server \
TIPROXY_BIN=/absolute/path/to/released/v1.3.2/tiproxy \
go run ./tests/sessionmigration -mode TRANSITION

TIDB_OLD_BIN=/absolute/path/to/new/tidb-server \
TIDB_NEW_BIN=/absolute/path/to/new/tidb-server \
TIPROXY_BIN=/absolute/path/to/released/v1.3.2/tiproxy \
go run ./tests/sessionmigration -mode STRICT
```

Each run has a four-minute overall deadline, bounded readiness and migration
waits, and cleanup of exactly the child processes and open sockets it created.
Success removes temporary artifacts. Failures retain the private temporary
directory (including local test keys and service logs) and print its path. Use
`-keep` to preserve successful-run artifacts too. All listeners bind loopback and
use dynamically selected ports; the brief release-before-bind interval can race
with other local processes, so a bind failure requires rerunning the harness.
