# Server workstream

This workstream owns the Rust SQL-node connection lifecycle above the
serialized MySQL protocol. The Go source of truth is `pkg/server/conn.go`,
`pkg/server/conn_stmt.go`, `pkg/server/server.go`, and the handshake/auth
owners under `pkg/server/`. Tests remain obligations in the Go-test ledger;
the current exact split for `pkg/server/conn_test.go:670 TestDispatch` is
recorded in `go_test_domain_manifest.tsv`.

The first executable leaf is `crates/tidb-server`: `Connection::dispatch`
consumes the protocol command decoder, owns connection-closed state, and
connects `COM_QUERY` to the shared `tidb-exec::Session` and
`tidb-distsql::DistSqlContext`. `Connection::dispatch_framed` adds the bounded
response boundary: it accepts one sequence-zero command frame, returns
sequence-one response frames for COM_QUERY/PING, and returns an explicit
no-response close for COM_QUIT. `Connection::dispatch_framed_auto` now closes
the safe parser→ResultField resolver→ColumnInfo paths for table-less and
single-table catalog-backed
`SELECT` while rejecting unsupported catalog-dependent shapes instead of
guessing; it now consumes bounded catalog schemas for INNER/CROSS/LEFT/USING
joins and proves metadata/rows, including LEFT null extension and USING
coalesced order. Direct columns, aliases, and qualified/bare wildcards cross
the isolated projection contract; typed expressions, redundant right-side
USING columns, and general join typing remain outside this seam. The
crate also owns source-shaped initial-handshake construction/response parsing
and an idempotent real-TCP listener lifecycle with explicit bind/active/
shutdown/closed states and health ordering. Its generic accept-loop leaf owns
listener/handler error propagation and shutdown without pulling in protocol or
auth. It supports only `COM_QUERY`,
`COM_PING`, and `COM_QUIT`; unsupported commands, malformed payloads, invalid
query UTF-8, unsupported automatic metadata, and lifecycle transitions are
explicit errors. Authentication verification, TLS/compression, database
selection, prepared statements, dynamic warning/error-context/session variables,
Unix sockets/PROXY handling, accept loops, and deployable bootstrap remain
separate owners.

Future agents must claim non-overlapping Go source families before touching
this workstream. Auth/TLS/compression, packet writing, prepared statements,
connection variables, accept-loop/bootstrap, HTTP status, and mixed-cluster
routing are separate owners. The protocol crate now exposes the
dependency-closed source-shaped ERR payload, typed error conversion, and the
isolated caller-rendered sequence-one error-response frame with optional
published status attachment; `Connection::frame_execution_error` now joins
the session's exact failed/successful status snapshot to that frame without
copying warnings into ERR; this workstream
still owns capability selection, packet framing, and actual write/flush
integration. `tidb-server` may consume protocol, DistSQL,
planner, and executor contracts; lower crates must not depend back on it.

The handshake owner now exposes `AuthHandshake`: it preserves the exact
client response payload/auth bytes, intersects client/server capabilities,
models an SSLRequest as an explicit `TlsRequested` phase, and classifies
native fallback, plugin-switch, or identity-store deferral without doing TLS,
user lookup, auth-switch writes, or password verification. `tls_established`
is the transport-owned transition back to the full response parser. The
remaining auth plugin implementation, session/user storage, and secure
transport policy stay outside this dependency-closed leaf.

`AuthExchange` is the adjacent post-handshake wire leaf: it preserves
`AuthSwitchRequest` and `AuthMoreData` payloads, sequence framing, and explicit
trailing/malformed-byte errors. It intentionally does not verify passwords,
consult a plugin registry or user store, perform TLS, or flush packets.

`AuthPluginRegistry` is the metadata-only custom-plugin leaf. It mirrors Go's
validation ordering and LDAP/`RequiredClientSidePlugin` mapping. Its
`AuthPluginAdmission` classifies built-in, validated custom, and unsupported
names without executing callbacks. `ClientPluginSelection` adds explicit
session-token passthrough, native fallback, plugin-switch, and legacy-client
rejection outcomes without writing packets. Password hashing/verification,
TLS, and privilege/session wiring remain unported.

`AuthTokenCheck` is the dependency-closed JWT compact-shape/retry leaf. It
preserves Go's exact three-segment check, initial-attempt plus retry ordering,
JWKS reload-after-failure, and explicit missing-JWKS/load-error outcomes while
leaving RSA/JWK verification, filesystem/network refresh, claims decoding, and
authenticated-session publication to the privilege/authentication owners.

`SecureTransportPolicy` is the adjacent admission leaf. It rejects plaintext
TCP only when `RequireSecureTransport` is enabled and accepts Unix/direct-TLS/
gateway-secure transport facts supplied by the transport owner. It never
performs TLS, certificate, gateway-attribute, or password validation.

`AuthChallenge`/`AuthSessionAttempt` retain the session-facing identity,
plugin, opaque authentication bytes, and salt, then stop at
`PendingVerification`; only the `auth_socket` Unix-only admission rule is
implemented. Password/plugin verification, privilege lookup, account locking,
and authenticated-session publication remain separate owners.

The bootstrap ring now exposes `tidb-server::bootstrap` as a pure decision
boundary: bootstrap/upgrade/normal mode selection, SYSTEM-keyspace version
guards, feature-gate outcomes, and the source phase-order contract. It does not
read or mutate KV, create system tables, load privileges/plugins, execute a
bootstrap SQL file, start the domain, or launch background workers; those
effects remain explicit queue items for storage, catalog, DDL, and server
owners.

`AuthSessionAttempt::begin_with_policy` composes the transport admission first,
so `RequireSecureTransport` rejects plaintext before plugin/session state is
created. The policy still consumes only listener-established transport facts;
TLS handshakes, certificates, gateway parsing, and password/user-store work are
not hidden in this session leaf.

`IdentityLookupRequest`/`IdentityLookupResult` are the pre-auth
`session.MatchIdentity` contract: they retain the requested user/remote host,
`skip-name-resolve` setting, and an externally selected canonical row or
explicit not-found result. Privilege-table access, password verification, and
authenticated-session publication remain outside this leaf.

`IdentityCatalog` now owns the dependency-closed matching rule itself: rows are
sorted most-specific-first, `%`/`_`/escaped-byte patterns, `localhost` loopback,
and valid IPv4 network masks are matched exactly, and reverse-DNS names are
caller-injected so this crate performs no network I/O. Privilege-cache loading,
actual DNS, role/privilege effects, and password/session authentication remain
open.

`IdentityLookupPolicy`'s `SkipWithGrant` mode now returns an explicit `Bypassed`
result with the requested user/host instead of fabricating a canonical
privilege row.
The wider role/privilege bypass, password verification, TLS, and authenticated
session lifecycle remain external.

`PrivilegeRowAdmission` closes the exact-row seam from `ConnectionVerification`:
it accepts only a canonical username/host pair that exists verbatim in the
catalog and never rematches `%` or network patterns. Password/plugin checks,
account policy, resource groups, and session authentication remain external.

`AuthPluginHandoff` now carries the exact row's opaque plugin name and whether
stored authentication data exists, with the source `SkipWithGrant` native
default as a separate bypass handoff. Plugin registry validation, password
hashing/verification, TLS, privilege publication, and authenticated-session
state remain outside this seam.

`AuthPluginRegistry::admit` now classifies built-in, validated custom, and
unsupported names without invoking authentication callbacks; callback
execution, password verification, TLS, and privilege/session integration
remain external.

`AuthPluginRegistry::select_client_plugin` now preserves the source handshake
selection outcomes: session-token passthrough, native-password fallback,
auth-token clear-password mapping, custom/LDAP switches, legacy-client
rejection, and explicit unsupported user plugins. It does not write packets or
execute callbacks.

Validation:

```bash
CARGO_BUILD_JOBS=12 cargo test --offline --locked -j12 -p tidb-server
cargo fmt --all -- --check
```
