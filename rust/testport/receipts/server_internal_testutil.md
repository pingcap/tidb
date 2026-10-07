# Go server test-support boundary

Go `pkg/server/internal/testutil` supplies `BytesConn` and `GetPort` for Go
server tests. Its two source/build artifacts remain in the Go inventory.
Rust packet tests use `std::io::Cursor` directly; real socket consumers use
`SocketAddr`. No production adapter is needed for these native APIs.

The private Rust `ReadOnlyBytesConn` had no consumer outside its own two tests.
Those tests checked its hard-coded no-ops and `SocketAddr::port`; their packet
read assertion is covered by the maintained `packetio_source` suite. The helper
and both self-checks are retired. This decision does not accept the complete Go
server test package or its network/platform obligations.

[Prior inventory and validation](https://github.com/pingcap/tidb/blob/c36e9b3be561108bf879080faae6b1b58c123c14/rust/testport/receipts/server_internal_testutil.md) remain archived.
Current cleanup and retained coverage: [validation](../../docs/parity/current-audit/mock-selfcheck-cleanup-validation.json).
