# Preserve Go shared response-buffer ownership

The existing complete kvproto packages are the protocol owner for client-rust
and TiDB Rust. At TiDB Go master 6b2781326b722f217a61852ab403350858549bd0,
kvproto v0.0.0-20260820070758-623e58e60fa9 declares four custom SharedBytes
fields: coprocessor.Response.data, BatchResponse.data,
StoreBatchTaskResponse.data and kvrpcpb.TiFlashSystemTableResponse.data.
Go pkg/sharedbytes.Unmarshal retains the received slice. Native prost generated
Vec fields instead, copying every body. TiDB's local coprocessor projection
also duplicated the native package to obtain Bytes for one of those fields.

The generator now selects Bytes for all four custom fields and regenerates
both complete modules. No wire schema changes. Native tests use explicit Bytes
conversion for existing fixtures. The new regression decodes the same owned
wire buffer through all four response types, verifies exact re-encoding and
checks each body's pointer belongs to the original buffer. Before the change,
all four pointer checks were false; after regeneration all are true.

Validation from the native repository root:

    cargo test --locked -p tikv-client-kvproto go_sharedbytes_fields_retain_the_received_buffer
    cargo run --locked -p tikv-client-proto-build
    cargo test --locked -p tikv-client-kvproto
    cargo test --locked --lib -- --test-threads=1
    cargo test --locked -p tikv-client-proto-build
    cargo check --locked --workspace --all-targets --all-features
    cargo clippy --locked --lib -- -D warnings

The first command reproduced the failure before generation changed. Protocol
and generator suites pass three tests each; the native library passes 1,401
with two existing ignored tests. All workspace targets/features compile;
strict library Clippy passes. Go's original pkg/coprocessor and pkg/mpp contain
no tests; their build checks and pkg/sharedbytes's original test pass from the
pinned Go module. Logs are /private/tmp/client-rust-sharedbytes-*.log and
/private/tmp/tidb-copro-mpp-go-tests.log.

The public Rust data field type becomes Bytes; Vec constructors need .into().
Decoding from Bytes shares ownership; decoding from a borrowed slice may copy
to provide a valid owned lifetime. Real-cluster integration and benchmarks were
not run. This removes a concrete copy and lets TiDB remove its competing
protocol owner; it is not a claim of complete client-go behavioral parity.
The separately rejected transaction-lifetime patches remain unapplied.
