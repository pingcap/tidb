# TestMemBufferSnapshotRead source contract

Source: `pkg/session/test/txn/txn_test.go:438`.

The Rust source-contract test retains the complete single-session mutation
path: begin, populate the transactional buffer with `(0,0)` through
`(100,100)`, then run `INSERT ... SELECT` through the target's buffered rows
with `ON DUPLICATE KEY UPDATE` and assert the invariant both before and after
`COMMIT`.

The source's TiKV scan/chunk/concurrency settings choose distributed operators
that the in-process seed does not implement. They are deliberately excluded;
the test covers the SQL-state contract only.
