# `pkg/ddl/session` package receipt

Pinned source: `e2788410d8d696605e8cb002585877a063ccc909`.

## Complete package inventory

| Go artifact | Lines | Blob | Rust disposition |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 45 | `eba324327a6bdb4e7c7583f92e099378d9ecb49f` | workspace crate `tidb-ddl-session` with only the source package's SQL, metric, failpoint, and pool dependencies |
| `session.go` | 151 | `0aeed9d7579a40eb7affe6f9694eea2e4236a846` | `Session`, transaction methods, request-source-preserving execution, exact histogram, and `RunInTxn` handshake in `src/lib.rs` |
| `session_pool.go` | 136 | `facf0b189ce1a51b48de086481b5b0fe008377ed` | typed `Pool` over the exact checkout/reset/registry lifecycle and all three Go destroy branches in `src/lib.rs` |
| `session_pool_test.go` | 183 | `7a55b0cd838fc652ac44d2c02ccf525ff070ac81` | all four original tests execute in `src/tests.rs`; the earlier four ignored documentary gaps were removed |

There is no package doc, benchmark, fixture, generated source/input,
build/platform variant, or ownership artifact in the pinned directory.

## Behavior and integration decision

The crate is deliberately distinct from Rust `tidb-syssession`, which is the
complete carrier for Go `pkg/session/syssession` and has ownership, capacity,
dirty-session, and avoid-reuse policy absent from this package. Native traits
replace Go interface assertions at the boundary but do not change the package
state machine. They expose only the `sessionctx.Context`, transaction, internal
SQL, infosync registry, and `util.SessionPool` operations these two source files
call. `DestroyMode` is the Rust adapter's closed representation of Go's runtime
type switch: destroyable pool, concrete resource pool, or unsupported fallback.

`Execute` preserves nil versus allocated-empty rows, retains an existing
request source or supplies exact `kv.InternalTxnDDL` (`"ddl"`), drains at eight
rows per chunk, always closes a non-nil record set, and records Go's exact
histogram name/help/buckets and `<label>-ok|err` value. `RunInTxn` preserves
begin/callback/rollback-or-commit ordering and the unbuffered two-party
`NotifyBeginTxnCh` failpoint handshake.

## WIP validation

Run from `rust/`:

```text
cargo fmt --all -- --check
cargo test --locked -p tidb-ddl-session
cargo check --locked -p tidb-ddl-session --features failpoints
```

The ordinary and feature-enabled checks pass. Compiler warnings printed by
vendored `tikv-client`, `tidb-model`, and `tidb-chunk` predate this package.
