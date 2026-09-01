# `pkg/session/sessionapi` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the public
`pkg/session/sessionapi` interface package and record a safe package-atomic
Rust ownership boundary. Read every Go source and build artifact before
editing; do not invent a second plugin/session interface around a partial
server implementation.

## Completed this batch

1. Inventoried both artifacts (111 lines): the public `Session` interface,
   identity error sentinel, and 21-line Bazel target. The interface embeds
   `sessionctx.Context` and declares 34 explicit methods. No tests, fixtures,
   generated outputs, benchmarks, fuzz inputs, or platform variants were
   omitted.
2. Compiled the exact Go-master package; it reported `[no test files]`.
3. Compared the package with Rust. Rust owns concrete SQL/session state,
   server connection context, authentication, prepared execution, and
   result metadata in adjacent crates, but lacks one dependency-closed public
   equivalent of the plugin-facing Go API.
4. Found no safe missing behavior to implement and no Rust-only behavior to
   remove. Recorded the inventory, hashes, validation evidence, and explicit
   SEED boundary in `rust/testport/receipts/session_sessionapi.md`.

## Validation gate

- [x] Complete Go source/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master package compilation passed (`[no test files]`).
- [x] No package-local fixture/generated/platform artifact omitted.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
  `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

The concrete session implementation, protocol/authentication surface,
prepared statements, transaction diagnostics, extension/plugin hooks, and
embedded session context remain explicit cross-crate boundaries. The
repository package loop continues after this receipt; this plan does not
claim whole-repository completion.
