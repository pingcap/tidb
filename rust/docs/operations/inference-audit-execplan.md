# `pkg/inference` parity audit ExecPlan

## Objective

Maintain a complete Go-master inventory for the inference runtime and record a
safe package-atomic Rust boundary. Read every provider, protocol, test helper,
test, and Bazel artifact before editing; do not enable a partial `EMBED_TEXT`
port without its session, expression, vector, and configuration owners.

## Completed this batch

1. Inventoried all 43 tracked Go artifacts (7,368 lines), including the root
   `EmbedFn`, Domain adaptor, base/batcher layers, seven provider adapters and
   protocol models, deterministic mock, shared contract fixtures, all 122
   top-level tests, and every Bazel target. No fixtures, generated outputs,
   benchmarks, fuzz targets, or platform/build-tag variants were omitted.
2. Compared the complete runtime with Rust. Rust currently has only embedding
   variable constants and explicit ignored `EMBED_TEXT`/key-redaction gap tests;
   it has no dependency-closed provider, batching/cache, Domain, or vector
   expression owner.
3. Found no Rust-only behavior to remove. A provider-only or parser-only fix
   would be a partial package port and would create an unsupported external
   network/credential boundary, so no production Rust edit was made.
4. Recorded the artifact hashes, function/test counts, Go-master validation,
   and explicit SEED boundary in `rust/testport/receipts/inference.md`.

## Validation gate

- [x] Complete Go source/support/Bazel inventory and Rust owner comparison.
- [x] Exact Go-master worktree `go test ./pkg/inference/... -count=1` (all
      packages pass).
- [x] Branch checkout confirmed not to contain the newly added Go package.
- [ ] Fetch remote, create one meaningful docs batch commit, push to
      `origin/hparser-integration`, and verify `rev-list` is `0 0`.

## Remaining boundaries

The provider network clients, Domain-owned lifecycle, SQL `EMBED_TEXT` function
and vector evaluation, API-key system-variable hooks, and secure-text redaction
remain explicit gaps. The repository package loop continues after this
receipt; this plan does not claim whole-repository completion.
