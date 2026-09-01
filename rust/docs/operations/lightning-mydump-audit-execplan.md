# `pkg/lightning/mydump` parity audit ExecPlan

## Objective

Inventory every Go-master mydump production, generated, test, fixture, and
BUILD input; compare the hparser branch with Go master; apply only
dependency-closed behavior changes; and record the Rust ownership boundary.

## Completed

- Read and inventoried all 37 pinned artifacts: 11 production Go files, the
  Ragel input and generated output, ten test/support files, BUILD metadata,
  four CSV fixtures (including the 43-byte zstd fixture), and ten example
  SQL/metadata fixtures.
- Counted 34,011 text lines, 162 production declarations, and 116 test/
  benchmark declarations. Confirmed no tracked Parquet fixture or additional
  platform/generated input exists beyond the BUILD glob and Ragel pair.
- Replaced regexp CSV/chunk unescaping with the Go-master byte scanner,
  adopted `io.ReadSeekCloser`, added deferred/eager `NewReaderOpener` behavior,
  and added the custom-`*` regression plus unescape benchmark.
- Compared `view_import`'s Go-master visitor migration and retained the
  branch-compatible `Accept` API because `ast.Walk`/`InPlaceVisitor` are not
  present without the broader parser migration.
- Confirmed no dependency-closed Rust mydump owner or consumer; no Rust-only
  behavior was removed.

## Validation gate

- [x] Focused custom-escape regression passes with failpoint refcount zero.
- [x] Complete current-branch failpoint-aware package suite passes.
- [x] Detached exact-Go-master package suite passes.
- [ ] `make bazel_prepare` — attempted, blocked by missing `bazel` executable.
- [x] Ready Rust formatting, repository lint, and diff checks pass before
      commit.
- [ ] Push to `origin/hparser-integration`, verify local/tracking/advertised
      SHAs, then pull the explicit branch ref.

## Remaining boundary

The in-place parser AST visitor migration is intentionally not included. It
must be coordinated with `pkg/parser/ast` generated visitor sources and all
downstream AST consumers. Rust parity remains blocked on the concrete parser,
storage, metadata, and Lightning import dependency closure.
