# `pkg/parser` root parity audit ExecPlan

## Objective

Inventory the complete root parser package against Go master, including grammar
inputs, generated outputs, tests, fixtures/support files, and build metadata;
then record the smallest dependency-closed Rust ownership decision.

## Completed

- Read all 33 pinned Go-master artifacts (64,892 lines), 345 function
  declarations, and 150 test/benchmark/fuzz entry points.
- Classified `parser.y`/`hintparser.y` as grammar inputs and
  `parser.go`/`hintparser.go`/`keywords.go` as generated outputs; no generated
  file was hand-edited.
- Compared the root package with Go master and confirmed the 89-artifact
  generated-parser consolidation cannot be safely split into file/function
  ports.
- Verified current and exact-Go-master root suites plus the focused Rust parser
  suite and Ready gates.
- Recorded `tidb-parser` as a partial owner and retained an explicit boundary;
  no speculative compatibility facade or Rust-only behavior removal was made.

## Validation gate

- [x] Current root Go parser suite passes.
- [x] Exact Go-master root parser suite passes.
- [x] Rust parser source-derived suite passes (90 passed, 1 ignored).
- [x] Ready Rust formatting, repository lint, and diff checks pass.
- [ ] Push this receipt/ExecPlan batch, verify remote SHAs, and pull
      `origin/hparser-integration`.

## Next work

Audit `pkg/parser/generate_keyword` and `pkg/parser/goyacc` as separate complete
generator packages. Any future root parser implementation must migrate grammar,
generated outputs, AST/visitor contracts, and tests atomically.

