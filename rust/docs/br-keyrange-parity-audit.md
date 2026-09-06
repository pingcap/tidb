# br key-range packages parity audit (baseline a85e0fd5df)

Audit of the three Go packages tidb-br claims ported complete —
`br/pkg/streamhelper/spans`, `br/pkg/rtree`, `br/pkg/restore/utils`
(all production + test files read) — against `rust/crates/tidb-br`.

## Result: no behavior-breaking divergences; complete-package claims hold

Test parity verified per package:
- spans: Go 4 tests -> Rust 4, case data byte-identical.
- rtree: Go 8 test funcs -> Rust 8 with all assertions (CallBack tests
  model Go's surviving `pr` pointer via an explicit detached clone;
  Callback2's MetaWriter narrowed to a recording MetaSink);
  FuzzMerge -> fuzz seed corpus; benchmark -> `#[ignore]` stub.
- restore/utils: Go 16 tests -> Rust 16, all case tables preserved
  (TestMergeRanges' 16 cases, TestSetTimeRangeFilter's 6, race test
  re-expressed as a borrow-checker argument).

Production highlights matching: spans join/Valued ordering/ValuedFull
merge-with-overlap trims and re-fuse rule/Overlaps empty-end handling/
Collapse operator precedence — including the Go quirk of deleting
value-index entries AFTER mergeWithOverlap mutates the leftmost start
key (so the index Delete can miss). rtree Intersect end-key case table,
Find, getOverlaps ascend-stop, updateForce, GetIncompleteRange pivot and
4-clause final append, MergedRanges index arithmetic, NeedsMerge matrix,
API-V2 keyspace trim, character-identical Insert/FindContained errors,
logging redact hex. restore/utils RewriteRules lifecycle, SetTimeRange-
Filter (incl. Go's "Wrtie" typo), ValidateFileRewriteRule hex message,
rewrite encoded/raw paths with the empty-old-prefix front insertion,
GetRewriteTableID 0-fallback, MergeAndRewriteFileRanges grouping and CF
counting, misc/common.

## Cosmetic narrowings (documented)

- Duplicate-range and rewrite-file error texts render the range via Debug
  vs Go's Display/%+v shapes; panic wording differs slightly.
- rtree drops two log.Warn diagnostics (key-head parse failure,
  duplicated progress range); Ok(None) semantics preserved.
- collectRangeFiles does not feed the process-wide br/pkg/summary
  CLI collectors.
- Rust builds filesMap/tableIDs from BTreeMaps (deterministic order)
  where Go iterates random-order maps — same results.
- Rust-only tests: int-key framing, row-key framing; goleak TestMain
  harness unported (no assertions).
