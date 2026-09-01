# `pkg/parser/types` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly six tracked artifacts and 1,441 text lines. Every
production, test, and BUILD line was read from the pinned tree before the
ownership decision.

| Go artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 39 | `7173f58ff2ba9f52eae65e4607fe57abfdf40aab` | library/test metadata |
| `etc.go` | 171 | `74c89ac0d1185020d2a06149b82d8017a31986d0` | type names, predicates, and error prototypes |
| `etc_test.go` | 34 | `0c67551990d63f1d3206d284db73c65d44828c98` | type-name regression cases |
| `eval_type.go` | 77 | `40694c4551e06eccba906bbc65ceffe425f634ca` | evaluation-type enum and classification |
| `field_type.go` | 789 | `24f2ad782aaed350f027773b529c9c4501d1ffcd` | FieldType metadata, restore, JSON, and sizing |
| `field_type_test.go` | 331 | `a35a097ff1bdeb0835fa084a893f562d879124f3` | field-type, charset, enum/set, and compact-string tests |

The production files contain 61 function declarations and the tests contain
six (`TestStrToType`, `TestFieldType`, `TestHasCharsetFromStmt`,
`TestEnumSetFlen`, `TestFieldTypeEqual`, and `TestCompactStr`). There are no
generated inputs, platform variants, fixtures, fuzz corpora, or build
artifacts beyond the BUILD target.

## Go-master comparison

`git diff HEAD..origin/master -- pkg/parser/types` is empty. The current
branch matches Go master for type-name conversion, evaluation-type
classification, FieldType flags and array state, decimal validity, enum/set
metadata, restore/format output, JSON round trips, memory accounting, and
storage-length calculations. No source fix or new Go regression test is
needed.

## Rust ownership and parity result

`tidb-datatype` is the dependency-closed Rust owner. Its `FieldType` and
`FieldTypeCode` model, evaluation types, type-name helpers, parser/runtime
default-type helpers, error prototypes, restore/format paths, JSON state, and
storage sizing are exercised by source-derived integration tests. The tests
cover the parser package cases and the related runtime `pkg/types` rows,
including binary-literal markers, unknown-type handling, array restoration,
charset detection, enum/set lengths, and aggregate promotion. No Rust-only
behavior requiring removal was found.

## Validation

Profile: Ready for this documentation-only boundary receipt.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 go test ./types -count=1 (current branch): PASS; 0.378s
Pinned Go master worktree: go test ./types -count=1: PASS; 0.397s
Rust `cargo +nightly-2026-08-22 test -p tidb-datatype --test all field_type`: PASS; 22 tests
Rust `cargo +nightly-2026-08-22 fmt --all -- --check`: PASS
Pinned-Go `make lint`: PASS
`git diff --check`: PASS
```

No Go/Rust/Bazel/module source changed, so `make bazel_prepare` is not
required for this receipt.

## Risks and next boundary

- Correctness: FieldType flags, array projection, and decimal/enum metadata
  feed parser, planner, codec, and schema consumers; changes must preserve
  the source's sentinel values and formatting rules.
- Compatibility: type-name text and restore output are externally visible in
  SQL and metadata APIs; unknown codes intentionally retain empty labels.
- Performance: FieldType cloning, hashing, and memory accounting are hot
  metadata paths; no additional allocation or conversion layer was introduced.
