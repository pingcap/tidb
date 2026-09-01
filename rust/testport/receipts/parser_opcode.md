# `pkg/parser/opcode` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly three tracked artifacts and 310 text lines. Every
production, test, and BUILD line was read from the pinned tree before editing.

| Go artifact | Lines | Blob | Role |
| --- | ---: | --- | --- |
| `BUILD.bazel` | 17 | `81a468ae1c57a2e01e4067938f7c578f5dbb4e7e` | library/test metadata |
| `opcode.go` | 246 | `17dd568e710e572f67da260344e54a735a3fe225` | operator enum, names, literals, and keyword bits |
| `opcode_test.go` | 47 | `82225e11698f91f28e9a68b571f2679daab274a8` | operator table/format smoke test |

The production file contains four function/method declarations and the test
file contains one `TestT` function. There are no generated inputs, platform
variants, fixtures, fuzz corpora, or additional build artifacts.

## Go-master delta and fix

Go master removes the obsolete `Binary` opcode and its `BINARY` metadata row.
The branch's Go source and Rust `tidb-ast::Op` both carried that stale value;
the fix removes it from both authorities, changes the Rust table/`ALL` lengths,
and adds a focused Go/Rust opcode-count regression. The pre-fix regression
failed in both implementations (`got 33 want 32` in Go and `Op::ALL` length
32 versus 31 in Rust); both pass after the fix.

The remaining `BINARY` spellings in charset, cast, weight-string, and binary
expression code are distinct source concepts and were not removed.

## Rust ownership and parity result

Rust's `tidb-ast` crate is the dependency-closed owner for this small operator
table and its expression adapters. No Rust-only operator remains, and the Go
source now matches Go master exactly for this package.

## Validation

Profile: Ready for this implementation batch.

```text
Before-fix focused regressions:
  Rust parser opcode contract: FAIL (expected Op::ALL length 32, Go master 31)
  Go TestT: FAIL (unexpected opcode count: got 33 want 32)

After-fix:
  cargo +nightly-2026-08-22 test -p tidb-ast --test all opcode
  PASS; 5 tests
  PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/parser/opcode -run '^TestT$' -count=1
  PASS; 0.271s; failpoint refcount 0

Rust `cargo +nightly-2026-08-22 fmt --all -- --check`: PASS
Pinned-Go `make lint`: PASS
`git diff --check`: PASS
```

Because Go production and test sources changed, `make bazel_prepare` is
required. It was attempted with the pinned toolchain but is blocked locally by
the missing `bazel` executable (`make: bazel: No such file or directory`); no
Bazel metadata could be regenerated.

## Risks and next boundary

- Correctness: numeric opcode values after `Falsity` shift down by one, which
  is the intended Go-master contract; no current production caller references
  the removed value.
- Compatibility: external code that imported `opcode.Binary` must migrate to
  the dedicated cast/weight-string representations.
- Performance: the table loses one unreachable row; no runtime path was
  otherwise changed.
