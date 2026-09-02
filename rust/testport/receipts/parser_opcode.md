# `pkg/parser/opcode` — complete package parity receipt

Pinned Go source: `c6054025ed4c32ab3672a2a24ea46892714d21ec` (`origin/master`).

## Complete inventory

The package contains exactly three tracked artifacts and 310 text lines:

| Artifact | Lines | Role |
| --- | ---: | --- |
| `BUILD.bazel` | 17 | library/test metadata |
| `opcode.go` | 246 | operator enum, names, literals, keyword bits, and four methods |
| `opcode_test.go` | 47 | one table/format smoke test (`TestT`) |

No `doc.go`, generated inputs, platform variants, fixtures, fuzz corpora,
benchmarks, or additional build artifacts exist. Every artifact was read in
full before editing.

## Restored Go behavior

Go master removes the obsolete fixed `len(ops) == 32` assertion from
`TestT`. The assertion made harmless additions to the operator table fail
without testing behavior; the test's per-op `Format`/`String` checks remain
the focused regression. The production operator table already matched Go
master, including the absence of the stale `Binary` opcode.

## Rust ownership and parity result

The Rust `tidb-ast` operator table is dependency-closed and already matches
the Go table. No Rust-only operator or behavior remains, and no Rust source
change was needed in this batch.

## Validation

Profile: Ready.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 tools/check/failpoint-go-test.sh ./pkg/parser/opcode -run '^TestT$' -count=1
PASS
make lint
git diff --check
make bazel_prepare (blocked: bazel executable is unavailable)
```

Because only an existing Go test changed, the package has no new production
API or runtime risk. Bazel regeneration remains the only unavailable gate.
