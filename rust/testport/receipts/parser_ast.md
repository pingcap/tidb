# `pkg/parser/ast` — complete package parity receipt

Pinned Go source: `17daba3dfde858eebef60f6e4e1bb37268269225`
(`origin/master`, fetched 2026-09-02).

## Complete inventory

The package has exactly 36 tracked artifacts and 34,448 text lines. The
inventory below includes every production file, generated visitor output and
input, test/support file, testdata fixture, and all three BUILD files.

| Go artifact | Lines | Blob |
| --- | ---: | --- |
| `BUILD.bazel` | 83 | `b0a7bcf30d7992990a9f4bd0a8f76486a4b35e34` |
| `ast.go` | 290 | `f4f6a7bb94683ff79c4711554ea6fc429f5cdd6e` |
| `base.go` | 441 | `5ff88977daf46adadee1cbac378331cdb2e8eae2` |
| `base_test.go` | 347 | `30f5e9c2191a9a8c96d968c3ac29bc5856cb32ee` |
| `ddl.go` | 6,044 | `88ab9306f40f2750c8388bc5664f36422f4c64ea` |
| `ddl_partition_visitor_test.go` | 306 | `98e2c808428c1a15573714cccda3e8a134f29a84` |
| `ddl_test.go` | 1,125 | `1ef8c57c1247ad368c7b8ca8c32e5c2b5f4b14ed` |
| `dml.go` | 4,379 | `d4c4ac8b5d09930f4b7d7b7bae6d3e23323e5f5b` |
| `dml_test.go` | 676 | `ed15a8f98fe9a7b30248ffa5d550ff87c3ed557f` |
| `expressions.go` | 1,734 | `0594d84affe39b3a6763c08de0e83c7291c609fb` |
| `expressions_test.go` | 412 | `f48b95117e78ab0c465a2216c6276e56ff87cd2b` |
| `flag.go` | 170 | `c688277e23d71e7093508c56785ac73fd919fd53` |
| `flag_test.go` | 139 | `aa78af9fd41b14fdcd71feafb692ff2700321618` |
| `format_test.go` | 98 | `3e102df3dc073269ad3518bd2d3ff8db9ae7870d` |
| `functions.go` | 1,255 | `e199a81aef4821d692903af29d37459531913855` |
| `functions_test.go` | 266 | `9d9f18f7a56afd54df336317c47106672314d74a` |
| `misc.go` | 4,697 | `0f904209db14101916a2d2609e8af48cd870deff` |
| `misc_test.go` | 526 | `6af5252d62910949f91384c301d18681e4798b2d` |
| `model.go` | 441 | `eebe4fb43498bf17c95c394e7915b6ae40a80ee2` |
| `model_test.go` | 47 | `67aa931f4feb7ce0010eadb0452b8e7e94d8cfdc` |
| `procedure.go` | 1,177 | `eb9f36cba12b3224e44524e9ad935f483eafb252` |
| `procedure_test.go` | 225 | `3fe3c8f4bcfbab0e6b55a791fb04d55b0532c436` |
| `sem.go` | 1,378 | `92aea032e67e17e0a9b2a4c2fabbe853adf34bae` |
| `sem_test.go` | 50 | `6c93c08f46ff9aa65664bf89e376cee864957d5d` |
| `stats.go` | 526 | `b549ce4cc82022617cbcb6d42626428f8de919c8` |
| `stats_test.go` | 256 | `c99a6d490d13ae6e47641f3ed7ef21643aec6a20` |
| `testdata/visitor_benchmark_master_test.go` | 94 | `d7cc9075d69b55e5b89d48c35c3f496cca84ca40` |
| `util.go` | 104 | `ad046afb135c70687e436d626798b810bb4e6e20` |
| `util_test.go` | 231 | `bdcb20c426545a148a0e4c3d9b1dda77ff630b3c` |
| `visitor_codegen/BUILD.bazel` | 18 | `7822c0c70fb503d6b52d5c5254ecb73b9cb4a5b0` |
| `visitor_codegen/cmd/BUILD.bazel` | 15 | `2f9c2fbcdb3989de0545135f16ecf8800ac09466` |
| `visitor_codegen/cmd/main.go` | 37 | `54ab8e50eb8c932ffac3461147125ea67f059ed6` |
| `visitor_codegen/generator.go` | 1,728 | `4bb7c65ba08f868244d0182c27d2a510a61872ab` |
| `visitor_codegen/generator_test.go` | 1,170 | `771b8ae100bd42b56902ff538d504f04282febb0` |
| `visitor_inplace_generated.go` | 2,902 | `67841544f3e27d08f8052fd7940d27991a65d83c` |
| `visitor_test.go` | 1,061 | `050ee2c656434e193f900fca999ed66f78ab1e71` |

The 15 production Go files contain 1,102 function/method declarations. The
17 test/support Go files contain 258 function declarations, including 151
`TestXxx`/`BenchmarkXxx`/`FuzzXxx` entry points. The generated visitor output
is derived from `visitor_codegen/generator.go` and its command; neither
generated output nor generated BUILD metadata was hand-edited. There are no
platform variants or binary fixtures.

## Go-master delta

Relative to the pre-audit hparser branch, Go master adds 8,177 lines and
removes 136. The semantic groups are:

- `ast.go` adds `InPlaceVisitor`, `AcceptInPlace`, `ast.Walk`, and materialized
  view statement labels.
- `visitor_codegen/*` and `visitor_inplace_generated.go` add the generated
  no-replacement traversal and its generator/test contract.
- `ddl.go` adds materialized-view and materialized-view-log nodes and restore/
   visitor methods; `dml.go` adds `FullJoin` and removes stale wildcard state;
  `misc.go`, `sem.go`, `util.go`, and related files carry the corresponding
  AST traversal updates.
- `base.go` replaces the atomic text-state/package mutex cache with per-node
  `sync.Once`; the new visitor and partition tests exercise traversal order,
  skip/stop propagation, and generated coverage.

These changes are coupled to parser grammar, AST consumers, planner/executor
materialized-view support, and generated visitor build rules. They cannot be
made dependency-closed by copying one file.

## Current-master follow-up

The fetched Go master adds the closed `embeddingAPIKeySysVars` allowlist and
redacts those six system-variable values in `VariableAssignment.Restore`
(`misc.go:1019-1049`). The Rust `tidb-ast` owner now carries the same
case-insensitive allowlist, emits the literal `'******'` for matching
`SystemVariableAssignment`s, and exposes `SetStmt::secure_text()` for the
same processlist/logging/audit boundary. User variables and similarly named
future system variables remain unredacted, matching the Go regression table.
The same AST-package batch also fills the new `IndexOptions::auto_pre_split`
field in its in-module restore fixture, keeping the owner’s all-target test
build complete after the existing pre-split port.

## Rust ownership and parity result

`rust/crates/tidb-ast` is a partial source-shaped owner with mutable
`Visitor`/`Visitable` traversal and broad AST restore coverage. It does not yet
provide the Go-master replacement-preserving plus in-place visitor pair,
generated visitor API, materialized-view AST nodes, or full-join semantics;
those omissions cross `tidb-parser`, planner, executor, and generated-model
boundaries. No dependency-closed Rust implementation can therefore satisfy
this Go package today. The bounded security behavior above is implemented in
the existing AST restore owner; no speculative AST facade was added.

## Validation

Profile: Ready for the Rust AST behavior and receipt update.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-ast --test all -- --test-threads=1
PASS; 127 passed, 0 failed, 9 ignored.

OPENSSL_DIR=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler DYLD_FALLBACK_LIBRARY_PATH=/Users/chenhuansheng/.cache/codex-runtimes/codex-primary-runtime/dependencies/native/poppler/poppler/lib cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-ast --all-targets
PASS.

cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex make lint
PASS.

git diff --check
PASS.

PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 TMPDIR=/tmp/tidb-codex ./tools/check/failpoint-go-test.sh ./pkg/parser/ast -count=1
PASS; 0.435s; failpoint refcount 0.
```

Only Rust sources, Rust source tests, and parity documentation changed in this
batch. No Go file, import section, Bazel target, or module dependency changed,
so `make bazel_prepare` is not required. The Rust package test, all-target
compile, formatter, repository lint, and whitespace checks above are the Ready
gates for this batch.

## Risks and next boundary

- Correctness: visitor order/short-circuit behavior, source-text caching,
  materialized-view restoration, full joins, and all generated traversal paths
  must remain aligned when this package is eventually ported.
- Compatibility: parser grammar, planner/executor support, and generated
  visitor code are one integration surface; partial AST nodes would produce
  compile-time or semantic drift.
- Performance: Go's new in-place visitor and per-node cache are deliberate
  hot-path optimizations; a Rust port must preserve their allocation and
  traversal properties.

Keep `pkg/parser/ast` as an explicit Rust ownership boundary until that
dependency closure is available.

## Rust follow-up: parser-owned charset validation

The complete 36-artifact `pkg/parser/ast` inventory above was rechecked
against `origin/master` at `049e0e2ba79d79a3a8b1e9ff93ee22fb1cea7dd5`; the
package has no Go-master delta. Its `functions_test.go::TestConvert` and
`TestChar` cases execute parser grammar actions in Go, so the AST crate's
source-shaped carrier remains ignored for dependency-direction reasons. The
actual Rust owner is `tidb-parser`, whose parser, lexer charset registry,
standalone source tests, aggregate test build input, and Cargo metadata were
inventoried before this change. No Go, generated, fixture, platform, or build
artifact changed.

Rust previously validated an invalid `USING` name only after the cursor had
advanced to the closing delimiter, producing a generic syntax diagnostic and
losing the offending token. Go reports the token-specific
`[parser:1115]Unknown character set: '<name>'` diagnostic. `parse_convert`
and `parse_char_func` now retain the raw charset token while validating and
emit the exact compatibility message. The new parser source test covers the
four valid case-insensitive/string forms plus invalid bare identifiers and
function-shaped names from both Go tests.

Pre-fix proof: the activated source table failed on
`SELECT CONVERT(a USING a)` (`line 1 column 25 near ")"` instead of the Go
1115 diagnostic). After the fix, the focused table passes.

Validation (Ready scoped evidence):

- `cargo +nightly-2026-08-22 test --manifest-path rust/Cargo.toml --offline --locked -p tidb-parser --test all test_function_charset_arguments_match_ast_source -- --nocapture --test-threads=1` — passed.
- `cargo +nightly-2026-08-22 check --manifest-path rust/Cargo.toml --offline --locked -p tidb-parser --all-targets` — owner all-target check.
- `cargo +nightly-2026-08-22 fmt --manifest-path rust/Cargo.toml --all -- --check`, repository `make lint`, and `git diff --check` — Ready gates.

Correctness risk is limited to parser diagnostics for `CONVERT(... USING)`
and `CHAR(... USING ...)`; valid charset canonicalization is unchanged.
Compatibility risk is reduced because invalid names now preserve Go's error
class/code and raw token. No hot-path evaluation or allocation behavior
changes beyond the short-lived validation string.
