# `pkg/parser/ast` — complete package parity receipt

Pinned Go source: `5e8a1a229a7591ddac49a0cd3b795587c2595ab9`
(`origin/master`).

## Complete inventory

The package has exactly 36 tracked artifacts and 34,426 text lines. The
inventory below includes every production file, generated visitor output and
input, test/support file, testdata fixture, and all three BUILD files.

| Go artifact | Lines | Blob |
| --- | ---: | --- |
| `BUILD.bazel` | 83 | `b0a7bcf30d7992990a9f4bd0a8f76486a4b35e34` |
| `ast.go` | 290 | `f4f6a7bb94683ff79c4711554ea6fc429f5cdd6e` |
| `base.go` | 441 | `5ff88977daf46adadee1cbac378331cdb2e8eae2` |
| `base_test.go` | 347 | `30f5e9c2191a9a8c96d968c3ac29bc5856cb32ee` |
| `ddl.go` | 6,024 | `1ecd11e7ba0f72ca15a632d6711be1bc6ef01d0e` |
| `ddl_partition_visitor_test.go` | 306 | `98e2c808428c1a15573714cccda3e8a134f29a84` |
| `ddl_test.go` | 1,123 | `5ed8a463c3db63df7bc9b43adab1e945089a899e` |
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

## Rust ownership and parity result

`rust/crates/tidb-ast` is a partial source-shaped owner with mutable
`Visitor`/`Visitable` traversal and broad AST restore coverage. It does not yet
provide the Go-master replacement-preserving plus in-place visitor pair,
generated visitor API, materialized-view AST nodes, or full-join semantics;
those omissions cross `tidb-parser`, planner, executor, and generated-model
boundaries. No dependency-closed Rust implementation can therefore satisfy
this Go package today. No Rust-only behavior was removed and no speculative
AST facade was added.

## Validation

Profile: Ready for this documentation-only boundary receipt.

```text
PATH=/Users/chenhuansheng/.cache/codex-go1.25.10/go/bin:$PATH GOPATH=/Users/chenhuansheng/.cache/codex-gopath-1.25.10 ./tools/check/failpoint-go-test.sh ./pkg/parser/ast -count=1
PASS; 0.426s; failpoint refcount 0

Detached origin/master (`5e8a1a229a7591ddac49a0cd3b795587c2595ab9`) exact-package
failpoint suite: PASS; 0.485s; failpoint refcount 0.
```

No Go/Rust/Bazel/module source changed in this receipt, so `make bazel_prepare`
is not required. Rust formatting, repository lint, and `git diff --check` are
run for the combined commit batch.

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
