# `infer_pushdown.go` lockdown evidence

This receipt owns exactly `pkg/expression/infer_pushdown.go`. It is a file-level
seed receipt, not a claim that the whole Go `pkg/expression` package is
transcreated. The Rust owner is `infer_pushdown.rs`; `distsql.go` and every
other Go source remain outside this unit.

The source drift gate pins:

| artifact | bytes | lines | SHA-256 |
| --- | ---: | ---: | --- |
| `pkg/expression/infer_pushdown.go` | 25020 | 598 | `f5f9c97ee653aca12116249131affd467a26ded1b9b24d98754e59cac4f570eb` |
| `pkg/expression/expr_to_pb_test.go` | 106420 | 2219 | `e03e21e0175014d938be6205a8a7b6f8b66333b36c2c06c2ab6a1d80a44e89d4` |
| `pkg/expression/scalar_function_test.go` | 8406 | 260 | `1712f4df758514aa42e0710ef8562c0ca8c0c43cec308337ada1971e09f8ce15` |
| `pkg/expression/fts_to_like_test.go` | 13898 | 340 | `aca2e0d0e1e5f7a4487209d225e65b4afad0828bbeb4b22efa0114c645b4ad0c` |

The unchanged repository Go AST tool runs against an isolated directory
containing only those exact artifacts. The direct-support allowlist is the 14
tests and three local helpers that call or construct inputs for this source.
The retained census is 211 production obligations plus 785 test/support
obligations, 996 total. `infer_pushdown.inventory.tsv` assigns exactly one
verdict to every identity: 140 PORTED and 856 DECLINED. There are zero
UNREACHABLE rows; the checker does not manufacture that verdict just to make
all three labels nonempty.

## PORTED evidence

- `P_STORE_MASK` ports the three concrete store bit positions and the exact
  `UnSpecified` union.
- `P_BLACKLIST_MASK` ports `IsPushDownEnabled`, including partial-mask
  admission. The pure `can_function_be_pushed` seam also performs Go's
  function-name check followed by lowercase `function.signature` check.
- `P_TIKV_POLICY` ports the complete TiKV name switch and every special
  signature/input boundary: UnixTimestamp, CONV, ROUND, RAND, and regexp
  charset/collation.
- `P_FLASH_POLICY` ports the complete TiFlash name/signature switch, all CAST
  result/source constraints, and the FTS modifier defense.
- `P_TIDB_UNION` ports the union of TiKV and TiFlash verdicts.
- `P_ENUM_POLICY` ports the CAST-only Int/Real/Decimal preliminary enum rule.

The local generated `ScalarFuncSig` is deliberately smaller than Go's current
TiPB enum. That was measured after the first compile attempt: many Go names,
including `PlusReal`, have no Rust enum variant. `PushDownPolicy.signature_name`
therefore carries the exact full TiPB name for policy decisions and falls back
to `ScalarFuncSig::as_str_name()` for locally generated variants. This avoids
silently narrowing the Go switch to the reduced protocol enum.

## DECLINED evidence

- `D_GLOBAL_ATOMIC`: Go owns an `atomic.Pointer[map[string]uint32]`, an atomic
  reload timestamp, and an `init`-built full-signature lowercase map. The Rust
  crate has no process-global expression-blacklist reload owner. The ported
  API receives the atomically published map as an explicit immutable input;
  global publication and plan-cache invalidation remain visible gaps.
- `D_FAILPOINT_GLOBAL`: Go says, “Use the failpoint to control whether to push
  down an expression in the integration test.” The Rust policy function has
  no failpoint runtime and deliberately does not claim the whole
  `canFuncBePushed` owner, even though its ordinary store and blacklist rules
  are available as the pure seam.
- `D_CONTEXT_RUNTIME`: Go `PushDownContext` owns an `EvalContext`, `kv.Client`,
  two statement warning handlers, session variables, and group-concat state.
  No compatible aggregate exists in `tidb-expr`; replacing those objects with
  booleans would not replicate constructor/selection semantics.
- `D_WARNING_RUNTIME`: `AppendWarning` delegates to Go's
  `contextutil.WarnAppender`. The Rust policy layer returns a conservative
  Boolean and has no statement warning sink.
- `D_EXPR_RUNTIME`: Go recursively switches over `CorrelatedColumn`,
  `Constant`, `Column`, and `ScalarFunction`, invokes `PbConverter`, validates
  TiFlash enum/bit/set/geometry/decimal types, constructs exact warnings, and
  protobuf-marshals optional function metadata. The Rust catalog has neither
  Go's dynamic expression hierarchy nor metadata/warning context. The existing
  `PbScalar` lowering subset is not relabeled as this full runtime.
- `D_GO_TEST_RUNTIME`: the declined direct tests require those Go context,
  failpoint, converter, metadata, warning, or full signature-matrix surfaces.
  Two direct rule tests are PORTED; the others remain classified rather than
  being treated as proof for a narrower recorded answer.

DECLINED is measured falsification of full runtime parity, not an omission.
Completeness here means that every Go obligation has a checked verdict. A
lockdown with no oracle-ratchet movement is still a successful deliverable.

## Mutation and gate evidence

`infer_pushdown.mutations.tsv` records 23 independently applied boundary
mutations. Every mutation was killed by a semantic assertion and the production
file was restored before the clean test run. The mutations cover masks,
two-stage blacklist lookup, whitelist/default boundaries, every TiKV special
case, TiFlash signature families, CAST target/source families, FTS modifiers,
the TiDB union, and enum preliminary admission.

`infer-pushdown-lockdown.py` checks the four artifact hashes, regenerates the
AST identities, enforces the category totals and exact allowlist, rejects every
unclassified/duplicate obligation, validates the mutation receipt, and rejects
any PORTED ledger symbol absent from `infer_pushdown_lockdown.rs`. The Rust
anchor module then makes every PORTED seam a compiler-checked reference.
