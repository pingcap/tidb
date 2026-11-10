# Build every scalar function through Go's function classes

This ExecPlan is a living document. Keep Progress, Surprises & Discoveries,
Decision Log and Outcomes & Retrospective current as work proceeds. It follows
`PLANS.md` at the repository root.

## Purpose / Big Picture

In Go TiDB one object answers every question about a scalar function call.
`pkg/expression/builtin.go` maps each function name to a function class
(`funcs map[string]functionClass`), and the class's `getFunction(ctx, args)`
returns a signature object (`builtinFunc`): the arguments wrapped in the
casts the signature needs, the return field type, the collation, the
protobuf signature code (`PbCode()`), and the evaluation methods. The
planner, the push-down checks, the TiPB converter and the executor all read
that one object.

The Rust rewrite makes the same decisions in five places that must agree by
hand: the return type per name (`rewriter/result_type.rs`), the argument
casts per name (`rewriter/signature_args.rs`), the type rules of arithmetic,
operators and comparison (`builtin_arithmetic.rs`, `builtin_op.rs`,
`builtin_compare.rs`), the signature code, re-derived from the name and the
argument types after the casts (`pushdown_catalog.rs`), and evaluation by
name (`scalar_function.rs`). Register row X01 lists what this costs: return
types that differ from Go's, 38 names whose arguments carry no casts, a
signature code that has to be re-resolved for every question, and errors Go
raises in `getFunction` that have no home here (`v div 2` over a vector says
"cannot cast from vector to decimal" where Go's class never builds that
cast).

After this plan a Rust function class per Go class builds the same signature
object, `new_function` is Go's `newFunctionImpl`, and the pieces above are
gone. Observable: for every builtin and every combination of argument types,
the Rust call has Go's signature code, Go's argument types after the casts,
Go's return field type and Go's result over a fixed set of rows -- checked
against fixtures Go itself wrote -- and the X01 row leaves the register.

## Progress

- [x] (2026-11-10) Milestone 0: the base (Go `builtin.go`:
  `newBaseBuiltinFuncWithTp` and what it calls) and the seam in
  `new_function_impl` and the rewriter's `binary_expression`. The other
  three base constructors arrive with the first class that calls them.
- [x] (2026-11-10) Milestone 1: `builtin_arithmetic.go` (6 classes). The six
  names evaluate through the signature's kernel; `prepare_numeric_arguments`
  and `wrap_arithmetic_operands` are deleted.
- [x] (2026-11-10) Milestone 2: `builtin_op.go` (13 classes, 15 names).
  The rewriter's operator, `IS`, unary, function-call and `BETWEEN` paths
  build through the classes; `wrapWithIsTrue` is one function for the
  logic classes, `CASE WHEN` and `IF`. Left: `compose_cnf_condition` /
  `compose_dnf_condition` have no build context at their 75 call sites and
  still construct `and` / `or` nodes directly, and the catalog's resolve
  rows for the operator names stay for such nodes.
- [x] (2026-11-10) Milestone 3: `builtin_fts.go` (2), `builtin_like.go`
  (1), `builtin_ilike.go` (1), `builtin_grouping.go` (1). The rewriter's
  `LIKE`, `MATCH ... AGAINST` and `GROUPING` arms build through the
  classes, and `cardinality.Selectivity` estimates the native builtin as
  the ILIKE it stands for. Left, and the planner's, not a class's: Go's
  `FTSFunctionIsUsed` statement flag and the alternative-plan rounds (the
  LIKE fallback here is a rewrite of the query's AST before its one build).
- [x] (2026-11-10) Milestone 4: `builtin_compare.go` (5 classes, 11
  names), `builtin_control.go` (3). `refineArgs` runs inside the compare
  class, and `refine_comparison*`, `generate_cmp_sigs` and the rewriter's
  compare wrappers are deleted. Left: `builtin_return_type`'s two callers
  (`simple_expr.rs`, `pushdown_catalog.rs`) still reach
  `rewriter/control_type.rs`, `result_type.rs`'s `gl_signature` /
  `extremum_return_type` and `set_gl_signature`; `is_constant` asks
  `folds_to_constant` where Go asks for a `*Constant`; `interval`'s
  `hasNullable` reads the cast arguments.
- [x] (2026-11-10) Milestone 5: `builtin_math.go` (25 classes, 29 names).
  `builtinConvSig`'s bit reading of a hybrid or binary-literal argument
  under the class's cast is in the `Conv` kernel; `rand` evaluates by its
  signature. Left: the legacy `signature_args.rs` / `result_type.rs` rows
  and `wrap_power_arguments` for these names, still reached through
  `builtin_return_type`.
- [x] (2026-11-10) Milestone 6: `builtin_miscellaneous.go` and
  `builtin_info.go` (56 names). `inet_aton` and `inet6_aton` answer as Go's
  two evaluations do: NULL from `vecEval*`, `errWrongValueForType` from
  `eval*` (which keeps a constant unfolded). `schema`, `session_user` and
  `system_user` are coercible, not system constants. Left: Go's
  `default_func`, `uuid_short` and `tidb_row_checksum` classes only raise
  errors and stay on the legacy path; `tidb_decode_sql_digests`' PROCESS
  check and the sequence functions' column-path argument stay in the
  rewriter, which holds the session.
- [x] Milestone 6b: `builtin_encryption.go` (15 names). Go derives the ASCII
  repertoire for `sha` and not for `sha1`.
- [x] (2026-11-10) Milestone 7: `builtin_json.go` (29), `builtin_regexp.go`
  (4 classes, 5 names) and `builtin_other.go`'s `in`, `row`, `bit_count`,
  `setvar` and `getparam`. `IN`'s constant list is deduplicated by the class
  after its casts (`buildHashMapForConstArgs`), and a prepared parameter in
  it is read through the resolver's `eval_constant`, which the planner's
  resolvers now answer from the live statement context. `getvar`, `values`,
  `to_binary` and `from_binary` carry a type their class is constructed with
  and keep their Go-named builders until Milestone 11.
- [x] (2026-11-10) Milestone 8: `builtin_string.go` (50 names). `CONVERT(x
  USING cs)` and `CHAR(... USING cs)` build through their classes.
  `weight_string` carries its padding in the signature and waits for
  Milestone 11; `insert_func` and `convert` have no matrix rows.
- [x] (2026-11-10) Milestone 9: `builtin_time.go` (63 names). `EXTRACT`,
  `GET_FORMAT`, `TIMESTAMPADD`, `TIMESTAMPDIFF` and the `DATE_ADD` family
  build through their classes. `Constant.EvalInt` reads a non-integer
  datum's carrier as Go's `GetInt64` does (`sysdate(1.5)` is fsp 0). The
  three literal classes (`dateliteral`, `timeliteral`, `timestampliteral`)
  and `timestamp` (its signature carries `isFloat`, and its evaluator lacks
  Go's `handleInvalidTimeError`) carry state in the signature and wait for
  Milestone 11.
- [x] (2026-11-10) Milestone 10: `builtin_cast.go` (8 classes) and
  `BuildCastFunctionWithCheck` (`builtin_cast.rs`). The fixture
  `cast_class_matrix.txt` is Go's answer for 21 targets, 3 modes (plain,
  in a UNION, explicit charset) and 22 arguments (1,386 rows). The
  rewriter's `CAST` and the plan builder's cast build through it.
- [x] (2026-11-10) Milestone 10b: the `wrap_with_cast_as_*` helpers,
  `build_cast_function` and `try_push_cast_into_control_function_for_hybrid_type`
  take the build context and fold only the cast they build, as Go's do; the
  class base no longer folds a cast it was handed (`fold_built_cast` is
  gone). A cast pushed into `IF` / `CASE` / `ELT` over an ENUM/SET branch is
  Go's tree (`cast(e, var_string(20))` for an integer target, read by
  `CastStringAsString` as the ENUM's name). Left: five callers with no
  context at hand pass `UnfoldedBuildContext` (`window.rs`
  `update_compare_cols`, `inject_extra_projection.rs`, `task_attach.rs`,
  `mv_index.rs`, `expr_util/builder.rs`); `castAsArrayFunctionClass`.
- [x] (2026-11-10) Milestone 11a: the name-keyed return-type,
  signature-argument and flag-stamping tables are deleted; `getvar`
  (`BuildGetVarFunction`, `convertReadonlyVarToConst` with the
  preprocessor's read-only variable set), `values` (`NewValuesFunc`),
  `to_binary` / `from_binary` (`BuildToBinaryFunction`,
  `BuildFromBinaryFunction`, `HandleBinaryLiteral`) build through their
  classes. `getvar` must stay in `is_unfoldable` (Go
  `unFoldableFunctions`): without it `SELECT @i := @i + 1, @i` folds the
  second read at plan time.
- [x] (2026-11-10) Milestone 11b-1: one evaluator. The AST evaluator
  (`eval_in`'s own tree walk, `func::eval_func`) and the runtime
  argument-cast layer (`arg_eval_type.rs` masks and `wrap_*_args`) are
  deleted. `eval_in` is Go `EvalSimpleAst`: a rewrite through
  `rewriter::ContextResolver` (builds and folds in the evaluation context)
  and one evaluation of the built tree.
- [ ] Milestone 11b-2: evaluation dispatches on the signature object alone;
  X01 leaves the register. Four parts, in order:
  - [ ] 11b-2a: every node is built by its function class.
    `ScalarFunction::new` (a node with a name and no signature) is deleted.
    Done: the rewriter (`row`, the negated row `IN`, the operator fallback),
    `compose_condition_with_binary_op`, the null-reject fold, the
    partition-id `IN` filters, `de_cor_col_from_eq_expr`, `CAST(.. AS ..
    ARRAY)` (Go `castAsArrayFunctionClass` inside
    `BuildCastFunctionWithCheck`). Open: the driver's
    `join_equality_with_collation` (the index join takes its matching from
    equality expressions where Go's takes hash keys and their types),
    `pushdown_catalog`'s `metadata_expression` (goes with 11b-2c), then the
    test call sites and the test-only `PreservingFunctionBuilder` (they go
    when 11b-2b makes the bound evaluation mandatory).
  - [ ] 11b-2b: the class's `getFunction` binds the signature's evaluation
    (Go's `builtinXxxSig.evalT`), one Go file at a time, until
    `ScalarFunction::eval_by_signature` and `func::eval_func_values` are
    empty; the PB decoder (`getSignatureByPB`) binds the same functions.
  - [ ] 11b-2c: `pushdown_catalog`'s second description of a call
    (`PbScalar`, `BuiltinSignature`, `resolve`) is deleted; `ExprToPB`
    reads the node's signature.
  - [ ] 11b-2d: the collation post-pass (`derive_tree_collation`,
    `restore_function_result_charset`, `returns_binary_string`) is deleted.

## Surprises & Discoveries

- Observation: the signature code alone does not say whether a function has
  a TiPB form. Go `scalarFuncToPBExpr` also asks
  `canFuncBePushed(.., kv.UnSpecified)`, so 69 builtins with a code are
  refused for some or all argument types (`tan`, `left` over a binary string,
  `regexp` over non-string arguments, every vector function).
  Evidence: `rust/crates/tidb-expr/testdata/pb_converter_refused.txt`, written
  by Go; `the_converter_refuses_the_signatures_no_store_lists`.

- Observation: the whole-class fixture is `NewFunction`'s output, not
  `getFunction`'s. `typeInferForNull` retypes a NULL constant to the other
  argument's type before the class sees it, so `NULL DIV i` is
  `IntDivideInt` over two bigints. A test that calls `get_function` directly
  fails 62 of the 1,176 arithmetic rows, all with a NULL argument.
  Evidence: `tests/function_class_source.rs` builds through `new_function`.
- Observation: a class folds only the cast it built. Go
  `BuildCastFunction` ends in `FoldConstant`; an argument the wrap returned
  unchanged is never folded by the class.
  Evidence: fixture slots marked `=` (the argument as given) against `c`
  with a folded value (`intdiv xi` has `c..:uint:3138`, the hex literal 0x12
  cast to the unsigned constant 18).

- Observation: the kernel a signature names must read its operands in the
  order the Go signature does, not the order the function name suggested.
  Go `builtinArithmeticPlusRealSig` and `builtinArithmeticModRealSig`
  evaluate both operands before the NULL check; every other arithmetic
  signature returns on a NULL left. The by-name evaluator had this per
  name; the kernel had it for MOD only.
  Evidence: `json_cast_arithmetic_batches_preserve_go_operand_passes`
  (`NULL + cast('notjson' as json)` is an invalid-JSON error, not NULL).
- Observation: Go `WrapWithCastAsDecimal` evaluates a `ConstStrict` cast in
  the build context, after `BuildCastFunction` folded it. A cast over a
  constant tree that is not a `Constant` node is not folded
  (`foldConstant` requires `*Constant` arguments) but is evaluated, so its
  truncation warning is raised once at build and again at every
  evaluation.
  Evidence: `strict_scalar_decimal_precision_probes_preserve_runtime_evaluation`
  (`cast_json('null') DIV c` warns 1292 at build in a non-strict mode).
- Observation: `typeInferForNull` leaves a NULL parameter alone. It writes
  the inferred type through `newarg.GetType(ctx)`, and `Constant.GetType`
  answers for a parameter with a fresh type on every call, so the write is
  lost: `? + 11` with a NULL parameter is `double`, the literal `NULL + 11`
  is `bigint`.
  Evidence: live Go PREPARE/EXECUTE, pinned in `driver/params.rs` and
  `a_null_parameter_keeps_its_own_type_through_null_inference`.
- Observation: a cast on the tree folds at plan time, so its warning is a
  plan-time warning and must precede every execution warning (Go has one
  buffer in order of occurrence). The fold's warnings travel in a
  thread-local stash; an executor warning raised on a worker thread used to
  overtake them. The stash now joins the statement's list where planning
  ends (`prepare_execution_plan`).
  Evidence: `select d DIV '' from du` warns 1292 then 1365 in Go, in both
  evaluation modes (`arithmetic_overflow_names_its_operands_like_go`).
- Observation: overflow and truncation texts come from one renderer, Go
  `ScalarFunction.StringWithCtx` (`(plus(t5.col7, t5.col2) * 9223372036854775807)`),
  and a string constant in a warning goes through `TruncatedStringify`.
  The by-name texts they replace differed for nested functions.

- Observation: Go `mock.NewContext` keeps `types.DefaultStmtFlags`, under
  which a truncation is an error, so a class's cast of the constant `'1.5'`
  to an integer fails to fold and stays on the tree (`bitand qi` has a `c`
  slot without a value). A SELECT folds it with a warning.
  Evidence: the fixture's 61 bit-operator rows with `q`; the Rust test's
  `MockContext` answers `truncate_level() == Error`.
- Observation: a testkit probe prints an integer cast's display width
  (`cast(test.t.dt, bigint(19) BINARY)`); a server deprecates it and prints
  `bigint BINARY`, as all 22 such casts in `tests/integrationtest/r` do.
  Plan text pinned from a probe has to be read with that in mind.
- Observation: Go's rewriter reaches a class from every spelling. The
  function-call form (`isnull(j)`), the `AND` / `OR` joining a `BETWEEN`
  and the `NOT` of `IS NOT TRUE` went around the operator typing here and
  now build through `class_function`.
  Evidence: `op_classes_print_gos_plans` (`isnull(cast(test.t.j,
  var_string(4294967295)))` from both spellings).
- Observation: `unaryNotFunctionClass` appends warning 3986 while it
  builds, once per `NOT` over JSON, so `select not j, !j` carries two and an
  EXPLAIN carries it too.
  Evidence: `op_classes_wrap_and_evaluate_like_go`.

- Observation: a class sets signatures Go's decoder (`getSignatureByPB`)
  has no arm for (`GroupingSig`, `UnaryNotJSON`), and a node built by a
  class must still answer its `PbCode()`. `PbBuiltin::new` is the decoder
  and answered `None` for them, so the node lost its signature.
  Evidence: `set_fts_mysql_match_against_modifier` failed with "unexpected
  builtin signature for match_against: None" until
  `PbBuiltin::of_builtin_func` kept the class's code.
- Observation: Go builds `MATCH ... AGAINST` whenever its four argument
  checks pass and fails only when a row evaluates a non-NULL search string.
  So `AGAINST(NULL)` is NULL, a read of no rows succeeds, and EXPLAIN prints
  the function; the error is 1105, not 1235.
  Evidence: `match_against_builds_and_fails_only_at_evaluation`.
- Observation: `groupingImplFunctionClass` never sees the user's arguments.
  The planner (`rewriteGrouping`) hands it the Expand operator's grouping-id
  column and writes the arguments' positions into the signature's metadata.
  Evidence: `grouping_reads_the_expand_operators_gid`.
- Observation: `LIKE`'s escape is a third argument, an integer constant
  typed by `types.DefaultTypeForValue`, and `NOT LIKE` is `not` over the
  folded `like` (Go `notToExpression`), so the class wraps nothing for it.
  Evidence: `like_and_ilike_cast_their_arguments_to_strings`.
- Observation: once the class refused a search that is not a constant, a
  prepared `AGAINST(?)` stopped falling back to ILIKE: the AST rewrite ran
  on the statement before its parameters were bound and left the marker
  alone. Go's `matchAgainstToLike` evaluates the parameter, bakes it into
  the pattern and calls `SetSkipPlanCache`, so the statement warns "skip
  prepared plan-cache: MATCH...AGAINST LIKE fallback bakes a mutable search
  string into plan constants" and is planned on every execution.
  Evidence: `a_prepared_like_fallback_bakes_its_parameter_and_skips_the_plan_cache`.
- Observation: only a statement whose plan is a logical plan gets Go's
  alternative rounds. A DELETE, UPDATE, INSERT ... SELECT or SET over a
  `MATCH` keeps the native builtin and fails at its first row (1105), and
  EXPLAIN of a query shows the ILIKE while EXPLAIN of a DELETE shows
  `match_against` at 0.1 selectivity.
  Evidence: `only_a_query_gets_the_like_fallback_round`,
  `a_native_match_against_is_estimated_as_its_ilike`.
- Observation: a constant's repertoire is a stored field in Go, set once
  by `initConstantRepertoire` when the rewriter turns a literal or a
  parameter marker into a `Constant`; a constant built any other way keeps
  the zero value. Deriving it lazily from the value made `CASE`'s and
  `COALESCE`'s aggregated collation differ from Go's for folded constants.
  Evidence: the `case` and `coalesce` rows of the function-class matrix.
- Observation: Go's `NewFunction` folds every function it builds
  (`newFunctionImpl` ends in `FoldConstant`), so the node a class returns
  is folded before its parent sees it. With the fold only at the rewriter's
  outer call, an `IN` list's `eq(0, 1)` members stayed unfolded and the
  index-merge plans of `a < 50 or b < 50` changed.
  Evidence: `an_always_false_refined_comparison_folds_away`, the
  `explain_indexmerge_stats` replay topic.
- Observation: `ifNullFoldHandler` (issue 51765) gives the IFNULL the
  charset and collation of its second argument when the first folds to
  NULL, before the fold keeps the node's collation. Both fold
  implementations (`constant_fold.rs`, `expr_util/fold.rs`) need the step.
  Evidence: `set_names_reaches_literal_and_folded_expression_collations`,
  the `expression/constant_fold` replay topic.
- Observation: every class-built node carries a signature, so "has a
  signature" no longer means "decoded from protobuf". The projection's
  string `GREATEST` / `LEAST` buffer shaping skipped such nodes and charged
  640 bytes more than Go.
  Evidence: `a_string_extremum_projection_peaks_like_gos_ping_pong`.
- Observation: Go's `newBaseBuiltinFuncWithTp` casts `args` in place, so a
  class that reads `args[0].GetType()` after it (round, ceil, floor,
  truncate, rand's `*Constant` test) sees the cast argument; abs reads it
  before.
- Observation: Go asks `arg.(*Constant)` of a tree `NewFunction` already
  folded, and `foldConstant` leaves a subtree whose evaluation fails
  unfolded. A resolver that defers folding (`NoResolver`, the planner
  without a statement context) hands the class an unfolded cast, so the
  question here is `folds_to_constant` plus a successful `eval_constant`
  (`builtin_math.rs` `folded_constant`).
  Evidence: matrix rows `rand(q)`, `round(dq)`, `truncate(dq)`.
- Observation: tests pinned to the raw kernel value contradicted Go's
  return type: `floor(<MEDIUMINT UNSIGNED>)` is signed
  (`setFlag4FloorAndCeil` names Long, Longlong and NewDecimal only) and
  `crc32` is unsigned.
- Observation: Go's one `ast.Cast` name is `cast_<target>` here; a body
  that asks `FuncName.L == ast.Cast` (`builtinConvSig.evalString`) tests
  the prefix.
- Observation: the matrix compares a cast argument's type only where the
  probe's column has that shape. `fromUnixTimeFunctionClass` widens the
  cast of a string with scale 0, which no probe column has; the replay
  caught the class asking for the name `cast` (`decimal(65,6)` printed
  `decimal(65,0)`).
  Evidence: `from_unixtime_widens_the_cast_of_a_string_argument`.
- Observation: `CAST(0xffff AS DECIMAL(1,0))` is 9 with warning 1690:
  `CastDecimalAsDecimal` fits a binary literal to the target, and only
  `CastRealAsReal` returns the argument as it is.
- Observation: the matrix probe had no ENUM or SET argument and no NULL
  literal beside a NOT NULL column, so the control classes' enum/set and
  nullability arms were unpinned (two mutants survived). The probe gained
  the letters `E` and `S` and the mixed shapes (37,206 rows).

## Decision Log

- Decision: port one whole Go file per milestone, never one function at a
  time across files.
  Rationale: a Go file's classes share helpers (`setFlenDecimal4RealOrDecimal`,
  `resolveType4Extremum`), and the fixtures compare whole classes. A file is
  the smallest unit whose legacy tables can be deleted when it lands.
  Date/Author: 2026-11-10, agent with the user's approval ("go ahead").
- Decision: the seam sits inside `new_function_impl`, after
  `verify_args_by_count`: a name with a ported class goes through
  `get_function`, every other name through the legacy body. No flag and no
  second entry point.
  Rationale: Go has one constructor. The seam shrinks to nothing as files
  land, so the special case removes itself.
  Date/Author: 2026-11-10, agent.
- Decision: the arguments and the return type stay on the `ScalarFunction`
  node (`args`, `ret_type`), and the signature object the node holds is the
  decoder's `PbBuiltin { signature, kernel }`, set by `get_function` as well
  as by the decoder.
  Rationale: Go keeps `args` inside the `builtinFunc` and `RetType` on the
  node; hundreds of Rust call sites read `function.args`. Moving the field
  changes no behavior. The decoder (`distsql_builtin.rs`) already builds
  `PbBuiltin` from a signature code, so both constructors end at one type.
  Date/Author: 2026-11-10, agent.
- Decision: land X01's admission work first.
  Rationale: its Go fixtures (`pb_code_matrix.txt`,
  `pb_code_vector_matrix.txt`, `pb_code_special.txt`,
  `pb_converter_refused.txt`) are this plan's regression net.
  Date/Author: 2026-11-10, agent.

- Decision: a class reads its context through a small `BuildContext` trait
  (connection charset, `NO_UNSIGNED_SUBTRACTION`, `div_precision_increment`,
  `fold_constant`, `eval_constant`) with two adapters, one over `Columns` (`new_function`) and
  one over the rewriter's `ColumnResolver`.
  Rationale: the rewriter folds through its resolver, which is not a
  `Columns`. One class body serves both callers, as Go's serves both through
  `NewFunction`.
  Date/Author: 2026-11-10, agent.
- Decision: Milestones 0 and 1 land together, and the kernel is not an
  `Option` beside the signature: the node's `pb_builtin` is
  `PbBuiltin::new(pb_code)`, which is `None` only for a signature without a
  code (vector arithmetic), and such a node evaluates by name.
  Rationale: the decoder's kernels for the arithmetic signatures already
  exist and are Go-tested; a base without a class has nothing to test it.
  Date/Author: 2026-11-10, agent.
- Decision: the fixture in the repository holds the rows of the ported
  names only; the probe writes every builtin.
  Rationale: a row of an unported name would fail. Each milestone adds its
  file's rows.
  Date/Author: 2026-11-10, agent.

- Decision: the LIKE fallback stays one AST rewrite
  (`tidb_executor::fts_like_rewrite::rewrite_stmt_fts`) with one gate -- the
  statement is a query and `tidb_opt_enable_alternative_logical_plans` is on
  -- called where a query, an EXPLAIN's target and a prepared statement's
  bound text are planned. Go's two rounds (a native build, then a rebuild
  under `AlternativeLogicalPlanFTSLikeFallback`) are the planner's port and
  are recorded in the register, with what the single rewrite gets wrong: an
  uncorrelated scalar or `EXISTS` subquery, which Go's first round evaluates
  natively and fails, and a view's body.
  Rationale: the rounds change every planning entry point; the class port
  needs only that its refusals no longer hide the fallback.
  Date/Author: 2026-11-10, agent.
- Decision: `BuildContext` gains `append_warning` (Go
  `ctx.GetEvalCtx().AppendWarning`). The rewriter's adapter records it with
  the fold's warnings, which join the statement's list where planning ends.
  Rationale: a statement's warnings are reset once it is planned; a
  build-time warning has to survive that exactly as a fold's does.
  Date/Author: 2026-11-10, agent.
- Decision: `compose_condition_with_binary_op` keeps building its `and` /
  `or` node directly, with the class's result type written out.
  Rationale: Go calls `NewFunctionInternal(ctx, ...)` there; the Rust
  function has no context and 75 callers. Its arguments are conditions
  already, so the class would wrap nothing; threading a context through is
  its own change.
  Date/Author: 2026-11-10, agent.

- Decision: a class-built node always keeps its class's signature:
  `ScalarFunction::from_builtin_func` installs
  `PbBuiltin::of_builtin_func(pb_code)`, whose kernel is the decoder's when
  Go's decoder has an arm for the code and `Kernel::Function` (the node's
  own evaluation) when it has none. This replaces the earlier `None`.
  Rationale: Go's `PbCode()` is whatever `setPbCode` stored; whether the
  decoder can rebuild the signature is a separate question.
  Date/Author: 2026-11-10, agent.
- Decision: the full-text modifier is a field of the node
  (`fts_mysql_match_against_modifier`), set by Go's
  `SetFTSMysqlMatchAgainstModifier` equivalent and read by
  `scalar_function_can_be_pushed` and the ILIKE fallback.
  Rationale: Go keeps it in `builtinFtsMysqlMatchAgainstSig`; the Rust
  signature object is the decoder's shared `PbBuiltin`, and the node is
  where Rust keeps a signature's own state (as it does the grouping
  metadata).
  Date/Author: 2026-11-10, agent.
- Decision: the planner answers `ColumnResolver::roll_expand_grouping` with
  the Expand's grouping-id column, mode and marks, and the rewriter builds
  `grouping` through the class.
  Rationale: Go's `rewriteGrouping` lives in the rewriter and calls
  `NewFunction`; the planner only owns the Expand.
  Date/Author: 2026-11-10, agent.

## Outcomes & Retrospective

Milestones 0 and 1 (2026-11-10): the six arithmetic names are built by
their Go classes from both `new_function` and the AST rewriter. All 1,176
fixture rows (signature, return field type, coercibility, repertoire and
every argument after its cast, constants included) equal Go's. Integer
division carries real casts on the tree.

Milestone 2 (2026-11-10): the fifteen names of `builtin_op.go` are built by
their classes; the fixture grows to 2,828 rows over 21 names. The casts
`NOT`, `IS TRUE`, the bit operators and unary minus need are on the tree
(`not(istrue_with_null(cast(test.t.dt, bigint BINARY)))`), `NOT` over a
vector raises Go's "VectorFloat32 is not supported for unary not operator"
from the class, and `-?` over a parameter holding `-9223372036854775808`
answers the decimal Go answers. `infer_op_type`, `infer_unary_op_type`,
`wrap_with_is_true_with_null`, `folded_value` and the operator rows of
`result_type.rs` and `signature_args.rs` are gone.

Milestone 3 (2026-11-10): `like`, `ilike`, `grouping`, `fts_match_word`
and `match_against` are built by their classes; `builtin::funcs` lists 26
names and the fixture holds 3,516 rows over 24 of them (the other two take
arguments no column shape reaches and are pinned by
`tests_core/builtin_fts_like_grouping.rs` against a Go testkit run).
`MATCH ... AGAINST` builds, prints in EXPLAIN and fails at evaluation as
Go's does; the invented "MATCH_AGAINST() is only supported in starter
deployment mode" refusal is gone, `fts_match_word` asks the deployment mode
as Go's class does, and `fts.rs`'s ILIKE fallback reads the `match_against`
node and its modifier (`BuildFTSToILikeExpressionFromBuiltin`). The `like` /
`ilike` rows of `result_type.rs` and `string_declared_args` and the legacy
`grouping` stamp are gone.

## Context and Orientation

All Rust paths are under `rust/crates/tidb-expr/src/` unless a crate is
named; all Go paths under `pkg/expression/`. Go is the source of truth and is
never edited. A throwaway Go probe lives outside `pkg/`
(`zzprobe/<name>/probe_test.go`, never committed) and runs with
`go test --tags=intest -v .`.

Terms. A function class (Go `functionClass`) is the per-name object whose
`getFunction` chooses a signature from the argument types. A signature (Go
`builtinFunc`, for example `builtinArithmeticPlusIntSig`) is one typed
implementation; its code is a `tipb.ScalarFuncSig`. An eval type (Go
`types.EvalType`) is the coarse type a signature computes in: Int, Real,
Decimal, String, Datetime, Timestamp, Duration, Json, VectorFloat32. A shape
is this plan's notation for the argument types of a call, one letter per
argument: `i` bigint, `u` bigint unsigned, `r` double, `d` decimal(20,4),
`s` varchar utf8mb4_bin, `b` varbinary, `t` datetime, `D` date, `T`
timestamp, `h` time, `j` json, `v` vector.

How Go builds a call. `scalar_function.go` `newFunctionImpl(ctx, fold,
funcName, retType, checkOrInit, args...)` handles `cast`, `getvar`,
`sysdate`, `in` with a NULL list and a few others itself, looks the class up
in `funcs`, replaces NULL-typed arguments' types (`typeInferForNull`), calls
`fc.getFunction(ctx, args)`, takes the signature's return type unless the
caller passed one and the signature's is unspecified, builds the
`ScalarFunction`, and folds constants. Go's expression rewriter has no typing
of its own: it calls `expression.NewFunction`.

How a class builds a signature. It checks the argument count
(`verifyArgs`), decides the eval types, and calls one of four base
constructors in `builtin.go`. `newBaseBuiltinFuncWithTp(ctx, funcName, args,
retType, argTps...)` derives the collation from the arguments as given
(before any cast), wraps each argument in the cast to its eval type
(`WrapWithCastAsInt` and its siblings; a string argument also goes through
`HandleBinaryLiteral`), builds the return field type for the eval type
(`newReturnFieldTypeForBaseBuiltinFunc`), stamps charset, collation,
coercibility and repertoire, and adjusts the NOT NULL flag by name
(`adjustNullFlagForReturnType`). `newBaseBuiltinFuncWithFieldTypes` is the
same with field types for the arguments (decimal, datetime, timestamp and
duration arguments are cast to the exact type; `if`, `ifnull` and `case` use
it). `newBaseBuiltinFunc(ctx, funcName, args, tp)` casts nothing and takes
the return type as given. `newBaseBuiltinFuncWithFieldType(tp, args)` skips
collation derivation. The class then refines the return type (length,
decimals, flags), constructs the signature struct and calls `setPbCode`.

What Rust has. `new_function.rs` `new_function_impl` follows Go up to
`verify_args_by_count` and `builtin_registry::get_function_refusal`, then
runs a legacy body that stands for `getFunction`: the return type from
`rewriter::result_type::builtin_return_type`;
`stamp_builtin_return_flags`; `builtin_compare::refine_comparison_dyn`;
`collation_derive::derive_collation_with_connection`;
`rewriter::signature_args::wrap_builtin_arguments`;
`handle_binary_literal`; collation stamping; constant folding. `rewriter.rs`
repeats much of it in `finish_function_call` and `binary_expression`. A name
`builtin::funcs` lists leaves both before any of that: its class's
`get_function` answers a `BaseBuiltinFunc`, and
`new_function::scalar_function_of` makes the node. `pushdown_catalog.rs`
`pb_code(expression)` re-derives the signature code from the name and the
argument types through per-name resolve tables, with stand-ins where the
tree lacks the casts Go's has. `scalar_function.rs` evaluates by name;
`scalar_function/pb_builtin.rs` holds the decoder's `PbBuiltin { signature,
kernel }`, where `Kernel` is an enum naming the evaluation routine, and a
node carrying one evaluates through it. The pieces to reuse exist already:
`wrap_cast.rs` (`wrap_with_cast_as_int` and its siblings),
`collation_derive.rs`, `binary_literal.rs`,
`builtin_arithmetic::new_return_field_type_for_base_builtin_func`,
`builtin_registry.rs` (names, class names, arities).

The regression net. `testdata/pb_code_matrix.txt` is Go's signature code for
every builtin over every shape of up to three arguments;
`pb_code_vector_matrix.txt` the same for shapes holding a vector;
`pb_code_special.txt` the calls no column shape reaches;
`pb_converter_refused.txt` the shapes `ExprToPB` refuses. They were written
by `zzprobe/pbmatrix/probe_test.go` and are read by
`tests/pb_code_matrix_source.rs`.

## Plan of Work

Milestone 0 adds the base and the seam, changing no behavior. Create
`builtin.rs` as the port of Go `builtin.go`'s constructors:
`BaseBuiltinFunc` (arguments, return field type, signature code, derived
collation), `new_base_builtin_func`, `new_base_builtin_func_with_tp`,
`new_base_builtin_func_with_field_types`,
`new_base_builtin_func_with_field_type`,
`new_return_field_type_for_base_builtin_func` (moved from
`builtin_arithmetic.rs`, completed with Go's string, JSON and vector arms and
the boolean-function flag) and `adjust_null_flag_for_return_type` (the three
Go name sets; today `stamp_builtin_return_flags`). Add the `FunctionClass`
trait with `get_function(ctx, args) -> Result<BuiltinFunc, EvalError>` and a
lookup by name beside `builtin_registry::function_class`. In
`new_function_impl`, after `get_function_refusal`, a name with a class takes
the Go path: `get_function`, the return-type rule quoted above, node
construction with the signature object installed, the `check_or_init`
callback, folding. Make `pushdown_catalog::pb_code` and `describe_function`
read the installed signature when the node has one. Make `rewriter.rs`
`finish_function_call` and `binary_expression` call `new_function` for a name
with a class, so the rewriter has no typing of its own for it. Extend the Go
probe with the whole-class fixture described under Validation and add its
Rust reader, restricted to the ported names (none yet).

Milestones 1 to 10 each port one Go file. For each class in the file, read
its `getFunction` and write the Rust class in the Rust file of the same name
(`builtin_arithmetic.rs`, `builtin_op.rs`, ...), reusing the existing type
helpers where they are already Go's and deleting them where the class
replaces them. Register the class. Delete the file's names from
`rewriter/result_type.rs`, `rewriter/signature_args.rs`, the legacy body's
special cases and the catalog's resolve tables. Move the file's evaluation
onto `Kernel` variants once the eval fixture proves the results equal; until
then the node evaluates by name over the now-correct arguments. Add the
file's names to the whole-class test.

Milestone 1 is `builtin_arithmetic.go`: plus, minus, multiply, divide,
integer divide and modulo. Integer divide becomes Go's: two integer
arguments build `IntDivideInt` over `(ETInt, ETInt)`, anything else
`IntDivideDecimal` over `(ETDecimal, ETDecimal)` with real casts on the
tree, so `prepare_numeric_arguments` goes and a vector operand fails in the
cast as Go's does. Milestone 2 is `builtin_op.go`: the logic operators,
bit operators, unary minus and unary not (whose default arm raises Go's
"%s is not supported for unary not operator" and replaces the check now in
`wrap_builtin_arguments`), `is true` / `is false` / `is null`. Milestone 3
ports the two full-text classes (removing the invented "MATCH_AGAINST() is
only supported in starter deployment mode" from
`builtin_registry::unresolved_error` and fixing the name check in `fts.rs`),
`like`, `ilike` and `grouping`. Milestones 4 to 9 follow the Go files in the
order of the Progress list, which puts the files behind X01's listed
return-type mismatches first (`greatest` / `least` over two dates and `<=>`
in compare; `any_value` and `name_const` in miscellaneous; the AES IV
argument in encryption; `tidb_encode_record_key` and `tidb_mvcc_info` in
info; `crc32` in math; `timestampdiff` in time). Milestone 10 ports the cast
classes and `BuildCastFunction`, replacing `build_cast_function`'s own
signature choice. Milestone 11 deletes what is then unreachable
(`result_type.rs`, `signature_args.rs`, the legacy body, the rewriter's
`finish_function_call` typing, the catalog's resolve tables and stand-ins,
evaluation by name) and removes X01 from the register.

## Concrete Steps

Build settings are fixed for every command (mixing them duplicates every
artifact):

    export CARGO_CACHE_AUTO_CLEAN_FREQUENCY=never CARGO_PROFILE_DEV_DEBUG=0 \
      CARGO_PROFILE_TEST_DEBUG=0 CARGO_INCREMENTAL=0

While iterating, from `rust/`:

    cargo check -j12 -p tidb-expr --tests
    cargo nextest run -j12 -p tidb-expr -E 'test(pb_code_matrix_source) | test(function_class_source)'

Expect every test to pass; a failure prints one `WRONG <name> <Go>!=<Rust>
<count> <shapes>` line per difference.

Before each commit, from `rust/`:

    timeout 1800 cargo nextest run -j12 --no-fail-fast -p tidb-expr \
      -p tidb-unistore-cophandler -p tidb-executor-internal-exec \
      -p tidb-expression-aggregation -p tidb-planner-core-operator-physicalop \
      -p tidb-planner-core-operator-logicalop -p tidb-planner -p tidb-distsql
    timeout 1800 cargo nextest run -j12 --no-fail-fast -p tidb-executor -p tidb-session

Before each push: `cargo build -j12 --locked -p tidb-server` exits 0, and the
integration replay passes:

    cargo test -p tidb-server --lib integration_corpus_matches_recordings -- --ignored

To regenerate a fixture, from `zzprobe/pbmatrix/`:

    ZZOUT=<absolute path of the fixture> go test --tags=intest -run 'TestProbe$' -v .

(`TestProbe` writes `pb_code_matrix.txt`, `TestProbeVector` the vector
matrix, `TestProbeConverter` the refused list, `TestFunctionClass` the
whole-class fixture for every builtin. The repository's
`function_class_matrix.txt` is that output reduced to the rows of the ported
names, every `A` line and the `T` lines those rows refer to.)

## Validation and Acceptance

Each milestone is accepted on four checks, all against Go's own output.

The signature matrix: `pb_code_matrix_source` passes unchanged. It fails
before a class lands only where the legacy path was already wrong; it must
never regress.

The whole-class fixture: `testdata/function_class_matrix.txt`, written by
`TestFunctionClass` from `NewFunction(mock.NewContext(), name,
TypeUnspecified, args...)`. A `T` line numbers a field type
(`type,flag,flen,decimal,charset,collation`); an `A` line gives a shape
letter's argument type; every other line is a name, a shape, the signature
code, the return type, `coercibility,repertoire` and, per argument after the
casts, `=` (the argument as given) or `c` (wrapped or replaced) with its type
and, for a constant a cast folded to, `:kind:value`. Shapes cover the column
letters, `I` (bigint NOT NULL) and the constants `N` NULL, `1`, `x` 0x12,
`q` '1.5', `m` 1.5 and `e` 1.5e0, alone and beside each of `iurdst`. The
Rust test (`tests/function_class_source.rs`) builds the same call through
`new_function` and compares every field, for the names ported so far. This
is the test that proves a class is Go's, and the one the 38 uncast names and
the return-type mismatches fail today.

The eval fixture: per ported file, a Go probe evaluates every signature over
a fixed row of values per shape letter (including NULL, zero, a negative, a
boundary value and a non-numeric string) and writes the result or the error
text; the Rust test evaluates the same calls. It gates moving a file's
evaluation onto the signature object.

Mutants: for every class, a mutant that changes its signature choice, one of
its casts or its return type must fail a test. Restore a mutated file with
`cp` (never `mv`, which keeps the old mtime and leaves a stale binary).

End to end, `SELECT v DIV 2 FROM t` over a vector column reports Go's error
rather than "cannot cast from vector to decimal", and the session and
executor suites and the integration replay pass.

## Idempotence and Recovery

Every milestone is one or more self-contained commits; the seam keeps
unported names on the legacy path, so the tree is releasable after each.
Fixtures are regenerated by rerunning the Go probe and are byte-stable. If a
ported class breaks a downstream suite, check the failing expectation
against Go (`./gorun`, or a testkit probe for exact text) before touching
either side: the legacy path was wrong in places, and a test may have pinned
its output. Reverting a milestone is reverting its commits; nothing outside
`rust/` changes.

## Interfaces and Dependencies

In `rust/crates/tidb-expr/src/builtin.rs` (Go `builtin.go`):

    pub(crate) trait BuildContext {
        fn connection_charset_info(&self) -> (&str, &str);
        fn no_unsigned_subtraction(&self) -> bool;
        fn div_precision_increment(&self) -> i64;
        fn fold_constant(&self, expression: &mut Expression);
        fn eval_constant(&self, expression: &Expression) -> Result<Datum, EvalError>;
        fn append_warning(&self, error: &TerrorError);
    }
    pub(crate) struct ColumnsBuildContext<'a>(pub(crate) &'a dyn Columns);
    pub(crate) struct ResolverBuildContext<'a, R: ColumnResolver>(pub(crate) &'a R);

    pub(crate) struct BaseBuiltinFunc {
        pub(crate) args: Vec<Expression>,
        pub(crate) tp: FieldType,
        pub(crate) pb_code: tipb::ScalarFuncSig,
        pub(crate) collation: CollationInfo,
    }

    pub(crate) fn new_base_builtin_func_with_tp(ctx: &dyn BuildContext,
        func_name: &str, args: Vec<Expression>, ret_type: EvalType,
        arg_tps: &[EvalType]) -> Result<BaseBuiltinFunc, EvalError>;

    pub(crate) trait FunctionClass: Sync {
        fn get_function(&self, ctx: &dyn BuildContext, args: Vec<Expression>)
            -> Result<BaseBuiltinFunc, EvalError>;
    }
    pub(crate) fn funcs(name: &str) -> Option<&'static dyn FunctionClass>;

`BuildContext` holds what the Go classes read from theirs and nothing else;
widen it only when a class being ported reads more. The argument count is
checked by the caller (`verify_args_by_count`, from the registry's arity).
`new_base_builtin_func`, `new_base_builtin_func_with_field_types` and
`new_base_builtin_func_with_field_type` are added by the first class that
calls each. `ScalarFunction::from_builtin_func(func_name, ret_type, f)`
makes the node: `f`'s arguments and collation, and `pb_builtin =
PbBuiltin::of_builtin_func(f.pb_code)`, the object the decoder installs for
a code it knows and the class's code with the node's own evaluation
otherwise.
`pushdown_catalog::pb_code` returns that signature when the node has one.

The work depends on `tidb-datatype` (field types, eval types), `tidb-charset`
(collations), `tipb` (signature codes) and nothing new.
