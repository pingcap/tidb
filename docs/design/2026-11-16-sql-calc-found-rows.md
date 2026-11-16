# Native SQL_CALC_FOUND_ROWS

## Result contract

`SELECT SQL_CALC_FOUND_ROWS ... LIMIT ...` returns the requested page. After consuming the result to successful EOF, `SELECT FOUND_ROWS()` on the **same connection** returns the number of rows the query would produce without its outer LIMIT/OFFSET. An empty page, LIMIT 0 or an offset beyond the result still computes the total. Only the outer pagination is removed: inner limits, DISTINCT and GROUP BY/HAVING remain part of the count. For an aggregate query, this is the number of output rows/groups, not the number of input rows.

```sql
CREATE TABLE found_rows_example (id INT PRIMARY KEY);
INSERT INTO found_rows_example VALUES (1), (2), (3);
SELECT SQL_CALC_FOUND_ROWS id FROM found_rows_example ORDER BY id LIMIT 1; -- 1
SELECT FOUND_ROWS(); -- 3
SELECT FOUND_ROWS(); -- 1: the preceding FOUND_ROWS() query returned one row
DROP TABLE found_rows_example;
```

Connection pools must keep the page query and FOUND_ROWS() call on the same physical session. Read the total before another statement changes it. The preserving EXPLAIN/SHOW forms below are exceptions; not every intervening statement preserves the total. A SELECT without the modifier retains its existing returned-row behavior.

The parser syntax and AST flag already existed. This implementation replaces noop handling for the modifier in all three `tidb_enable_noop_functions` modes: OFF, ON and WARN. Those modes neither disable native counting nor bypass its unsupported-shape checks.

## Supported queries and placement

| Query shape | Current behavior |
| --- | --- |
| Deterministic top-level SELECT, with or without LIMIT | Native page and total |
| Joins, DISTINCT, GROUP BY/HAVING, EXISTS/IN and tested correlated-aggregate forms | Native counting preserves the output-row semantics |
| Subqueries or CTEs without their own modifier | Supported as part of a supported outer counted SELECT; inner LIMIT/OFFSET is retained |
| Inline, materialized/reused and recursive CTEs | Supported subject to existing CTE limits and cache eligibility |
| Prepared statements, including parameterized LIMIT/OFFSET | Supported, including SQL PREPARE and protocol preparation |
| Outer ORDER BY RAND() | Allowed: dropping only the outer ordering does not change count membership |
| Ordinary EXPLAIN or DESCRIBE of a supported SELECT | Plans both branches without executing them; preserves the previous FOUND_ROWS value |
| EXPLAIN ANALYZE of a supported counted SELECT | Executes both branches; uses the query total without adding plan-output rows |

A top-level SELECT may contain UNION inside a subquery or CTE. This differs from placing the modifier on a top-level set operation, which remains unsupported.

### Rejected forms

Placement and implementation support are separate checks in [calc_found_rows.go](../../pkg/planner/core/calc_found_rows.go). A dedicated visitor checks modifiers in the same traversal as preprocessing, preserving statement-level errors and skipped subtrees. `findSQLCalcFoundRowsAllowedSelect` identifies the SELECT where the modifier may appear, including blocks owned by unsupported statements; finding that block does not enable those statements. The visitor computes this SELECT and the unsupported-statement reason once, unwrapping EXPLAIN, without storing the statement root on the general preprocessor. TABLE and VALUES also use `SelectStmt` but have no `SelectStmtOpts`, so the options check must accept nil.

| Form | Error and reason |
| --- | --- |
| Modifier in a scalar/derived subquery, CTE definition or later UNION branch | **1234 / 42000**: incorrect usage/placement, matching the tested MySQL rule |
| Modifier on the first SELECT of a top-level set operation | **1235 / 42000**: valid placement in MySQL's UNION case, but unsupported here |
| Outer modifier in INSERT/REPLACE SELECT, including EXPLAIN of that statement | **1235 / 42000**: statement shape unsupported; not an illegal nested modifier |
| Outer modifier in CREATE VIEW / CREATE OR REPLACE VIEW | **1235 / 42000**: view-definition semantics are not implemented by this feature |
| Direct TRACE SELECT/TRACE PLAN or PLAN REPLAYER DUMP EXPLAIN SELECT with an outer modifier | **1235 / 42000**: these TiDB diagnostic wrappers are outside the supported scope |
| CREATE TABLE ... SELECT | Existing **1105 / HY000** statement-level rejection, before modifier validation; this feature does not enable CTAS |
| Window functions, locking reads, SELECT INTO or variable assignments in a counted query | **1235 / 42000** |
| RAND() outside the outer ORDER BY, and checked non-repeatable/session-changing functions | **1235 / 42000** |

The function checks include UUID variants, SYSDATE, SLEEP/BENCHMARK, advisory-lock functions, sequence functions, SETVAR, RANDOM_BYTES, EMBED_TEXT and LAST_INSERT_ID with an argument. This is an explicit set of checks, not a general proof that every accepted expression is repeatable. See the checker for the authoritative list. Restrictions also apply to nested expressions/SELECTs within a counted query. If a statement has multiple invalid or unsupported properties, no complete MySQL error-precedence guarantee is made.

### MySQL compatibility boundary

A local MySQL **8.0.46** comparison checked placement, wrappers and count state. It is evidence for that version and those cases, not certification of every MySQL release or statement form. MySQL's [placement check](https://github.com/mysql/mysql-server/blob/mysql-8.0.46/sql/sql_lex.cc#L4331) compares query blocks, rather than requiring SELECT to be the whole statement AST.

MySQL accepts an outer modifier in INSERT/REPLACE SELECT, CTAS and view definitions, but their counting semantics differ. With three source rows and LIMIT 1, the local oracle observed a total of 3 after INSERT/REPLACE, but 1 after CTAS; CREATE/ALTER VIEW accepted the modifier and omitted it from the stored definition. TiDB does not implement those behaviors in this change. MySQL does not have TiDB's TRACE or PLAN REPLAYER SQL syntax, so their support cannot be inferred from MySQL compatibility.

## Planning, execution and state constraints

[PlanBuilder.buildSelectWithFoundRows](../../pkg/planner/core/calc_found_rows.go) builds a count branch from the SELECT with only the outer LIMIT/OFFSET and ordering removed. It wraps the result in COUNT to preserve DISTINCT, grouping, HAVING and nested-query semantics. Column bindings and correlated aggregate mappings are rebuilt for the data branch. Both branches receive ordinary optimization; data-side limit/TopN pushdown remains available. The point-get fast path is bypassed so the count branch cannot be skipped.

`PhysicalCalcFoundRows` exposes only the data schema. [CalcFoundRowsExec](../../pkg/executor/calc_found_rows.go) observes these constraints:

1. Execute the data branch first and stream page rows. Do not delay the first row by eagerly running count.
2. At data EOF, close the data child and release completed data-side CTE producer storage before opening count. Normal CTE Close retains shared results for other consumers until statement cleanup, so this explicit release prevents unnecessary overlap with count storage.
3. Execute count in the same statement transaction context/snapshot. Data and count have independent CTE storage IDs and **do not share one materialized result**.
4. Set the statement's final total only after count succeeds and its child closes successfully. Publish it to session `LastFoundRows` at successful result EOF. Errors, cancellation and early close must not publish an unfinished total. A page can already have streamed when count fails, so the client must still consume/check EOF.

[StatementContext](../../pkg/sessionctx/stmtctx/stmtctx.go) protects the count with its mutex. Returned rows are accumulated locally in `recordSet.returnedRows`; `CalcFoundRowsExec` retains its unlimited total locally. At successful result EOF, `recordSet` calls `Executor.FoundRows()` to obtain a completed total, falls back to the returned-row count when none is available, and calls `SetFoundRows` once. EXPLAIN ANALYZE forwards the analyzed query's total through the same interface, excluding plan-output rows without requiring concrete executor checks in the adapter. `BaseExecutorV2` returns unavailable by default, so ordinary executors retain returned-row counting. A zero total is distinct from an unavailable total. Statement reset and retry clear the statement count; each result set and opened count executor starts with its own zero count. `PreserveFoundRows` independently controls whether successful result consumption replaces the preceding session value. Counts are execution state, never cached-plan state.

### Intervening statements

| Statement | Effect on FOUND_ROWS |
| --- | --- |
| Ordinary EXPLAIN / DESCRIBE SELECT | Preserve previous value; do not run the target count |
| EXPLAIN ANALYZE of a supported counted SELECT | Replace with the completed query total, excluding plan-output rows |
| DESCRIBE table | Follow SHOW COLUMNS returned-row behavior, not EXPLAIN preservation |
| SHOW WARNINGS / ERRORS | Preserve previous value |
| SHOW COUNT(*) WARNINGS / ERRORS | Replace with 1 |
| SHOW CREATE TABLE / VIEW / DATABASE / USER; SHOW GRANTS, PRIVILEGES, PROFILES, MASTER STATUS (BINARY LOG STATUS alias), PROCESSLIST | Preserve previous value |
| SHOW SESSION_STATES | Preserve for TiDB session migration |
| Other result-producing SHOW statements | Retain returned-row counting |

Prepared execution classifies the underlying statement rather than the EXECUTE wrapper. SHOW compatibility was checked against MySQL 8.0.46's default legacy PROCESSLIST implementation; other PROCESSLIST configurations were not certified. This change does not align all no-result statements: the existing DO behavior remains a known separate discrepancy.

## Plan cache

Prepared, non-prepared and instance-cache eligibility is preserved; the feature does not add a blanket cache exclusion. Tests compare ordinary and counted queries, change parameters/data, invalidate schemas and execute concurrent sessions. Some materialized/recursive CTE shapes are already ineligible for instance cache; native counting does not make all CTEs cacheable. Physical-plan codec and generated clone support retain both children without caching a runtime total.

An independent prerequisite fix in [plan_cache_rebuild.go](../../pkg/planner/core/plan_cache_rebuild.go) rebuilds ranges in PhysicalCTE/PhysicalCTEStorage seed and recursive definition plans. These are not ordinary consumer children. Its regression uses an ordinary SELECT, proving that stale cached parameter ranges are a preexisting CTE issue rather than a counting-only issue.

## Shared CTE column pruning

Ordinary SELECT, explicit COUNT and the native count branch use the same column-pruning rule. PlanBuilder only constructs the count plan; it does not enable or initialize CTE pruning. `ColumnPruner` collects the union of requirements from CTE consumers and processes producers before their dependencies, including references inside other producers. This allows ordinary SQL to avoid reading unused wide outputs as well.

Unused producer outputs become typed nullable NULL constants, preserving storage positions and column IDs. Normal producer pruning then removes unnecessary source reads. Consumer schemas accept the nullable slots before statistics derivation. Required outputs, filter inputs, DISTINCT/grouping/HAVING inputs and inner-order/limit inputs are retained; existing projection pruning preserves assignment and SLEEP side effects.

The transient `ColumnUsage` union survives nested optimization until the shared producer obtains its physical plan. Clearing it at the end of an outer pruning pass would allow optimization of an intermediate CTE to forget requirements from outer consumers. It is cleared after producer optimization, before plan caching; there is no count-specific or persistent pruning flag.

Recursive, duplicate-eliminating (`CTEClass.IsDistinct`), correlated/Apply (including producers containing Apply), storage-only and already optimized producers are not rewritten with NULL slots. Their output requirements still propagate to dependent CTEs. Repeated column-pruning passes also retain the inputs referenced by predicates already pushed into the CTE. These boundaries preserve the existing query semantics and cache eligibility.

## Test coverage and evidence

Maintained regression coverage:

| Area | Test entry points and coverage |
| --- | --- |
| Result semantics | [found_rows_test.go](../../pkg/expression/integration_test/found_rows_test.go): `TestSQLCalcFoundRows`; page/empty/LIMIT 0/out-of-range cases, no LIMIT, ordering, joins, DISTINCT, grouping/HAVING, inner limits and nested queries |
| Placement and noop modes | `TestSQLCalcFoundRowsUnsupported` in the same file; the full SQL/error matrix under OFF, representative OFF/ON/WARN behavior, direct and applicable PREPARE paths, 1234 versus 1235 and CTAS's earlier rejection. [TestValidator](../../pkg/planner/core/preprocess_test.go) checks TABLE/VALUES without options, error precedence and skipped binding subtrees |
| Planning and completion | `TestSQLCalcFoundRowsPlanningAndClose`; EXPLAIN/PREPARE do not execute data, EXPLAIN preservation, native ANALYZE total, multiple chunks, repeated ordinary/native statements, empty results and early close |
| Snapshot consistency | `TestSQLCalcFoundRowsStatementSnapshot`; RC and RR with a writer inserting between data and count, for ordinary, inline, materialized and recursive cases |
| CTE lifecycle | `TestSQLCalcFoundRowsCTE`; repeated consumers, recursion, release before count, early close/cancellation, count recursion errors and tracker cleanup. Forced spill covers **nonrecursive materialized** page/count paths |
| Cache behavior | `TestSQLCalcFoundRowsPlanCache`, `TestSQLCalcFoundRowsNonPreparedCache`, `TestSQLCalcFoundRowsProtocolPrepare`, `TestSQLCalcFoundRowsCTEPlanCache`; parameters/data/schema changes and eligibility comparisons |
| Cache isolation and prerequisite fix | [TestSQLCalcFoundRowsConcurrentInstanceCache](../../pkg/planner/core/casetest/plancache/plan_cache_suite_test.go): ordinary and inline-CTE shared plans, two sessions with changing parameters; [TestPreparedPlanCacheWithCTE/materialized-parameter-rebuild](../../pkg/planner/core/casetest/plancache/plan_cacheable_checker_test.go) |
| Generic statement state | [TestInfoBuiltin](../../pkg/expression/integration_test/integration_test.go): SHOW/EXPLAIN/description and prepared forms; [TestResetStmtCtx](../../pkg/sessionctx/stmtctx/stmtctx_test.go): final total, retry and reuse; [session-state tests](../../pkg/sessionctx/sessionstates/session_states_test.go): roundtrip state |
| SQL integration | [executor/calc_found_rows.test](../../tests/integrationtest/t/executor/calc_found_rows.test): **38 SQL cases**, including WordPress-style queries, cache rebinding, placement errors and EXPLAIN counts; [expression/noop_functions.test](../../tests/integrationtest/t/expression/noop_functions.test): **64 cases**, including native totals under all three modes and unchanged other noop behavior |
| Plan infrastructure | [TestPlanClone](../../pkg/planner/core/generator/plan_cache/plan_clone_test.go) and [TestPlanIDChanged/TestReverse](../../pkg/util/plancodec/id_test.go): generated cloning and stable codec IDs, including 65/66/67 |
| Real TiKV | [TestSQLCalcFoundRowsPushdown/TestSQLCalcFoundRowsCTEPushdown](../../tests/realtikvtest/pushdowntest/calc_found_rows_test.go): index lookup, grouped results, storage-backed materialized CTE/inner-limit results and prepared rebinding/cache hits |
| Shared CTE pruning | [TestCTEColumnPruning](../../pkg/planner/core/tests/cte/cte_test.go): ordinary SELECT/COUNT, multiple/nested/shared consumers, pushed predicates, GROUP BY/DISTINCT/inner LIMIT, recursive fallback, side effects and prepared rebinding.  `TestSQLCalcFoundRowsCTE/count-column-pruning`: source scan width, nullable placeholders, preserved page payloads, prepared parameters, semantic fallbacks and removal of temporary demand state before caching |

Ubuntu Ready validation passed the native feature/InfoBuiltin, ordinary CTE pruning, CTE execution/spill, plan-cache, shared-CTE MPP planning and plan-clone tests, server build and `make lint`. Five SQL integration suites passed **523 cases**: native feature 38, noop settings 64, CTE 236, CTE EXPLAIN 52 and CTE execution 133. Golden changes retain grouping keys and required predicates while replacing unused output slots with NULL.

Local acceptance on real TiKV and TiFlash passed **54 result/cache assertions** (27 per engine), covering ordinary SELECT/COUNT, nested shared payloads, native semantics and prepared rebinding. A TiFlash comparison on 100,000 rows with an 800-byte payload, four warmup rounds and 20 randomized rounds measured page-plus-total median times of **259.50 ms native**, **256.32 ms explicit COUNT with the wide CTE SQL** and **259.36 ms explicit COUNT with a manually narrowed CTE**. Both explicit COUNT forms now use general CTE pruning.

A fresh WordPress 7.1.4/PHP 8.3.36 installation on TiKV passed **48 core assertions and 20 browser checks**, including publication, frontend content, homepage/search pagination and admin pagination, without an active compatibility plugin. This local application/storage acceptance supplements the maintained tests above.

### Targeted reproduction

Run from repository root with the repository toolchain. Follow [testing-flow.md](../agents/testing-flow.md) and root AGENTS.md for conditional Bazel preparation, failpoints and isolated real-storage cluster setup. The helper below enables and disables failpoints; `-tags=intest,deadlock` alone does not.

```sh
export GOMAXPROCS=1
export GOFLAGS=-p=1
./tools/check/failpoint-go-test.sh pkg/expression/integration_test -run '^(TestSQLCalcFoundRows|TestInfoBuiltin)' -count=1
./tools/check/failpoint-go-test.sh pkg/planner/core -run '^TestValidator$' -count=1
./tools/check/failpoint-go-test.sh pkg/planner/core/tests/cte -run '^TestCTE' -count=1
./tools/check/failpoint-go-test.sh pkg/sessionctx/stmtctx -run '^TestResetStmtCtx$' -count=1
make server
# Use the integration runner's unistore backend; mysql_tester must be installed.
(cd tests/integrationtest && TIDB_TEST_STORE_NAME=unistore TIKV_PATH= NEXT_GEN= ./run-tests.sh -b n -s ../../bin/tidb-server -t executor/calc_found_rows)
(cd tests/integrationtest && TIDB_TEST_STORE_NAME=unistore TIKV_PATH= NEXT_GEN= ./run-tests.sh -b n -s ../../bin/tidb-server -t expression/noop_functions)
(cd tests/integrationtest && TIDB_TEST_STORE_NAME=unistore TIKV_PATH= NEXT_GEN= ./run-tests.sh -b n -s ../../bin/tidb-server -t cte)
(cd tests/integrationtest && TIDB_TEST_STORE_NAME=unistore TIKV_PATH= NEXT_GEN= ./run-tests.sh -b n -s ../../bin/tidb-server -t explain_cte)
(cd tests/integrationtest && TIDB_TEST_STORE_NAME=unistore TIKV_PATH= NEXT_GEN= ./run-tests.sh -b n -s ../../bin/tidb-server -t executor/cte)
make lint
```

Use the runner's `-r <case>` only when intentionally recording changed fixtures; inspect the result diff and independently rerun with `-t`. The commands above are the scoped semantic checks, not a substitute for the separately listed cache, codec/clone or real-storage coverage when those subsystems change.

## Remaining limits and cost

- Data and count may scan twice and repeat planning/execution work. Data-first execution helps first-row latency, but receiving the final total still waits for count. Materialized results are not shared across the two branches.
- Shared CTE column pruning can avoid unused wide reads in eligible plans; it does not make execution single-pass. Local TiFlash width-control measurements are directional evidence, not universal latency parity. Compare equivalent page/count work on one host and distinguish cumulative allocation from peak memory.
- Non-repeatable-expression checks are deliberately explicit. Arbitrary functions, plugins and all SQL combinations are not certified.
- Recursive CTE results, recursion errors and cancellation are covered; **forced recursive-CTE spill is not**.
- Generic EXPLAIN ANALYZE counting without SQL_CALC_FOUND_ROWS, EXPLAIN FOR CONNECTION count state, and Plan Replayer file/string-list forms are not aligned or certified by this correction.
- MySQL-compatible write/view semantics, arbitrary intervening statements and other MySQL versions/configurations remain outside the implemented contract.
- NextGen, production-scale load, production capacity and arbitrary WordPress plugins were not validated. Collation and full-text-search migration incompatibilities are separate work.
