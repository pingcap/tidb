# Ranked test-coverage gaps, and how much to trust the inventory

Companion to the machine-generated
[`test-coverage-inventory.md`](test-coverage-inventory.md). That file counts;
this file judges. Regenerate the counts with
`python3 rust/scripts/test-coverage-inventory.py` from the repository root;
this file is hand-written and must be re-read, not re-run.

## Why this exists

`AGENTS.md` non-negotiable 6 says a package claim includes every original test
artifact. Coverage has repeatedly been *assumed* rather than measured here, and
the assumption has been wrong in expensive ways:

- deleting the cluster statement-rollback savepoint left 96 tests green,
  because every one of them failed during planning and staged nothing -- a
  property with zero coverage that looked covered;
- the cop-`LIMIT` silent-wrong fix shipped into a file with no test module;
- 728 lines of cost model landed with one assertion observing it.

The point of the inventory is that "we ported that package" is now a checkable
claim instead of a memory.

## Ranked gaps

Ranked by blast radius -- what goes silently wrong if the behavior is wrong --
not by how many tests are missing. Every entry is a Go test in the `NONE`
bucket: no Rust test carries its name, its words, or a citation of it.

| # | Go test | Unguarded behavior |
| --- | --- | --- |
| 1 | `pkg/types/convert_test.go:44` `TestConvertType` | The whole implicit-coercion matrix. Every comparison, insert, and index lookup goes through it; a wrong edge silently changes which rows match. |
| 2 | `pkg/types/datum_test.go:124` `TestToInt64` | Datum -> int64 rounding and overflow, including the unsigned boundary. Wrong here means wrong stored values, not an error. |
| 3 | `pkg/types/convert_test.go:843` `TestGetValidInt`, `:921` `TestGetValidFloat` | Prefix-parsing of numeric strings and the truncation warning that MySQL clients observe. Governs `'12abc' + 0` and strict-mode admission. |
| 4 | `pkg/util/ranger/ranger_test.go:314` `TestIndexRangeForUnsignedAndOverflow` | Index range construction across the signed/unsigned boundary. A wrong range does not error -- it returns fewer rows. |
| 5 | `pkg/util/ranger/ranger_test.go:1037` `TestPrefixIndexRangeScan` | Prefix-index ranges. This exact area already produced a live bug in this tree (prefix index vs SQL mode). |
| 6 | `pkg/util/chunk/column_test.go:432` `TestReconstructFixedLen`, `:488` `TestReconstructVarLen` | The columnar buffer every executor writes and every expression reads. Silent corruption, not a crash. |
| 7 | `pkg/util/chunk/chunk_util_test.go:57` `TestCopySelectedJoinRows` | Join output materialization with a selection vector. Wrong = wrong join result set. |
| 8 | `pkg/expression/builtin_cast_test.go:292` `TestCastFuncSig` | Every cast signature's result and flag propagation. Feeds comparison, index selection, and pushdown. |
| 9 | `pkg/expression/collation_test.go:387` `TestDeriveCollation` | Collation derivation for binary operators. Decides string comparison and ordering; already a live-bug source here (order-by collation, cluster collation). |
| 10 | `pkg/expression/expr_to_pb_test.go:1547` `TestExprPushDownToTiKV`, `:668` `TestExprPushDownToFlash` | Which expressions are pushed to the coprocessor. Push down something the store evaluates differently and rows vanish with no error -- the exact shape of the cop-`LIMIT` bug. |
| 11 | `pkg/expression/builtin_vectorized_test.go:836` `TestVecEvalBool` | Filter truthiness in the vectorized path, including NULL handling. Governs every `WHERE`. |
| 12 | `pkg/types/json_binary_test.go:294` `TestCompareBinary` | Binary-JSON ordering across type precedence. Drives `ORDER BY`, index ordering, and comparison of JSON columns. |
| 13 | `pkg/types/time_test.go:1918` `TestTimeOverflow`, `:1205` `TestCheckTimestamp` | Temporal range admission. Decides whether a value is stored, zeroed, or rejected. |
| 14 | `pkg/server/conn_stmt_params_test.go:319` `TestParseExecArgsAndEncode` | Binary-protocol parameter decoding for prepared statements. A wrong type tag misreads the client's value on the wire. |
| 15 | `pkg/server/conn_test.go:434` `TestParseHandshakeResponse`, `:427` `TestMalformHandshakeHeader` | Handshake parsing, including malformed input. Connection-level, and a parsing bug here is reachable pre-auth. |
| 16 | `pkg/executor/insert_test.go:534` `TestInsertLockUnchangedKeys`, `pkg/executor/delete_test.go:27` `TestDeleteLockKey` | Which keys a DML pessimistically locks. Under-locking is a lost update that no test observes at the SQL layer. |
| 17 | `pkg/executor/executor_failpoint_test.go:126` `TestPointGetRepeatableRead` | Point-get under repeatable read. Isolation violations are invisible to single-session tests. |
| 18 | `pkg/meta/meta_test.go:241` `TestMeta`, `:662`-`:753` the key-format tests | Meta key encoding for schemas, tables, auto-IDs, sequences. A wrong key is data written where nothing will look for it. |
| 19 | `pkg/statistics/histogram_test.go:79` `TestMergePartitionLevelHist`, `:493` `TestMergeBucketNDV` | Histogram merging. Wrong estimates pick wrong plans -- slow, not incorrect, but invisible until production. |
| 20 | `pkg/types/vector_test.go:24` `TestVectorEndianess`, `:149` `TestVectorSerialize` | Vector column wire bytes. A byte-order mistake is a cross-language corruption that only a differential test catches. |

### Closed by porting

| # | Ported to | What the port found |
| --- | --- | --- |
| 10 | `tidb-executor/src/scan_pushdown.rs` `tests_push_down_verdict` | Nothing wrong: all 9 expressions Go refuses are refused here too. All 55 Go pushes are refused (this engine lowers only a column-vs-constant comparison), which is a performance gap, tracked in an `#[ignore]`d test carrying Go's verdict. `TestExprPushDownToFlash` (`:668`) is still open. |
| 11 | `tidb-session/src/tests_eval_bool.rs` | FOUR live bugs, one root cause (four divergent truth tests, none of them `Datum.ToBool`): `WHERE varchar_col` and `WHERE json_col` returned NO ROWS where TiDB returns rows (silent row loss), `IF(varchar_col,…)` always took the false branch, and `varchar_col IS TRUE` was true for every non-NULL row. |
| 14 | `tidb-protocol/tests/binary_params_source.rs` | `parse_binary_params`'s charset decode was an identity stub, so a gbk client's parameter was stored as if its bytes were UTF-8. Fixed. The *live* `COM_STMT_EXECUTE` decoder still has no charset seam -- `#[ignore]`d test with TiDB's answer. |
| 16 | `tidb-session/src/tests_dml_lock_keys.rs` | ONE live bug: `REPLACE` over a row IDENTICAL to the one being written deleted and re-inserted it and reported 2 affected, where Go's `InsertValues.removeRow` leaves it in place and reports 1 -- the very site `tidb_lock_unchanged_keys` governs. Fixed. The DML key sets themselves were right: a `DELETE` does take every index key. No DML lock path exists at all (`tidb_lock_unchanged_keys` is registered and unread, nothing calls `Transaction::lock_keys`), so the blocking halves are `#[ignore]`d with Go's answer, each paired with a RUNNING guard on today's behavior. |
| 18 | `tidb-meta/tests/key_prefix_and_element_source.rs` | Already covered under another name, and better: `tidb-meta/tests/go_vectors.rs` pins every meta key byte-for-byte against hex captured from Go. The genuine hole was the `Is*Key`/`Parse*Key` round trip for the auto-ID, auto-increment, auto-random and sequence prefixes (no `parse_*` existed), and `meta.Element` -- the DDL reorg backfill element, an on-disk contract -- which had no port at all. Both landed; nothing was found wrong in what already existed. `TestMeta` (`:241`), which drives a live `Mutator` over a store, is still open. |

### Closed from the `pkg/planner/core` pool

Not ranked above -- the ranked table predates the decision to work the 97%
uncovered `pkg/planner/core` list directly.

| Go tests | Ported to | What the port found |
| --- | --- | --- |
| `logical_plans_test.go:202` `TestSimplifyOuterJoin` (8 rows), `:153` `TestOuterWherePredicatePushDown` (3), `:112` `TestJoinPredicatePushDown` (14), `:272` `TestDeriveNotNullConds` (13) | `tidb-session/src/tests_join_predicate_placement.rs` | All 38 rows ported, none dropped. **No row-level divergence:** every result set matches a `gorun` capture of real TiDB cell for cell, so no predicate is placed illegally. **Five plan-shape gaps, all row-correct and cost-wrong,** each `#[ignore]`d with Go's answer beside a running guard on today's behavior: no outer-to-inner join conversion at all (`tidb_planner::outer_to_inner_join` is the rule wrapper over a plan adapter nothing implements -- 4 of the 8 `TestSimplifyOuterJoin` rows), no predicate reaching either scan (19 of the 30 `Left`/`Right` expectations), `<=>` not used as an equal join key, a `WHERE` equality not promoted into the join condition, and `NOT EXISTS` planned as a correlated `Selection` instead of an anti semi join. Separately, the port exposed a TEST-INFRASTRUCTURE bug: `tests_support::row_text` rendered every temporal column as the text `NULL` (it delegated to the *system variable* text function, whose fallback arm covers `Datum::Time`), so any assertion over a `DATE`/`DATETIME`/`TIMESTAMP`/`TIME` column would have pinned a wrong answer. Fixed to use the wire renderer, with a guard. |

Runners-up worth naming: `pkg/meta/model/job_args_test.go` (40 uncovered DDL
argument round-trips -- every one is a job that would deserialize wrong), and
`pkg/session/bootstrap_test.go` (37 uncovered upgrade-path tests; each pins one
historical cluster state that an upgrade must survive).

## Matching-confidence accounting

Read this before quoting any number from the inventory.

**The Go side is strong.** It comes from
`rust/difftests/tools/go_test_declaration_inventory`, which parses every
`*_test.go` with `go/parser` and resolves testify suite methods to their
running parents. Comments and string literals cannot manufacture a test. What
it does not do: count table-driven subtests. A Go test with 200 cases counts
once, so a Rust test that ports one case of it matches at full credit. **The
inventory therefore overstates coverage wherever Go used a table.** That is the
single largest systematic bias in the numbers, and it points the wrong way.

**The Rust side is weak, and name matching is weaker.** Four tiers:

- `NAME-EXACT` / `NAME-FUZZY` -- name evidence only. Nobody checked that the
  Rust test asserts what the Go test asserts. Treat as "worth a human look",
  never as parity.
- `REFERENCED` -- the Go test name appears in Rust text but no Rust test
  carries it. In this tree that is usually a citation in a module doc
  explaining what was *not* ported. Closer to negative evidence.
- `NONE` -- the trustworthy column, in one direction: no name, no words, no
  citation.

Per-package confidence, honestly:

| Package | Confidence in its row | Basis |
| --- | --- | --- |
| `pkg/util/codec` | **HIGH (verified)** | Read `rust/crates/tidb-codec/tests/codec_package_source.rs`. Its module doc claims "exact named obligations from every `pkg/util/codec/*_test.go`", and spot-checking `test_decimal_codec` against `pkg/util/codec/decimal_test.go` showed the same vectors and the same injected-failure case. The 31/31 is real. |
| `pkg/store/driver/txn` | **MEDIUM (verified one)** | `TestUnionIterErrors` first scored `NONE`; reading `rust/crates/tidb-txnkv/tests/union_iter_source.rs` found `test_union_iter_source_error_identity_order_and_close` covering the same injected-error cases. That false negative is what the token-matching tier was added for. |
| `pkg/util/*` leaves, `pkg/util/hack`, `pkg/meta/metadef` | **MEDIUM** | Near-100% `NAME-EXACT` with 1:1 module mapping. Small packages, little room for a same-named test to assert something else. Not individually read. |
| `pkg/util/rowcodec`, `pkg/tablecodec`, `pkg/kv`, `pkg/parser/mysql` | **UNKNOWN, reads as complete** | 100% `NAME-EXACT` but the matches come largely from crate-level `tests/` directories that this inventory attributes generously. Nobody compared assertions. A 0% uncovered row here means "no missing names", not "no missing behavior". |
| `pkg/parser`, `pkg/types` | **UNKNOWN** | High exact-match rates, but these are the two packages where Go leans hardest on giant tables. The real per-case coverage is certainly lower than 83% / 61%. |
| `pkg/expression`, `pkg/executor`, `pkg/planner/core`, `pkg/session`, `pkg/util/chunk`, `pkg/server` | **HIGH confidence in the gap** | 69-97% uncovered with most Go test *files* never cited. These packages are not close to test-complete, and no amount of matching nuance changes that. |
| `pkg/util/chunk` specifically | **not a claim at all** | `rust/crates/tidb-chunk/src/lib.rs` self-declares "SEED SCOPE" and lists deferrals. It should not appear in any completed-package accounting until it stops saying that. Same for `tidb-session`. |
| everything else | **UNKNOWN** | Name evidence only. |

**Not reached.** `pkg/ddl`, `pkg/infoschema`, `pkg/domain`, `pkg/privilege`,
`pkg/table`, `pkg/store/copr`, `pkg/store/gcworker`, `pkg/util/*` packages with
no transcreate commit, and everything under `br/`, `dumpling/`,
`lightning/`. The mapping table in the script is the list of what *is* covered;
anything absent from it was not measured, not measured-and-clean. Also not
reached: `tests/integrationtest` and `tests/realtikvtest`, which are SQL-level
surfaces this inventory has no way to attribute to a Rust test.

## Divergence-pinning: what the sweep found

The task expected Rust tests asserting behavior Go does not have, presented as
correct. The sweep found the opposite pattern dominating: divergences are
loudly labeled. `rust/crates/tidb-session/src/` carries ~40 `DIVERGENCE` /
`DOCUMENTED DIVERGENCE` comments, and the strongest examples weaken their own
assertions rather than pin a wrong answer -- for example
`tests_harvested_relation_engine.rs:645` names Go's `Can't group on 'count(*)'`
versus this tier's restored `COUNT(1)`, and then asserts only the error code
and the message prefix. That is the right shape.

One class still deserves watching, and it is not a bug today: assertions of the
form `Err(DriverError::Unsupported(_))` for behavior Go implements
(`tests_core.rs:2819` rollup over a non-column grouping expression,
`:2958` `GROUPING(a) + 1`, `:3051` `INTO OUTFILE`). Each is honestly commented
"Deferred", but each is a test that **must be deleted or inverted when the
feature lands**. If one is forgotten, it becomes a test defending a refusal --
exactly the NTILE shape. They are cheap to find (`grep -rn "DriverError::Unsupported"
rust/crates/*/src/tests_*.rs`) and should be swept whenever a deferral closes.

No Rust test was found asserting a Go-contradicting *value* as if it were
correct.

## What to do with this

Close gaps package by package, highest risk first, and re-run the script. The
number to drive down is the `NONE` column on risk-3 rows. Do not celebrate a
`NAME-EXACT` rate; it is not evidence.
