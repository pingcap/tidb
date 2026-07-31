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
not by how many tests are missing.

**Re-verified against the tree, and nine of the twenty rows were stale.** This
table was written from a `NONE` list, and `NONE` has been wrong in three
separate ways since (see the audit section below). Every row below now says
whether a Rust test actually carries it, checked by reading, not by matching.
The stale rows all failed the same way: the port exists under a name that
shares no distinctive word with the Go test. Re-verify before working a row.

| # | Go test | Unguarded behavior |
| --- | --- | --- |
| 1 | ~~`pkg/types/convert_test.go:44` `TestConvertType`~~ | **CLOSED, and this row was stale.** `tidb-datatype/src/datum_convert.rs` `go_tests::go_test_convert_type` carries it (one row `#[ignore]`d: an out-of-range ENUM ordinal returns `Err` without Go's best-effort empty ENUM). |
| 2 | ~~`pkg/types/datum_test.go:124` `TestToInt64`~~ | **CLOSED, and this row was stale.** `datum_convert.rs` `go_tests::go_test_to_int64`. |
| 3 | ~~`pkg/types/convert_test.go:843` `TestGetValidInt`, `:921` `TestGetValidFloat`~~ | **CLOSED.** `TestGetValidFloat` was already ported as `convert.rs` `source_valid_float_prefix_rows` + `source_float_string_to_integer_rows` (all 23 + 17 rows) and this row never noticed. `TestGetValidInt` and `TestRoundIntStr` are ported below. |
| 4 | ~~`pkg/util/ranger/ranger_test.go:314` `TestIndexRangeForUnsignedAndOverflow`~~ | **CLOSED, and this row was stale.** All 19 rows are in `tidb-executor/src/index_range.rs` `unsigned_and_overflow_ranges_match_go`, `#[ignore]`d with Go's answers beside a running guard -- 12 rows still need `handleUnsignedCol` clamping and `RefineCompareArgs`. That remains real work; it is just not *unmeasured* work. |
| 5 | `pkg/util/ranger/ranger_test.go:1037` `TestPrefixIndexRangeScan` | **OPEN, verified.** Prefix-index ranges. Do not confuse it with `TestPrefixIndexRange` (`:2342`), which IS ported (`prefix_index_ranges_match_go`, `#[ignore]`d) -- the near-miss queue pairs the two, and reading them is how you tell. This exact area already produced a live bug here (prefix index vs SQL mode). |
| 6 | ~~`pkg/util/chunk/column_test.go:432` `TestReconstructFixedLen`, `:488` `TestReconstructVarLen`~~ | **CLOSED, and this row was stale.** `tidb-chunk/src/column.rs` `reconstruct_fixed_len` / `reconstruct_var_len`, both citing the Go line and both driving the same 8 seeds x 1024 rows. |
| 7 | ~~`pkg/util/chunk/chunk_util_test.go:57` `TestCopySelectedJoinRows`~~ | **CLOSED, and this row was stale.** `tidb-chunk/src/chunk_util.rs` `copy_selected_join_rows_matches_row_by_row_append`, which is the Go assertion exactly: batch copy must equal row-by-row append. |
| 8 | `pkg/expression/builtin_cast_test.go:292` `TestCastFuncSig` | **OPEN, verified.** Every cast signature's result and flag propagation. Feeds comparison, index selection, and pushdown. |
| 9 | ~~`pkg/expression/collation_test.go:387` `TestDeriveCollation`~~ | **CLOSED, and this row was stale.** `tidb-expr/src/collation_derive_go_tests.rs` is a row-for-row translation of the whole table, helper constructors included. |
| 10 | `pkg/expression/expr_to_pb_test.go:1547` `TestExprPushDownToTiKV`, `:668` `TestExprPushDownToFlash` | Which expressions are pushed to the coprocessor. Push down something the store evaluates differently and rows vanish with no error -- the exact shape of the cop-`LIMIT` bug. |
| 11 | ~~`pkg/expression/builtin_vectorized_test.go:836` `TestVecEvalBool`~~ | **CLOSED** -- see the porting table below; it found four live bugs. Filter truthiness in the vectorized path, including NULL handling. Governs every `WHERE`. |
| 12 | `pkg/types/json_binary_test.go:294` `TestCompareBinary` | **OPEN, verified.** Binary-JSON ordering across type precedence. Drives `ORDER BY`, index ordering, and comparison of JSON columns. |
| 13 | `pkg/types/time_test.go:1918` `TestTimeOverflow`, `:1205` `TestCheckTimestamp` | **OPEN, verified.** Temporal range admission. Decides whether a value is stored, zeroed, or rejected. |
| 14 | ~~`pkg/server/conn_stmt_params_test.go:319` `TestParseExecArgsAndEncode`~~ | **CLOSED** -- see the porting table below; it found a live charset bug. Binary-protocol parameter decoding for prepared statements. A wrong type tag misreads the client's value on the wire. |
| 15 | `pkg/server/conn_test.go:434` `TestParseHandshakeResponse`, `:427` `TestMalformHandshakeHeader` | **OPEN, verified.** Handshake parsing, including malformed input. Connection-level, and a parsing bug here is reachable pre-auth. |
| 16 | ~~`pkg/executor/insert_test.go:534` `TestInsertLockUnchangedKeys`, `pkg/executor/delete_test.go:27` `TestDeleteLockKey`~~ | **CLOSED** -- see the porting table below; it found a live `REPLACE` bug. Which keys a DML pessimistically locks. Under-locking is a lost update that no test observes at the SQL layer. |
| 17 | `pkg/executor/executor_failpoint_test.go:126` `TestPointGetRepeatableRead` | **OPEN, verified.** Point-get under repeatable read. Isolation violations are invisible to single-session tests. |
| 18 | `pkg/meta/meta_test.go:241` `TestMeta` (the key-format tests are closed; see below) | **PARTLY OPEN, verified.** Meta key encoding for schemas, tables, auto-IDs, sequences. A wrong key is data written where nothing will look for it. |
| 19 | `pkg/statistics/histogram_test.go:79` `TestMergePartitionLevelHist`, `:493` `TestMergeBucketNDV` | **OPEN, verified.** Histogram merging. Wrong estimates pick wrong plans -- slow, not incorrect, but invisible until production. |
| 20 | `pkg/types/vector_test.go:24` `TestVectorEndianess`, `:149` `TestVectorSerialize` | **OPEN, verified.** Vector column wire bytes. A byte-order mistake is a cross-language corruption that only a differential test catches. |

### Closed by porting

| # | Ported to | What the port found |
| --- | --- | --- |
| 10 | `tidb-executor/src/scan_pushdown.rs` `tests_push_down_verdict` | Nothing wrong: all 9 expressions Go refuses are refused here too. All 55 Go pushes are refused (this engine lowers only a column-vs-constant comparison), which is a performance gap, tracked in an `#[ignore]`d test carrying Go's verdict. `TestExprPushDownToFlash` (`:668`) is still open. |
| 11 | `tidb-session/src/tests_eval_bool.rs` | FOUR live bugs, one root cause (four divergent truth tests, none of them `Datum.ToBool`): `WHERE varchar_col` and `WHERE json_col` returned NO ROWS where TiDB returns rows (silent row loss), `IF(varchar_col,…)` always took the false branch, and `varchar_col IS TRUE` was true for every non-NULL row. |
| 14 | `tidb-protocol/tests/binary_params_source.rs` | `parse_binary_params`'s charset decode was an identity stub, so a gbk client's parameter was stored as if its bytes were UTF-8. Fixed. The *live* `COM_STMT_EXECUTE` decoder still has no charset seam -- `#[ignore]`d test with TiDB's answer. |
| 16 | `tidb-session/src/tests_dml_lock_keys.rs` | ONE live bug: `REPLACE` over a row IDENTICAL to the one being written deleted and re-inserted it and reported 2 affected, where Go's `InsertValues.removeRow` leaves it in place and reports 1 -- the very site `tidb_lock_unchanged_keys` governs. Fixed. The DML key sets themselves were right: a `DELETE` does take every index key. No DML lock path exists at all (`tidb_lock_unchanged_keys` is registered and unread, nothing calls `Transaction::lock_keys`), so the blocking halves are `#[ignore]`d with Go's answer, each paired with a RUNNING guard on today's behavior. |
| 18 | `tidb-meta/tests/key_prefix_and_element_source.rs` | Already covered under another name, and better: `tidb-meta/tests/go_vectors.rs` pins every meta key byte-for-byte against hex captured from Go. The genuine hole was the `Is*Key`/`Parse*Key` round trip for the auto-ID, auto-increment, auto-random and sequence prefixes (no `parse_*` existed), and `meta.Element` -- the DDL reorg backfill element, an on-disk contract -- which had no port at all. Both landed; nothing was found wrong in what already existed. `TestMeta` (`:241`), which drives a live `Mutator` over a store, is still open. |

### Third measurement audit: three false-gap classes, 87 phantom `NONE`s

The reference scan was wrong twice before (a renamed `pkg/meta` port; `.txt`
corpus headers unread). A third audit found three more classes. Totals moved
from **1391 `NONE` (53%) to 1304 (49%)** with one test written.

| Class | Evidence | Effect |
| --- | --- | --- |
| **Wrong crate in the mapping.** `pkg/parser/ast` mapped only to `tidb-ast/src`, which holds the node structs. Every ported `ast/*_test.go` restore/visitor test lives in `tidb-parser/src/tests/` beside the grammar that builds the node. | **73 of 73** `NONE`s in that package matched a Rust test *by exact name* elsewhere in the tree. Not one was a coincidence. | `pkg/parser/ast` 126 tests: 73 `NONE` (90% uncovered) -> **0 `NONE` (3%)**. The single largest wrong number in the inventory. |
| **SQL-level ports of executor/planner tests live in `tidb-session`.** `tests_dml_lock_keys.rs` (<- `pkg/executor/{insert,delete}_test.go`) and `tests_join_predicate_placement.rs` / `tests_column_prune.rs` (<- `pkg/planner/core/logical_plans_test.go`) were outside both packages' search paths. | The gaps table above already credits these ports; the inventory could only see them as `REFERENCED`. | `pkg/executor` 364 -> 358 `NONE`, `pkg/planner/core` 179 -> 175. Small, and it confirms the two packages really are ~92% uncovered. |
| **Extension filter, again.** The scan read `.rs`/`.md`/`.txt` and skipped `.py`/`.tsv`, so `crates/tidb-datatype/scripts/generate_collation_data.py` and `difftests/corpus/coverage/*.tsv` -- which name the Go tests they carry -- counted for nothing. | ~10 names, all in packages outside the mapping. | No total moved. Fixed anyway: an extension is not a judgement about provenance. |

A fourth class was found, **measured, and deliberately not fixed**: a port
renamed to drop one Go word. `TestGetValidFloat` sat in `NONE` -- and was
ranked #3 above as an unguarded behavior -- while
`convert.rs::source_valid_float_prefix_rows` carried all 23 of its rows. The
only difference was the Go verb `Get`. Relaxing the token rule to "all Go
words but one" closes it, and 354 others; reading them showed most are
coincidences (`TestMakeRefTo` "matching"
`a_refresh_makes_a_stale_cache_usable_again`). Manufacturing parity is worse
than overstating the gap, so the rule was left alone and the candidates are
printed in the inventory's **near-miss review queue**, which is explicitly not
counted. Read one before porting it. Ranked rows 1 and 2 above were stale for
the same reason -- ports named `go_test_convert_type` / `go_test_to_int64`.

`rust/docs/` is still deliberately excluded from the reference scan: the script
writes the uncovered list there, so scanning it would let every `NONE` certify
itself as `REFERENCED` on the next run.

### Closed from `pkg/types` numeric-prefix parsing

| Go tests | Ported to | What the port found |
| --- | --- | --- |
| `convert_test.go:843` `TestGetValidInt` (both tables, 15 + 14 rows), `:828` `TestRoundIntStr` (3 rows) | `tidb-datatype/src/convert.rs` `source_valid_integer_prefix_{warning,strict}_rows`, `source_round_integer_string_rows` | Every value matches Go. **One API-shape bug, latent:** `valid_integer_prefix` fused the truncation into an error that also short-circuited `floatStrToIntStr`, so the warning-mode answers were unreachable -- `"123..34"` can only ever be `"123."`, never Go's `"123"`. The two Go tables differ in exactly that way, which is why the source `Context` truncation policy is now a parameter rather than something the caller applies afterwards. No caller exists yet, so nothing was live-wrong. Recorded not fixed: Go raises `ErrTruncatedWrongVal("INTEGER", str)` where this tier reports `InvalidUnsignedInteger`; with no caller the error identity reaches nothing, so only the values are pinned. |

### Closed from the `pkg/expression` string and arithmetic pool

Not ranked above. `pkg/expression` was 68% uncovered with the largest
evaluable surface left, and its most valuable tables are plain value tables
that the differential corpus can carry directly.

| Go tests | Ported to | What the port found |
| --- | --- | --- |
| `builtin_string_test.go` `TestLeft`, `TestRight`, `TestLower`, `TestUpper`, `TestStrcmp`, `TestReplace`, `TestSubstring`, `TestSubstringIndex`, `TestLocate`, `TestInstr`, `TestFindInSet`, `TestInsert`, `TestOrd`, `TestBin`, `TestExportSet`, `TestToBase64`, `TestFromBase64`, `TestFormat` | `corpus/expr/{left_right,case_conversion,strcmp_replace,substring,locate_instr,find_in_set_insert,ord_bin_export_set,base64,format}_source` | 271 rows, every one in-domain (no `SKIP`, no `ERR`), so nothing was quietly unasserted. **One live bug:** `INSERT(str, pos, len, newstr)` read a NEGATIVE `len` as zero and spliced `newstr` in without removing anything, where `builtinInsertUTF8Sig` clamps a negative `len` to the rest of the string exactly as it clamps an oversized one (`length > runeLength-pos+1 \|\| length < 0`). Fixed. One row does not reproduce its Go unit-test expectation and the corpus records the production answer instead: `TestBin` says `BIN('')` is NULL, but the real expression path answers `'0'` — the unit test builds the signature by hand and skips the implicit cast. |
| `builtin_arithmetic_test.go` `TestArithmeticPlus`, `TestArithmeticMinus`, `TestArithmeticMultiply`, `TestArithmeticDivide`, `TestArithmeticIntDivide`, `TestArithmeticMod` | `corpus/expr/int_arithmetic_source` | The existing `decimal_*`/`float_*` topics already covered those domains; the integer domain and the four signed/unsigned `MOD`/`DIV` signatures had no executable guard. **One live bug:** an integer `DIV` with a FLOAT operand dropped the unsigned flag, so `1u DIV -2` answered a signed 0 where Go answers an unsigned 0, and `1u DIV -1` answered `-1` where Go raises `BIGINT UNSIGNED value is out of range` (`ConvertDecimalToUint` rejects a negative quotient rather than wrapping it). Fixed. Five rows are held out as unevaluable today — hex/bit-literal arithmetic, string-operand arithmetic, `CAST(... AS TIME)` — each named in the topic header with Go's captured answer and pinned by a running guard in `difftests/result-tests/tests/expr_corpus_holdouts.rs`. |

The same pass fixed a **systematic blind spot in the inventory script**: its
reference scan read only `*.rs` and `*.md`, so `corpus/<ns>/<topic>.txt` — where
essentially all of this tree's `pkg/expression` provenance lives, each header
naming the Go test its rows come from — counted for nothing. `.txt` is now
scanned (excluding the machine-written `.golden.txt` dumps), and
`pkg/expression`'s search paths include the corpus and the result-test
directory. That alone moved 9 `pkg/expression` tests, plus a handful in
`pkg/types`, `pkg/executor` and `pkg/planner/core`, out of the `NONE` column
without anyone writing a test. Read older `NONE` counts with that in mind.

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

**Not reached.** `pkg/domain`, `pkg/store/gcworker`, `pkg/util/*` packages with
no transcreate commit, and everything under `br/`, `dumpling/`,
`lightning/`. (`pkg/ddl`, `pkg/infoschema`, `pkg/privilege/privileges`,
`pkg/table` and `pkg/store/copr` used to be on this list while the tree
implemented them; see "Instrument bug 4" below.)
The mapping table in the script is the list of what *is* covered;
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

## Instrument bug 4: the denominator, not the ratio

The three instrument bugs above all moved tests between buckets. This one
removed them from the accounting entirely.

Thirteen Go packages that this tree demonstrably implements were named in no
`Mapping` row: `pkg/ddl`, `pkg/meta/autoid`, `pkg/table`, `pkg/table/tables`,
`pkg/table/temptable`, `pkg/infoschema`, `pkg/privilege/privileges`,
`pkg/executor/{join,sortexec,aggregate}`, `pkg/expression/aggregation`,
`pkg/planner/core/rule`, `pkg/store/copr`. Their 921 counted Go tests were not
reported as uncovered -- they were not reported. The "Not reached" paragraph
above named some of them as future work, which is exactly how a silent
denominator survives: the omission was documented in prose and invisible in the
number everyone quotes.

That matters more than the arithmetic, because the omitted packages own every
silent-wrong-answer bug class this project has shipped: `AUTO_INCREMENT`
(`pkg/meta/autoid`), `PARTITION BY` and the partial-index name (`pkg/ddl`),
`CREATE TEMPORARY TABLE` (`pkg/table/temptable`, `pkg/ddl`). The measurement
was blindest exactly where the port had already been caught being wrong.

Honest totals move from 1304/2622 (49%) uncovered to **2035/3543 (57%)**.
Nothing regressed to produce that; the earlier figure was measured against a
smaller world. Between the two runs ~500 new Rust tests landed in mapped paths
and closed exactly ZERO named Go gaps, which is worth knowing on its own: new
tests in this tree are overwhelmingly new behaviour, not Go-test transcreation.

## Measured negatives

Recorded so the next worker does not re-derive them. Each is a gap the
inventory reports that turns out not to be one, or not to be one at this tier.

- **`pkg/meta/autoid` reads 14/14 uncovered and is behaviourally covered.**
  `rust/crates/tidb-session/src/tests_auto_increment.rs` carries 24 SQL-level
  tests over the same rules `TestSignedAutoid`/`TestUnsignedAutoid` assert
  (monotonic allocation, rebase-up-only, the domain end reporting 1467, the
  unsigned domain above `i64::MAX`, TRUNCATE restarting the counter, a rolled
  back transaction burning its ids). None of them carries a Go test's name, so
  the row is a NAME-level false negative end to end. Do not "close" this
  package by porting names onto tests that already exist.
- **`auto_increment_increment`/`auto_increment_offset` are refused, not
  discarded.** `TestSignedAutoid`'s increment/offset half (`CalcNeededBatchSize`,
  `SeekToFirstAutoIDSigned`) does not apply: `StmtContext::auto_increment_step_is_default`
  refuses an insert into an auto-increment table when either variable is off 1,
  and `tests_auto_increment.rs:262` pins that refusal. This is the good shape --
  a value the engine cannot honour is rejected rather than accepted and ignored --
  and it means the arithmetic those Go helpers test has no counterpart to test.
- **`pkg/meta/autoid`'s `autoid_service_test.go` (3 tests) is not this tier.**
  `TestAllocCanceledRPCReturnsQuickly`, `TestRebaseCanceledRPCReturnsQuickly`
  and `TestBackoffCtxAware` test the separate autoid *service* RPC client. This
  tree keeps the counter in a meta key read by the session
  (`tidb-exec/src/cluster_auto_id.rs`); there is no service to cancel.
- **No fourth wrong-crate mapping.** Every `NONE` Go test name was re-matched
  against a `#[test]` index built over the WHOLE `rust/` tree, ignoring the
  mapping table. Four hits, all coincidences (`TestCurrentRole` and
  `TestWeightString` against parser tests named for the same SQL keyword,
  `TestLastInsertID` against a session status-value test, `TestDifferential`
  against the parser difftest harness). The `pkg/parser/ast` class of bug did
  not recur.
- **`pkg/util/tiflash` has no `*_test.go`.** It is absent from the mapping table
  and, uniquely among the transcreated packages, that costs nothing.

## What to do with this

Close gaps package by package, highest risk first, and re-run the script. The
number to drive down is the `NONE` column on risk-3 rows. Do not celebrate a
`NAME-EXACT` rate; it is not evidence.

And before believing any of it, check the mapping table against the crates that
exist. Four instrument bugs in, the pattern is unbroken: every one was in how
the Go package was mapped to Rust, and none was in the port.
