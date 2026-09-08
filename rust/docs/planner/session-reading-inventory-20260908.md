# Session prerequisite reading inventory

Authority: `f5cf8f6337612c6ae51fb6e384e4bb3469dde680`.

All tracked artifacts are enumerated below; enumeration is not reading or parity evidence. Nested Go packages are separate reading units. No session production edits are authorized by this inventory alone.

| Artifact | Blob | Lines | Bytes | Read |
| --- | --- | ---: | ---: | --- |
| `pkg/session/BUILD.bazel` | `1cbdf4d3f7fcb02d13ce4cefa0c7a08bc2bd35f5` | 246 | 7843 | complete |
| `pkg/session/OWNERS` | `2d61c83ccc01f32f4509c0de84e6fb77661753f8` | 13 | 336 | complete |
| `pkg/session/advisory_locks.go` | `8cca55aa7bef21120f8d4236add7df2c76c7e45c` | 111 | 3772 | complete |
| `pkg/session/bench_test.go` | `b3dfaef0426efc291e6f204e6cc4b1d1a2ac9cb8` | 2143 | 72138 | complete |
| `pkg/session/bootstrap.go` | `354f216e2ae1ca9cd4e29c341d9134e1ee84b7dd` | 623 | 28201 | complete |
| `pkg/session/bootstrap_test.go` | `65100197a1aa2699cae508532585a2990e8456fb` | 2011 | 70248 | complete |
| `pkg/session/contextimpl.go` | `9ddb247e23f8071a65cea34f723560e0c05614d8` | 38 | 1292 | complete |
| `pkg/session/cursor/BUILD.bazel` | `097f324a1cc4dfb62d9a030a2df21fe38abe060c` | 21 | 478 | pending |
| `pkg/session/cursor/state.go` | `9c3ad343b6e70a2999b5c565fffdeaabf80e118d` | 20 | 691 | pending |
| `pkg/session/cursor/tracker.go` | `3a8cd3cf0774ee01c03d130d3f14b7375eb7ec8e` | 91 | 1990 | pending |
| `pkg/session/cursor/tracker_test.go` | `0b89aa5704e1f5d8d8e52009fe0baa004523913d` | 115 | 2586 | pending |
| `pkg/session/global_init.go` | `0380adf47eae758b4bcd9ac4376f7adfa188d43d` | 79 | 2741 | complete |
| `pkg/session/main_test.go` | `087a938aa96965e439cddb6e6cd30ddb5bea03f3` | 84 | 3353 | complete |
| `pkg/session/metrics/BUILD.bazel` | `78cf4465a8c4b658e2a939bfce95ca29034f0dc6` | 12 | 328 | pending |
| `pkg/session/metrics/metrics.go` | `31aa904291bce69df76ccf7bf9411f187670e34f` | 146 | 9653 | pending |
| `pkg/session/mock_bootstrap.go` | `a1d907266bb5ae07d325248528f6bbc9acf3c6ce` | 221 | 8990 | complete |
| `pkg/session/nontransactional.go` | `c86f260a2a8c3bf51e788c9d229f84772c48ef81` | 873 | 30477 | complete |
| `pkg/session/session.go` | `84b35f48c3ff252b29920abf5b3c9c2ae662dbaf` | 6051 | 207075 | complete |
| `pkg/session/session_nextgen_test.go` | `987d0b1a127c059b203409164d4e73da0cbf78d0` | 180 | 5409 | complete |
| `pkg/session/session_test.go` | `614b42a2b4de6be6abb02d92d010af9f13fcf1be` | 310 | 11644 | complete |
| `pkg/session/sessionapi/BUILD.bazel` | `a6806e098295b64d92d6e6fa4a69472aed780292` | 21 | 597 | pending |
| `pkg/session/sessionapi/session.go` | `9df9b57da4dd3340b20a1530a8ba80376e77fc6a` | 90 | 4228 | pending |
| `pkg/session/sessmgr/BUILD.bazel` | `abffc7c479dfd75ca267efba18bc6b832e97f181` | 35 | 941 | pending |
| `pkg/session/sessmgr/processinfo.go` | `3642c810de6d9a9aa91a89c00868de6221342be4` | 290 | 10455 | pending |
| `pkg/session/sessmgr/processinfo_test.go` | `08d5c45cb5e89b538c967e83aa21c3c3569d49cf` | 67 | 2242 | pending |
| `pkg/session/starter_bootstrap_file.go` | `6ee8acd007ad25cf170dfa17155d259335844a2c` | 694 | 22510 | complete |
| `pkg/session/starter_bootstrap_file_test.go` | `7d6cffdefe118e1ea04c0c164f34719e57979573` | 928 | 30912 | complete |
| `pkg/session/sync_upgrade.go` | `c759bbd8d02ca476ac7a81ba5c485710ff30eb0b` | 155 | 5160 | complete |
| `pkg/session/syssession/BUILD.bazel` | `294bbde25c4060566acd62e96bc1be7d7ff24550` | 57 | 1526 | pending |
| `pkg/session/syssession/main_test.go` | `ab38e335c0c8aa6e8077d600d255a1b3f6d0c1e9` | 34 | 1269 | pending |
| `pkg/session/syssession/pool.go` | `85c7b7c664ffaa0196ac80d196dd44186d5d2b43` | 354 | 10361 | pending |
| `pkg/session/syssession/pool_test.go` | `1b9ea3fd29347e277a1e5b09da560f416072b763` | 433 | 11668 | pending |
| `pkg/session/syssession/session.go` | `b60af663ab0f4386cdb296852d5de848128b8ec7` | 603 | 20904 | pending |
| `pkg/session/syssession/session_integration_test.go` | `08cb5c0ea824168afb199dd598f5d5a044007b07` | 230 | 6693 | pending |
| `pkg/session/syssession/session_test.go` | `fa6452506372167f2adb1597aa00a2f6049c9b34` | 1356 | 38829 | pending |
| `pkg/session/syssession/session_test_util.go` | `38a871cbad9d8b36403cf6c5c3227c00b300ce1b` | 63 | 1917 | pending |
| `pkg/session/test/BUILD.bazel` | `a0bd637c1122f9f1a6a58d1e51389aedf7f8aca5` | 51 | 1557 | pending |
| `pkg/session/test/bootstraptest/BUILD.bazel` | `fb586f65661eee0d04db07f2d127790c94528faf` | 46 | 1320 | pending |
| `pkg/session/test/bootstraptest/boot_test.go` | `6084dfe408a576045a3fc46df0e2787e680ccc03` | 1061 | 39561 | pending |
| `pkg/session/test/bootstraptest/bootstrap_upgrade_test.go` | `a6cbe5f540bbe2f899c22fe1ca3ba540eaeb6493` | 1794 | 67042 | pending |
| `pkg/session/test/bootstraptest/main_test.go` | `dd86e800eb799a586641337290d31e662c7da942` | 66 | 2886 | pending |
| `pkg/session/test/bootstraptest2/BUILD.bazel` | `dc889ab54a60039800b6fb6392b698fe20b39cd2` | 27 | 693 | pending |
| `pkg/session/test/bootstraptest2/boot_test.go` | `5af7a460bd1a84cf64128f2b9b12d3b70ab126d1` | 284 | 9902 | pending |
| `pkg/session/test/bootstraptest2/main_test.go` | `e61dc30b987be6558b71964cc86071b6c0c691d2` | 66 | 2887 | pending |
| `pkg/session/test/clusteredindextest/BUILD.bazel` | `b305a017de5c7715fae19ef508a76bf3df521a15` | 25 | 624 | pending |
| `pkg/session/test/clusteredindextest/clustered_index_test.go` | `960c3f6b60ac56d8730b8cd3eaf1d19233462a1f` | 162 | 7226 | pending |
| `pkg/session/test/clusteredindextest/main_test.go` | `f8b2a5f3655bd7141115daf9e3ca6eb2f1c4daad` | 66 | 2707 | pending |
| `pkg/session/test/common/BUILD.bazel` | `c277caf12d56cc60fb08001ccc5054550f14de62` | 28 | 701 | pending |
| `pkg/session/test/common/common_test.go` | `1dd2a1589ef520db6d944ee81e7c8e3024e30b54` | 288 | 10658 | pending |
| `pkg/session/test/common/main_test.go` | `b51233b7b930f4a0d6b4dd93ae1119ff35ae8c0b` | 62 | 2511 | pending |
| `pkg/session/test/common/prepare_dedup_cache_test.go` | `89ec0c09d6c18c7504a97b7c407425415b3b9e89` | 222 | 8009 | pending |
| `pkg/session/test/main_test.go` | `111165d04a63f7dc4b0f88dc49fade40ac5ba332` | 62 | 2509 | pending |
| `pkg/session/test/meta/BUILD.bazel` | `15d59839ea19ba457dfb0444e89e0e0de3cc8afd` | 34 | 900 | pending |
| `pkg/session/test/meta/main_test.go` | `ea7f53f7452f1818458553e955d961142ae2f11d` | 62 | 2509 | pending |
| `pkg/session/test/meta/session_test.go` | `71841cce2a504f062970e8fbcca3b68f55fa10e6` | 280 | 10671 | pending |
| `pkg/session/test/nontransactionaltest/BUILD.bazel` | `92608921fec8a1971d09d44f939478b6d42cebd4` | 26 | 754 | pending |
| `pkg/session/test/nontransactionaltest/main_test.go` | `be4f11d5a13b2f9023d4ee90471695342c3a4a56` | 62 | 2525 | pending |
| `pkg/session/test/nontransactionaltest/nontransactional_test.go` | `9507e9c5aa7bdf022cdc2c1521d05f075804074e` | 526 | 21994 | pending |
| `pkg/session/test/privileges/BUILD.bazel` | `06de6c79970723deb98cfe02a09172544b5a1230` | 23 | 569 | pending |
| `pkg/session/test/privileges/main_test.go` | `53f54cf3747fb72bb951bfe24a35103d38551781` | 62 | 2515 | pending |
| `pkg/session/test/privileges/privileges_test.go` | `77d62dd8cba2f0b792d74f893e65e0f10806d8d8` | 53 | 1927 | pending |
| `pkg/session/test/resourcegrouptest/BUILD.bazel` | `bfc41da9ad232a05d05ed9acb759d5ec34dd47c1` | 13 | 326 | pending |
| `pkg/session/test/resourcegrouptest/resource_group_test.go` | `9f9752823e88a5e2fcd33412a2ef37594065fa9c` | 63 | 2825 | pending |
| `pkg/session/test/schematest/BUILD.bazel` | `9562a5bab6fd11effc70f882b45a2a71aaa72a30` | 32 | 840 | pending |
| `pkg/session/test/schematest/main_test.go` | `92c0ad64170822b288e60a9a182d804f7d94705e` | 63 | 2603 | pending |
| `pkg/session/test/schematest/schema_test.go` | `b7905fdd3b0d6b8bddb8fb2c87376cc78085b3cd` | 411 | 11891 | pending |
| `pkg/session/test/session_test.go` | `acfce0c145e70ef348f56f6d03a680fc3a6f1043` | 1201 | 44434 | pending |
| `pkg/session/test/temporarytabletest/BUILD.bazel` | `7e1b148d1cf589ec2393cbbd0c07b423ba5bb9e5` | 27 | 670 | pending |
| `pkg/session/test/temporarytabletest/main_test.go` | `b66e3e92c5921d5ed700887df53a7ec80eca73fa` | 62 | 2523 | pending |
| `pkg/session/test/temporarytabletest/temporary_table_test.go` | `56e67917483e1742518360402b403fab859623a4` | 423 | 16194 | pending |
| `pkg/session/test/tidb_test.go` | `242e4b650169ee99909bd22c34c749246f9c2005` | 99 | 3132 | pending |
| `pkg/session/test/txn/BUILD.bazel` | `7cf6d4ddaf72cf11bcba3c3fe5ce60979eb9809d` | 31 | 827 | pending |
| `pkg/session/test/txn/main_test.go` | `eb0e748eeb0782956813494ed2bf79f06f33a9cb` | 62 | 2508 | pending |
| `pkg/session/test/txn/txn_test.go` | `e85ea63cb49b283ae378774ca5f21a8d4a77aba4` | 529 | 20034 | pending |
| `pkg/session/test/variable/BUILD.bazel` | `d847b42d2e10a7c3484ea68ff0c1e78b1e73ec11` | 33 | 903 | pending |
| `pkg/session/test/variable/main_test.go` | `e6425eb22a6121822bcbebc5e6ea3506bf00f6f4` | 62 | 2513 | pending |
| `pkg/session/test/variable/variable_test.go` | `710fe048abfd11d5f82210c9212f4290d9f28d3d` | 498 | 19149 | pending |
| `pkg/session/test/vars/BUILD.bazel` | `021a7f5c2439fb4125fb7e8609f9bfc221a18f3b` | 31 | 848 | pending |
| `pkg/session/test/vars/main_test.go` | `e5904501fe47157d72b4933a0871be70c5aaaa32` | 62 | 2509 | pending |
| `pkg/session/test/vars/vars_test.go` | `43571ea0254782e61dd712e8b99efb859983bbe5` | 545 | 22086 | pending |
| `pkg/session/testutil.go` | `7f0c6df0953aff7d1e9375063506d0aa24afc79f` | 111 | 3700 | complete |
| `pkg/session/tidb.go` | `92c183085fbf10e342c25cbc1f76ad64874683c4` | 473 | 15160 | complete |
| `pkg/session/tidb_test.go` | `ead4aa27d14a036154583885a5f0fff594cd88db` | 589 | 21892 | complete |
| `pkg/session/txn.go` | `2295510b2cd00b2d5332d68c4b9e21f043a404d7` | 766 | 23133 | complete |
| `pkg/session/txninfo/BUILD.bazel` | `8062dd58b6d28ba6d8de0d735faed82e79711f16` | 20 | 532 | pending |
| `pkg/session/txninfo/summary.go` | `b4e102c9b2ad51dd1b04497bdcb4bc719d58330b` | 162 | 4487 | pending |
| `pkg/session/txninfo/txn_info.go` | `84c5478f6f59f0efdec53ee34474208fce17a835` | 291 | 10036 | pending |
| `pkg/session/txnmanager.go` | `d76e71960535b27ac593feba206cd996557f2f56` | 410 | 12820 | complete |
| `pkg/session/upgrade_backfill_test.go` | `3c94de6aba305ea7d0e7d27f8c41c4c907c7ee9d` | 500 | 19170 | complete |
| `pkg/session/upgrade_def.go` | `ccb7b548f9bb0f4ae86f49c91f6f47de98fa334d` | 2345 | 107207 | complete |
| `pkg/session/upgrade_run.go` | `28db74b0637fc1a64192b5d806bd144b2f1d507f` | 122 | 3916 | complete |
| `pkg/session/upgrade_test.go` | `5aa5401876ffad7e0153b8a545ea772a4fbaf3d6` | 69 | 2365 | complete |

Reading checkpoint: 5/26 direct artifacts complete; 92 tracked artifacts total
including nested packages, 34,341 source/artifact lines. No doc.go, fixture directory,
or generated inputs are present in the tracked inventory. The nextgen test is
tracked but absent from the ordinary Bazel test srcs; its build constraint must
be read separately. The library explicitly includes testutil.go.

contextimpl.go embeds the actual session together with PlanCtxExtended, retaining
shared session identity across planner interface conversions. Global initialization
uses a separate temporary domain because TableCommon captures the collation setting;
system timezone and new-collation state are loaded before normal domain use.
TestMain configures a 20ms schema lease, zero async-commit safety windows, enabled
TiKV failpoints and explicit goroutine exclusions. Its row matcher deliberately
skips time.Time expected values; this is not full timestamp equality evidence.

No Rust session source changed at this checkpoint. Existing MAX/MIN quota wiring
remains WIP pending completion of this prerequisite and actual session propagation.

Latest checkpoint: 11/26 direct artifacts read completely. Completed the six
artifacts listed above in their entirety (900 lines), including nextgen-only
tests and all mock-upgrade DDL strings/callbacks. The nextgen build tag is
explicit; its four tests cover Starter pipelined-DML warning/fallback, post-lock
bootstrap-version handling for GCV2 abort, and external-workload manager identity
in normal/bootstrap domains. These tests are not covered by the ordinary Bazel
srcs list.

The test SQL helper uses Execute for no arguments, otherwise PrepareStmt followed
by ExecutePreparedStmt; it does not deallocate prepared statements. Advisory
locks allocate separate pessimistic transactions, track repeated acquisitions,
and release through rollback; IsUsedLock always defers cleanup, whereas GetLock
explicitly closes on INSERT failure. No behavior changes are inferred solely
from these helper distinctions.

Upgrade state synchronization logs resume-job errors but still attempts the
normal-state update; global state read retry returns on any successful read,
including a non-upgrading state. Versioned upgrades compare each function against
the original version, initialize MDL first, and recheck the committed version
after COMMIT failure before treating it as fatal. Mock upgrade hooks and sleeps
are test-controlled and must not become unconditional Rust behavior.

Latest checkpoint: 15/26 direct artifacts complete. Fully read session_test.go
(310), upgrade_test.go (69), txnmanager.go (410), tidb.go (473).

Transaction providers are selected from explicit provider, stale-read timestamp,
then transaction mode/isolation; two optimistic providers alternate to avoid
resetting the active provider. Initialization failure rolls back before publication.
OnStmtStart stores the statement before checking provider existence; retry hooks
delegate to the current provider. Bulk DML skips warmup until optimization can
check feasibility. These lifecycle distinctions constrain future state wiring.

finishStmt records only successful retry-safe writes; LOAD DATA LOCAL disables
retry because its client stream cannot be replayed. Pending unused transactions
are invalidated after autocommit handling. Shared-lock loss aborts any valid
transaction, whereas deadlock aborts only pessimistic transactions. Statement
count limiting occurs after completion and can create a new transaction under
batch commit. Parse appends parser warnings before returning parse failure.

Tests verify retryable bootstrap transaction failure, virtual-time system-keyspace
bootstrap waiting, fatal user-keyspace version guards before session creation,
reserved upgrade versions, and memory-arbitrator SQL/database digest distinctions.
The DDL table uniqueness helper compares through IsSortedFunc rather than an
independent set, so its name-uniqueness coverage should not be overstated.

Latest checkpoint: 16/26 direct artifacts complete. txn.go was read in contiguous
1–390 and 391–766 segments, covering all production and failpoint branches.
LazyTxn distinguishes invalid, pending and valid states; pending-to-valid consumes
the future before waiting, preserves recorded SQL digests, applies the statement
resource group and transaction entry-size limit, and defers fair-lock activation.
Statement staging is bypassed for pipelined transactions; commit and rollback
both reset state even on failure. Rollback replaces the memory footprint callback
before operating on an invalidated tracker. Only successful commits update the
last commit timestamp.

KeyNeedToLock prioritizes meta keys, deferred constraint checking, and presume-not-
exists flags before record/index handling. Nextgen locks temporary-index keys
unconditionally; classic decodes the current temporary-index value. Nonunique
ordinary index keys require the explicit lock flag. TSO future failures propagate
for UniStore but otherwise permit a store.Begin without supplied StartTS.
StmtCommit logs provider hook errors and still flushes the buffer; StmtRollback
logs hook errors and still cleans it. These are source observations, not new
Rust behavior or proof of full session parity.

Latest checkpoint: 18/26 direct artifacts complete. tidb_test.go (589 lines)
fully read; bootstrap.go (623 lines) read in contiguous 1–320 and 321–EOF segments.
Tests distinguish per-statement RU metrics from restricted-SQL restoration of
outer metrics, consume pending parser accounting once, require actual nil for
cross-keyspace RU reporters, and gate byte paging on hard-capped resource groups.
Scalar-subquery replay verifies both final rows and registry/physical-tree counts
[1,0] across rebuilt statements. Bootstrap endpoint-claim tests skip Windows and
nextgen; those exclusions limit the evidence from a default test run.

Bootstrap seeds only GLOBAL-scoped catalog entries using
GlobalSystemVariableInitialValue, so defaults for new installs are part of the
initialization path. Classic skips nextGenOnly bootstrap schema entries; nextgen
uses versioned reserved-ID metadata creation. Initialization SQL file read/parse
failures return errors in tests but are fatal outside tests; individual execution
failures warn and continue. Result sets are closed without draining. Bootstrap
COMMIT failure rechecks the bootstrap flag before fatal handling. These are
source observations, not claims that Rust session integration is complete.

Latest checkpoint: 19/26 direct artifacts complete. upgrade_backfill_test.go
fully read (500 lines), including all four tests and every digest/status helper.
The tests reboot a downgraded store after deleting persisted global variable
rows, then check both mysql.global_variables and SQL GLOBAL getters. They cover
ignore-inlist digest, default string-match selectivity and analyze bucket/TopN
defaults; all skip nextgen. Merely registering the variables is insufficient
evidence for these upgrade cases.

The binding refresh test explicitly rewrites current-code rows into the older
normalization form, introduces timestamp-ordered duplicate bindings and an
unparsable binding, and checks loser deletion plus NULL digest fields. The
unparsable row remains enabled with its SQL digest but loses plan_digest to
avoid uniqueness conflicts. It reloads bindings and verifies last_plan_from_binding
for every parenthesis variant. This validates binding selection, not the exact
physical plan shape. No Rust change or runtime validation was performed here.

Latest checkpoint: 20/26 direct artifacts complete. nontransactional.go read
fully in contiguous 1–300, 301–610 and 611–873 segments. NT-DML saves/restores
ReadStaleness and BulkDMLEnabled; shard enumeration temporarily disables the
select limit and raw MaxExecutionTime and restores them at their distinct
source points. Jobs never split equal shard values under the column collator,
and clone boundary datums across chunk reuse. NULL bounds get IS NULL or a
NULL-inclusive upper bound; ordinary bounds use inclusive BETWEEN.

The first job's error cancels all remaining jobs even with ignore-error enabled.
Later errors can accumulate; dry-run split mode emits only first/last examples.
Explicit shard columns require a public visible leading index column or integer
handle; automatic selection rejects multi-column common handles. These source
contracts and the complete embedded SQL/AST generation were read, but no Rust
NT-DML parity or runtime validation is claimed.

Latest checkpoint: 21/26 direct artifacts complete. starter_bootstrap_file.go
fully read in contiguous 1–350 and 351–694 segments. Bootstrap DML is parsed
and validated before privilege deletion; privilege deletion itself commits in
128-row batches, while bootstrap DML/root verification/SQL version publication
share a transaction. Upgrade SQL instead commits statement by statement.

The codec snapshot supports the no-reset fast path; pending reset markers are
refreshed from PD after the distributed lock, and completion uses exact observed
marker values as preconditions. SQL version and store completion version are
separate crash-recovery boundaries. JSON decoding rejects unknown fields and
trailing objects, validates and sorts unique positive upgrade versions, and
permits only the keyspace placeholder. Parse and execution helpers restore the
previous InRestrictedSQL value on all return paths. Bootstrap blocks must each
contain one INSERT/REPLACE/UPDATE/DELETE; upgrade blocks are not constrained to
those statement types. No Rust change or runtime parity claim follows yet.

Latest checkpoint: 22/26 direct artifacts complete. starter_bootstrap_file_test.go
fully read (928 lines) in contiguous 1–330, 331–650 and 651–EOF segments. Tests
cover escaping keyspace quotes, sorted upgrades, malformed configuration, mode
gating, missing-root rollback and SQL-mode changes affecting subsequent parsing.
Upgrade partial failure is checked from a second session: the first statement
persists while the old version remains. Store-version recovery and no-op domain
identity are explicitly asserted.

Privilege-reset tests model a transient PD update failure after SQL/store version
publication, then verify convergence, no redundant update after completion, and
refresh of a stale codec snapshot. A 32KiB transaction limit rejects an unbounded
delete but permits batched reset; password history survives and recreated users
lose previous SELECT privileges. The mock validates supplied CAS preconditions,
but does not require that a caller supply every expected precondition; do not
overstate this as an exhaustive CAS-request test. Classic mock-store execution
tests skip nextgen while the Starter file-loading test skips classic.

Benchmark reading cursor: bench_test.go 1–760 read contiguously; resume 761
inside BenchmarkPartitionPruning's embedded partition DDL. Complete direct count
remains 22/26. Helpers disable slow logging and lower global logging; sort data
uses a time-seeded RNG. readResult stops after consuming at least the requested
row count, does not prove exact EOF or row values, and ignores Close errors.
hasPlan always explains the fixed string predicate col = 'hello 64', even for
integer/decimal lookup callers, so it does not verify their measured query plan.
Prepared point-get execution reuses parsed parameters and resets the chunk
allocator after draining each result. No Rust change or test run at this cursor.

Latest checkpoint: 23/26 direct artifacts complete. Finished bench_test.go
761–1180, 1181–1700, 1701–2143; all 2,143 lines now read, including every
embedded partition definition. Partition benchmarks use empty tables and drain
results without asserting selected partitions or values; the TO_DAYS benchmark
compares a datetime column against numeric TO_DAYS expressions as written.
CompileStmt benchmarks repeated compiler calls on a prepared INSERT SELECT.
TestBenchDaily explicitly includes 26 benchmarks, excluding ExplainTableScan,
Sort2 and all six pipelined benchmarks. The pipelined loops ignore execution
errors and directly mutate statement flags, so their timing output cannot prove
successful execution or proper statement lifecycle integration. No benchmarks
were executed and no Rust changes were made in this checkpoint.

Bootstrap test cursor: bootstrap_test.go 1–650 read contiguously; resume at
651 inside TestTiDBServerMemoryLimitUpgradeTo651_1. Direct complete count remains
23/26. Initial tests validate globally unique reserved IDs with a sorted combined
list, bootstrap global variable count, interrupted bootstrap recovery and DDL
table recreation. New-cluster tests assert index merge, advanced join hints and
cost-model defaults through SQL. Upgrade tests distinguish persisted historical
settings, including GC-aware tracking being reset. The file itself warns about
legacy bootstrap helper races and discourages adding tests there; use appropriate
Rust regression surfaces rather than reproducing its unsafe setup patterns.
No Rust edit or runtime validation in this checkpoint.

Bootstrap test cursor advanced through 1050 (651–1050 read contiguously).
Resume 1051 inside TestTiDBStatsLoadPseudoTimeoutUpgradeFrom610To650. Direct
completion remains 23/26. Tests distinguish server memory limit zero replacement
from preserving 70%, and store batch size zero migration from preserving an
explicit value of one. Historical foreign-key/statistics/capture switches are
reset by the tested upgrade chain. Nonprepared cache upgrade backfills OFF and
size 100. Version-140 test creates its reset session before domain closure,
explicitly avoiding schema validation against a closed domain. No Rust change
or runtime validation at this reading cursor.


Latest checkpoint: bootstrap_test.go completed through EOF (1051–1450 and
1451–2011); direct artifacts complete: 24/26. Remaining direct files are
session.go and upgrade_def.go; nested package inventory remains pending.
Stats-load pseudo timeout, NAAJ and replica-read threshold migrations assert
updated defaults. Fresh-statistics plan-cache invalidation backfills OFF and
checks both session and global values. Strict resource-control migration checks
persisted OFF, the SQL global value and the global atomic flag.

Coverage limits: v240 checks existing analyze-job index preservation; v254
explicitly drops runaway indexes and verifies recreation. v252 checks timestamp
column precision in both kernels, but does not assert stored timestamp values.
Cluster-ID coverage asserts nonempty only. Duplicate binding upgrade coverage
asserts successful completion/version, not survivor contents. Versioned-schema
tests check ordering, uniqueness and the nextgen storage-class transition.
Etcd namespace tests use a real single-node server and verify prefixed visibility
and bare-key absence. System-table DDL tests reject partitioning and
AUTO_ID_CACHE=1. These are source-reading findings, not executed Rust parity
proof. Documentation remains local; no Ready claim or publication.

Upgrade definition cursor: upgrade_def.go 1–1400 read contiguously. Resume
1401; direct completed count stays 24/26. The ordered upgrade registry ends at
version286 and deliberately omits superseded migrations. Version97's comment
specifies compatibility range quota zero for older clusters; its implementation
has not yet been read at this cursor. Do not infer fresh-session defaults from
this migration comment.

Early upgrade bodies distinguish conditional replacement, missing-row INSERT
IGNORE, unconditional rewrites and deletion to expose defaults. Version29 runs
only for original version28. Reentrant DDL ignores only supplied errors, with
other execution errors fatal. Version55 changes concurrency settings only if
all returned values match historical defaults (missing rows are not explicitly
required). Version67 locks bindings in a pessimistic transaction, reads newest
first, filters enabled/using/builtin statuses, normalizes with the default DB,
keeps the first duplicate and rewrites nonbuiltin rows. Parse errors are fatal;
this is not the later invalid-binding preservation migration. No Rust edit or
validation follows from this partial read.


Latest checkpoint: upgrade_def.go fully read through EOF (2345 lines), including
1401–1870 and 1871–2345. Direct completion is 25/26; session.go remains pending.
Version97 implementation backfills range quota zero only when absent; it does
not override a configured value or define the fresh-cluster default. Version141
returns before both cache-size import and replica threshold replacement if the
source query fails to yield a non-null row. Version177 explicitly sets async
merge global statistics OFF through the global accessor.

Version255 only rewrites persisted analyze version exactly "1"; v278 inherits
scan concurrency only when the source row exists and is nonempty. v279 uses OFF,
v281 uses historical string selectivity 0.8, and v283 fills missing bucket/TopN
rows while preserving existing values. Version282 reads newest bindings with
explicit timestamp/row-ID tie breakers, excludes builtin sources, and treats
null plan digests separately from empty strings. It clears invalid rows' plan
digests, marks duplicate rows deleted with both digests null, then updates
survivors in independent writes. Version284 is nextgen-only, locks the legacy
variable in a pessimistic transaction, inverts its boolean value via REPLACE,
retains the legacy row and rolls back on injected failure. These observations
complete source reading of this file, not Rust parity or runtime validation.

Session source cursor: session.go 1–1250 read contiguously; resume 1251 inside
retry. Direct completion remains 25/26. Session cache is lazily allocated when
either prepared or nonprepared caching is enabled and returns nil when both are
disabled. Commit invalidates transaction state even on failure; restricted SQL
bypasses the cluster-read-only privilege recheck. Cached-table leases must be
strictly greater than commitTS. Temporary local-table changes use a staged
session buffer released only after successful transaction commit.

Commit retries exclude batch insert, pessimistic and pipelined transactions;
statistics deltas reach the collector only after success and only for positive
table IDs. The replay prefix swaps each history item's statement context, resets
CTE storage, retry state and plan parameters, and clears scalar subqueries before
RebuildPlan. Execution retry counts are set only after statement-start setup
succeeds and execution is reached. The rest of replay remains unread. No new
Rust edit or runtime validation in this checkpoint.


Session source cursor advanced through 1950 (1251–1950 contiguous), resume 1951
at Parse body. Global variable reads during initialization return empty without
storage access; normal reads reject unregistered variables, fall back to catalog
default on table-read failure, and apply type-only validation to persisted values.
Global writes validate and run hooks before persistence and cache notification;
SetGlobalSysVarOnly skips validation/alias updates. Internal session factories
mark common globals loaded and disable chunk RPC. ParseSQL strips
NO_BACKSLASH_ESCAPES for internal SQL and clones parser-owned statement slices.
Memory arbitration can evict session plans before waiting or rejecting parsing.
ExecuteInternal restores restricted status with defer and appends execution
errors to the statement context. Deprecated Execute accepts exactly one parsed
statement. Replay stops on nonretryable errors or its retry limit. No Rust edit,
Ready validation, commit or push in this source-reading checkpoint.


Session source cursor advanced to 2650 (1951–2650 contiguous), resume 2651.
Parse loads common globals before parsing; ParseWithParams escapes only when
arguments exist and internal parsing omits client charset parameters. Both reset
pending parser counters. ReleaseAllAdvisoryLocks sums reference counts despite
the comment describing unique locks. Restricted execution restores selected
snapshot/analyze/pruning and RU fields; pooled cleanup propagates warnings unless
IgnoreWarning, resets selected settings and returns the session to the pool.
ExecRestrictedStmt and ExecRestrictedSQL use unnamed return values, so assigning
a local err in deferred Close handlers does not replace an already evaluated
returned error. Do not claim Close-error propagation from those assignments.

ExecuteStmt prepares transaction context, loads globals, resets statement context
before compilation, then checks caller cancellation before binary parameter
conversion. Pending parser metrics are consumed once. Transaction-manager start,
request-source setup and memory arbitration precede compilation. The first defer
records RU failure for errors/panics after a nonnil compiled statement; its success
publication depends on later paths still unread. No Rust edit or runtime parity
claim in this checkpoint; direct artifacts complete remains 25/26.


Session cursor: 2651–3450 read contiguously, resume 3451 at DropPreparedStmt.
Point-get fast execution invalidates its transaction after PointGet; ordinary
result sets defer statement finishing to execStmtResult. Finish uses sync.Once,
but its error is local per call; underlying Finish errors are preferred in the
first return, while finishStmt receives the initially nil local error. Close
prefers Finish error over underlying Close error and returns nil once closed.
Detach requires read-only autocommit and a detachable result, creates a cursor,
and cleans up the detached result if statement finishing fails.

Prepare dedup keys include SQL, charset, collation, current DB and SQL mode.
Reuse checks schema version, reparses an independent AST, reruns preprocessing
with a fresh ResolveCtx, rechecks schema version, clones mutable related-version
state and recollects AST-linked metadata. PointGet and normalized-plan state are
not inherited. Rebuild errors fall through to full prepare. ExecutePreparedStmt
constructs ExecuteStmt and enters the ordinary ExecuteStmt path. No Rust edit or
runtime validation; direct completion remains 25/26.


Session source cursor: 3451–4250 read contiguously, resume 4251 at
splitAndScatterTable. GetRangerCtx lazily caches a context on the statement and
passes pointers to that same statement's PlanCacheTracker and
RangeFallbackHandler, not isolated per-call trackers. This directly supports
the pending Rust shared warning/cache integration. DistSQL cached context
refreshes its runaway checker and RU metrics after creation because optimization
subqueries can initialize it early.

DropPreparedStmt records deferred deletion in retry information. Close rolls
back with the closing marker, withdraws prepared statements, closes the session
cache and detaches trackers. Authentication lock tracking uses pessimistic
transactions; identity and default roles publish after successful checks.
Test session construction randomizes chunk RPC, while test options set small
chunk limits and connection charset. CreateSessionWithOpt binds privileges and
conditionally attaches statistics/index collectors before cursor tracking.
DDL table descriptors and version groups read; initialization bodies remain
pending. No new Rust edit or validation; direct completed count stays 25/26.


Session source cursor advanced through 5050 (4251–5050 contiguous); resume
5051 inside waitSystemBootVersion. Direct completion remains 25/26. Raw system
table creation builds all metadata with ModeNone and validates constraints before
splitting/creating; split failures warn and do not stop creation. DDL-table
version and metadata are written together. Classic masking-policy prerequisite
creation checks names and allocates IDs only for missing tables.

Bootstrap initializes persisted collation/time zone before Starter SQL and full
domain startup; sysvar cache precedes binding handle, statistics setup precedes
TTL. Upgrade lock acquisition is followed by a fresh version read before GCV2
abort and prerequisite creation. createSessionWithOpt constructs SessionVars,
expression/plan/table contexts, binds its own GlobalVarsAccessor and initializes
transaction/binding state; common-global loading body is still ahead. Cross-keyspace
sessions intentionally have no domain. No Rust edit, validation, commit or push.


Latest checkpoint: session.go completed through EOF (6051 lines), with
5051–5600 and 5601–6051 read contiguously. All 26 direct artifacts are now read;
66 nested artifacts remain independently pending in this inventory. This is
reading completion for the direct Go package, not implementation parity.

loadCommonGlobalVariablesIfNeeded preserves already set variables and applies
cache entries using SetSystemVarWithRelaxedValidation; stale unregistered names
are skipped. CommonGlobalLoaded is set before cache fetch and is not cleared on
failure. Bootstrap initializes only max_allowed_packet here and avoids global
storage. Interactive clients then inherit interactive_timeout into wait_timeout.
Session migration decodes handlers first, dependency-orders variable setters,
logs setter failures and restores statement state last so warnings survive.
Pipelined eligibility checks are ordered first-failure warnings: Starter, MDL,
batch mode, statement kind, internal/transaction/autocommit, constraint checking,
then table metadata restrictions. Bulk mode overrides pessimistic-autocommit
with a warning. No Rust edit, runtime validation, commit or push in this checkpoint.
