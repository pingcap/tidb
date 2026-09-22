# Physicalop source inventory

Audit reference: TiDB `origin/master`, fetched 2026-09-22 (local time).
Resolved revision: `2339f8171265558ab30b6e564b2e3a854e53e8ea`.

The user defines Go parity as TiDB master. Read reference artifacts with
`git show <resolved-master-revision>:<path>`; the working branch Go files
can differ. Refresh master and record its resolved commit when advancing
the audit. Dependent-package inventories below retain their historical pins
and require re-audit against master before acceptance.

Status: **incomplete; no whole-package parity claim**. This inventory records
every tracked artifact in the upstream package. Hashes identify source bytes,
not behavioral coverage. All artifacts remain pending whole-package acceptance;
the focused regression receipts in the ExecPlan do not discharge that gate.

## Package artifacts

| Artifact (relative to `pkg/planner/core/operator/physicalop/`) | Role | SHA-256 |
| --- | --- | --- |
| `BUILD.bazel` | build metadata | `2cf3a3de47fc1c3b7274cc86c93fd3b2ee66a2edaefa5a8195555cb0400a9a33` |
| `base_physical_agg.go` | production | `1d28bb64282cfaa2f2e714c63301c16403fd482814571e70efc2159bd44be9dc` |
| `base_physical_join.go` | production | `fb4adb2a13c982764839fa063a3b9ff3565866ce0320b8315333b92c9588cbe4` |
| `base_physical_plan.go` | production | `fac3b9fcd4a51241af5a53d875d1d9696c2c345b35794a3aa7324be174bcbfda` |
| `enforce.go` | production | `056e1c6ccad302f24e2dc35cc26ab136bfcd584a48d9f52b29c2dc86ad03ea5a` |
| `foreign_key.go` | production | `29414c494148f5fe9dcc17741317ac8541284b4a0c0b0818e36c132ed014f549` |
| `fragment.go` | production | `8d6aabb499a84136e098bd02cc55ec2f1642efdd5a16b67c0e8f2ae87110d373` |
| `fragment_test.go` | original tests | `b7f9622583def1133d023512f01de1124b9116f381aad532caeadb3e052d1f41` |
| `nominal_sort.go` | production | `e92994942d6b21bd7a3403a80fec6f600ef77d065352447b3032aeb37cdcc34d` |
| `physical_apply.go` | production | `8a7cfce083b8d2dc4c3b4cda95ef653415d18c099dbacda730b6ea78512a4daf` |
| `physical_batch_point_get.go` | production | `c662f7783d35df21336174c5573ca4d52970d5e0427c11a2002abbe70a3876a9` |
| `physical_batch_point_get_test.go` | original tests | `82a4274c686d9febfe487e7dee605a1ebc485aa41de7b5a1c8edd722e2e20215` |
| `physical_common_plans.go` | production | `b1a0a4b76eb18951fbb4c536aa3710f6b02534b17a92b46950c89214d7c64a68` |
| `physical_cte.go` | production | `6223410e99a11ab15400dc218786c4cb4d300ea58370a461704c5b86217401f9` |
| `physical_cte_table.go` | production | `4ca4d93c9ebbeda3e37eea7d656f2a424eb959a46f162e265ae2d7bdbad4e1b1` |
| `physical_exchange_receiver.go` | production | `dbac31e1b885c149dd7cef0bd654384e48a2124bfe23ef811263697c2421f64f` |
| `physical_exchange_sender.go` | production | `f6bf05352b37a278f3550b14a6eb33af87d5c65b8df9b2f3fd18f47dd03cc048` |
| `physical_expand.go` | production | `cd88b89d960100563aa780b5d4c9b5bc78a7354861c00849768adead297b4ea0` |
| `physical_hash_agg.go` | production | `ff2730c8214b404b864bdb8b1da82af98274c075c360123bd9d836b960f4168b` |
| `physical_hash_join.go` | production | `c7cd643ade39c2303a71c7afe0c20573b02f41bf787f1c4184c67158e7f9bed8` |
| `physical_index_hash_join.go` | production | `d83fbe5afadaa2a854f28335104eee5526b77ac97882892e20199a5d202a30a3` |
| `physical_index_join.go` | production | `fa9f0e8bdc8f05e996f86dd07e67e5f4fb5950841fb6f93d3171f9c092c416d9` |
| `physical_index_merge_join.go` | production | `ba15d3255af623a23474a708890ee1ae7eef436d5c1a25e7c5f82d02e2ccf471` |
| `physical_index_reader.go` | production | `975e2c2c596c4ce699db1e66664402f9631bfdb52aaedafd8f4d01838c6a050b` |
| `physical_index_scan.go` | production | `6214122edfbc4c5d4129e94ed44757cc64ac6ba31ec33d2f0dac3eadf4e63755` |
| `physical_indexlookup.go` | production | `87b822c2085290ba81ec043a6c00172e6f26426e6198bb7727aefbb5c2e23b0d` |
| `physical_indexlookup_reader.go` | production | `c65e1cb478ab3332006a7c225685edca3eaa54b1136ccd01eab072e58eedb3e9` |
| `physical_indexmerge_reader.go` | production | `4a23c43c5413a967c0c556fe4e3bf371488439d5d38622408ffa3efc1c603281` |
| `physical_limit.go` | production | `22273734e75155ccd3cd343e5ef1672acbdf43358e1336d9ce96669c050fb0e4` |
| `physical_lock.go` | production | `352d2a50c176404c945c1914ea1e19144e84a4e5b6032ce477a12de7124f3e60` |
| `physical_max_one_row.go` | production | `6e1d3638dbfbc68e52fb83be2af896aa403012d09121cc099862170749ca4f16` |
| `physical_mem_table.go` | production | `099ccee70a0637e72d4103270d8ee58a8c01c4edf2606bf2497c1dbf48f8a792` |
| `physical_merge_join.go` | production | `51f51825653a8d8cd6b41459a0d2625672099687693cc2310f8842c50455443e` |
| `physical_plan_misc.go` | production | `14261ef0089be495a2f3d9ddc58e6888ef6a7de927d02f3edce7f6af7d51ae85` |
| `physical_projection.go` | production | `c762a81526579927c12731e2a9e9cbfee315aff07a981229e8e9e43fd500f2cb` |
| `physical_schema_producer.go` | production | `1e5604fbd0f2545ef4f468576d848f280ff30886d7a687514c5d6cc3047c7c48` |
| `physical_selection.go` | production | `9c77bfb803ee8661d5ed06e631565b6224c0d9947543e1df50ecea80a83cc451` |
| `physical_sequence.go` | production | `70dbc48bc14ab6323a89c86d58f9e28d5fd81b430a3da73b7b6c6d66a4688992` |
| `physical_show.go` | production | `4dd70ef16fa82b6ff65867764222eec00b2a000f53b1aec6267509b17225e028` |
| `physical_shuffle.go` | production | `6721de010c25ba7aee259abcbcc5914a64ab167ccdbf7fb096de199ce85c183d` |
| `physical_sort.go` | production | `d374ed22792ebcd39baa4b50470a9b74f20703fb80807a1f3a3720498d37eae7` |
| `physical_stream_agg.go` | production | `0e7946ee8c567a124fcd12a4c593718a4810ef3f54040cd1cb1c84e3a3ada06b` |
| `physical_table_dual.go` | production | `9eaac8ac03f0fad25f6115c2fd386907d5655e4718ea59afe0c17a04fb3b0648` |
| `physical_table_reader.go` | production | `9869ea4aa9cc175c1d35f79072d652913a36602deed4ddacc5332567a9d3cc72` |
| `physical_table_sample.go` | production | `ca9c5f1d0ef7b20c03102bce975c2881380db19de4d1a3f6044d11c926f66123` |
| `physical_table_scan.go` | production | `c66a900cd9d45bf98ddbd86d8c9c13b74be04dba32ef082294bb9eb31aae7a87` |
| `physical_topn.go` | production | `4606f48f670d717ed5114b77d2c916dbf2af7d87b766c89393674fa120824c32` |
| `physical_union_all.go` | production | `528703cc3969e2e68a6c6be61e05de3d9bb33618d8e7966d4617ee37b466e0fa` |
| `physical_union_scan.go` | production | `045559e3b39d84659dc33290d9a54ff94f498669423a582f70c56113ce7f126f` |
| `physical_utils.go` | production | `5bb906977898130c0a50a76f1854feaea37a74f37a3472f7d71d0b89c61648f2` |
| `physical_utils_test.go` | original tests | `ae0a20839af8d9aa3442de6f3e37e05fbace1f5bb12fffc1a9511cb70aa1a96d` |
| `physical_window.go` | production | `16aae9ab1fc41dbfd03e23b2d5b4e999d94c404b20f2cefd74417014f5568189` |
| `plan_clone_generated.go` | generated production | `ee5deff27d09c8f57ed5aba9079adba04c8350296ce45c4f8169c460e77ecd50` |
| `single_scan_index_join.go` | production | `bdda651f445f64317fcef63777f0e85e6ad72aa2d0974bca62afa442a8ed630f` |
| `storage_engine_usage.go` | production | `3db1feeaefd930d2326f1ece8ee07000ab9e2650371b6756bd9f33fea0c6d454` |
| `task.go` | production | `76da2a7be7f39141f53ff556133b91909ae89dbc56d17264119467dbeb42e169` |
| `task_base.go` | production | `04940c451d9ed9cf823a84535becdc7aecc29c8df892725cc7d874dea395185c` |
| `tiflash_predicate_push_down.go` | production | `4dfe67be2920170614d800a906ac026242c9a6f6bd9fdbe74e418b78cb06920e` |

The package has 53 handwritten production Go files, one generated production
Go file, three original test files, and one Bazel build file. There is no `doc.go`,
nested fixture directory, or platform/build-tag source variant in this pinned
tree. Production files can still contain runtime platform-dependent behavior;
absence of build tags is not evidence that those branches are covered.

## Direct generation and module inputs

`base_physical_plan.go` invokes `go run
../../generator/plan_cache/plan_clone_generator.go -- plan_clone_generated.go`.
The generator reflects physicalop types and their clone tags; its templates are
embedded in the generator source. The original generator test and Bazel build
metadata are retained as validation inputs. Imported Go packages remain a
dependency audit obligation; this list does not claim to enumerate their
transitive sources.

| Repository path | SHA-256 |
| --- | --- |
| `pkg/planner/core/generator/plan_cache/BUILD.bazel` | `18478c79434c04481a69853ced44be7bcad7707075544b237757e2a0c6b5ee87` |
| `pkg/planner/core/generator/plan_cache/plan_clone_generator.go` | `bfdd34d08ec2db413226ef282cf624f7206d28dda43b032e0b602434f289a7ac` |
| `pkg/planner/core/generator/plan_cache/plan_clone_test.go` | `f7d3e07caf11e9d7b7700bd5f40ffb970f8c75fc31ddae2273913c195bee2ce8` |
| `go.mod` | `4145c35b252c18bf570e8084adf4847b08f47637e843a10522eb8ba1d0119d52` |
| `go.sum` | `34c46c6495a85778238c4f95320b1179612f1fe41064a8872db408252db61775` |

## Confirmed current integration gap

`physical_shuffle.go` defines PhysicalShuffle and its receiver stub. Rust now
has a PhysicalPlan::Shuffle variant and a physical-builder path using owned
receiver substitutions. Boundary references use IDs into the owned child tree.
`tidb-planner/src/find_best_task/dispatch.rs` now invokes the shuffle rewrite
for unordered non-MPP candidates. Session concurrency and group-NDV skew
settings reach the rewrite, and a SQL regression verifies window selection.
The standalone `tidb-executor/src/shuffle.rs` now runs
sources and workers concurrently with recycled input/output chunks, cancellation,
and panic recovery. Focused node/builder/scheduler tests pass. They do not establish production
parallel-window, merge-join, or stream-aggregation parity.

Earlier ExecPlan entries describing shuffle integration are historical;
current node, builder, scheduler, and selection receipts are recorded separately.
Receiver stubs now have distinct statement IDs, separately owned data sources,
and shared worker runtime counters. Focused source-preservation and idempotence
regressions pass, as do five Go join/aggregate comparisons. These cover the
identified gaps, not every boundary rewrite or SQL variant. Remaining
obligations include comprehensive receiver/plan traversal and clone coverage,
complete merge-join and stream-agg SQL comparisons, upstream error/failpoint
coverage, and workload measurements.

The 2026-09-21 hybrid arithmetic audit closes observed ENUM/SET rounding and
conversion-error gaps, signed BIT arithmetic, and binary-literal cast warning
and diagnostic differences in the shared expression dependency. Its native
scalar/vector/SQL fixtures and pinned Go overlays are recorded in the ExecPlan.
This is evidence for the ongoing dependency audit; expression, datatype and
all inventoried executor/planner packages remain unaccepted.

Temporal and JSON numeric source domains now participate in the shared
arithmetic dependency path, with strict/warning scalar/vector and SQL precision
fixtures. The ExecPlan records the source conversion choices and remaining
whole-expression validation gaps. This adds evidence without changing any
package's unaccepted status or its complete artifact obligations.

## Acceptance obligations

- Reconcile every production method and branch, including generated clone
  behavior, across Rust crates; a matching type name is insufficient.
- Account for every original test and support artifact, generated output/input,
  and build target. Record intentional native Rust representation decisions.
- Validate planner selection, expression resolution, costing, plan cache,
  distributed serialization/task attachment, and executor integration.
- Run required package/regression and lint gates. Record failures explicitly.
- Measure sysbench, TPC-C, TPC-H, and YCSB against a matching Go revision and
  equivalent configuration. Current sysbench observations alone do not establish
  performance parity; the other three workloads remain unverified.


## Dependent package inventory: executor/internal/vecgroupchecker

Pinned revision: `aba629bb455dc09d6a5d98b3c39a542bb1189b9d`.
The entire package is the atomic acceptance unit, not its individual helper.
Status: audited integration progress; **not accepted as a complete package**.

| Go artifact | SHA-256 | Rust ownership / validation |
| --- | --- | --- |
| `pkg/executor/internal/vecgroupchecker/BUILD.bazel` | `3167139245f1ab3f4eb5867b3a4db66cc49da8b8cb1d5245c127a541db3ee531` | Library/test inputs and dependencies inspected; Rust builds under tidb-executor Cargo targets. No Bazel file edit. |
| `pkg/executor/internal/vecgroupchecker/main_test.go` | `2c78f211de0fac54b059512b8fd8afd30edae9cb772d5390248083133d31c874` | Go testsetup/config/client failpoint-registration/goleak harness runs in the Go oracle. Native checker owns no workers; no corresponding background services are introduced. |
| `pkg/executor/internal/vecgroupchecker/vec_group_checker.go` | `25e9f350932453ed382742d805b50e056021e221aa5f2df2fbbf63b31f720bfe` | tidb-executor/src/vec_group_checker.rs; window normal/pipelined, shuffle range splitter and GroupedStreamAggExec call production split_into_groups. The stream duplicate hash-key implementation is removed; its integer-cell fast path is now shared. Merge join now uses the same checker on both inputs, retaining its native spill container. The typed-domain audit now covers all eight Go evaluation domains through 16 field/flag cases, boundary bytes, session timezone, NULLs and continuation; BIT overflow diagnostics cover strict/warn/ignore. Runtime-mode/exact-name collation and timestamp encoding diagnostics now match Go oracle fixtures, including empty-key continuation. Typed constants now use raw lazy values and runtime parameter types, with Go conversion diagnostics and decimal scale padding; plain vectorized constants evaluate once per batch, including NaN comparison. Ordinary string columns borrow cells and compute each collation key once per row. Correlated typed bindings and deferred forwarding to literal/correlated/same-domain column leaves now match Go in both modes, including one-read warning counts and child decimal scale. Audited numeric +, -, *, /, DIV and MOD trees now share typed scalar/batch evaluation with projection, preserving Go NULL/error order, batch warning counts, numeric widening and unsigned bits. DIV distinguishes integer versus decimal operand signatures, bounded decimal warnings, declared real cast scale and scalar/batch overflow diagnostics. Integer-to-decimal wrappers inserted by the rewriter retain child batch evaluation. Other scalar signatures/cast shapes, deferred mismatched-domain columns and remaining codec-error contracts still require audit before acceptance. |
| `pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go` | `ac1cce0c1ae1eb2a8be2fc69cc6b9a20332dd4a898c453b26326c339c0299795` | Four original tests map to tests_executor_internal_source.rs: datum ownership, group-count matrix, collation/padding, reset. Ports now call production chunk evaluation. Additional local fixtures and warning tests are in vec_group_checker.rs, including the pinned-Go typed matrix and exact BIT code/message/warning-count regression. The four original Go tests plus typed, BIT, runtime-collation and timestamp-error oracles pass under the Go race detector; new fixtures cover both vectorization modes. Constant-domain (27 scenarios in both modes), interior-warning and eight-case decimal-error oracles also pass under the race detector; native checker now has 28 passing tests. The expanded 16-field correlated matrix verifies all eight domains, selection, NULL rebinding and continuation; lazy-batch and deferred-column oracles preserve Go mode-dependent diagnostics. The expression owner also runs the original GetType independence contract using owned FieldTypes. Arithmetic batch/warning and unnamed-overflow oracles plus division batch/value oracles expand the accumulated Go race gate to 18 test functions. A coefficient-bound oracle adds 576 scalar/vector observations and raises that gate to 19 test functions; six additional mixed Real/Decimal/UInt SQL observations verify the value-level DIV route. Twenty-four SQL cases cover scalar/vector NULL-overflow behavior; four more verify Real DIV through column projection and constant folding; native expression tests additionally cover selected rows, preflight side effects and unsigned widening. |

The package has no doc.go, platform/build-tag variant, generated source/input,
external fixture, or non-listed build artifact at this pin. BUILD.bazel lists
one production Go file and both test files. Original randomized count fixtures
remain in the Go oracle; Rust uses deterministic group values. No new Go files
or imports, Bazel targets or module changes were made. The source-test Go
command and exact Rust commands are in the latest ExecPlan receipt.

Native representation decisions: retain owned encoded boundary keys instead
of aliased first/last datums, use a reusable boolean row mask for group starts,
and compare already evaluated datums without a Go temporary-column allocator.
The original allocation-reuse test is represented by replacing source chunk
values and verifying that the remembered key survives. Rust plan expression
metadata is owned; ordinary collation variants rebuild the checker instead
of mutating a shared Go FieldType pointer. Parameter constants derive their
type and refresh their collation on every execution, independently of stored
planning metadata. These decisions do not establish
complete expression vectorization or whole-package acceptance on their own.


String arithmetic follow-up adds 72 scalar/vector order and selection cases,
42 malformed decimal diagnostic cases, 12 selected-row constant construction
observations and 24 SQL scenarios. The accumulated Go checker race selection
now contains 22 test functions. Strict string literal casts fold at construction;
ordinary column casts retain source-specific scalar/vector warnings. These are
dependent expression/integration receipts only: hybrid/binary literals, other
expression vectorization and whole-package acceptance remain open.


## Dependent package inventory: executor/aggregate

Pinned revision: `aba629bb455dc09d6a5d98b3c39a542bb1189b9d`.
Status: **not accepted as a complete package**. This is the full tracked
package-artifact inventory, not a claim that all listed implementations have
been audited. Stream grouping and requested-row regressions are integration
evidence within the complete aggregate package's open acceptance unit.

| Go artifact | SHA-256 | Audit / validation decision |
| --- | --- | --- |
| `pkg/executor/aggregate/BUILD.bazel` | `5450ae313fbd6ef1219b028252c1c2f5b73e9431c3125dcdead70a2c48640701` | Seven production sources, original spill test and dependencies inventoried. Native Cargo targets build; no Bazel edits. |
| `pkg/executor/aggregate/OWNERS` | `10e8453eaa4a21fbbf499614fc16a1e8228c8c9ea8a72034ca2de3abc48073f6` | Original ownership/support artifact retained in Go tree; no runtime translation. |
| `pkg/executor/aggregate/agg_hash_base_worker.go` | `e884b69d72e2d23ce90938f65467d74b8b45eff6ba93d7d499dd052e8d0ab210` | Inventoried; complete source-to-Rust audit and acceptance remain open. |
| `pkg/executor/aggregate/agg_hash_executor.go` | `56ba7694ea940016aee244fbed346cd4652c0139a7dcf35e89c25aebb556ff06` | Inventoried; complete source-to-Rust audit and acceptance remain open. |
| `pkg/executor/aggregate/agg_hash_final_worker.go` | `da79acfaa3496b984aedce52e83a98f3ff84593a76ab8e783527dfb7ab1181e4` | Inventoried; complete source-to-Rust audit and acceptance remain open. |
| `pkg/executor/aggregate/agg_hash_partial_worker.go` | `b4f146d79d722b2e3bac9fbfaacf47141458e2e2459701b9bccb626b1967cacc` | Inventoried; complete source-to-Rust audit and acceptance remain open. |
| `pkg/executor/aggregate/agg_spill.go` | `a5c2a1622d6545e2601b20fe6707da6f0fed977c5a35ac7f9336faf5cae105de` | Inventoried; complete source-to-Rust audit and acceptance remain open. |
| `pkg/executor/aggregate/agg_spill_test.go` | `3588491bfd671bf6816601a7f0bdc7408786c118fa3ad239ce9ed8dfe0138d9d` | Original test artifact inventoried; full failpoint-aware acceptance remains open. Rust hash-aggregate tests are supplementary, not a replacement claim. |
| `pkg/executor/aggregate/agg_stream_executor.go` | `7f2e19f01404894bbc47a6c6aba1d9496ca7d047892ca208822694d315d92502` | Grouping and Next output limit reviewed against native GroupedStreamAggExec. Memory tracking, aggregate evaluation/error ordering and injected-failure contracts remain open. |
| `pkg/executor/aggregate/agg_util.go` | `80dec716958aeee8be572334ea947d1ce9f04720fa8b6d915c5b95883fb8feb7` | Inventoried; complete source-to-Rust audit and acceptance remain open. |

No package doc.go, build-tag/platform variant, generator directive, embedded
fixture or separate package testdata directory is present at this pin.
The separate SQL test package `pkg/executor/test/aggregate` and its support
harness are also required integration evidence before full acceptance. This
checkpoint ran a temporary SQL oracle in the existing windows test harness;
it did not run the aggregate package's failpoint-dependent original tests.


## Dependent package inventory: executor/join

Go baseline `aba629bb455dc09d6a5d98b3c39a542bb1189b9d` is unchanged by Rust
checkpoint 7c79401941. The entire package remains **unaccepted**. This manifest
covers all 47 top-level package artifacts. The merge-input grouping
fix is integration evidence, not a partial-package completion claim.

| Go artifact | SHA-256 | Current audit decision |
| --- | --- | --- |
| `pkg/executor/join/BUILD.bazel` | `667717f219b0f272b8ddff27f5e4b5d65e3f0ed690d2c63c5b6ed7eb9301251c` | Build/test source manifest retained; no Bazel edits. Full package gate remains open. |
| `pkg/executor/join/OWNERS` | `10e8453eaa4a21fbbf499614fc16a1e8228c8c9ea8a72034ca2de3abc48073f6` | Ownership artifact retained; no runtime translation. |
| `pkg/executor/join/anti_semi_join_probe.go` | `7549d16ba1cbacadb421b92a733cbff725761d5bdb8f71e1e4282aeb78bc80d3` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/anti_semi_join_probe_test.go` | `e982dc946421117a3ba5af0af26f40066d97719b5cc65baf03ab219e05407132` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/base_join_probe.go` | `182a67ee39e1a6a089733d6e4744d2f78a440a2d6a409df80d00c07aca48dd6a` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/base_semi_join.go` | `0dd05407bfd38043f3b283eef671f44600f3c6b39a5a0d1cc341f70ba9615958` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/bench_test.go` | `90428e0e9cd435827e24799fa00d1d3b4c8d2af9dcab8005207683ee82aa8824` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/concurrent_map.go` | `e937a742aed26360b442a10a6a74cc4f21dd780a69938515d23d3e8b7b3da6b9` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/concurrent_map_test.go` | `247943795e48c11be7e451402a2409d3c919dd81d73c14ea833315784ee9b389` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/hash_join_base.go` | `e338c587135d53e506acfff49b09dd9d22232d8f5d288343fb1bb16787e4a854` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/hash_join_spill.go` | `47314a599375d3dd3e8f16f35e07d312d9d4e4a1a04c2011c4f3582cdd91bd56` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/hash_join_spill_helper.go` | `5cc9461a92150cb7b98f3370a7eae6bc2335ec2063c5749a6c4f817b4191942d` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/hash_join_stats.go` | `6d9cc03e902536539a573f8579d46fd71efbb1816407d84e128aeda94c560f63` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/hash_join_test_util.go` | `77be044b1a3b77bf249ea45bade55e4a5cfcbc6b7d49c06ee65affcd86903996` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/hash_join_v1.go` | `e2f16d8afcb24f01a2d6727658176dff625d3bc0362500ea373da123f958d78b` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/hash_join_v2.go` | `d8d44bac60b23b1d8c8c52a730ab5120e0a9c603eab7356527cce98473ec94e8` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/hash_table_v1.go` | `380a503ebb9cd82678fc93f4268f71d378fcbacf4101318521276a18de85c50f` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/hash_table_v1_test.go` | `b9375d83fd127c7405bc1029781c7ece2e2d650ce89749d926efbc39587fc84d` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/hash_table_v2.go` | `58db3be848015b58be6b7b567ef8d7a60c65175c9db635ef9d939a308288424d` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/hash_table_v2_test.go` | `e622f1e4cc0c89b1bf5922a8905a1a1e11b2f660da7691ae31a85e3477b0df83` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/index_lookup_hash_join.go` | `b224dcc8efff944e5c8b44d4643e5937c6d80b33c2d94ed3191d8e51d7e6871d` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/index_lookup_join.go` | `11f3d11fb0f391a102db34b7df2c66c1d743f95e48bdf3d3df90adbf59ee7e9e` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/index_lookup_merge_join.go` | `7d37845a23d3f4602396b8cbd5154e78e7d25c7f9dc2b11d968dd8eb0319abde` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/inner_join_probe.go` | `0308293a62beef3e097baad0625afb78e2e243c32ac61700ed06779496c88327` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/inner_join_probe_test.go` | `8da9e7fe2e35d59e82867fc286b094bb0da0d3675e066f78ee666ef07e34b06f` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/inner_join_spill_test.go` | `a9495e679c5691fb2fb7b9e91284892c36900372674b663e670889f16a1b731c` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/join_row_table.go` | `78e9dab50e24e4ef39efe49607eac5e1050c57b909671f19bc7d85288a581be3` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/join_row_table_test.go` | `056e4530e08729caebd939a7990dd9a79e949c51c0ade2a22b3e1502ec5c7b43` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/join_stats_test.go` | `930df8e3932a099749ce39600e5a5b6156e8679e7b7cd890a58d12ee09387ff8` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/join_table_meta.go` | `fd76aca85482ffa674cf14b6c1133be6829679b87c831a62a12ea23385b602e5` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/join_table_meta_test.go` | `72dc6ff4cad8a0934130cf7c1d8f1c4bbb481e1bcab2c01a2eec0a33f0cff1dc` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/joiner.go` | `0e168313310016d23c11481f73066b9f298cdb2d3f8d5592d0f00e101b8f5049` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/joiner_test.go` | `6c9380a35fcffccba28ac097734abe155f4fb6866b4fc5067d9ec34d81dc42e3` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/left_outer_anti_semi_join_probe_test.go` | `f3212f9a3b02ff1d1b976d77d7495896ec32435c34220dcd066cd36e4ad63115` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/left_outer_join_probe_test.go` | `38cf71006d4e16c796fff2956fbb95871340fcdd108896b301b4f268d52ed4ca` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/left_outer_semi_join_probe.go` | `f3b99488002825a6efde3c09ba29fb8247932a4d8c8ef0455dd87e1ddd58ae62` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/left_outer_semi_join_probe_test.go` | `c0ac78502f369fa8c38c6a3659eac015910b1f633f337077a6efbd6360168ed2` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/merge_join.go` | `3cfa59ee958c23df7db847a1da714df207e7d1795def4dcf0045d997ba3783f1` | Input grouping, NULL skipping, chunk/container handoff and output continuation reviewed. Rust join.rs now uses shared VecGroupChecker; whole executor contracts remain open. |
| `pkg/executor/join/outer_join_probe.go` | `477b1623dd4b98bbba31fe512a6b96722b5a9b21f258cd081c9bd06344604ef4` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/outer_join_spill_test.go` | `7a35a444358900f80fdb67a2acc123f16e50a992b64875f0f228b87c23cc10b3` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/right_outer_join_probe_test.go` | `6152cdc7b55e4008bce57a4e1fdfb6049862f48fa5c9f9c9a7d5a8c175a259f1` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/row_table_builder.go` | `02fa7160e4ddc4a81139d02ff0085e575371d8cb684aee084f818ad3d93ef364` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/row_table_builder_test.go` | `1c9d460881f0f4d1f20119aeee6065d80fd6f2bbdc804f21873ea2a924eb934d` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/semi_join_probe.go` | `d5bcd8fe7060779419fb5bb42a734648f3d753d8412d3995d73ca972e6998bfd` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/semi_join_probe_test.go` | `be18aee2921869453fa0c6ab76228781f364924b5ee749ef6e4ddfaac523356e` | Original test artifact inventoried; complete original-test mapping/gates remain open. |
| `pkg/executor/join/tagged_ptr.go` | `bbeeef94519a718ca615d2249759c7ffc45f68f43d52bc338e44742ecd137144` | Inventoried; complete source-to-Rust audit and validation remain open. |
| `pkg/executor/join/tagged_ptr_test.go` | `9612f1486aae58a3f4abc9212cc93f94efae2f7017038bc3bf635782ece5a220` | Original test artifact inventoried; complete original-test mapping/gates remain open. |

No doc.go, build-tag/platform variants, generator directives, embedded fixtures
or package-local testdata were found. Nested joinversion is a separate Go
package. Separate SQL test packages test/mergejoin and test/indexjoin include
their own BUILD.bazel and original SQL test sources; their complete validation
is required before claiming the corresponding executor integration accepted.
The current SQL oracle uses the existing windows test harness. The join
package's failpoint-dependent tests have not been run or counted here.
