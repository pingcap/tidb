# Physicalop source inventory

Audit revision: `aba629bb455dc09d6a5d98b3c39a542bb1189b9d`.

Status: **incomplete; no whole-package parity claim**. This inventory records
every tracked artifact in the upstream package. Hashes identify source bytes,
not behavioral coverage. All artifacts remain pending whole-package acceptance;
the focused regression receipts in the ExecPlan do not discharge that gate.

## Package artifacts

| Artifact (relative to `pkg/planner/core/operator/physicalop/`) | Role | SHA-256 |
| --- | --- | --- |
| `BUILD.bazel` | build metadata | `4d46d3519ce30e351ebaad439cce82aefcf8852ebb17181ef1b347ecffe2a42e` |
| `base_physical_agg.go` | production | `f1b5b22be1bb6bf81bd6c26e374ca541d9d3e19dec2131b52b5fe10f046bb250` |
| `base_physical_join.go` | production | `d852de5c0d2d779c792edf00336bcc6463deae55f0869dea760001774dd49039` |
| `base_physical_plan.go` | production | `fac3b9fcd4a51241af5a53d875d1d9696c2c345b35794a3aa7324be174bcbfda` |
| `enforce.go` | production | `056e1c6ccad302f24e2dc35cc26ab136bfcd584a48d9f52b29c2dc86ad03ea5a` |
| `foreign_key.go` | production | `29414c494148f5fe9dcc17741317ac8541284b4a0c0b0818e36c132ed014f549` |
| `fragment.go` | production | `8d6aabb499a84136e098bd02cc55ec2f1642efdd5a16b67c0e8f2ae87110d373` |
| `fragment_test.go` | original tests | `b7f9622583def1133d023512f01de1124b9116f381aad532caeadb3e052d1f41` |
| `nominal_sort.go` | production | `e92994942d6b21bd7a3403a80fec6f600ef77d065352447b3032aeb37cdcc34d` |
| `physical_apply.go` | production | `8a7cfce083b8d2dc4c3b4cda95ef653415d18c099dbacda730b6ea78512a4daf` |
| `physical_batch_point_get.go` | production | `af1fbc5b0c66747ba2314645269ea6eb82894f4c5ef3a448335e8cc6244e44c7` |
| `physical_common_plans.go` | production | `b1a0a4b76eb18951fbb4c536aa3710f6b02534b17a92b46950c89214d7c64a68` |
| `physical_cte.go` | production | `6223410e99a11ab15400dc218786c4cb4d300ea58370a461704c5b86217401f9` |
| `physical_cte_table.go` | production | `4ca4d93c9ebbeda3e37eea7d656f2a424eb959a46f162e265ae2d7bdbad4e1b1` |
| `physical_exchange_receiver.go` | production | `dbac31e1b885c149dd7cef0bd654384e48a2124bfe23ef811263697c2421f64f` |
| `physical_exchange_sender.go` | production | `f6bf05352b37a278f3550b14a6eb33af87d5c65b8df9b2f3fd18f47dd03cc048` |
| `physical_expand.go` | production | `cd88b89d960100563aa780b5d4c9b5bc78a7354861c00849768adead297b4ea0` |
| `physical_hash_agg.go` | production | `2d3d1ff0a9a5eaf27d8f3be9f1794d072c614ccf5a2bfb45e57eea0702ff690e` |
| `physical_hash_join.go` | production | `71e1c5b255bf31dc344da673d6581353f0d9278e51bf437ff7e57cdd9ad58e86` |
| `physical_index_hash_join.go` | production | `d83fbe5afadaa2a854f28335104eee5526b77ac97882892e20199a5d202a30a3` |
| `physical_index_join.go` | production | `fa9f0e8bdc8f05e996f86dd07e67e5f4fb5950841fb6f93d3171f9c092c416d9` |
| `physical_index_merge_join.go` | production | `ba15d3255af623a23474a708890ee1ae7eef436d5c1a25e7c5f82d02e2ccf471` |
| `physical_index_reader.go` | production | `975e2c2c596c4ce699db1e66664402f9631bfdb52aaedafd8f4d01838c6a050b` |
| `physical_index_scan.go` | production | `289009d9709fa748bc9a1dc95b33f72b09860fe56e73b93273b187734446e653` |
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
| `physical_utils_test.go` | original tests | `9cd537e41d93212bef96d024b2566035e2a7c7c049df05d9fff42e9cb803399b` |
| `physical_window.go` | production | `16aae9ab1fc41dbfd03e23b2d5b4e999d94c404b20f2cefd74417014f5568189` |
| `plan_clone_generated.go` | generated production | `ee5deff27d09c8f57ed5aba9079adba04c8350296ce45c4f8169c460e77ecd50` |
| `single_scan_index_join.go` | production | `bdda651f445f64317fcef63777f0e85e6ad72aa2d0974bca62afa442a8ed630f` |
| `storage_engine_usage.go` | production | `3db1feeaefd930d2326f1ece8ee07000ab9e2650371b6756bd9f33fea0c6d454` |
| `task.go` | production | `76da2a7be7f39141f53ff556133b91909ae89dbc56d17264119467dbeb42e169` |
| `task_base.go` | production | `04940c451d9ed9cf823a84535becdc7aecc29c8df892725cc7d874dea395185c` |
| `tiflash_predicate_push_down.go` | production | `4dfe67be2920170614d800a906ac026242c9a6f6bd9fdbe74e418b78cb06920e` |

The package has 53 handwritten production Go files, one generated production
Go file, two original test files, and one Bazel build file. There is no `doc.go`,
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
| `go.mod` | `a2f0229f01a3156b8ff95ef39b557236125435d91200429856ff81ed10c7b7ac` |
| `go.sum` | `833b6f2127500f40eb2d9e76bcd472869221c4ac93fdc1065f176c99568a879b` |

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
| `pkg/executor/internal/vecgroupchecker/vec_group_checker.go` | `25e9f350932453ed382742d805b50e056021e221aa5f2df2fbbf63b31f720bfe` | tidb-executor/src/vec_group_checker.rs; window normal/pipelined, shuffle range splitter and GroupedStreamAggExec call production split_into_groups. The stream duplicate hash-key implementation is removed; its integer-cell fast path is now shared. Merge-join integration and complete typed evaluation/error contracts remain to audit. |
| `pkg/executor/internal/vecgroupchecker/vec_group_checker_test.go` | `ac1cce0c1ae1eb2a8be2fc69cc6b9a20332dd4a898c453b26326c339c0299795` | Four original tests map to tests_executor_internal_source.rs: datum ownership, group-count matrix, collation/padding, reset. Ports now call production chunk evaluation. Additional local fixtures and warning tests are in vec_group_checker.rs. |

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
metadata is owned and fixed; collation variants rebuild the checker instead
of mutating a shared Go FieldType pointer. These decisions do not establish
complete expression vectorization or whole-package acceptance on their own.


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
