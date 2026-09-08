# Ranger callsite follow-up audit

Authority: Go f5cf8f6337612c6ae51fb6e384e4bb3469dde680; Rust dbe2add614.
This follows the complete ranger/context and core/rule package inventories.
Other owner packages require their complete inventory before production edits.

| Rust callsite | Observed quota | Go source decision | Status |
| --- | --- | --- | --- |
| logical/rule_max_min_elimination.rs | session quota + handler | rule_max_min_eliminate.go:88 | Published batch |
| physical_plan_cache.rs rebuilds | 0 | plan_cache_rebuild.go:204,265,333,424 uses 0 | Preserve exemption |
| driver/planner_bridge.rs partition_indices_for_spec | 0 | rule_partition_processor.go:168,284,1334 uses session RangeMaxSize | Fixed and SQL-validated in current batch |
| driver/planner_bridge.rs locate_list_columns_condition | 0 | rule_partition_processor.go:752 uses session RangeMaxSize | Context threaded and recursive SQL regression passed |
| find_best_task/dispatch.rs common-handle and index enumeration | 0 | stats.go:461 uses session RangeMaxSize | Owner package audit required before edits |
| logical/rewrite.rs index selectivity detachment | 0 | Statistics caller contract not yet established | Do not change speculatively |
| driver/planner_bridge.rs access-cost index detacher | Executor AST helper | Different representation; source correspondence pending | Unresolved |

Partition and index wrapper options differ: partition sets convert_to_sort_key=false
and merge_consecutive=false. A handler-aware partition entry must retain both
settings, not route through the index wrapper. Cache rebuild's explicit zero
must not be replaced by the session quota. The two physical enumeration sites
need source-owned context propagation rather than a global replacement.

A new static HASH partition SQL regression sets quota 1 and selects IN(1,2),
requiring unchanged rows and a capacity warning. It failed with empty warnings in /tmp/partition-quota-red.log. After adding the
partition handler entry and context propagation it passes in
/tmp/partition-quota-green.log. Expanded partition SQL tests (4), ranger tests (64), partition rule tests (2),
and make lint now pass; see the ExecPlan for exact commands and logs.
