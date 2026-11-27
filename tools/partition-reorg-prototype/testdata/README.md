# Negative admission fixture

`retired-merge-negative.json` intentionally invokes the production merge step outside its normal lifecycle: Public/Inapplicable metadata, deleted rows/indexes, and a stale backfill put. The required result is no write. The current unguarded step instead produces an orphan index entry.

This is fault injection, not a normal exported snapshot. `adapter_verified=true` deliberately bypasses the harness's provenance gate; it must not be cited as successful adapter equivalence. The `fault_injection` field records this explicitly. Use only the runner's newly owned, empty cluster.

Expected current result: exit 1 and six failed batch/CPU cases. See the ExecPlan for evidence and the outstanding transactional-fence requirements. This does not establish that existing ADD INDEX scheduling can reach this state.
