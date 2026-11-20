# Partition split storage prototype

This executable experiment follows the [partition global-sort design](../../docs/design/2026-11-02-partition-global-sort.md). It uses real Classic TiKV transactions, TiDB row/index encoders, production `simplesst` sorted runs and merge iteration, and real ImportSST `Write`/`MultiIngest`. The online-index follow-up also reuses production temp-index value encoding, b/m version markers and `FilterOverwritten`, with a standalone merge adapter. It is an isolated storage harness; SQL `REORGANIZE PARTITION` still uses its existing implementation.

From repository root:

```bash
GOMAXPROCS=2 go test -p 2 -tags=intest,deadlock ./tools/partition-reorg-prototype -run '^TestPrototype' -count=1
python3 tools/partition-reorg-prototype/run.py --rows-per-parent 1000 --workers 4
```

The Python standard-library runner builds the Go tool, starts its own one-replica PD/TiKV cluster on unused loopback ports, checks readiness, runs the experiment and stops its processes in `finally`. It defaults to installed `~/.tiup/components/{pd,tikv}/v8.5.8` binaries; use `--pd-binary` and `--tikv-binary` for other installed Classic binaries. It does not connect to an existing user cluster. The Go tool also checks the exact cluster ID supplied by the runner.

Each invocation writes `.codex-tmp/partition-reorg-prototype-*/result.json`, version information, the exact command, component logs, and sorted-run files. Cluster data is removed after shutdown unless `--keep-data` is supplied. `--binary <path>` reuses an already compiled tool. Limits are 16–100,000 rows per parent (200,000 total), 1–16 sort workers, and 1–10,000 requested background updates per index group. `--dml-iterations` controls the latter; successful runs complete all requested updates and verify each handle's terminal value. `--timeout-seconds` sets a 1–3,600 second bound (default 300). `--split-regions` pre-splits the owned cluster by physical namespace, exercising multiple ingest Regions. The harness holds input/merged KVs in memory for independent byte comparisons; it is not a large-data benchmark.

The fixed RANGE topology is `[0,100), [100,200)` split into `[0,50), [50,100), [100,150), [150,200)`. Both old parents deliberately contain the same implicit handles. Row values include a 512-byte payload so record import exercises both default and write CFs. Boundaries, IDs, and generation numbers are integers throughout.

The experiment performs these checks:

- Scan the source at a fixed timestamp, then commit deletes, updates, inserts and a cross-parent move. Source and DirtyRows endpoints change in the same transaction; rollback exposes neither change.
- Produce overlapping sorted runs on disk with multiple workers, globally merge them, reject duplicate keys, validate every output namespace and compare the merged bytes against the input.
- Import the record baseline through one-shot RPCs. Persist Intent before sending, and receipt after success. Import every CF returned by Write. An unfinished phase cannot be restarted or resent by this executable.
- Repair dirty keys from the current old parent. Deterministically overlap an open repair transaction with a foreground commit; require a real TiKV write conflict. Close the repair gate while another transaction is open; require conflict and reject new repair attempts.
- Compare the complete new record KV set against the current source projection, including absent keys. Build two local and two global indexes while bounded background DML continues. Persist Inactive → Running → Merging → Direct state per group; capture b-version temp events during baseline, double-write normal/m-version entries during merge, and maintain completed groups normally. Global indexes use the existing V1 partition-handle encoding. Decode stored entries and look up their row by handle/PID to check the encoding in the read direction as well.
- Exercise unique-key A → B → delete B → E reuse, including repeated inner handles in different PIDs, NULL/non-NULL transitions and cross-partition moves. A canonical fixture uniqueness guard rejects business duplicates; incomplete shadow indexes do not enforce business constraints. Check rollback of source/shadow/constraint/normal-index/temp-index KVs together.
- Overlap real TiKV transactions with Inactive → Running, Running → Merging and Merging → Direct changes. Require conflicts for stale DML or merge attempts. Concurrent foreground writes conflict with temp merge; retries use fresh transactions. Completed local indexes must remain correct throughout global construction.
- Independently check the intended logical identity set. INSERT explicitly treats a proposed handle as an allocation candidate, even if a prior move has occupied it. An early fixture used an upsert for this case and lost identity 3 while source/shadow equality still passed; the logical oracle now detects that error.
- Atomically publish an MVCC layout record only after phase sealing and a durable row-verification certificate. Bind layout and data reads to the same timestamp. Retain an old-TS/new-PID negative control that returns no target rows.
- Actually apply a later generation's SST, deliberately lose its successful ingest receipt, and require a pending Intent and permanent quarantine. Reject publication even though its records exist. Rebuild with fresh PIDs/global IDs, publish, and retain reads of the earlier published layout.
- Reject import replay on a published generation without sending another RPC. Verify that the rejected call leaves the published state intact.

The core Go executable still uses the independent client-go adapter. It now also exports `output/merge-fixtures.json.gz` before each index group merges: a single-TS snapshot of actual row, normal-index and temp-history bytes. A frozen in-memory adapter replay and physical-row-derived indexes must agree before the snapshot is written. An optional runner mode invokes the production DXF merge step on these same bytes in another fresh owned cluster. It does not enable SQL REORGANIZE or a DXF scheduler task.

The phase coordinator and layout record belong to this harness. They are not DXF task scheduling, TiDB metadata, MDL, or a SQL DML hook. IDs come from a fixed test namespace; hidden-handle collision tests use bounded fixture linear probing, not the TiDB allocator. Rows explicitly use the no-checksum codec mode. Every foreground mutation writes coarse per-group state keys, including Inactive groups, so phase changes invalidate old mutation transactions. This deliberately serializes writers and substitutes for schema/MDL barriers only within the experiment. Group starts occur before baseline snapshots; ordinary SQL queries are not served during the harness. There is no GC worker in the owned cluster. The tool does not implement RR own-write admission, pessimistic allocation, source GC leases, the SQL online-index mutation/merge integration, unaffected partitions, general SQL admission, owner failover/resume, quarantine reclamation, CDC, or multi-replica ingest. The loss injection happens after a real successful apply; it does not simulate a delayed server apply after cancellation. Future SQL integration must satisfy those parts of the design separately.

The [ExecPlan](../../docs/plans/2026-11-10-partition-reorg-prototype.md) records validation, observed failures, and promotion criteria. A passing report establishes that the storage primitives compose for these cases; it does not certify online SQL correctness or a speedup at hundreds of GB.

Latest online evidence is recorded in `.codex-tmp/partition-reorg-prototype-vky_52eq/result.json`: 8,192 seed rows plus six fixture rows, 82 background commits, 288 sorted files, 15 passing checks, 2 merge conflicts and 6 phase conflicts. Counts of background commits vary with scheduling. The small empty-range case also passes. Exact commands and the failing-before-fix logical-row regression report are retained in the ExecPlan.

Production merge replay uses same-package tests in `pkg/ddl/partition_merge_proto_test.go`. The default matrix runs on unistore. To compile instrumented test binaries and replay an exported snapshot on real TiKV:

```bash
mkdir -p .codex-tmp/partition-production-tests
GOMAXPROCS=2 ./tools/check/failpoint-go-test.sh pkg/ddl -c -p 2 \
  -o "$PWD/.codex-tmp/partition-production-tests/" . ./tests/indexmerge
GOMAXPROCS=2 python3 tools/partition-reorg-prototype/run.py \
  --production-test-binary .codex-tmp/partition-production-tests/ddl.test \
  --replay-fixtures "<prototype-run-directory>/output/merge-fixtures.json.gz"
```

The replay rejects non-loopback endpoints, a mismatching cluster/keyspace UUID, and any initially nonempty cluster before clearing or seeding fixture ranges. The snapshot reader accepts legacy JSON and gzip JSON, bounds decoded input at 1 GiB, and rejects corrupt/trailing/oversized input. The matrix checks batch sizes 1/2/16 and CPU allocations 1/2, b/m/d versions, unique reuse, NULL, duplicate rejection, row immutability and repeated subtask execution. Failure tests inject an error after index/temp mutations have been staged, require all bytes unchanged, then replay successfully. A foreground commit also overwrites an in-flight worker's write set to test actual production conflict retry.

The runner enforces Go build `-p 2` and GOMAXPROCS=2. Replay is a frozen-snapshot compatibility test, not continuous foreground load inside the production step. SQL mutation/planner/MDL integration, generation fencing around that step, full DXF scheduling, real GC and multi-replica storage remain separate requirements.

2026-11-12 production replay evidence: the final unistore matrix passes 147 cases / 412 production subtask executions. The real TiKV replay of the 8,192-row prototype snapshots passes 15 cases / 124 production subtask executions. Six existing SQL ADD INDEX corner-case regressions also pass. Reports and exact commands are recorded in the ExecPlan; both producer and replay clusters report successful shutdown. These are functional tests, not performance measurements.

Scale examples (all checks remain full-data checks):

```bash
python3 tools/partition-reorg-prototype/run.py --rows-per-parent 20000 --workers 8 \
  --dml-iterations 1024 --split-regions --timeout-seconds 900
python3 tools/partition-reorg-prototype/run.py --rows-per-parent 100000 --workers 16 \
  --dml-iterations 4096 --split-regions --timeout-seconds 1200
```

The tool validates index-to-row lookups using bounded BatchGet rather than one RPC per entry. It still materializes test data and oracles in memory; the runner defaults Go's soft heap target to 3 GiB. This is a bounded correctness harness, not a streaming implementation or a hundreds-of-GB benchmark. The row cap keeps seed handles/identities disjoint from the reserved scenario namespace. Source changes after S still use the same protocol.

Multi-Region lost-receipt testing validates the exact nonempty subset that actually applied, permanently rejects publication of that generation, and requires a complete fresh-generation rebuild. It does not demand that unsent Regions have data. The common retry helper handles only confirmed write conflicts, uses short context-bounded backoff, and never retries an unknown commit outcome or SST RPC.

Latest larger-scale results and limitations: [200,000-row validation report](../../docs/plans/2026-11-12-partition-reorg-scale-results.md).
