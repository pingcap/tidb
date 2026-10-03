# Statement observation, persistent startup and TopSQL counters batch

Go master: `93a01d31f6da205ae4bf376825293903a6899fdb`. Source commit: `a7bb8c3e074dc70a41193f67337f86510c501723`. Native master remains `19a56ccda1e128218cd33c69709038219aced9bc` and is unchanged. See [validation](statement-observation-batch-validation.json) and [living ExecPlan](../../statement-observation-batch-execplan.md).

## Rechecked findings and delivered behavior

O18, O11 and N03 were source-rechecked together; their disconnected live producers and unchecked persistent startup were still true. Real completed SQL now feeds the existing summary and StatementStats owners, without fabricating records in readers. Session close retires counters; both store runners share process worker startup and joined shutdown. The canonical SummaryStmt gate retains disabled/internal/PREPARE/COMMIT policy. SQL-level and binary prepared execution use retained SQL attribution. Ordinary text and binary-prepared DML wait for the final outer storage verdict, including retries, instead of publishing scratch success. Streaming records close after transaction completion. Routed SET/DDL/accounts/statistics and transaction controls publish once; routed metadata cannot inherit the preceding query's binary plan.

The full 127-column current/history schema reads the existing v1 memory or v2 memory/disk owners with user/PROCESS filtering and fixed-offset session timezones. Persistent mode refuses the cumulative reader with Go's error 1235. A usable writer is initialized before workers/global ownership; an invalid sink disables persistent mode through shared configuration and serves v1. Successful shutdown flushes JSON records. All five read-only instance getters read the live shared config, including ON/OFF fallback state and filename/rotation settings.

## Validation

Five distinct Rust regressions fail before and pass after: missing live summary records, missing TopSQL execution counters, accepting a directory as a sink, retaining persistent mode after setup failure, and five empty live instance getters. Old-server wire retrieval/cumulative publication fails; intermediate wire tests separately exposed prepared digest attribution and empty instance settings. Compile failures and incorrect test drafts are excluded from red/green evidence.

**306 distinct Rust cases pass**: 172 owner cases, 67 session cases and 67 server transaction/prepared cases. Follow-up runs are duplicates. The actual server storage-rejection regression covers both text and binary prepared writes, asserts one recorded error after scratch success, no durable row, and exactly one subsequent successful completion. Affected six-crate all-target checks, make lint (five Python cases separate), standalone locked server build and the actual normal source-commit precommit locked build pass.

Real MySQL/unistore checks pass separately in memory, persistent and failed-sink fallback modes: all 127 columns, current/history selection, SET/DDL success and error counts, text/named/binary prepared DML, binary prepared DDL attribution, COMMIT predecessor, all five instance getters, cumulative 1235 policy and persistent shutdown JSON flushing. These are single-node checks, not live multi-node acceptance.

## Stale-test maintenance

Replace the injected cumulative record fixture with two real authenticated SQL executions and retrieval. Correct three stale assertions against current owners: all six system schemas, mysql.password_history enumeration and arrival order for unordered text/prepared index-merge results. Behavioral plan/index/row checks remain. No meaningful original obligation is ignored or disabled, and no removed fixture is counted as passing coverage.

## Status and remaining scope

**86 tracked: 28 repaired, 58 unresolved (42 open, 16 partial).** O18 and O11 advance from open to partial; N03 remains partial. No full finding closes in this batch. The other 55 unresolved IDs retain their recorded evidence; they were not freshly reproduced here.

Remaining: Plan digests/encoded plans and complete statement table/phase/RPC/RU/CPU/network/retry/write-response/keyspace-ID measurements; Explicit restricted SQL / EXPLAIN EXPLORE context and complete evicted/cluster retrieval owners; TopSQL SQL/plan registration, profiling, KV/RU/network attribution, generated TiPB transport, sinks and reporter lifecycle; Native startup defaults, TLS policy A03 and other N03 runtime consumers; Complete upstream Go package inventories/platform/generated/test/fixture acceptance, full Go suites, live multi-node TiKV/PD/TiFlash and workload performance. Empty measurements do not establish parity. No complete upstream package, performance improvement or multi-node/platform acceptance is claimed.

## Cloud retention

Source/build work stays in Codex Cloud. No push was attempted under the user's latest instruction. Preserve exact intended destinations and concurrent commits, the local recovery bundle and installed filesystem snapshot. Actual hooks remain enabled. A future explicitly authorized push still requires a fresh locked server build immediately before each push. The reusable draft is saved separately; saving does not publish or prove fresh-task restoration of unpublished commits.
