# TiDB Jsonnet Migration

## Plan
| Row | Title | Status | Verification | Commit |
| --- | ----- | ------ | ------------ | ------ |
| 1 | Query Summary | done | targets/descriptions matched | |
| 2 | Query Detail | pending | pending | |
| 3 | Server | pending | pending | |
| 4 | Transaction | pending | pending | |
| 5 | Executor | pending | pending | |
| 6 | Distsql | pending | pending | |
| 7 | KV Errors | pending | pending | |
| 8 | KV Request | pending | pending | |
| 9 | PD Client | pending | pending | |
| 10 | Schema Load | pending | pending | |
| 11 | DDL | pending | pending | |
| 12 | Dist Execute Framework | pending | pending | |
| 13 | Statistics & Plan Management | pending | pending | |
| 14 | Owner | pending | pending | |
| 15 | Meta | pending | pending | |
| 16 | GC | pending | pending | |
| 17 | Batch Client | pending | pending | |
| 18 | TopSQL | pending | pending | |
| 19 | TTL | pending | pending | |
| 20 | Resource Manager | pending | pending | |
| 21 | Follower Read | pending | pending | |
| 22 | Import Into | pending | pending | |
| 23 | Global Sort | pending | pending | |
| 24 | Network Transmission | pending | pending | |
| 25 | Memory Arbitrator | pending | pending | |

## Notes
- Generated JSON should be written to `pkg/metrics/grafana/tidb.new.json` using `pkg/metrics/grafana/generate_json.sh` as a reference for jsonnet invocation.
- Record verification details per row in the table and summary below.

## Verification Log
- Row 1 (Query Summary): targets and descriptions match tidb.json; generated via jsonnet and compared against original for this row.
