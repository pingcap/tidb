# TiDB Dashboard Migration Plan

This document tracks the migration of `tidb.json` to `tidb.jsonnet`.

## Overview

- **Source**: `tidb.json` (793 KB, manually maintained)
- **Target**: `tidb.jsonnet` (programmatic dashboard definition)
- **Reference**: `tidb_summary.jsonnet` (pattern to follow)

## Migration Progress

| Row # | Row Name | Panels | Status | Commit |
|-------|----------|--------|--------|--------|
| 1 | Query Summary | 12 | ✅ Done | 47fc2affb1 |
| 2 | Query Detail | 7 | ✅ Done | 827cec6c41 |
| 3 | Server | 23 | ✅ Done | 9c6e6d4abf |
| 4 | Transaction | 35 | ✅ Done | fd02c6ad66 |
| 5 | Executor | 16 | ✅ Done | TBD |
| 6 | Distsql | 8 | ✅ Done | c36c24b8db |
| 7 | KV Errors | 5 | ✅ Done | b536b3d6e1 |
| 8 | KV Request | 16 | ⏳ Pending | - |
| 9 | PD Client | 15 | ⏳ Pending | - |
| 10 | Schema Load | 11 | ⏳ Pending | - |
| 11 | DDL | 14 | ⏳ Pending | - |
| 12 | Dist Execute Framework | 7 | ⏳ Pending | - |
| 13 | Statistics & Plan Management | 17 | ⏳ Pending | - |
| 14 | Owner | 2 | ⏳ Pending | - |
| 15 | Meta | 4 | ⏳ Pending | - |
| 16 | GC | 11 | ⏳ Pending | - |
| 17 | Batch Client | 4 | ⏳ Pending | - |
| 18 | TopSQL | 6 | ⏳ Pending | - |
| 19 | TTL | 17 | ⏳ Pending | - |
| 20 | Resource Manager | 2 | ⏳ Pending | - |
| 21 | Follower Read | 3 | ⏳ Pending | - |
| 22 | Import Into | 5 | ⏳ Pending | - |
| 23 | Global Sort | 6 | ⏳ Pending | - |
| 24 | Network Transmission | 1 | ⏳ Pending | - |
| 25 | Memory Arbitrator | 8 | ⏳ Pending | - |

**Total**: 25 rows, ~228 panels

## Panel Details by Row

### Row 1: Query Summary (12 panels)
- [ ] Duration
- [ ] Command Per Second
- [ ] QPS
- [ ] CPS By Instance
- [ ] Failed Query OPM
- [ ] Affected Rows By Type
- [ ] Slow Query
- [ ] Connection Idle Duration
- [ ] 999 Duration
- [ ] 99 Duration
- [ ] 95 Duration
- [ ] 80 Duration

### Row 2: Query Detail (7 panels)
- [ ] Duration 80 By Instance
- [ ] Duration 95 By Instance
- [ ] Duration 99 By Instance
- [ ] Duration 999 By Instance
- [ ] Failed Query OPM Detail
- [ ] Internal SQL OPS
- [ ] Queries In Multi-Statement

### Row 3: Server (23 panels)
- [ ] TiDB Server Status
- [ ] Uptime
- [ ] CPU Usage
- [ ] Memory Usage
- [ ] Runtime GC Rate And GOMEMLIMIT
- [ ] Open FD Count
- [ ] Connection Count
- [ ] Events OPM
- [ ] Disconnection Count
- [ ] Prepare Statement Count
- [ ] Goroutine Count
- [ ] Panic And Critial Error
- [ ] Keep Alive OPM
- [ ] Get Token Duration
- [ ] Time Jump Back OPS
- [ ] Client Data Traffic
- [ ] Skip Binlog Count
- [ ] RCCheckTS WriteConflict Num
- [ ] Handshake Error OPS
- [ ] Internal Sessions
- [ ] Active Users
- [ ] Connections Per TLS Cipher
- [ ] Connections Per TLS Version

### Row 4: Transaction (35 panels)
- [ ] Transaction OPS
- [ ] Duration
- [ ] Transaction Statement Num
- [ ] Transaction Retry Num
- [ ] Session Retry Error OPS
- [ ] Commit Token Wait Duration
- [ ] KV Transaction OPS
- [ ] KV Transaction Duration
- [ ] Transaction Regions Num
- [ ] Transaction Write KV Num Rate And Sum
- [ ] Transaction Write KV Num
- [ ] Statement Lock Keys
- [ ] Send HeartBeat Duration
- [ ] Transaction Write Size Bytes Rate And Sum
- [ ] Transaction Write Size Bytes
- [ ] Acquire Pessimistic Locks Duration
- [ ] TTL Lifetime Reach Counter
- [ ] Load Safepoint OPS
- [ ] Pessimistic Statement Retry OPS
- [ ] Transaction Types Per Second
- [ ] Transaction Commit P99 Backoff
- [ ] SafeTS Update Conuter
- [ ] Max SafeTS Gap
- [ ] Assertion
- [ ] Transaction Execution States Duration
- [ ] Transaction With Lock Execution States Duration
- [ ] Transaction Execution States Duration (2)
- [ ] Transaction Enter State
- [ ] Transaction Leave State
- [ ] Transaction State Count Change
- [ ] Fair Locking Usage
- [ ] Fair Locking Keys
- [ ] Pipelined Flush Keys
- [ ] Pipelined Flush Size
- [ ] Pipelined Flush Duration

### Row 5: Executor (16 panels)
- [ ] Parse Duration
- [ ] Compile Duration
- [ ] Execution Duration
- [ ] Expensive Executors OPS
- [ ] Queries Using Plan Cache OPS
- [ ] Plan Cache Miss OPS
- [ ] Read From Table Cache OPS
- [ ] Plan Cache Memory Usage
- [ ] Plan Cache Plan Num
- [ ] Plan Cache Process Duration
- [ ] Mpp Coordinator Counter
- [ ] Mpp Coordinator Latency
- [ ] IndexLookUp OPS
- [ ] IndexLookUp Duration
- [ ] IndexLookUp Rows
- [ ] IndexLookUp Row Num With PushDown Enabled

### Row 6: Distsql (8 panels)
- [ ] Distsql Duration
- [ ] Distsql QPS
- [ ] Distsql Partial QPS
- [ ] Scan Keys Num
- [ ] Scan Keys Partial Num
- [ ] Partial Num
- [ ] Coprocessor Cache
- [ ] Coprocessor Seconds 999

### Row 7: KV Errors (5 panels)
- [ ] KV Backoff Duration
- [ ] TiClient Region Error OPS
- [ ] KV Backoff OPS
- [ ] Lock Resolve OPS
- [ ] Replica Selector Failure Per Second

### Row 8: KV Request (16 panels)
- [ ] KV Request OPS
- [ ] KV Request Duration 99 By Store
- [ ] KV Request Duration 99 By Type
- [ ] KV Request Forwarding OPS
- [ ] KV Request Forwarding OPS By Type
- [ ] Successful KV Request Wait Duration
- [ ] Region Cache OK OPS
- [ ] Region Cache Error OPS
- [ ] Load Region Duration
- [ ] RPC Layer Latency
- [ ] Stale Read Hit/Miss OPS
- [ ] Stale Read Req OPS
- [ ] Stale Read Req Traffic
- [ ] Client-side Slow Score
- [ ] TiKV-side Slow Score
- [ ] Read Req Traffic

### Row 9: PD Client (15 panels)
- [ ] PD Client CMD OPS
- [ ] PD Client CMD Duration
- [ ] PD Client CMD Fail OPS
- [ ] PD TSO OPS
- [ ] PD TSO Wait Duration
- [ ] PD TSO RPC Duration
- [ ] Estimate TSO RTT Latency
- [ ] Async TSO Duration
- [ ] Request Forwarded Status
- [ ] PD HTTP Request Duration
- [ ] PD HTTP Request OPS
- [ ] PD HTTP Request Fail OPS
- [ ] Stale Region From PD
- [ ] Circuit Breaker Event
- [ ] TiDB Wait TSO Future Duration

### Row 10: Schema Load (11 panels)
- [ ] Load Schema Duration
- [ ] Load Schema Action Duration
- [ ] Schema Lease Error OPM
- [ ] Load Schema OPS
- [ ] Load Data From Cached Table Duration
- [ ] Schema Cache OPS
- [ ] Lease Duration
- [ ] Infoschema V2 Cache Operation
- [ ] Infoschema V2 Cache Size
- [ ] TableByName API Duration
- [ ] Infoschema V2 Cache Table Count

### Row 11: DDL (14 panels)
- [ ] OPS
- [ ] Execute Duration
- [ ] Job Worker Operations Duration
- [ ] Sync Schema Version Operations Duration
- [ ] System Table Operations Duration
- [ ] Waiting Job Count
- [ ] Running Job Count By Worker Pool
- [ ] Deploy Syncer Duration
- [ ] DDL META OPM
- [ ] Backfill Progress In Percentage
- [ ] Backfill Data Rate
- [ ] Add Index Scan Rate
- [ ] Retryable Error
- [ ] Add Index Backfill Import Speed

### Row 12: Dist Execute Framework (7 panels)
- [ ] Task Status
- [ ] Completed/Total Subtask Count
- [ ] Pending Subtask Count
- [ ] SubTask Running Duration
- [ ] Subtask Pending Duration
- [ ] Uncompleted Subtask Distribution On TiDB Nodes
- [ ] Slots Usage

### Row 13: Statistics & Plan Management (17 panels)
- [ ] Auto/Manual Analyze Duration
- [ ] Auto/Manual Analyze Queries Per Minute
- [ ] Stats Inaccuracy Rate
- [ ] Pseudo Estimation OPS
- [ ] Update Stats OPS
- [ ] Stats Healthy Distribution
- [ ] Sync Load QPS
- [ ] Sync Load Latency P9999
- [ ] Stats Cache Cost
- [ ] Stats Cache OPS
- [ ] Plan Replayer Task OPM
- [ ] Historical Stats OPM
- [ ] Stats Loading Duration
- [ ] Stats Meta/Usage Updating Duration
- [ ] Binding Cache Memory Usage
- [ ] Binding Cache Hit / Miss OPS
- [ ] Number Of Bindings In Cache

### Row 14: Owner (2 panels)
- [ ] New ETCD Session Duration 95
- [ ] Owner Watcher OPS

### Row 15: Meta (4 panels)
- [ ] AutoID QPS
- [ ] AutoID Duration
- [ ] Meta Operations Duration 99
- [ ] AutoID Client Conn Reset Counter

### Row 16: GC (11 panels)
- [ ] Worker Action OPM
- [ ] GC Duration By Stage
- [ ] Config
- [ ] GC Failure OPM
- [ ] Delete Range Failure OPM
- [ ] Too Many Locks Error OPM
- [ ] Action Result OPM
- [ ] Resolve Locks Range Tasks Status
- [ ] Resolve Locks Range Tasks Push Task Duration 95
- [ ] Ongoing User Transaction Duration
- [ ] Ongoing Internal Transaction Duration

### Row 17: Batch Client (4 panels)
- [ ] No Available Connection Counter
- [ ] Batch Client Unavailable Duration 95
- [ ] Wait Connection Establish Duration
- [ ] Batch Receive Average Duration

### Row 18: TopSQL (6 panels)
- [ ] Ignore Event Per Minute
- [ ] 99 Report Duration
- [ ] Report Data Count Per Minute
- [ ] Total Report Count
- [ ] CPU Profiling OPS
- [ ] TiKV Stat Task OPS

### Row 19: TTL (17 panels)
- [ ] TiDB CPU Usage
- [ ] TiKV IO MBps
- [ ] TiKV CPU
- [ ] TTL QPS By Type
- [ ] TTL Insert Rows Per Second
- [ ] TTL Processed Rows Per Second
- [ ] TTL Insert Rows Per Hour
- [ ] TTL Delete Rows Per Hour
- [ ] TTL Scan Query Duration
- [ ] TTL Delete Query Duration
- [ ] Scan Worker Time By Phase
- [ ] Delete Worker Time By Phase
- [ ] TTL Job Count By Status
- [ ] TTL Task Count By Status
- [ ] Table Count By TTL Schedule Delay
- [ ] TTL Insert/Delete Rows By Day
- [ ] TTL Event Count Per Minute

### Row 20: Resource Manager (2 panels)
- [ ] GOGC
- [ ] EMA CPU Usage

### Row 21: Follower Read (3 panels)
- [ ] Closest Replica Hit Count Per Second
- [ ] Coprocessor Response Size Per Instance And TiKV
- [ ] Coprocessor Response Size

### Row 22: Import Into (5 panels)
- [ ] Total Encode/Deliver/Import-kv Speed
- [ ] Encoded/Delivered/Imported Data Size
- [ ] Delivered KV Count
- [ ] Data Read/Encode/Deliver Average Duration
- [ ] Import/Normal Mode

### Row 23: Global Sort (6 panels)
- [ ] Write To Cloud Storage Duration
- [ ] Write To Cloud Storage Rate
- [ ] Read From Cloud Storage Duration
- [ ] Read From Cloud Storage Rate
- [ ] Ingest Worker Count
- [ ] Active Parallel Upload Worker Count

### Row 24: Network Transmission (1 panel)
- [ ] Query Network Trasmission Bytes

### Row 25: Memory Arbitrator (8 panels)
- [ ] Work Mode
- [ ] Arbitration Exec
- [ ] Events
- [ ] Mem Quota Stats
- [ ] Mem Quota Arbitration Duration
- [ ] Mem Pool Stats
- [ ] Runtime Mem Pressure
- [ ] Waiting Tasks Stats
