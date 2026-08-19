# TiDB Design Documents

- Author(s): [JaySon-Huang](https://github.com/JaySon-Huang)
- Discussion PR: [https://github.com/pingcap/tidb/pull/XXX](https://github.com/pingcap/tidb/pull/XXX)
- Tracking Issue: [https://github.com/pingcap/tidb/issues/70524](https://github.com/pingcap/tidb/issues/70524)

## Table of Contents

- [Introduction](#introduction)
- [Motivation or Background](#motivation-or-background)
- [Detailed Design](#detailed-design)
  - [Goals](#goals)
  - [Non-goals](#non-goals)
  - [Endpoint](#endpoint)
  - [Handler flow](#handler-flow)
  - [Response contract](#response-contract)
  - [Error handling](#error-handling)
  - [Cluster operator usage](#cluster-operator-usage)
  - [Compatibility](#compatibility)
- [Test Design](#test-design)
  - [Functional Tests](#functional-tests)
  - [Scenario Tests](#scenario-tests)
  - [Compatibility Tests](#compatibility-tests)
  - [Benchmark Tests](#benchmark-tests)
- [Impacts & Risks](#impacts--risks)
- [Investigation & Alternatives](#investigation--alternatives)
- [Unresolved Questions](#unresolved-questions)



## Introduction

This document specifies a new HTTP GET endpoint `GET /tiflash/replica` on the TiDB status port. A cluster operator (automation or an administrator, not a tenant SQL session) uses it to decide whether it is safe to disable Columnar Storage on a logical cluster (keyspace): whether any **live** table still has TiFlash / columnar replica metadata.

The endpoint is a **new contract for cluster operators**. It does not replace or change `GET|POST /tiflash/replica-deprecated`, which remains the classic TiFlash replica-list and progress-report API.

Related kernel work: the cluster-level DDL gate `tidb_columnar_storage_enabled` ([issue #70524](https://github.com/pingcap/tidb/issues/70524)). That gate rejects `SET TIFLASH REPLICA n` (`n > 0`) and other replica-writing DDL when the flag is `OFF`. This API answers the complementary question: *are there still live replica metadata rows before the cluster operator turns the flag off?*

## Motivation or Background

On TiDB X, Columnar Storage could be an explicit opt-in feature when deploying a TiDB X cluster. The cluster operator must not disable it while user tables still have `TiFlashReplica` metadata: queries would keep choosing the columnar / TiFlash path after `tiflash-compute` is gone.

We need a HTTP API returning how many tables in this keyspace still have TiFlash replicas. Today that path does not exist. The closest surface is `GET /tiflash/replica-deprecated`, which is the wrong contract for disable:

- GET lists **physical** table / partition IDs, not logical tables.
- GET also walks **all history DDL jobs** and recovers `TableInfo` from drop / truncate snapshots (GC-not-yet-done leftovers). That scan is expensive and answers "is replica data still waiting for GC?", not "does any user table still use TiFlash(Columnar Storage)?".
- POST is classic TiFlash progress reporting (`UpdateTableReplicaInfo`). Mixing that with a GET for cluster operators is unsafe.

The cluster operator therefore needs a dedicated GET that is cheap, fail-closed, keyed by logical table, and default-excludes drop / truncate leftovers.

## Detailed Design



### Goals

- Expose `GET /tiflash/replica` on the status port (default `10080`).
- Any TiDB in the target keyspace can answer. The handler always `Reload()` local InfoSchema first, then lists live tables.
- Return a summary that the cluster operator can use without counting partitions or filtering leftovers: `can_disable`, `table_count`, plus keyspace identity. Do not return table names, IDs, or other per-table details.
- Keep `/tiflash/replica-deprecated` behavior unchanged.



### Non-goals

- Scanning `mysql.tidb_ddl_job` or DDL history for in-flight `SET TIFLASH REPLICA` / `CREATE TABLE` with replica. The job-path gate already rejects those jobs once `tidb_columnar_storage_enabled` is `OFF`. Pending-job scan does not close the check-then-SET-OFF TOCTOU window.
- Returning drop / truncate leftovers that have not been GC'd. Residual columnar files will be GC-ed after gc safepoint increased.
- Returning per-table identity (schema, name, table id, replica count per table). That is user data the disable precheck does not need.
- A kernel-atomic "count == 0 and SET OFF" API. The cluster operator sequences HTTP check and `SET GLOBAL`.
- Changing classic TiFlash (`columnar-store-type = tiflash`) replica reporting.
- Authenticating the status port. Callers remain internal, same as other status APIs.



### Endpoint

```
GET /tiflash/replica
```

- **Port**: status port, same as `/schema` and `/tiflash/replica-deprecated`.
- **Methods**: GET only. POST is not registered. Classic TiFlash progress report stays on `/tiflash/replica-deprecated`.
- **Query parameters**: none. The endpoint never includes drop / truncate leftovers (it would reintroduce history-job + snapshot scan).
- **Identity**: the receiving TiDB's store / keyspace. The list is always "live tables on **this** TiDB", never a cross-keyspace merge.

Do not reuse `FlashReplicaHandler.ServeHTTP`. That type owns both GET-with-leftovers and POST status report. Add a separate handler registered only for GET.

#### Visibility by deployment

The same handler reads local InfoSchema after `Reload()`. What "correct replica information" means depends on which TiDB is queried.

| Deployment | What the response describes | Correct? |
| --- | --- | --- |
| Classic | The whole cluster. Every TiDB shares one schema; `keyspace` is empty / implied. `table_count` is the number of live tables with `TiFlashReplica` in that cluster. | Yes, for live tables. Same caveats as next-gen: no drop / truncate leftovers, no pending DDL. |
| Next-gen user keyspace (e.g. `ks1`) | Only that logical cluster. | Yes, for that keyspace. |
| Next-gen `SYSTEM` | Only the SYSTEM keyspace. `mysql` / other mem-or-sys DBs cannot take TiFlash replicas (`ErrUnsupportedTiFlashOperationForSysOrMemTable`), but non-system databases **in SYSTEM** still can. Those rows are counted. User keyspaces (`ks1`, …) are **not** visible. | Yes, for SYSTEM itself. Not a substitute for querying a user-keyspace TiDB. |

To disable Columnar Storage on a user logical cluster, the operator must call the user-keyspace TiDB, not SYSTEM. SYSTEM is a valid target only when the operator is inspecting or disabling SYSTEM itself.

### Handler flow

```
1. Domain.Reload()
   - Fail the request if Reload returns an error. Do not fall back to a stale
     InfoSchema and do not return an empty table list.
2. Read InfoSchema after Reload (dom.InfoSchema()).
3. ListTablesWithSpecialAttribute(TiFlashAttribute)
   - Filter is `TableInfo.TiFlashReplica != nil`.
   - `SET TIFLASH REPLICA 0` already sets `TiFlashReplica` to nil, so this is
     equivalent to "live tables with replica_count > 0".
4. Count at **logical table** granularity (a partitioned table contributes 1, not one per partition). Do not put table names or IDs in the response.
5. Write JSON via handler.WriteData.
```

`Reload()` is the freshness mechanism. Non-owner nodes watch schema version through etcd; a local reload brings InfoSchema in line with the latest committed version in the store. That is enough for this API. Forwarding to the DDL owner is rejected (see [Investigation & Alternatives](#investigation--alternatives)).

Do not call `getDropOrTruncateTableTiflash` / `IterAllDDLJobs` / `GetDropOrTruncateTableInfoFromJobs`.

### Response contract

Success is HTTP 200 with pretty-printed JSON (existing `WriteData` behavior). Suggested body:

```json
{
  "keyspace": "ks1",
  "keyspace_id": 123,
  "tidb_columnar_storage_enabled": "ON",
  "can_disable": false,
  "table_count": 2
}
```

Do **not** return `tables`, schema names, table names, or table IDs. Per-table identity is user information the disable precheck does not need. If the operator must name tables, they already have SQL (`information_schema.TIFLASH_REPLICA`) under their own privilege model.

Constraints on the body:


| Field                           | Rule                                                                                                                                                              |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `keyspace` / `keyspace_id`      | Identity of **this** TiDB. Classic kernel: empty name / the implied keyspace. Lets the cluster operator detect a wrong instance.                                  |
| `tidb_columnar_storage_enabled` | Current global value from the domain sysvar cache (`ON` / `OFF`). Informational; `can_disable` does not require the flag to already be `OFF`.                     |
| `table_count`                   | Number of **logical** tables with `TiFlashReplica != nil`. Partitioned tables count as 1.                                                                         |
| `can_disable`                   | `table_count == 0`. No other inputs (pending DDL, dropped leftovers, `available`, store count).                                                                   |


`can_disable == true` means: **no live table currently has replica metadata on this node after Reload**. It does **not** mean:

- drop / truncate physical leftovers have been GC'd
- `tiflash-compute` / tikv-worker data is gone
- no replica DDL can complete between this GET and `SET GLOBAL ... = OFF`



### Error handling

Fail closed.


| Failure                         | HTTP          | Body                                                | Cluster operator action                  |
| ------------------------------- | ------------- | --------------------------------------------------- | ---------------------------------------- |
| `Reload()` error                | 5xx (not 200) | error text (`WriteErrorWithCode`)                   | Retry; **do not** treat as `can_disable` |
| Domain / InfoSchema unavailable | 5xx           | error text                                          | Retry                                    |
| Success with zero tables        | 200           | `can_disable: true`, `table_count: 0`               | May proceed to SET OFF, then GET again   |


Never map a load failure to an empty array. The deprecated GET already returns `[]` when there are no replicas; a failed Reload must not look like that.

### Cluster operator usage

Recommended sequence:

1. `GET /tiflash/replica` on a TiDB of the target cluster (classic: any TiDB; next-gen: a TiDB bound to that keyspace). Abort if HTTP is not 200, or if `keyspace` does not match.
2. If `can_disable` is false, refuse disable. `table_count` is the only remaining-replica signal; this API does not name tables.
3. `SET GLOBAL tidb_columnar_storage_enabled = OFF` (SEM `RESTRICTED_VARIABLES_ADMIN`).
4. `GET /tiflash/replica` again. If `table_count > 0`, fail the disable operation and consider turning the variable back `ON`.

Step 4 exists because GET and SET OFF are not atomic. A tenant can finish `SET TIFLASH REPLICA 1` in between. After OFF, the DDL gate blocks further `n > 0` replica DDL; a second GET detects metadata that landed in the window.

`SET TIFLASH REPLICA 0` stays allowed when the flag is `OFF`, so tenants can still remove leftovers after a failed disable.

### Compatibility

- **Deprecated API**: no change to `/tiflash/replica-deprecated` path, methods, JSON, or drop / truncate leftover scan.
- **TiFlash**: continues to use the deprecated path for list + POST progress. Confirm TiFlash no longer calls `/tiflash/replica` before registering the new GET on that path. If any in-tree or in-cloud client still uses the old path, keep the new API on a distinct path (unresolved below).
- **Upgrade**: old kernels do not serve `GET /tiflash/replica`. The cluster operator must only call this contract on kernels that document it.
- **Downgrade**: removing the handler is enough; no persisted metadata.
- **SEM / SQL**: this is a status-port API, not SQL. It does not honor `information_schema.TIFLASH_REPLICA` privilege filtering.
- **Docs**: implementation MUST update `docs/tidb_http_api.md` in the same change (`pkg/server/AGENTS.md`).



## Test Design



### Functional Tests

Extend `pkg/server/handler/tests` HTTP tests:

- Empty cluster: 200, `can_disable=true`, `table_count=0`. Body has no table list.
- `SET TIFLASH REPLICA 1` on a table: `can_disable=false`, `table_count=1`.
- Partitioned table with replica: still `table_count=1`.
- `SET TIFLASH REPLICA 0`: `table_count=0`, `can_disable=true`.
- `DROP TABLE` of a replica table: `table_count` does not keep the dropped table (unlike `/tiflash/replica-deprecated`).
- `Reload` failure (failpoint): non-200, body is not a success JSON with `table_count=0`.



### Scenario Tests

- After Reload, a replica set on another session is visible without waiting for schema lease.
- Next-gen / keyspace: response `keyspace` matches the serving TiDB (unit or realtikv, whichever is already used for status HTTP). A SYSTEM TiDB does not count user-keyspace replicas; a user-keyspace TiDB does not count SYSTEM replicas.
- Classic: after `SET TIFLASH REPLICA` on any TiDB, `table_count` reflects the cluster-wide live replica tables.



### Compatibility Tests

- Existing `/tiflash/replica-deprecated` tests still pass, including dropped-table leftover listing and POST progress.
- POST to `/tiflash/replica` is not handled as TiFlash status report (404 or method not allowed).



### Benchmark Tests

Not required. The handler is cluster-operator traffic, not a query path. Do not reintroduce history-job iteration.

## Impacts & Risks

**Impacts**

- The cluster operator gets a stable, cheap disable precheck.
- Status port grows one GET. Documented in `docs/tidb_http_api.md`.
- `Reload()` on each call adds a schema load when the node is behind. That is intentional and bounded; it is cheaper than scanning DDL history.

**Risks**

- TOCTOU between GET and SET OFF remains. Mitigated by the second GET after OFF, not by this handler.
- Hitting the wrong keyspace TiDB yields a correct-looking empty or non-empty summary for the **wrong** cluster. `keyspace` in the body is the detection signal; the cluster operator must check it.
- If TiFlash still GET/POSTs `/tiflash/replica`, resurrecting that path would break TiFlash. Verify before landing; otherwise pick a different path.
- `Reload()` failure under PD / KV stress causes disable precheck to fail closed (retries), which may delay a disable operation. Prefer that over a false `can_disable=true`.



## Investigation & Alternatives

**Reuse** `/tiflash/replica-deprecated` **with** `include_dropped=false`**.** Rejected. GET+POST share one handler; JSON is a TiFlash contract; partition-level `id` is inconvenient; default must stay "include leftovers" for TiFlash. A query parameter cannot make that API a good summary for cluster operators.

**Forward non-owner HTTP to the DDL owner.** Rejected. There is no existing TiDB-to-TiDB owner proxy. It needs loop prevention, etcd owner id → status address mapping, TLS / status-port reachability, owner failover, and next-gen per-keyspace owners. Owner forwarding also does not close TOCTOU. Local `Reload()` matches committed schema version without those failure modes.

**Scan running DDL jobs for in-flight replica DDL.** Rejected. `mysql.tidb_ddl_job` is small, but decoding every replica-writing job type is easy to get wrong, and it does not catch jobs that finish between GET and SET OFF. After OFF, the job-path gate already fails those jobs. A second GET after OFF is the cheaper correctness tool.

**A cluster operator queries** `information_schema.TIFLASH_REPLICA`**.** Possible, but it requires a SQL user, follows privilege filtering, and does not force Reload. A status-port GET is the kernel contract the wiki asked for.

**Atomic kernel "disable if count == 0".** Stronger, out of scope for this API. Can be a later cluster-operator / kernel issue.

## Unresolved Questions

- Confirm no remaining TiFlash / cloud client calls `GET|POST /tiflash/replica` (non-deprecated). If any do, register this API under a distinct path (for example `/tiflash/replica/summary`) and keep `/tiflash/replica` unused or aliased to deprecated.
- Whether `tidb_columnar_storage_enabled` in the response should be omitted on kernels that have not registered the sysvar (upgrade mixed versions).

