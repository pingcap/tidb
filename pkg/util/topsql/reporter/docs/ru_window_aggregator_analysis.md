# ru_window_aggregator 锁粒度与中间态分析

## 问题概述

- **位置**: `pkg/util/topsql/reporter/ru_window_aggregator.go` — `addSecondBatch`、`sealBucketLocked`、`takeReportRecords`
- **现象**: sealing（200×200 compact）与 report merge（100×100）在同一把 `mu` 下执行；高负载时阻塞 `collectRUWorker` 写入，放大 `collectRUIncrementsChan`（buffer=2）满导致的丢数概率；sealed buckets 以 `[]tipb.TopRURecord` 作为中间态，存在「proto→内部→proto」二次转换与额外分配。

---

## 1. 锁竞争与丢数放大

### 1.1 当前锁使用

| 调用路径 | 持锁操作 |
|----------|----------|
| `collectRUWorker` → `addSecondBatch` | 整段持 `a.mu`：`sealBucketsBeforeLocked`（可能多次 `sealBucketLocked`）+ 当前 bucket 的 `addBatch` |
| `reportWorker` → `takeDataAndSendToReportChan` → `takeReportRecords` | 整段持 `a.mu`：`sealBucketsBeforeLocked` + `buildReportRecordsLocked`（merge + 多轮 100×100）+ `delete` buckets |

`ru_window_aggregator` 仅有一把 `mu`，两路并发：

- **写路径**: 每 1s 的 RU 增量由 aggregator 推到 `collectRUIncrementsChan`，`collectRUWorker` 从 chan 取后调用 `addSecondBatch`，需要获取 `mu`。
- **读路径**: 每 60s（或配置的 report_interval）`reportWorker` 调用 `takeReportRecords`，持 `mu` 做 seal + 整窗 merge + 过滤。

因此：

- 一旦 `takeReportRecords` 持锁时间较长（例如 4 个 15s bucket 都要 seal，再 merge 成多组、每组 100×100，最后再 100×100 输出），`addSecondBatch` 会长时间拿不到锁。
- `collectRUWorker` 被阻塞时，上游仍按 1s 往 `collectRUIncrementsChan` 送数据，而 **channel buffer 只有 2**（`reporter.go` 中 `collectChanBufferSize = 2`）。
- 若 2 秒内写路径仍拿不到锁，后续 `CollectRUIncrements` 的 `select` 会走 default，触发 **`IgnoreCollectRUChannelFullCounter.Inc()`**，即丢数。高负载下 seal + merge 耗时增加，持锁时间变长，丢数概率被放大。

### 1.2 建议：缩小锁粒度

目标：**锁内只做最少必要工作（seal + 拷贝要用的 bucket/records），锁外做 merge/filter**。

- **方案 A（推荐）**  
  - 锁内：  
    - `sealBucketsBeforeLocked(boundary)`（只做状态变更 + 对需要 seal 的 bucket 做 200×200 compact，仍可保留在锁内，因为 seal 是写共享状态）。  
    - 对当前要上报的 60s 窗口，**只拷贝** 涉及的 bucket 的引用或最小必要数据（见下节「中间态」），并从 `buckets` 中 delete 这些 bucket，更新 `lastReportedEndTs`。  
  - 锁外：  
    - 用拷贝出的 bucket 数据做 `buildReportRecordsLocked` 的等价逻辑（merge 多个 sealed bucket、按 granularity 分组、100×100 过滤），最终得到 `[]tipb.TopRURecord`。  
  - 这样 **merge/filter 不再占锁**，`addSecondBatch` 只需在「seal + 拷贝」时与 report 路径短暂竞争，锁持有时间显著缩短。

- **方案 B（折中）**  
  - 锁内：只做 seal + 将「待上报窗口」的 bucket 从 map 中移出并放入一个局部变量（例如 `[]*ruPointBucket`）。  
  - 锁外：对这批 bucket 做 merge + 100×100；做完后再在锁内一次更新 `lastReportedEndTs` 并做 Defensive cleanup（若与当前实现语义一致）。  
  - 若当前「lastReportedEndTs 必须在取数同时更新」的语义可以接受延后更新，则可在锁外做完 merge 再入锁更新。

实现时注意：从 map 中 delete 并交给锁外处理时，**不要再持有 bucket 的写引用**，避免与后续 `addSecondBatch` 对同一 bucket 的写并发（当前设计下 seal 后该 bucket 不再被写，只要 report 路径只读拷贝即可）。

---

## 2. 中间态「proto→内部→proto」与分配

### 2.1 当前数据流

1. **Seal 阶段**（`sealBucketLocked`）  
   - `bucket.records = bucket.collecting.getReportRecordsWithLimits(nil, 200, 200)`  
   - **内部结构** `ruCollecting` → **proto** `[]tipb.TopRURecord`，并丢弃 `bucket.collecting`。

2. **Report 阶段**（`buildReportRecordsLocked`）  
   - 遍历窗口内每个 15s bucket 的 `bucket.records`（**proto**）。  
   - `mergeRURecordsIntoCollecting(groupCollecting, bucket.records, ...)`：  
     - 遍历 `rec.Items`，用 `item.TimestampSec/TotalRu/ExecCount/ExecDuration` 调 `dst.addRecordItem(..., &stmtstats.RUIncrement{...})`，即 **proto → 内部**（`ruCollecting`）。  
   - 再对 `groupCollecting` 做 `getReportRecordsWithLimits(..., 100, 100)`，即 **内部 → proto**。  
   - 最终再对 `mergedOutput` 做一次 `getReportRecordsWithLimits`， again **内部 → proto**。

因此：

- Sealed 的中间态是 **proto**。
- Report 时：**proto → 内部（merge）→ proto（输出）**，形成二次转换与多轮分配（proto 切片、ruCollecting/userRUCollecting/ruRecord 的 map/slice、再转回 proto）。

### 2.2 建议：sealed 存为内部结构，减少转换与分配

- **方案 A**  
  - Sealed bucket 不存 `[]tipb.TopRURecord`，改为存 **内部结构**，例如「只读的 `*ruCollecting`」或自定义的 `sealedRUBucket struct { users map[string]*userRUCollecting; othersUser *userRUCollecting }`（与当前 `ruCollecting` 一致，但 seal 后不再写入）。  
  - Seal 时：`bucket.sealedCollecting = bucket.collecting; bucket.collecting = nil`（或做浅拷贝/只读视图，避免 report 时仍被写）。  
  - Report 时：  
    - 直接对多个 `bucket.sealedCollecting` 做「内部→内部」的 merge（例如现有的 `addRecordItem` 或新增 `mergeFromCollecting(other *ruCollecting)`），得到单个 `ruCollecting`。  
    - 最后 **只做一次** `getReportRecordsWithLimits(..., keyspaceName, 100, 100)` → `[]tipb.TopRURecord` 作为上报结果。  
  - 这样 sealed 阶段不再分配 proto，report 阶段不再「proto→内部」，只有 **内部→内部 merge + 一次内部→proto 输出**，分配和转换次数明显减少。

- **方案 B（若暂时不改 sealed 形态）**  
  - 至少将 **merge 与最终 100×100** 移到锁外（见 1.2），这样锁内只做「seal + 拷贝 []tipb.TopRURecord」，锁外再做 proto→内部→proto，虽仍有二次转换，但锁竞争减轻，丢数概率先降下来。

---

## 3. 可观测性：丢弃与阻塞

- **已有**: `reporter_metrics.IgnoreCollectRUChannelFullCounter` 在 `CollectRUIncrements` 中 channel 满时自增，可用于观察「因 channel 满导致的丢弃」。
- **建议补充**（可选）：  
  - 若在 `addSecondBatch` 中因「晚到数据」丢弃（例如 `bucketStart < lastReportedEndTs`），可打一条 debug 日志或单独 counter，便于区分「channel 满丢弃」与「迟到丢弃」。  
  - 若实现「锁外 merge」，可在锁外 merge 耗时超过某阈值时打一条 warn 日志，便于发现 report 路径是否成为瓶颈。

---

## 4. 小结与推荐顺序

| 问题 | 建议 | 优先级 |
|------|------|--------|
| 锁内做 seal + 整窗 merge，阻塞 collectRUWorker，放大 channel 满丢数 | 锁内只 seal + 拷贝待上报 buckets；锁外做 merge + 100×100 | 高 |
| Sealed 存 proto，report 时 proto→内部→proto，多轮分配 | Sealed 存内部结构（如只读 ruCollecting），report 时仅一次内部→proto | 中 |
| 可观测性 | 保留 IgnoreCollectRUChannelFullCounter；可选：迟到丢弃 metric/日志、锁外 merge 耗时日志 | 低 |

推荐实现顺序：先做 **锁粒度缩小**（锁内 seal + 拷贝，锁外 merge/filter），验证 IgnoreCollectRUChannelFullCounter 在高负载下是否明显下降；再考虑将 sealed 改为内部结构，进一步减少分配与转换。
