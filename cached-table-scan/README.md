# Cached Table Scan 性能优化工作计划

## 问题概述

Profile 来源：
- `profile_1.pb.gz`（29.56s 采样，460.91s total，16 core）
- `cpu_tidb_tidb-1-peer_4000_28272148.proto`（29.14s 采样）

Prepared Statement + Plan Cache + UnionScan 场景下的两大性能问题：

1. **Constant.GetType() 堆分配**：ParamMarker 每次调用都堆分配 `FieldType`
   - `Constant.GetType` 累计占 26.21% CPU
   - `NewFieldType` 累计占 16.54% CPU
2. **逐行 decode + eval**：`memRowsIterForTable.Next()` 每行都完整解码 + 标量过滤
   - `DecodeToChunk` 占 38.15% CPU
   - `EvalBool` 占 16.50% CPU
   - `findColID` 占 5.01% CPU
   - `DecodeRowKey` 占 4.66% CPU

## 任务列表

| 任务 | 描述 | 优先级 | 预期收益 |
|------|------|--------|----------|
| [WORK01](WORK01.md) | EvalInt 消除重复 GetType 调用 | P0 | ~13% GetType 调用量 |
| [WORK02](WORK02.md) | CompareInt 消除额外 GetType 调用 | P1 | ~5% GetType 调用量 |
| [WORK03](WORK03.md) | GetType 避免堆分配（核心） | P0 | ~16% CPU + GC 压力 |
| [WORK04](WORK04.md) | memRowsIter 批量化（初步思路） | P2 | 待 profile 验证 |
| [WORK05](WORK05.md) | Cached table 跳过 UnionScan merge | P1 | ~1-2% CPU，为 WORK06 铺路 |
| [WORK06](WORK06.md) | 批量 DecodeToChunk + Filter | P0 | ~15-25% CPU |
| [WORK07](WORK07.md) | findColID 列映射缓存 | P1 | ~3-4% CPU |
| [WORK08](WORK08.md) | DecodeRowKey 快速路径 | P2 | ~1-2% CPU |

## 建议执行顺序

### 第一阶段：低风险高收益
1. **WORK01** — 最简单，5分钟改完
2. **WORK03** — 核心优化，消除堆分配
3. **WORK02** — 补充优化

### 第二阶段：decode 路径优化
4. **WORK05** — cached table 专用路径（为 WORK06 铺路）
5. **WORK06** — 批量 decode + filter（最大收益）
6. **WORK07** — findColID 映射缓存

### 第三阶段：收尾
7. **WORK08** — DecodeRowKey 快速路径
8. 重新采集 profile，评估是否需要 WORK04 或其他优化
