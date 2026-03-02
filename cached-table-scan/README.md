# Cached Table Scan 性能优化工作计划

## 问题概述

Profile 来源：`cpu_tidb_tidb-1-peer_4000_28272148.proto`（29.14s 采样）

Prepared Statement + Plan Cache + UnionScan 场景下，`Constant.GetType()` 为 ParamMarker 每次调用都堆分配 `FieldType`，且在 eval 路径中被重复调用。这导致：
- `Constant.GetType` 累计占 26.21% CPU
- `NewFieldType` 累计占 16.54% CPU
- `mallocgc` 累计占 19.08% CPU
- GC 相关占 ~7% CPU

## 任务列表

| 任务 | 描述 | 优先级 | 预期收益 |
|------|------|--------|----------|
| [WORK01](WORK01.md) | EvalInt 消除重复 GetType 调用 | P0 | ~13% GetType 调用量 |
| [WORK02](WORK02.md) | CompareInt 消除额外 GetType 调用 | P1 | ~5% GetType 调用量 |
| [WORK03](WORK03.md) | GetType 避免堆分配（核心） | P0 | ~16% CPU + GC 压力 |
| [WORK04](WORK04.md) | memRowsIter 批量化（可选） | P2 | 待 profile 验证 |

## 建议执行顺序

1. **WORK01** — 最简单，5分钟改完，立即有效果
2. **WORK03** — 核心优化，消除堆分配
3. **WORK02** — 补充优化
4. **WORK04** — 重新 profile 后再决定
