# TiDB TopSQL 源码阅读

## 概述

TopSQL 是 TiDB 4.0 引入的一项重要功能，用于实时收集和分析数据库中消耗资源最多的 SQL 语句。通过 TopSQL，用户可以快速定位系统中的性能瓶颈和资源消耗大户，是数据库性能诊断和优化的重要工具。

本文将深入分析 TopSQL 的源码实现，帮助读者理解其核心设计思想和技术细节。

## 目录

1. [整体架构](#整体架构)
2. [核心数据结构](#核心数据结构)
3. [数据采集流程](#数据采集流程)
4. [数据聚合流程](#数据聚合流程)
5. [数据上报流程](#数据上报流程)
6. [关键技术点](#关键技术点)
7. [配置与状态管理](#配置与状态管理)

## 整体架构

TopSQL 的代码主要位于 `pkg/util/topsql` 目录下，整体架构可以分为以下几个核心模块：

```
pkg/util/topsql/
├── topsql.go              # 入口文件，提供对外接口
├── state/                 # 状态管理模块
│   └── state.go
├── collector/             # CPU 时间采集模块
│   └── cpu.go
├── stmtstats/             # 语句统计模块
│   ├── stmtstats.go
│   └── aggregator.go
└── reporter/              # 数据上报模块
    ├── reporter.go        # 核心报告器
    ├── datamodel.go       # 数据模型定义
    ├── datasink.go        # 数据汇入接口
    ├── single_target.go   # 单目标数据汇报
    └── pubsub.go          # 发布订阅模式
```

### 架构图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              SQL 执行层 (Executor)                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐    │
│  │  ExecStmt.observeStmtBeginForTopSQL() / observeStmtFinishedForTopSQL()  │    │
│  └─────────────────────────────────────────────────────────────────────────┘    │
└─────────────────────┬────────────────────────────────┬──────────────────────────┘
                      │                                │
          ┌───────────▼───────────┐        ┌──────────▼──────────┐
          │   topsql.RegisterSQL  │        │  StatementStats     │
          │   topsql.RegisterPlan │        │  OnExecutionBegin   │
          │   AttachSQLAndPlanInfo│        │  OnExecutionFinished│
          └───────────┬───────────┘        └──────────┬──────────┘
                      │                               │
┌─────────────────────▼───────────────────────────────▼─────────────────────────┐
│                           TopSQL 核心模块                                      │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌────────────────────┐   │
│  │   normalizedSQLMap   │  │   normalizedPlanMap  │  │    stmtstats       │   │
│  │   (SQL 元信息缓存)    │  │   (Plan 元信息缓存)  │  │   aggregator       │   │
│  └──────────────────────┘  └──────────────────────┘  └─────────┬──────────┘   │
│                                                                │              │
│  ┌─────────────────────────────────────────────────────────────▼────────────┐ │
│  │                        RemoteTopSQLReporter                               │ │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────────────┐    │ │
│  │  │  collectWorker  │  │  reportWorker   │  │   SQLCPUCollector      │    │ │
│  │  │  (数据收集协程)  │  │  (数据上报协程)  │  │   (CPU Profile 解析)   │    │ │
│  │  └────────┬────────┘  └────────┬────────┘  └────────────┬───────────┘    │ │
│  │           │                    │                        │                │ │
│  │           └────────────────────┴────────────────────────┘                │ │
│  └──────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                    ┌───────────────────┴───────────────────┐
                    │            DataSink 接口               │
                    │  ┌─────────────────────────────────┐  │
                    │  │    SingleTargetDataSink         │  │
                    │  │    (gRPC 推送到远端 Agent)       │  │
                    │  ├─────────────────────────────────┤  │
                    │  │    pubSubDataSink               │  │
                    │  │    (发布订阅模式)                │  │
                    │  └─────────────────────────────────┘  │
                    └───────────────────────────────────────┘
                                        │
                                        ▼
                              ┌─────────────────┐
                              │  TiDB Dashboard │
                              │  / TopSQL Agent │
                              └─────────────────┘
```

## 核心数据结构

### 1. 状态管理 (state/state.go)

```go
// State 控制 TopSQL 功能的全局状态
type State struct {
    enable                *atomic.Bool   // 是否启用 TopSQL
    PrecisionSeconds      *atomic.Int64  // 采样精度（秒）
    MaxStatementCount     *atomic.Int64  // 最大语句数量（Top N）
    MaxCollect            *atomic.Int64  // 最大收集数量
    ReportIntervalSeconds *atomic.Int64  // 上报间隔（秒）
}

// 默认配置
const (
    DefTiDBTopSQLEnable                = false
    DefTiDBTopSQLPrecisionSeconds      = 1      // 1秒精度
    DefTiDBTopSQLMaxTimeSeriesCount    = 100    // Top 100
    DefTiDBTopSQLMaxMetaCount          = 5000   // 最多5000条元信息
    DefTiDBTopSQLReportIntervalSeconds = 60     // 60秒上报一次
)
```

### 2. CPU 时间记录 (collector/cpu.go)

```go
// SQLCPUTimeRecord 表示某个 SQL Plan 在一秒内消耗的 CPU 时间
type SQLCPUTimeRecord struct {
    SQLDigest  []byte  // SQL 摘要
    PlanDigest []byte  // 执行计划摘要（可为空）
    CPUTimeMs  uint32  // CPU 时间（毫秒）
}
```

### 3. 语句统计项 (stmtstats/stmtstats.go)

```go
// StatementStatsItem 表示可合并的统计指标集合
type StatementStatsItem struct {
    KvStatsItem     KvStatementStatsItem  // KV 层指标
    ExecCount       uint64                // SQL 执行次数
    SumDurationNs   uint64                // 总执行时间（纳秒）
    DurationCount   uint64                // 用于计算平均执行时间
    NetworkInBytes  uint64                // 网络输入字节数
    NetworkOutBytes uint64                // 网络输出字节数
}

// SQLPlanDigest 作为 Map 的 Key 区分不同 SQL
type SQLPlanDigest struct {
    SQLDigest  BinaryDigest
    PlanDigest BinaryDigest
}
```

### 4. 时序数据项 (reporter/datamodel.go)

```go
// tsItem 表示某个时间戳下的完整数据
type tsItem struct {
    timestamp uint64                      // 时间戳
    cpuTimeMs uint32                      // CPU 时间
    stmtStats stmtstats.StatementStatsItem // 语句统计
}

// record 表示当前分钟窗口内累积的 tsItem
type record struct {
    sqlDigest      []byte           // SQL 摘要
    planDigest     []byte           // Plan 摘要
    tsItems        tsItems          // 时序数据列表
    tsIndex        map[uint64]int   // 时间戳到索引的映射
    totalCPUTimeMs uint64           // 总 CPU 时间
}

// collecting 包含 Reporter 正在收集的所有数据
type collecting struct {
    records map[string]*record              // sqlPlanDigest => record
    evicted map[uint64]map[string]struct{}  // 被淘汰的记录
    keyBuf  *bytes.Buffer
}
```

## 数据采集流程

TopSQL 的数据采集主要通过两个渠道：**CPU Profile 采样** 和 **语句执行统计**。

### 1. CPU Profile 采样流程

CPU Profile 采样利用 Go 语言内置的 `pprof` 机制，通过 Goroutine Label 关联 SQL 信息。

#### 1.1 设置 Goroutine Label

在 SQL 执行开始时，通过 `pprof.SetGoroutineLabels` 将 SQL 和 Plan 的 Digest 附加到当前 Goroutine：

```go
// pkg/util/topsql/topsql.go
func AttachSQLAndPlanInfo(ctx context.Context, sqlDigest *parser.Digest, planDigest *parser.Digest) context.Context {
    // 将 sql_digest 和 plan_digest 作为 label 附加到 context
    ctx = collector.CtxWithSQLAndPlanDigest(ctx, sqlDigestStr, planDigestStr)
    // 设置 Goroutine Labels
    pprof.SetGoroutineLabels(ctx)
    return ctx
}

// pkg/util/topsql/collector/cpu.go
func CtxWithSQLAndPlanDigest(ctx context.Context, sqlDigest, planDigest string) context.Context {
    return pprof.WithLabels(ctx, pprof.Labels(
        labelSQLDigest, sqlDigest,
        labelPlanDigest, planDigest,
    ))
}
```

#### 1.2 CPU Profile 收集器

`SQLCPUCollector` 负责从全局 CPU Profiler 消费 profile 数据并解析：

```go
// pkg/util/topsql/collector/cpu.go
func (sp *SQLCPUCollector) collectSQLCPULoop() {
    profileConsumer := make(cpuprofile.ProfileConsumer, 1)
    ticker := time.NewTicker(defCollectTickerInterval)
    
    for {
        // 根据 TopSQL 是否启用决定是否注册到 CPU Profiler
        if topsqlstate.TopSQLEnabled() {
            sp.doRegister(profileConsumer)
        } else {
            sp.doUnregister(profileConsumer)
        }

        select {
        case <-sp.ctx.Done():
            return
        case <-ticker.C:
        case data := <-profileConsumer:
            // 处理 profile 数据
            sp.handleProfileData(data)
        }
    }
}

func (sp *SQLCPUCollector) handleProfileData(data *cpuprofile.ProfileData) {
    // 解析 profile 数据
    p, err := profile.ParseData(data.Data.Bytes())
    if err != nil {
        return
    }
    // 按 SQL Labels 聚合 CPU 时间
    stats := sp.parseCPUProfileBySQLLabels(p)
    // 发送到收集器
    sp.collector.Collect(stats)
}
```

#### 1.3 解析 CPU Profile

从 CPU Profile 中提取带有 `sql_digest` 和 `plan_digest` 标签的样本：

```go
func (sp *SQLCPUCollector) parseCPUProfileBySQLLabels(p *profile.Profile) []SQLCPUTimeRecord {
    sqlMap := make(map[string]*sqlStats)
    idx := len(p.SampleType) - 1
    
    for _, s := range p.Sample {
        // 获取 sql_digest 标签
        digests, ok := s.Label[labelSQLDigest]
        if !ok || len(digests) == 0 {
            continue
        }
        
        for _, digest := range digests {
            stmt, ok := sqlMap[digest]
            if !ok {
                stmt = &sqlStats{plans: make(map[string]int64)}
                sqlMap[digest] = stmt
            }
            // 累加 CPU 时间
            stmt.total += s.Value[idx]
            
            // 获取 plan_digest 标签
            plans := s.Label[labelPlanDigest]
            for _, plan := range plans {
                stmt.plans[plan] += s.Value[idx]
            }
        }
    }
    return sp.createSQLStats(sqlMap)
}
```

### 2. 语句执行统计流程

语句统计通过 `StatementStats` 接口在 SQL 执行的关键节点进行计数。

#### 2.1 执行开始时

```go
// pkg/executor/adapter.go
func (a *ExecStmt) observeStmtBeginForTopSQL(ctx context.Context) context.Context {
    sqlDigest, planDigest := a.getSQLPlanDigest()
    stats := a.Ctx.GetStmtStats()
    
    if stats != nil {
        // 记录执行开始
        stats.OnExecutionBegin(sqlDigestByte, planDigestByte, vars.InPacketBytes.Load())
    }
    
    // 注册 SQL 和 Plan 元信息
    topsql.RegisterSQL(normalizedSQL, sqlDigest, vars.InRestrictedSQL)
    topsql.RegisterPlan(normalizedPlan, planDigest)
    
    return topsql.AttachSQLAndPlanInfo(ctx, sqlDigest, planDigest)
}
```

#### 2.2 执行结束时

```go
// pkg/executor/adapter.go
func (a *ExecStmt) observeStmtFinishedForTopSQL() {
    if stats := a.Ctx.GetStmtStats(); stats != nil && topsqlstate.TopSQLEnabled() {
        sqlDigest, planDigest := a.getSQLPlanDigest()
        execDuration := vars.GetTotalCostDuration()
        // 记录执行结束
        stats.OnExecutionFinished(sqlDigest, planDigest, execDuration, vars.OutPacketBytes.Load())
    }
}
```

#### 2.3 StatementStats 实现

```go
// pkg/util/topsql/stmtstats/stmtstats.go
func (s *StatementStats) OnExecutionBegin(sqlDigest, planDigest []byte, inNetworkBytes uint64) {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    item := s.GetOrCreateStatementStatsItem(sqlDigest, planDigest)
    item.ExecCount++
    item.NetworkInBytes = inNetworkBytes
}

func (s *StatementStats) OnExecutionFinished(sqlDigest, planDigest []byte, execDuration time.Duration, outNetworkBytes uint64) {
    s.mu.Lock()
    defer s.mu.Unlock()
    
    item := s.GetOrCreateStatementStatsItem(sqlDigest, planDigest)
    item.SumDurationNs += uint64(execDuration.Nanoseconds())
    item.DurationCount++
    item.NetworkOutBytes = outNetworkBytes
}
```

## 数据聚合流程

### 1. Aggregator 聚合器

每个 Session 都有独立的 `StatementStats`，全局 `aggregator` 负责收集和聚合所有 Session 的统计数据：

```go
// pkg/util/topsql/stmtstats/aggregator.go
type aggregator struct {
    statsSet   sync.Map // map[*StatementStats]struct{}
    collectors sync.Map // map[Collector]struct{}
}

func (m *aggregator) aggregate() {
    total := StatementStatsMap{}
    
    // 遍历所有注册的 StatementStats
    m.statsSet.Range(func(statsR, _ any) bool {
        stats := statsR.(*StatementStats)
        if stats.Finished() {
            m.unregister(stats)
        }
        // 合并统计数据
        total.Merge(stats.Take())
        return true
    })
    
    // 如果 TopSQL 启用，发送给所有 Collector
    if len(total) > 0 && state.TopSQLEnabled() {
        m.collectors.Range(func(c, _ any) bool {
            c.(Collector).CollectStmtStatsMap(total)
            return true
        })
    }
}
```

### 2. 数据合并逻辑

`StatementStatsMap.Merge` 将相同 SQLPlanDigest 的统计项合并：

```go
func (m StatementStatsMap) Merge(other StatementStatsMap) {
    for newDigest, newItem := range other {
        item, ok := m[newDigest]
        if !ok {
            m[newDigest] = newItem
            continue
        }
        item.Merge(newItem)
    }
}

func (i *StatementStatsItem) Merge(other *StatementStatsItem) {
    i.ExecCount += other.ExecCount
    i.SumDurationNs += other.SumDurationNs
    i.DurationCount += other.DurationCount
    i.NetworkInBytes += other.NetworkInBytes
    i.NetworkOutBytes += other.NetworkOutBytes
    i.KvStatsItem.Merge(other.KvStatsItem)
}
```

## 数据上报流程

### 1. RemoteTopSQLReporter

`RemoteTopSQLReporter` 是 TopSQL 的核心报告器，负责协调数据收集和上报：

```go
// pkg/util/topsql/reporter/reporter.go
type RemoteTopSQLReporter struct {
    collectCPUTimeChan      chan []collector.SQLCPUTimeRecord  // CPU 时间数据通道
    collectStmtStatsChan    chan stmtstats.StatementStatsMap   // 语句统计数据通道
    reportCollectedDataChan chan collectedData                  // 上报数据通道
    collecting              *collecting                         // 正在收集的数据
    normalizedSQLMap        *normalizedSQLMap                   // SQL 元信息
    normalizedPlanMap       *normalizedPlanMap                  // Plan 元信息
    stmtStatsBuffer         map[uint64]stmtstats.StatementStatsMap
    DefaultDataSinkRegisterer                                   // DataSink 注册器
}

func (tsr *RemoteTopSQLReporter) Start() {
    tsr.sqlCPUCollector.Start()
    go tsr.collectWorker()
    go tsr.reportWorker()
}
```

### 2. collectWorker - 数据收集协程

```go
func (tsr *RemoteTopSQLReporter) collectWorker() {
    reportTicker := time.NewTicker(time.Second * time.Duration(currentReportInterval))
    
    for {
        select {
        case data := <-tsr.collectCPUTimeChan:
            // 处理 CPU 时间数据
            timestamp := uint64(nowFunc().Unix())
            tsr.processCPUTimeData(timestamp, data)
            
        case data := <-tsr.collectStmtStatsChan:
            // 缓存语句统计数据
            timestamp := uint64(nowFunc().Unix())
            tsr.stmtStatsBuffer[timestamp] = data
            
        case <-reportTicker.C:
            // 定时触发数据处理和上报
            tsr.processStmtStatsData()
            tsr.takeDataAndSendToReportChan()
        }
    }
}
```

### 3. processCPUTimeData - CPU 时间数据处理

该函数实现了 **Top N 筛选** 和 **Others 聚合**：

```go
func (tsr *RemoteTopSQLReporter) processCPUTimeData(timestamp uint64, data cpuRecords) {
    // 获取 Top N（按 CPU 时间排序）
    top, evicted := data.topN(int(topsqlstate.GlobalState.MaxStatementCount.Load()))
    
    // 收集 Top N 记录
    for _, r := range top {
        tsr.collecting.getOrCreateRecord(r.SQLDigest, r.PlanDigest).appendCPUTime(timestamp, r.CPUTimeMs)
    }
    
    // 被淘汰的记录聚合到 "others"
    totalEvictedCPUTime := uint32(0)
    for _, e := range evicted {
        totalEvictedCPUTime += e.CPUTimeMs
        tsr.collecting.markAsEvicted(timestamp, e.SQLDigest, e.PlanDigest)
    }
    tsr.collecting.appendOthersCPUTime(timestamp, totalEvictedCPUTime)
}
```

### 4. reportWorker - 数据上报协程

```go
func (tsr *RemoteTopSQLReporter) reportWorker() {
    for {
        select {
        case data := <-tsr.reportCollectedDataChan:
            // 等待可能的并发写入完成
            time.Sleep(time.Millisecond * 100)
            
            rs := data.collected.getReportRecords()
            // 转换为 protobuf 格式并上报
            tsr.doReport(&ReportData{
                DataRecords: rs.toProto(tsr.keyspaceName),
                SQLMetas:    data.normalizedSQLMap.toProto(tsr.keyspaceName),
                PlanMetas:   data.normalizedPlanMap.toProto(tsr.keyspaceName, tsr.decodePlan, tsr.compressPlan),
            })
        }
    }
}

func (tsr *RemoteTopSQLReporter) doReport(data *ReportData) {
    // 发送到所有注册的 DataSink
    _ = tsr.trySend(data, time.Now().Add(reportTimeout))
}
```

### 5. DataSink 接口

TopSQL 通过 `DataSink` 接口支持多种数据上报方式：

```go
// pkg/util/topsql/reporter/datasink.go
type DataSink interface {
    // TrySend 将数据推送到目标
    TrySend(data *ReportData, deadline time.Time) error
    // OnReporterClosing 通知 DataSink Reporter 正在关闭
    OnReporterClosing()
}
```

#### 5.1 SingleTargetDataSink - 单目标 gRPC 上报

```go
// pkg/util/topsql/reporter/single_target.go
func (ds *SingleTargetDataSink) doSend(addr string, task sendTask) {
    ctx, cancel := context.WithDeadline(context.Background(), task.deadline)
    defer cancel()
    
    // 建立连接
    if err = ds.tryEstablishConnection(ctx, addr); err != nil {
        return
    }
    
    // 并行发送三种类型的数据
    var wg sync.WaitGroup
    wg.Add(3)
    
    go func() {
        defer wg.Done()
        errCh <- ds.sendBatchSQLMeta(ctx, task.data.SQLMetas)
    }()
    go func() {
        defer wg.Done()
        errCh <- ds.sendBatchPlanMeta(ctx, task.data.PlanMetas)
    }()
    go func() {
        defer wg.Done()
        errCh <- ds.sendBatchTopSQLRecord(ctx, task.data.DataRecords)
    }()
    wg.Wait()
}
```

#### 5.2 pubSubDataSink - 发布订阅模式

支持 TiDB Dashboard 等客户端通过 gRPC 流订阅 TopSQL 数据：

```go
// pkg/util/topsql/reporter/pubsub.go
func (ps *TopSQLPubSubService) Subscribe(_ *tipb.TopSQLSubRequest, stream tipb.TopSQLPubSub_SubscribeServer) error {
    ds := newPubSubDataSink(stream, ps.dataSinkRegisterer)
    if err := ps.dataSinkRegisterer.Register(ds); err != nil {
        return err
    }
    return ds.run()
}

func (ds *pubSubDataSink) doSend(ctx context.Context, data *ReportData) error {
    if err := ds.sendTopSQLRecords(ctx, data.DataRecords); err != nil {
        return err
    }
    if err := ds.sendSQLMeta(ctx, data.SQLMetas); err != nil {
        return err
    }
    return ds.sendPlanMeta(ctx, data.PlanMetas)
}
```

## 关键技术点

### 1. Goroutine Labels 机制

TopSQL 利用 Go 1.9 引入的 [Profiler Labels](https://rakyll.org/profiler-labels/) 特性，在 CPU Profile 采样时关联 SQL 信息：

```go
// 设置 Labels
ctx = pprof.WithLabels(ctx, pprof.Labels("sql_digest", sqlDigest, "plan_digest", planDigest))
pprof.SetGoroutineLabels(ctx)

// Profile 数据中包含 Labels
for _, s := range profile.Sample {
    digests := s.Label["sql_digest"]  // 获取关联的 SQL Digest
}
```

这种方式的优点是：
- 零侵入：不需要修改业务代码的执行路径
- 精确关联：CPU 时间可以精确关联到具体的 SQL
- 低开销：利用现有的 pprof 基础设施

### 2. Top N 算法

TopSQL 使用 **QuickSelect 算法** 高效筛选 Top N 记录：

```go
func (rs cpuRecords) topN(n int) (top, evicted cpuRecords) {
    if len(rs) <= n {
        return rs, nil
    }
    // QuickSelect 在 O(n) 时间复杂度内找到第 k 大元素
    if err := quickselect.QuickSelect(rs, n); err != nil {
        return rs, nil
    }
    return rs[:n], rs[n:]
}
```

### 3. CPU 时间调整 (tune)

由于 CPU Profile 是基于采样的，在优化器生成执行计划之前，SQL 只有 `sql_digest` 标签而没有 `plan_digest` 标签。`tune` 函数用于处理这种情况：

```go
func (s *sqlStats) tune() {
    if len(s.plans) == 0 {
        s.plans[""] = s.total  // 没有 plan 时，全部算作空 plan
        return
    }
    if len(s.plans) == 1 {
        for k := range s.plans {
            s.plans[k] = s.total  // 只有一个 plan 时，直接使用 total
            return
        }
    }
    // 多个 plan 时，计算优化器消耗的时间
    planTotal := int64(0)
    for _, v := range s.plans {
        planTotal += v
    }
    optimize := s.total - planTotal
    if optimize > 0 {
        s.plans[""] += optimize  // 差值为优化器消耗的时间
    }
}
```

### 4. 数据压缩策略

对于大型执行计划，TopSQL 采用压缩策略避免传输过多数据：

```go
func (m *normalizedPlanMap) toProto(keyspaceName []byte, decodePlan planBinaryDecodeFunc, compressPlan planBinaryCompressFunc) []tipb.PlanMeta {
    // ...
    if originalMeta.isLarge {
        // 大型 Plan 使用压缩格式
        protoMeta.EncodedNormalizedPlan = compressPlan(hack.Slice(originalMeta.binaryNormalizedPlan))
    } else {
        // 正常 Plan 直接解码
        protoMeta.NormalizedPlan, err = decodePlan(originalMeta.binaryNormalizedPlan)
    }
    // ...
}
```

### 5. 无锁数据结构

元信息缓存使用 `sync.Map` + `atomic.Pointer` 实现高效的并发访问：

```go
type normalizedSQLMap struct {
    data   atomic.Pointer[sync.Map]
    length atomic2.Int64
}

func (m *normalizedSQLMap) register(sqlDigest []byte, normalizedSQL string, isInternal bool) {
    if m.length.Load() >= topsqlstate.GlobalState.MaxCollect.Load() {
        return  // 超过限制直接丢弃
    }
    data := m.data.Load()
    _, loaded := data.LoadOrStore(string(sqlDigest), sqlMeta{...})
    if !loaded {
        m.length.Add(1)
    }
}

func (m *normalizedSQLMap) take() *normalizedSQLMap {
    // 原子交换，实现无锁的数据交接
    data := m.data.Load()
    length := m.length.Load()
    r := &normalizedSQLMap{}
    r.data.Store(data)
    r.length.Store(length)
    m.data.Store(&sync.Map{})
    m.length.Store(0)
    return r
}
```

## 配置与状态管理

### 1. 系统变量

TopSQL 通过以下系统变量控制：

| 变量名 | 默认值 | 说明 |
|-------|--------|------|
| `tidb_enable_top_sql` | `false` | 是否启用 TopSQL |
| `tidb_top_sql_precision_seconds` | `1` | 采样精度（秒） |
| `tidb_top_sql_max_time_series_count` | `100` | 最大语句数（Top N） |
| `tidb_top_sql_max_meta_count` | `5000` | 最大元信息数量 |
| `tidb_top_sql_report_interval_seconds` | `60` | 上报间隔（秒） |

### 2. 动态启用/禁用

TopSQL 支持通过系统变量动态启用或禁用：

```go
// pkg/util/topsql/reporter/datasink.go
func (r *DefaultDataSinkRegisterer) Register(dataSink DataSink) error {
    r.dataSinks[dataSink] = struct{}{}
    if len(r.dataSinks) > 0 {
        topsqlstate.EnableTopSQL()  // 有 DataSink 注册时启用
    }
    return nil
}

func (r *DefaultDataSinkRegisterer) Deregister(dataSink DataSink) {
    delete(r.dataSinks, dataSink)
    if len(r.dataSinks) == 0 {
        topsqlstate.DisableTopSQL()  // 没有 DataSink 时禁用
    }
}
```

## 初始化流程

TopSQL 在 TiDB Server 启动时初始化：

```go
// pkg/util/topsql/topsql.go
func init() {
    remoteReporter := reporter.NewRemoteTopSQLReporter(plancodec.DecodeNormalizedPlan, plancodec.Compress)
    globalTopSQLReport = remoteReporter
    singleTargetDataSink = reporter.NewSingleTargetDataSink(remoteReporter)
}

func SetupTopSQL(keyspaceName []byte, updater collector.ProcessCPUTimeUpdater) {
    globalTopSQLReport.BindKeyspaceName(keyspaceName)
    globalTopSQLReport.BindProcessCPUTimeUpdater(updater)
    globalTopSQLReport.Start()
    singleTargetDataSink.Start()

    stmtstats.RegisterCollector(globalTopSQLReport)
    stmtstats.SetupAggregator()
}

func RegisterPubSubServer(s *grpc.Server) {
    if register, ok := globalTopSQLReport.(reporter.DataSinkRegisterer); ok {
        service := reporter.NewTopSQLPubSubService(register)
        tipb.RegisterTopSQLPubSubServer(s, service)
    }
}
```

## 总结

TopSQL 的设计体现了以下几个关键思想：

1. **低侵入性**：通过 Goroutine Labels 机制，无需修改 SQL 执行的核心路径即可采集 CPU 时间数据。

2. **高效聚合**：使用 QuickSelect 算法实现 O(n) 复杂度的 Top N 筛选，同时将被淘汰的数据聚合到 "others"，确保数据完整性。

3. **灵活上报**：通过 DataSink 接口支持多种数据消费方式（推送到远端 Agent、发布订阅模式等）。

4. **资源保护**：通过各种限制（MaxCollect、MaxStatementCount 等）防止 TopSQL 本身消耗过多资源。

5. **动态可控**：支持通过系统变量动态启用/禁用，以及调整各种参数。

通过这些设计，TopSQL 能够在对系统性能影响最小的情况下，提供准确的 SQL 资源消耗分析数据，帮助用户快速定位性能问题。

## 参考资料

- [TiDB Dashboard TopSQL 用户文档](https://docs.pingcap.com/zh/tidb/stable/top-sql)
- [Go Profiler Labels](https://rakyll.org/profiler-labels/)
- [QuickSelect 算法](https://en.wikipedia.org/wiki/Quickselect)
