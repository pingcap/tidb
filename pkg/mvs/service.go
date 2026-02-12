package mvs

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	basic "github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

type mvLogItem Item[*mvLog]
type mvItem Item[*mv]

type MVMetricsReporter interface {
	reportMetrics(*MVService)
	observeTaskDuration(taskType, result string, duration time.Duration)
	observeFetchDuration(fetchType, result string, duration time.Duration)
	observeRunEvent(eventType string)
}

type MVServiceHelper interface {
	ServerHelper
	MVTaskHandler
	MVMetricsReporter
}

type MVService struct {
	sysSessionPool basic.SessionPool
	lastRefresh    atomic.Int64
	ctx            context.Context
	sch            *ServerConsistentHash

	executor *TaskExecutor
	notifier Notifier
	ddlDirty atomic.Bool

	mh MVServiceHelper

	fetchInterval         time.Duration
	basicInterval         time.Duration
	serverRefreshInterval time.Duration

	retryBaseDelayNanos atomic.Int64
	retryMaxDelayNanos  atomic.Int64
	backpressureMu      sync.RWMutex
	backpressureCfg     TaskBackpressureConfig

	metrics struct {
		mvCount                atomic.Int64
		mvLogCount             atomic.Int64
		runningMVRefreshCount  atomic.Int64
		runningMVLogPurgeCount atomic.Int64
	}

	reportCache struct {
		submittedCount int64
		completedCount int64
		failedCount    int64
		timeoutCount   int64
		rejectedCount  int64
	}

	mvRefreshMu struct {
		sync.Mutex
		pending map[string]mvItem
		prio    PriorityQueue[*mv]
	}
	mvLogPurgeMu struct {
		sync.Mutex
		pending map[string]mvLogItem
		prio    PriorityQueue[*mvLog]
	}
}

const (
	defaultMVTaskMaxConcurrency      = 10
	defaultMVTaskTimeout             = 60 * time.Second
	defaultMVFetchInterval           = 30 * time.Second
	defaultMVBasicInterval           = time.Second
	defaultTiDBServerRefreshInterval = 5 * time.Second
	defaultMVTaskRetryBase           = 10 * time.Second
	defaultMVTaskRetryMax            = 120 * time.Second
	maxNextScheduleTs                = 9e18

	defaultServerConsistentHashReplicas = 10

	mvTaskDurationTypeRefresh = "mv_refresh"
	mvTaskDurationTypePurge   = "mvlog_purge"
	mvTaskDurationResultOK    = "success"
	mvTaskDurationResultErr   = "failed"

	mvFetchDurationTypeMLogPurge = "fetch_mlog"
	mvFetchDurationTypeMViews    = "fetch_mviews"
	mvFetchDurationResultOK      = "success"
	mvFetchDurationResultErr     = "failed"

	mvRunEventInitFailed         = "init_failed"
	mvRunEventRecoveredPanic     = "mv_service_panic"
	mvRunEventServerRefreshOK    = "server_refresh_ok"
	mvRunEventServerRefreshError = "server_refresh_error"
	mvRunEventFetchByDDL         = "fetch_meta_trigger_ddl"
	mvRunEventFetchByInterval    = "fetch_meta_trigger_interval"
	mvRunEventFetchMLogOK        = "fetch_mlog_ok"
	mvRunEventFetchMLogError     = "fetch_mlog_error"
	mvRunEventFetchMViewsOK      = "fetch_mviews_ok"
	mvRunEventFetchMViewsError   = "fetch_mviews_error"
)

// MVServiceConfig is the config for constructing MVService.
type MVServiceConfig struct {
	TaskMaxConcurrency int
	TaskTimeout        time.Duration

	FetchInterval         time.Duration
	BasicInterval         time.Duration
	ServerRefreshInterval time.Duration

	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration

	ServerConsistentHashReplicas int

	TaskBackpressure TaskBackpressureConfig
}

// DefaultMVServiceConfig returns the default MV service config.
func DefaultMVServiceConfig() MVServiceConfig {
	return MVServiceConfig{
		TaskMaxConcurrency:           defaultMVTaskMaxConcurrency,
		TaskTimeout:                  defaultMVTaskTimeout,
		FetchInterval:                defaultMVFetchInterval,
		BasicInterval:                defaultMVBasicInterval,
		ServerRefreshInterval:        defaultTiDBServerRefreshInterval,
		RetryBaseDelay:               defaultMVTaskRetryBase,
		RetryMaxDelay:                defaultMVTaskRetryMax,
		ServerConsistentHashReplicas: defaultServerConsistentHashReplicas,
	}
}

func normalizeMVServiceConfig(cfg MVServiceConfig) MVServiceConfig {
	def := DefaultMVServiceConfig()
	if cfg.TaskMaxConcurrency <= 0 {
		cfg.TaskMaxConcurrency = def.TaskMaxConcurrency
	}
	if cfg.TaskTimeout < 0 {
		cfg.TaskTimeout = 0
	}
	if cfg.FetchInterval <= 0 {
		cfg.FetchInterval = def.FetchInterval
	}
	if cfg.BasicInterval <= 0 {
		cfg.BasicInterval = def.BasicInterval
	}
	if cfg.ServerRefreshInterval <= 0 {
		cfg.ServerRefreshInterval = def.ServerRefreshInterval
	}
	if cfg.RetryBaseDelay <= 0 {
		cfg.RetryBaseDelay = def.RetryBaseDelay
	}
	if cfg.RetryMaxDelay <= 0 {
		cfg.RetryMaxDelay = def.RetryMaxDelay
	}
	if cfg.ServerConsistentHashReplicas <= 0 {
		cfg.ServerConsistentHashReplicas = def.ServerConsistentHashReplicas
	}
	return cfg
}

// NewMVService creates a new MVService with the given helper and config.
func NewMVService(ctx context.Context, se basic.SessionPool, helper MVServiceHelper, cfg MVServiceConfig) *MVService {
	if helper == nil || se == nil {
		panic("invalid arguments")
	}
	cfg = normalizeMVServiceConfig(cfg)
	mgr := &MVService{
		sysSessionPool: se,
		sch:            NewServerConsistentHash(ctx, cfg.ServerConsistentHashReplicas, helper),
		executor:       NewTaskExecutor(ctx, cfg.TaskMaxConcurrency, cfg.TaskTimeout),

		notifier: NewNotifier(),
		ctx:      ctx,
		mh:       helper,

		fetchInterval:         cfg.FetchInterval,
		basicInterval:         cfg.BasicInterval,
		serverRefreshInterval: cfg.ServerRefreshInterval,
	}
	def := DefaultMVServiceConfig()
	if err := mgr.SetRetryDelayConfig(cfg.RetryBaseDelay, cfg.RetryMaxDelay); err != nil {
		logutil.BgLogger().Warn("invalid MV service retry config, fallback to defaults",
			zap.Error(err),
			zap.Duration("retry_base", cfg.RetryBaseDelay),
			zap.Duration("retry_max", cfg.RetryMaxDelay))
		_ = mgr.SetRetryDelayConfig(def.RetryBaseDelay, def.RetryMaxDelay)
	}
	if err := mgr.SetTaskBackpressureConfig(cfg.TaskBackpressure); err != nil {
		logutil.BgLogger().Warn("invalid MV service backpressure config, disable backpressure",
			zap.Error(err),
			zap.Bool("enabled", cfg.TaskBackpressure.Enabled),
			zap.Float64("cpu_threshold", cfg.TaskBackpressure.CPUThreshold),
			zap.Float64("mem_threshold", cfg.TaskBackpressure.MemThreshold),
			zap.Duration("delay", cfg.TaskBackpressure.Delay))
		_ = mgr.SetTaskBackpressureConfig(TaskBackpressureConfig{})
	}
	return mgr
}

// SetTaskExecConfig sets the execution config for MV tasks.
func (t *MVService) SetTaskExecConfig(maxConcurrency int, timeout time.Duration) {
	t.executor.UpdateConfig(maxConcurrency, timeout)
}

// GetTaskExecConfig returns the current execution config for MV tasks.
func (t *MVService) GetTaskExecConfig() (maxConcurrency int, timeout time.Duration) {
	return t.executor.GetConfig()
}

// SetRetryDelayConfig sets retry delay config.
func (t *MVService) SetRetryDelayConfig(base, max time.Duration) error {
	if t == nil {
		return fmt.Errorf("mv service is nil")
	}
	if base <= 0 || max <= 0 {
		return fmt.Errorf("retry delay must be positive")
	}
	if base > max {
		return fmt.Errorf("retry base delay must be less than or equal to max delay")
	}
	t.retryBaseDelayNanos.Store(int64(base))
	t.retryMaxDelayNanos.Store(int64(max))
	return nil
}

// GetRetryDelayConfig returns retry delay config.
func (t *MVService) GetRetryDelayConfig() (base, max time.Duration) {
	if t == nil {
		return defaultMVTaskRetryBase, defaultMVTaskRetryMax
	}
	base = time.Duration(t.retryBaseDelayNanos.Load())
	max = time.Duration(t.retryMaxDelayNanos.Load())
	if base <= 0 {
		base = defaultMVTaskRetryBase
	}
	if max <= 0 {
		max = defaultMVTaskRetryMax
	}
	if base > max {
		max = base
	}
	return base, max
}

// SetTaskBackpressureConfig sets task backpressure config.
func (t *MVService) SetTaskBackpressureConfig(cfg TaskBackpressureConfig) error {
	if t == nil {
		return fmt.Errorf("mv service is nil")
	}
	if cfg.Enabled {
		if cfg.CPUThreshold < 0 || cfg.CPUThreshold > 1 {
			return fmt.Errorf("cpu threshold out of range")
		}
		if cfg.MemThreshold < 0 || cfg.MemThreshold > 1 {
			return fmt.Errorf("memory threshold out of range")
		}
		if cfg.CPUThreshold <= 0 && cfg.MemThreshold <= 0 {
			return fmt.Errorf("at least one threshold must be positive when backpressure is enabled")
		}
		if cfg.Delay < 0 {
			return fmt.Errorf("backpressure delay must be non-negative")
		}
		t.SetTaskBackpressureController(NewCPUMemBackpressureController(cfg.CPUThreshold, cfg.MemThreshold, cfg.Delay))
	} else {
		t.SetTaskBackpressureController(nil)
	}
	t.backpressureMu.Lock()
	t.backpressureCfg = cfg
	t.backpressureMu.Unlock()
	return nil
}

// GetTaskBackpressureConfig returns task backpressure config.
func (t *MVService) GetTaskBackpressureConfig() TaskBackpressureConfig {
	if t == nil {
		return TaskBackpressureConfig{}
	}
	t.backpressureMu.RLock()
	cfg := t.backpressureCfg
	t.backpressureMu.RUnlock()
	return cfg
}

// SetTaskBackpressureController sets the task backpressure controller.
func (t *MVService) SetTaskBackpressureController(controller TaskBackpressureController) {
	t.executor.SetBackpressureController(controller)
}

// NotifyDDLChange marks MV metadata as dirty and wakes the service loop.
func (t *MVService) NotifyDDLChange() {
	t.ddlDirty.Store(true)
	t.notifier.Wake()
}

type mv struct {
	ID              string
	lastRefresh     time.Time
	refreshInterval time.Duration
	nextRefresh     time.Time

	orderTs    int64 // unix timestamp in milliseconds
	retryCount atomic.Int64
}

type mvLog struct {
	ID            string
	baseTableID   string
	lastPurge     time.Time
	purgeInterval time.Duration
	nextPurge     time.Time

	orderTs    int64 // unix timestamp in milliseconds
	retryCount atomic.Int64
}

// TaskBackpressureConfig is the runtime config for task backpressure.
type TaskBackpressureConfig struct {
	Enabled      bool
	CPUThreshold float64
	MemThreshold float64
	Delay        time.Duration
}

func (m *mv) Less(other *mv) bool {
	return m.orderTs < other.orderTs
}

func (m *mvLog) Less(other *mvLog) bool {
	return m.orderTs < other.orderTs
}

func calcRetryDelay(retryCount int64, base, max time.Duration) time.Duration {
	if retryCount <= 0 {
		return base
	}
	delay := base
	for i := int64(1); i < retryCount && delay < max; i++ {
		delay *= 2
		if delay >= max {
			delay = max
			break
		}
	}
	return delay
}

func resetTimer(timer *mvsTimer, delay time.Duration) {
	if delay < 0 {
		delay = 0
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}

func (t *MVService) Run() {
	defer func() {
		if r := recover(); r != nil {
			t.mh.observeRunEvent(mvRunEventRecoveredPanic)
			logutil.BgLogger().Error("MVService panicked", zap.Any("error", r))
		}
	}()
	if !t.sch.init() {
		t.mh.observeRunEvent(mvRunEventInitFailed)
		return
	}
	t.executor.Run()
	timer := mvsNewTimer(0)
	metricTimer := mvsNewTimer(t.basicInterval)

	defer func() {
		timer.Stop()
		metricTimer.Stop()
		t.executor.Close()
		t.mh.reportMetrics(t)
	}()

	lastServerRefresh := time.Time{}
	for {
		ddlDirty := false
		select {
		case <-timer.C:
		case <-metricTimer.C:
			t.mh.reportMetrics(t)
			resetTimer(metricTimer, t.basicInterval)
		case <-t.notifier.C:
			t.notifier.clear()
			ddlDirty = t.ddlDirty.Swap(false)
		case <-t.ctx.Done():
			return
		}

		now := mvsNow()

		if now.Sub(lastServerRefresh) >= t.serverRefreshInterval {
			if err := t.sch.refresh(); err != nil {
				t.mh.observeRunEvent(mvRunEventServerRefreshError)
				logutil.BgLogger().Warn("refresh all TiDB server info failed", zap.Error(err))
			} else {
				t.mh.observeRunEvent(mvRunEventServerRefreshOK)
			}
			lastServerRefresh = now
		}

		shouldFetch := t.shouldFetchMVMeta(now)
		if ddlDirty || shouldFetch {
			if ddlDirty {
				t.mh.observeRunEvent(mvRunEventFetchByDDL)
			}
			if shouldFetch {
				t.mh.observeRunEvent(mvRunEventFetchByInterval)
			}
			// if server info is stale, wait for the next refresh cycle to fetch server info
			if err := t.fetchAllMVMeta(); err != nil {
				// avoid tight retries on fetch failures
				t.lastRefresh.Store(now.UnixMilli())
			}
		}

		mvLogToPurge, mvToRefresh := t.fetchExecTasks(now)
		t.purgeMVLog(mvLogToPurge)
		t.refreshMV(mvToRefresh)

		next := t.nextScheduleTime(now)
		resetTimer(timer, mvsUntil(next))
	}
}

func (t *MVService) shouldFetchMVMeta(now time.Time) bool {
	last := t.lastRefresh.Load()
	if last == 0 {
		return true
	}
	return now.Sub(mvsUnixMilli(last)) >= t.fetchInterval
}

func (t *MVService) nextFetchTime(now time.Time) time.Time {
	last := t.lastRefresh.Load()
	if last == 0 {
		return now
	}
	next := mvsUnixMilli(last).Add(t.fetchInterval)
	if next.Before(now) {
		return now
	}
	return next
}

func (t *MVService) nextDueTime() (time.Time, bool) {
	next := time.Time{}
	has := false
	{
		t.mvRefreshMu.Lock()
		if item := t.mvRefreshMu.prio.Front(); item != nil {
			next = mvsUnixMilli(item.Value.orderTs)
			has = true
		}
		t.mvRefreshMu.Unlock()
	}

	{
		t.mvLogPurgeMu.Lock()
		if item := t.mvLogPurgeMu.prio.Front(); item != nil {
			due := mvsUnixMilli(item.Value.orderTs)
			if !has || due.Before(next) {
				next = due
				has = true
			}
		}
		t.mvLogPurgeMu.Unlock()
	}
	return next, has
}

func (t *MVService) nextScheduleTime(now time.Time) time.Time {
	next := t.nextFetchTime(now)
	if due, ok := t.nextDueTime(); ok && due.Before(next) {
		next = due
	}
	if next.Before(now) {
		return now
	}
	return next
}

func (t *MVService) fetchExecTasks(now time.Time) (mvLogToPurge []*mvLog, mvToRefresh []*mv) {
	{
		t.mvLogPurgeMu.Lock() // guard mvlog purge queue
		for t.mvLogPurgeMu.prio.Len() > 0 {
			it := t.mvLogPurgeMu.prio.Front()
			l := it.Value
			if l.orderTs == maxNextScheduleTs {
				break
			}
			if mvsUnixMilli(l.orderTs).After(now) {
				break
			}
			mvLogToPurge = append(mvLogToPurge, l)
			l.orderTs = maxNextScheduleTs // set to max to avoid being picked again before reschedule
			t.mvLogPurgeMu.prio.Update(it, l)
		}
		t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard
	}
	{
		t.mvRefreshMu.Lock() // guard mv refresh queue
		for t.mvRefreshMu.prio.Len() > 0 {
			it := t.mvRefreshMu.prio.Front()
			m := it.Value
			if m.orderTs == maxNextScheduleTs {
				break
			}
			if mvsUnixMilli(m.orderTs).After(now) {
				break
			}
			mvToRefresh = append(mvToRefresh, m)
			m.orderTs = maxNextScheduleTs // set to max to avoid being picked again before reschedule
			t.mvRefreshMu.prio.Update(it, m)
		}
		t.mvRefreshMu.Unlock() // release mv refresh queue guard
	}
	return
}

func (t *MVService) refreshMV(mvToRefresh []*mv) {
	if len(mvToRefresh) == 0 {
		return
	}
	for _, m := range mvToRefresh {
		t.executor.Submit("mv-refresh/"+m.ID, func() error {
			t.metrics.runningMVRefreshCount.Add(1)
			defer t.metrics.runningMVRefreshCount.Add(-1)
			taskStart := mvsNow()
			nextRefresh, err := t.mh.RefreshMV(t.ctx, t.sysSessionPool, m.ID)
			result := mvTaskDurationResultOK
			if err != nil {
				result = mvTaskDurationResultErr
			}
			t.mh.observeTaskDuration(mvTaskDurationTypeRefresh, result, mvsSince(taskStart))
			if err != nil {
				retryCount := m.retryCount.Add(1)
				t.rescheduleMV(m, mvsNow().Add(t.retryDelay(retryCount)).UnixMilli())
				t.notifier.Wake()
				return err
			}
			if nextRefresh.IsZero() {
				m.retryCount.Store(0)
				t.removeMVTask(m)
				t.notifier.Wake()
				return nil
			}
			m.retryCount.Store(0)
			t.rescheduleMVSuccess(m, nextRefresh)
			t.notifier.Wake()
			return nil
		})
	}
}

func (t *MVService) purgeMVLog(mvLogToPurge []*mvLog) {
	if len(mvLogToPurge) == 0 {
		return
	}
	for _, l := range mvLogToPurge {
		t.executor.Submit("mvlog-purge/"+l.ID, func() error {
			t.metrics.runningMVLogPurgeCount.Add(1)
			defer t.metrics.runningMVLogPurgeCount.Add(-1)
			taskStart := mvsNow()
			nextPurge, err := t.mh.PurgeMVLog(t.ctx, t.sysSessionPool, l.ID)
			result := mvTaskDurationResultOK
			if err != nil {
				result = mvTaskDurationResultErr
			}
			t.mh.observeTaskDuration(mvTaskDurationTypePurge, result, mvsSince(taskStart))
			if err != nil {
				retryCount := l.retryCount.Add(1)
				t.rescheduleMVLog(l, mvsNow().Add(t.retryDelay(retryCount)).UnixMilli())
				t.notifier.Wake()
				return err
			}
			if nextPurge.IsZero() {
				l.retryCount.Store(0)
				t.removeMVLogTask(l)
				t.notifier.Wake()
				return nil
			}
			l.retryCount.Store(0)
			t.rescheduleMVLogSuccess(l, nextPurge)
			t.notifier.Wake()
			return nil
		})
	}
}

func (t *MVService) removeMVLogTask(l *mvLog) {
	t.mvLogPurgeMu.Lock() // guard mvlog purge queue
	if it, ok := t.mvLogPurgeMu.pending[l.ID]; ok && it.Value == l {
		delete(t.mvLogPurgeMu.pending, l.ID)
		t.mvLogPurgeMu.prio.Remove(it)
	}
	t.metrics.mvLogCount.Store(int64(len(t.mvLogPurgeMu.pending)))
	t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard
}

func (t *MVService) removeMVTask(m *mv) {
	t.mvRefreshMu.Lock() // guard mv refresh queue
	if it, ok := t.mvRefreshMu.pending[m.ID]; ok && it.Value == m {
		delete(t.mvRefreshMu.pending, m.ID)
		t.mvRefreshMu.prio.Remove(it)
	}
	t.metrics.mvCount.Store(int64(len(t.mvRefreshMu.pending)))
	t.mvRefreshMu.Unlock() // release mv refresh queue guard
}

func (t *MVService) rescheduleMV(m *mv, next int64) {
	t.mvRefreshMu.Lock() // guard mv refresh queue
	if it, ok := t.mvRefreshMu.pending[m.ID]; ok && it.Value == m {
		m.orderTs = next
		t.mvRefreshMu.prio.Update(it, m)
	}
	t.mvRefreshMu.Unlock() // release mv refresh queue guard
}

func (t *MVService) rescheduleMVSuccess(m *mv, nextRefresh time.Time) {
	orderTs := nextRefresh.UnixMilli()

	t.mvRefreshMu.Lock() // guard mv refresh queue
	if it, ok := t.mvRefreshMu.pending[m.ID]; ok && it.Value == m {
		m.nextRefresh = nextRefresh
		m.orderTs = orderTs
		t.mvRefreshMu.prio.Update(it, m)
	}
	t.mvRefreshMu.Unlock() // release mv refresh queue guard
}

func (t *MVService) rescheduleMVLog(l *mvLog, next int64) {
	t.mvLogPurgeMu.Lock() // guard mvlog purge queue
	if it, ok := t.mvLogPurgeMu.pending[l.ID]; ok && it.Value == l {
		l.orderTs = next
		t.mvLogPurgeMu.prio.Update(it, l)
	}
	t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard
}

func (t *MVService) rescheduleMVLogSuccess(l *mvLog, nextPurge time.Time) {
	orderTs := nextPurge.UnixMilli()

	t.mvLogPurgeMu.Lock() // guard mvlog purge queue
	if it, ok := t.mvLogPurgeMu.pending[l.ID]; ok && it.Value == l {
		l.nextPurge = nextPurge
		l.orderTs = orderTs
		t.mvLogPurgeMu.prio.Update(it, l)
	}
	t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard
}

/*
对于 newPending 中每条记录：

	更新 purgeInterval 和 nextPurge 字段
	如果 nextPurge 字段发生变化, 表示核心元信息变动
		如果正在执行任务（orderTs == maxNextScheduleTs）, 则等待任务执行完毕后根据最新的 nextPurge 更新 orderTs 并调整优先级队列中的位置
		如果未在执行任务, 则根据 nextPurge 更新 orderTs 并调整优先级队列中的位置
*/
func (t *MVService) buildMLogPurgeTasks(newPending map[string]*mvLog) error {
	t.mvLogPurgeMu.Lock()         // guard mvlog purge queue
	defer t.mvLogPurgeMu.Unlock() // release mvlog purge queue guard

	if t.mvLogPurgeMu.pending == nil {
		t.mvLogPurgeMu.pending = make(map[string]mvLogItem, len(newPending))
	}
	for id, nl := range newPending {
		if ol, ok := t.mvLogPurgeMu.pending[id]; ok {
			ol.Value.purgeInterval = nl.purgeInterval
			changed := ol.Value.nextPurge != nl.nextPurge
			ol.Value.nextPurge = nl.nextPurge
			if ol.Value.orderTs != maxNextScheduleTs { // not running
				if changed {
					ol.Value.orderTs = ol.Value.nextPurge.UnixMilli()
					t.mvLogPurgeMu.prio.Update(ol, ol.Value)
				}
			}
			continue
		}
		t.mvLogPurgeMu.pending[id] = t.mvLogPurgeMu.prio.Push(nl)
	}
	for id, item := range t.mvLogPurgeMu.pending {
		if _, ok := newPending[id]; ok {
			continue
		}
		delete(t.mvLogPurgeMu.pending, id)
		t.mvLogPurgeMu.prio.Remove(item)
	}

	t.metrics.mvLogCount.Store(int64(len(t.mvLogPurgeMu.pending)))

	return nil
}

func (t *MVService) buildMVRefreshTasks(newPending map[string]*mv) error {
	t.mvRefreshMu.Lock()         // guard mv refresh queue
	defer t.mvRefreshMu.Unlock() // release mv refresh queue guard

	if t.mvRefreshMu.pending == nil {
		t.mvRefreshMu.pending = make(map[string]mvItem, len(newPending))
	}
	for id, nm := range newPending {
		if om, ok := t.mvRefreshMu.pending[id]; ok {
			om.Value.refreshInterval = nm.refreshInterval
			changed := om.Value.nextRefresh != nm.nextRefresh
			om.Value.nextRefresh = nm.nextRefresh
			if om.Value.orderTs != maxNextScheduleTs { // not running
				if changed {
					om.Value.orderTs = om.Value.nextRefresh.UnixMilli()
					t.mvRefreshMu.prio.Update(om, om.Value)
				}
			}
		} else {
			t.mvRefreshMu.pending[id] = t.mvRefreshMu.prio.Push(nm)
		}
	}
	for id, item := range t.mvRefreshMu.pending {
		if _, ok := newPending[id]; ok {
			continue
		}
		delete(t.mvRefreshMu.pending, id)
		t.mvRefreshMu.prio.Remove(item)
	}

	t.metrics.mvCount.Store(int64(len(t.mvRefreshMu.pending)))

	return nil
}

func (t *MVService) fetchAllTiDBMLogPurge() (map[string]*mvLog, error) {
	start := mvsNow()
	result := mvFetchDurationResultOK
	defer func() {
		t.mh.observeFetchDuration(mvFetchDurationTypeMLogPurge, result, mvsSince(start))
	}()

	newPending, err := t.mh.fetchAllTiDBMLogPurge(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvFetchDurationResultErr
		t.mh.observeRunEvent(mvRunEventFetchMLogError)
		return nil, err
	}
	t.mh.observeRunEvent(mvRunEventFetchMLogOK)
	for id := range newPending {
		if id == "" || !t.sch.Available(id) {
			delete(newPending, id)
		}
	}
	return newPending, nil
}

func (t *MVService) fetchAllTiDBMViews() (map[string]*mv, error) {
	start := mvsNow()
	result := mvFetchDurationResultOK
	defer func() {
		t.mh.observeFetchDuration(mvFetchDurationTypeMViews, result, mvsSince(start))
	}()

	newPending, err := t.mh.fetchAllTiDBMViews(t.ctx, t.sysSessionPool)
	if err != nil {
		result = mvFetchDurationResultErr
		t.mh.observeRunEvent(mvRunEventFetchMViewsError)
		return nil, err
	}
	t.mh.observeRunEvent(mvRunEventFetchMViewsOK)
	for id := range newPending {
		if id == "" || !t.sch.Available(id) {
			delete(newPending, id)
		}
	}
	return newPending, nil
}

func (t *MVService) fetchAllMVMeta() error {
	newMLogPending, err := t.fetchAllTiDBMLogPurge()
	if err != nil {
		return err
	}
	newMViewPending, err := t.fetchAllTiDBMViews()
	if err != nil {
		return err
	}
	if err = t.buildMLogPurgeTasks(newMLogPending); err != nil {
		return err
	}
	if err = t.buildMVRefreshTasks(newMViewPending); err != nil {
		return err
	}

	t.lastRefresh.Store(mvsNow().UnixMilli())
	return nil
}

func (t *MVService) retryDelay(retryCount int64) time.Duration {
	base, max := t.GetRetryDelayConfig()
	return calcRetryDelay(retryCount, base, max)
}
