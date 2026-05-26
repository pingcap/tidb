// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stmtsummary

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/kvcache"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/stmtsummary"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	defaultEnabled             = true
	defaultEnableInternalQuery = false
	defaultMaxStmtCount        = 3000
	defaultMaxSQLLength        = 32768
	defaultRefreshInterval     = 30 * 60 // 30 min
	defaultRotateCheckInterval = 1       // s

	// evictedLogChanCap bounds the buffer of per-record evicted entries waiting
	// to be logged. When full, new evictions are dropped so Add() never blocks.
	evictedLogChanCap = 1024

	// evictedLogBatchSize and evictedLogFlushInterval bound the async logger's
	// batching. They reduce write frequency under eviction bursts while keeping
	// single-record latency low.
	evictedLogBatchSize       = 64
	evictedLogFlushInterval   = 100 * time.Millisecond
	evictedDropReportInterval = 30 * time.Second
)

var (
	// GlobalStmtSummary is the global StmtSummary instance, we need
	// to explicitly call Setup() to initialize it. It will then be
	// referenced by SessionVars.StmtSummary for each session.
	GlobalStmtSummary *StmtSummary

	timeNow = time.Now
)

// Setup initializes the GlobalStmtSummary.
func Setup(cfg *Config) (err error) {
	GlobalStmtSummary, err = NewStmtSummary(cfg)
	return
}

// Close closes the GlobalStmtSummary.
func Close() {
	if GlobalStmtSummary != nil {
		GlobalStmtSummary.Close()
	}
}

// Config is the static configuration of StmtSummary. It cannot be
// modified at runtime.
type Config struct {
	Filename       string
	FileMaxSize    int
	FileMaxDays    int
	FileMaxBackups int
}

// StmtSummary represents the complete statements summary statistics.
// It controls data rotation and persistence internally, and provides
// reading interface through MemReader and HistoryReader.
type StmtSummary struct {
	ctx    context.Context
	cancel context.CancelFunc

	optEnabled             *atomic2.Bool
	optEnableInternalQuery *atomic2.Bool
	optMaxStmtCount        *atomic2.Uint32
	optMaxSQLLength        *atomic2.Uint32
	optRefreshInterval     *atomic2.Uint32
	optPersistEvicted      *atomic2.Bool

	window     *stmtWindow
	windowLock sync.Mutex
	storage    stmtStorage
	closeWg    sync.WaitGroup
	closed     atomic.Bool

	// evictedCh carries per-record evictions to the async logger.
	// Eviction persistence is controlled by optPersistEvicted; sends are non-blocking.
	evictedCh      chan *StmtRecord
	evictedDropped atomic.Uint64
}

// NewStmtSummary creates a new StmtSummary from Config.
func NewStmtSummary(cfg *Config) (*StmtSummary, error) {
	if cfg.Filename == "" {
		return nil, errors.New("stmtsummary: empty filename")
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &StmtSummary{
		ctx:    ctx,
		cancel: cancel,

		// These options can be changed dynamically at runtime.
		// The default values here are just placeholders, and the real values in
		// sessionctx/variables/tidb_vars.go will overwrite them after TiDB starts.
		optEnabled:             atomic2.NewBool(defaultEnabled),
		optEnableInternalQuery: atomic2.NewBool(defaultEnableInternalQuery),
		optMaxStmtCount:        atomic2.NewUint32(defaultMaxStmtCount),
		optMaxSQLLength:        atomic2.NewUint32(defaultMaxSQLLength),
		optRefreshInterval:     atomic2.NewUint32(defaultRefreshInterval),
		optPersistEvicted:      atomic2.NewBool(false),
		storage: newStmtLogStorage(&log.Config{
			File: log.FileLogConfig{
				Filename:   cfg.Filename,
				MaxSize:    cfg.FileMaxSize,
				MaxDays:    cfg.FileMaxDays,
				MaxBackups: cfg.FileMaxBackups,
			},
		}),
		evictedCh: make(chan *StmtRecord, evictedLogChanCap),
	}
	s.window = newStmtWindow(timeNow(), uint(defaultMaxStmtCount), s.onEvict)

	s.closeWg.Add(1)
	go func() {
		defer s.closeWg.Done()
		s.rotateLoop()
	}()
	s.closeWg.Add(1)
	go func() {
		defer s.closeWg.Done()
		s.evictedLogLoop()
	}()

	return s, nil
}

// NewStmtSummary4Test creates a new StmtSummary for testing purposes.
func NewStmtSummary4Test(maxStmtCount uint) *StmtSummary {
	ctx, cancel := context.WithCancel(context.Background())

	ss := &StmtSummary{
		ctx:    ctx,
		cancel: cancel,

		optEnabled:             atomic2.NewBool(defaultEnabled),
		optEnableInternalQuery: atomic2.NewBool(defaultEnableInternalQuery),
		optMaxStmtCount:        atomic2.NewUint32(defaultMaxStmtCount),
		optMaxSQLLength:        atomic2.NewUint32(defaultMaxSQLLength),
		optRefreshInterval:     atomic2.NewUint32(60 * 60 * 24 * 365), // 1 year
		optPersistEvicted:      atomic2.NewBool(false),
		storage:                &mockStmtStorage{},
		evictedCh:              make(chan *StmtRecord, evictedLogChanCap),
	}
	ss.window = newStmtWindow(timeNow(), maxStmtCount, ss.onEvict)

	ss.closeWg.Add(1)
	go func() {
		defer ss.closeWg.Done()
		ss.evictedLogLoop()
	}()

	return ss
}

// Enabled returns whether the StmtSummary is enabled.
func (s *StmtSummary) Enabled() bool {
	return s.optEnabled.Load()
}

// SetEnabled is used to enable or disable StmtSummary. If disabled, in-memory
// data will be cleared, (persisted data will still be remained).
func (s *StmtSummary) SetEnabled(v bool) error {
	s.optEnabled.Store(v)
	if !v {
		s.Clear()
	}

	return nil
}

// EnableInternalQuery returns whether the StmtSummary counts internal queries.
func (s *StmtSummary) EnableInternalQuery() bool {
	return s.optEnableInternalQuery.Load()
}

// SetEnableInternalQuery is used to enable or disable StmtSummary's internal
// query statistics. If disabled, in-memory internal queries will be cleared,
// (persisted internal queries will still be remained).
func (s *StmtSummary) SetEnableInternalQuery(v bool) error {
	s.optEnableInternalQuery.Store(v)
	if !v {
		s.ClearInternal()
	}

	return nil
}

// MaxStmtCount returns the maximum number of statements.
func (s *StmtSummary) MaxStmtCount() uint32 {
	return s.optMaxStmtCount.Load()
}

// SetMaxStmtCount is used to set the maximum number of statements.
// If the current number exceeds the maximum number, the excess will be evicted.
func (s *StmtSummary) SetMaxStmtCount(v uint32) error {
	if v < 1 {
		v = 1
	}
	s.optMaxStmtCount.Store(v)
	s.windowLock.Lock()
	_ = s.window.lru.SetCapacity(uint(v))
	s.windowLock.Unlock()

	return nil
}

// MaxSQLLength returns the maximum size of a single SQL statement.
func (s *StmtSummary) MaxSQLLength() uint32 {
	return s.optMaxSQLLength.Load()
}

// SetMaxSQLLength sets the maximum size of a single SQL statement.
func (s *StmtSummary) SetMaxSQLLength(v uint32) error {
	s.optMaxSQLLength.Store(v)

	return nil
}

// RefreshInterval returns the period (in seconds) at which the statistics
// window is refreshed (persisted).
func (s *StmtSummary) RefreshInterval() uint32 {
	return s.optRefreshInterval.Load()
}

// SetRefreshInterval sets the period (in seconds) for the statistics window
// to be refreshed (persisted). This may trigger a refresh (persistence) of
// the current statistics window early.
func (s *StmtSummary) SetRefreshInterval(v uint32) error {
	if v < 1 {
		v = 1
	}
	s.optRefreshInterval.Store(v)

	return nil
}

// PersistEvicted reports whether per-record evictions are persisted.
func (s *StmtSummary) PersistEvicted() bool {
	return s.optPersistEvicted.Load()
}

// SetPersistEvicted enables or disables per-record eviction persistence.
func (s *StmtSummary) SetPersistEvicted(v bool) error {
	s.optPersistEvicted.Store(v)
	return nil
}

// Add adds a single stmtsummary.StmtExecInfo to the current statistics window
// of StmtSummary. Before adding, it will check whether the current window has
// expired, and if it has expired, the window will be persisted asynchronously
// and a new window will be created to replace the current one.
func (s *StmtSummary) Add(info *stmtsummary.StmtExecInfo) {
	if s.closed.Load() {
		return
	}

	k := stmtsummary.StmtDigestKeyPool.Get().(*stmtsummary.StmtDigestKey)
	// Init hash value in advance, to reduce the time holding the lock.
	k.Init(info.SchemaName, info.Digest, info.PrevSQLDigest, info.PlanDigest, info.ResourceGroupName)

	// Add info to the current statistics window.
	s.windowLock.Lock()
	if s.closed.Load() {
		s.windowLock.Unlock()
		stmtsummary.StmtDigestKeyPool.Put(k)
		return
	}
	var record *lockedStmtRecord
	v, exist := s.window.lru.Get(k)
	if exist {
		record = v.(*lockedStmtRecord)
	} else {
		record = &lockedStmtRecord{StmtRecord: NewStmtRecord(info)}
		s.window.lru.Put(k, record)
	}
	s.windowLock.Unlock()

	record.Lock()
	record.Add(info)
	record.Unlock()
	if exist {
		stmtsummary.StmtDigestKeyPool.Put(k)
	}
}

// Evicted returns the number of statements evicted for the current
// time window. The returned type is one row consisting of three
// columns: [BEGIN_TIME, END_TIME, EVICTED_COUNT].
func (s *StmtSummary) Evicted() []types.Datum {
	s.windowLock.Lock()
	count := int64(s.window.evicted.count())
	s.windowLock.Unlock()
	if count == 0 {
		return nil
	}
	begin := types.NewTime(types.FromGoTime(s.window.begin), mysql.TypeTimestamp, 0)
	end := types.NewTime(types.FromGoTime(timeNow()), mysql.TypeTimestamp, 0)
	return types.MakeDatums(begin, end, count)
}

// Clear clears all data in the current window, and the data that
// has been persisted will not be cleared.
func (s *StmtSummary) Clear() {
	s.windowLock.Lock()
	defer s.windowLock.Unlock()
	s.window.clear()
}

// ClearInternal clears all internal queries of the current window,
// and the data that has been persisted will not be cleared.
func (s *StmtSummary) ClearInternal() {
	s.windowLock.Lock()
	defer s.windowLock.Unlock()
	for _, k := range s.window.lru.Keys() {
		v, _ := s.window.lru.Get(k)
		if v.(*lockedStmtRecord).IsInternal {
			s.window.lru.Delete(k)
		}
	}
}

// Close closes the work of StmtSummary.
func (s *StmtSummary) Close() {
	s.windowLock.Lock()
	if !s.closed.CompareAndSwap(false, true) {
		s.windowLock.Unlock()
		return
	}
	s.windowLock.Unlock()

	if s.cancel != nil {
		s.cancel()
		s.closeWg.Wait()
	}
	s.flush()
}

func (s *StmtSummary) flush() {
	now := timeNow()

	s.windowLock.Lock()
	window := s.window
	s.window = newStmtWindow(now, uint(s.MaxStmtCount()), s.onEvict)
	s.windowLock.Unlock()

	if window.lru.Size() > 0 {
		s.storage.persist(window, now)
	}
	err := s.storage.sync()
	if err != nil {
		logutil.BgLogger().Error("sync stmt summary failed", zap.Error(err))
	}
}

func (s *StmtSummary) rotateLoop() {
	tick := time.NewTicker(defaultRotateCheckInterval * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-tick.C:
			now := timeNow()
			s.windowLock.Lock()
			// The current window has expired and needs to be refreshed and persisted.
			if now.After(s.window.begin.Add(time.Duration(s.RefreshInterval()) * time.Second)) {
				s.rotate(now)
			}
			s.updateMetrics()
			s.windowLock.Unlock()
		}
	}
}

// updateMetrics reports the current window's record count and eviction count
// to Prometheus gauges. Must be called with windowLock held.
func (s *StmtSummary) updateMetrics() {
	metrics.SetStmtSummaryWindowMetrics(
		metrics.StmtSummaryTypeV2,
		float64(s.window.lru.Size()),
		float64(s.window.evictedCount.Load()),
	)
}

func (s *StmtSummary) rotate(now time.Time) {
	w := s.window
	s.window = newStmtWindow(now, uint(s.MaxStmtCount()), s.onEvict)
	size := w.lru.Size()
	if size > 0 {
		// Persist window asynchronously.
		s.closeWg.Add(1)
		go func() {
			defer s.closeWg.Done()
			s.storage.persist(w, now)
		}()
	}
}

// onEvict is the LRU eviction hook installed on every stmtWindow.
// Called while the record's lock is held (see newStmtWindow). We copy the
// fields we need and hand the clone off to the async log goroutine. A
// non-blocking send is used so the hot Add() path never stalls on log I/O.
func (s *StmtSummary) onEvict(_ *stmtsummary.StmtDigestKey, r *StmtRecord, begin, end time.Time) bool {
	if !s.optPersistEvicted.Load() {
		return false
	}
	if s.evictedCh == nil {
		return false
	}
	clone := cloneRecordForLog(r)
	clone.Begin = begin.Unix()
	clone.End = end.Unix()
	select {
	case s.evictedCh <- clone:
		return true
	default:
		s.evictedDropped.Add(1)
		return false
	}
}

// evictedLogLoop drains evictedCh and writes each record to the stmt log.
// When group_by_user is also enabled, each logged record represents exactly
// one (digest, user) group that fell out of the LRU.
func (s *StmtSummary) evictedLogLoop() {
	reportTicker := time.NewTicker(evictedDropReportInterval)
	defer reportTicker.Stop()

	flushTimer := time.NewTimer(evictedLogFlushInterval)
	if !flushTimer.Stop() {
		<-flushTimer.C
	}
	defer flushTimer.Stop()

	var lastDropReport uint64
	report := func() {
		cur := s.evictedDropped.Load()
		if cur > lastDropReport {
			logutil.BgLogger().Warn("stmt summary evicted log dropped records",
				zap.Uint64("dropped_total", cur),
				zap.Uint64("since_last_report", cur-lastDropReport),
			)
			lastDropReport = cur
		}
	}

	stopFlushTimer := func() {
		if !flushTimer.Stop() {
			select {
			case <-flushTimer.C:
			default:
			}
		}
	}

	batch := make([]*StmtRecord, 0, evictedLogBatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.storage.logEvicted(batch)
		for i := range batch {
			batch[i] = nil
		}
		batch = batch[:0]
		stopFlushTimer()
	}
	appendRecord := func(r *StmtRecord) {
		batch = append(batch, r)
		if len(batch) == 1 {
			flushTimer.Reset(evictedLogFlushInterval)
		}
		if len(batch) >= evictedLogBatchSize {
			flush()
		}
	}
	drainAvailable := func() {
		for len(batch) > 0 && len(batch) < evictedLogBatchSize {
			select {
			case r := <-s.evictedCh:
				appendRecord(r)
			default:
				return
			}
		}
	}

	for {
		select {
		case <-s.ctx.Done():
			// Close sets closed while holding windowLock before canceling this
			// context, and Add rechecks closed under the same lock. At this
			// point no Add can enqueue more evicted records.
			for {
				select {
				case r := <-s.evictedCh:
					appendRecord(r)
				default:
					flush()
					report()
					return
				}
			}
		case r := <-s.evictedCh:
			appendRecord(r)
			drainAvailable()
		case <-flushTimer.C:
			flush()
		case <-reportTicker.C:
			report()
		}
	}
}

// stmtWindow represents a single statistical window, which has a begin
// time and an end time. Data within a single window is eliminated
// according to the LRU strategy. All evicted data will be aggregated
// into stmtEvicted.
type stmtWindow struct {
	begin        time.Time
	lru          *kvcache.SimpleLRUCache // *StmtDigestKey => *lockedStmtRecord
	evicted      *stmtEvicted
	evictedCount atomic.Int64 // total number of LRU evictions in this window
}

// onEvictFn is invoked for every LRU eviction. The callback receives the
// locked record (caller holds r.Lock) so it can copy fields cheaply. It
// returns true when the record has been handed off for per-record persistence,
// in which case the caller can skip adding it to the persisted aggregate.
// Must not block.
type onEvictFn func(key *stmtsummary.StmtDigestKey, r *StmtRecord, begin, end time.Time) bool

func newStmtWindow(begin time.Time, capacity uint, onEvict onEvictFn) *stmtWindow {
	w := &stmtWindow{
		begin:   begin,
		lru:     kvcache.NewSimpleLRUCache(capacity, 0, 0),
		evicted: newStmtEvicted(),
	}
	w.lru.SetOnEvict(func(k kvcache.Key, v kvcache.Value) {
		w.evictedCount.Add(1)
		r := v.(*lockedStmtRecord)
		r.Lock()
		defer r.Unlock()
		key := k.(*stmtsummary.StmtDigestKey)
		logged := false
		if onEvict != nil {
			logged = onEvict(key, r.StmtRecord, w.begin, timeNow())
		}
		w.evicted.add(key, r.StmtRecord, !logged)
	})
	return w
}

func (w *stmtWindow) clear() {
	w.lru.DeleteAll()
	w.evicted = newStmtEvicted()
	w.evictedCount.Store(0)
}

type stmtStorage interface {
	persist(w *stmtWindow, end time.Time)
	// logEvicted writes evicted records to durable storage. It may be
	// called concurrently with persist; implementations must be safe to call
	// from the evictedLogLoop goroutine.
	logEvicted(records []*StmtRecord)
	sync() error
}

type stmtEvicted struct {
	sync.Mutex
	keys     map[string]struct{}
	other    *StmtRecord
	unlogged *StmtRecord
}

func newStmtEvicted() *stmtEvicted {
	return &stmtEvicted{
		keys:     make(map[string]struct{}),
		other:    newEvictedAggregateRecord(),
		unlogged: newEvictedAggregateRecord(),
	}
}

func (e *stmtEvicted) add(key *stmtsummary.StmtDigestKey, record *StmtRecord, persistAsAggregate bool) {
	if key == nil || record == nil {
		return
	}
	e.Lock()
	defer e.Unlock()
	e.keys[string(key.Hash())] = struct{}{}
	e.other.Merge(record)
	if persistAsAggregate {
		e.unlogged.Merge(record)
	}
}

func (e *stmtEvicted) count() int {
	e.Lock()
	defer e.Unlock()
	return len(e.keys)
}

func newEvictedAggregateRecord() *StmtRecord {
	return &StmtRecord{
		AuthUsers:    make(map[string]struct{}),
		MinLatency:   time.Duration(math.MaxInt64),
		BackoffTypes: make(map[string]int),
		FirstSeen:    time.Now(),
		LastSeen:     time.Now(),
	}
}

type lockedStmtRecord struct {
	sync.Mutex
	*StmtRecord
}

type mockStmtStorage struct {
	sync.Mutex
	windows []*stmtWindow
	evicted []*StmtRecord
}

func (s *mockStmtStorage) persist(w *stmtWindow, _ time.Time) {
	s.Lock()
	s.windows = append(s.windows, w)
	s.Unlock()
}

func (s *mockStmtStorage) logEvicted(records []*StmtRecord) {
	s.Lock()
	s.evicted = append(s.evicted, records...)
	s.Unlock()
}

func (*mockStmtStorage) sync() error {
	return nil
}

// cloneRecordForLog returns a shallow copy of r with its two mutable maps
// (AuthUsers, BackoffTypes) cloned, so the async logger can marshal the
// snapshot without racing with further updates on the retained StmtRecord.
// Called with r's lock held (see onEvict).
func cloneRecordForLog(r *StmtRecord) *StmtRecord {
	c := *r
	if len(r.AuthUsers) > 0 {
		c.AuthUsers = make(map[string]struct{}, len(r.AuthUsers))
		for u := range r.AuthUsers {
			c.AuthUsers[u] = struct{}{}
		}
	}
	if len(r.BackoffTypes) > 0 {
		c.BackoffTypes = make(map[string]int, len(r.BackoffTypes))
		for k, v := range r.BackoffTypes {
			c.BackoffTypes[k] = v
		}
	}
	// IndexNames is a slice; shallow copy is fine because it is append-only.
	return &c
}

/* Public proxy functions between v1 and v2 */

// Add wraps GlobalStmtSummary.Add and stmtsummary.StmtSummaryByDigestMap.AddStatement.
func Add(stmtExecInfo *stmtsummary.StmtExecInfo) {
	if config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		GlobalStmtSummary.Add(stmtExecInfo)
	} else {
		stmtsummary.StmtSummaryByDigestMap.AddStatement(stmtExecInfo)
	}
}

// Enabled wraps GlobalStmtSummary.Enabled and stmtsummary.StmtSummaryByDigestMap.Enabled.
func Enabled() bool {
	if config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		return GlobalStmtSummary.Enabled()
	}
	return stmtsummary.StmtSummaryByDigestMap.Enabled()
}

// EnabledInternal wraps GlobalStmtSummary.EnableInternalQuery and stmtsummary.StmtSummaryByDigestMap.EnabledInternal.
func EnabledInternal() bool {
	if config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		return GlobalStmtSummary.EnableInternalQuery()
	}
	return stmtsummary.StmtSummaryByDigestMap.EnabledInternal()
}

// SetEnabled wraps GlobalStmtSummary.SetEnabled and stmtsummary.StmtSummaryByDigestMap.SetEnabled.
func SetEnabled(v bool) error {
	if config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		return GlobalStmtSummary.SetEnabled(v)
	}
	return stmtsummary.StmtSummaryByDigestMap.SetEnabled(v)
}

// SetEnableInternalQuery wraps GlobalStmtSummary.SetEnableInternalQuery and
// stmtsummary.StmtSummaryByDigestMap.SetEnabledInternalQuery.
func SetEnableInternalQuery(v bool) error {
	if config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		return GlobalStmtSummary.SetEnableInternalQuery(v)
	}
	return stmtsummary.StmtSummaryByDigestMap.SetEnabledInternalQuery(v)
}

// SetRefreshInterval wraps GlobalStmtSummary.SetRefreshInterval and stmtsummary.StmtSummaryByDigestMap.SetRefreshInterval.
func SetRefreshInterval(v int64) error {
	if config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		return GlobalStmtSummary.SetRefreshInterval(uint32(v))
	}
	return stmtsummary.StmtSummaryByDigestMap.SetRefreshInterval(v)
}

// SetHistorySize wraps stmtsummary.StmtSummaryByDigestMap.SetHistorySize.
func SetHistorySize(v int) error {
	if config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		return nil // not support
	}
	return stmtsummary.StmtSummaryByDigestMap.SetHistorySize(v)
}

// SetMaxStmtCount wraps GlobalStmtSummary.SetMaxStmtCount and stmtsummary.StmtSummaryByDigestMap.SetMaxStmtCount.
func SetMaxStmtCount(v int) error {
	if config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		return GlobalStmtSummary.SetMaxStmtCount(uint32(v))
	}
	return stmtsummary.StmtSummaryByDigestMap.SetMaxStmtCount(uint(v))
}

// SetMaxSQLLength wraps GlobalStmtSummary.SetMaxSQLLength and stmtsummary.StmtSummaryByDigestMap.SetMaxSQLLength.
func SetMaxSQLLength(v int) error {
	if config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		return GlobalStmtSummary.SetMaxSQLLength(uint32(v))
	}
	return stmtsummary.StmtSummaryByDigestMap.SetMaxSQLLength(v)
}

// SetPersistEvicted toggles per-record eviction persistence. Only v2
// (persistent) honors this flag; v1 has no log sink, so the call is a no-op
// for it.
func SetPersistEvicted(v bool) error {
	if GlobalStmtSummary != nil {
		return GlobalStmtSummary.SetPersistEvicted(v)
	}
	return nil
}
