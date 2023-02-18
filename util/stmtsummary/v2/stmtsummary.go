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
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/kvcache"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/stmtsummary"
	atomic2 "go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
)

const (
	defaultEnabled             = true
	defaultEnableInternalQuery = false
	defaultMaxStmtCount        = 3000
	defaultMaxSQLLength        = 4096
	defaultRefreshInterval     = 30 * 60 // 30 min
	defaultRotateCheckInterval = 1       // s
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

	window     *stmtWindow
	windowLock sync.Mutex
	storage    stmtStorage
	closeWg    sync.WaitGroup
	closed     atomic.Bool
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
		window:                 newStmtWindow(timeNow(), uint(defaultMaxStmtCount)),
		storage: newStmtLogStorage(&log.Config{
			File: log.FileLogConfig{
				Filename:   cfg.Filename,
				MaxSize:    cfg.FileMaxSize,
				MaxDays:    cfg.FileMaxDays,
				MaxBackups: cfg.FileMaxBackups,
			},
		}),
	}

	s.closeWg.Add(1)
	go func() {
		defer s.closeWg.Done()
		s.rotateLoop()
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
		window:                 newStmtWindow(timeNow(), maxStmtCount),
		storage:                &mockStmtStorage{},
	}

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

// Add adds a single stmtsummary.StmtExecInfo to the current statistics window
// of StmtSummary. Before adding, it will check whether the current window has
// expired, and if it has expired, the window will be persisted asynchronously
// and a new window will be created to replace the current one.
func (s *StmtSummary) Add(info *stmtsummary.StmtExecInfo) {
	if s.closed.Load() {
		return
	}

	k := &stmtKey{
		schemaName: info.SchemaName,
		digest:     info.Digest,
		prevDigest: info.PrevSQLDigest,
		planDigest: info.PlanDigest,
	}
	k.Hash() // Calculate hash value in advance, to reduce the time holding the window lock.

	// Add info to the current statistics window.
	s.windowLock.Lock()
	var record *lockedStmtRecord
	if v, ok := s.window.lru.Get(k); ok {
		record = v.(*lockedStmtRecord)
	} else {
		record = &lockedStmtRecord{StmtRecord: NewStmtRecord(info)}
		s.window.lru.Put(k, record)
	}
	s.windowLock.Unlock()

	record.Lock()
	record.Add(info)
	record.Unlock()
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
	if s.cancel != nil {
		s.cancel()
		s.closeWg.Wait()
	}
	s.closed.Store(true)
	s.flush()
}

func (s *StmtSummary) flush() {
	now := timeNow()

	s.windowLock.Lock()
	window := s.window
	s.window = newStmtWindow(now, uint(s.MaxStmtCount()))
	s.windowLock.Unlock()

	if window.lru.Size() > 0 {
		s.storage.persist(window, now)
	}
	err := s.storage.sync()
	if err != nil {
		logutil.BgLogger().Error("sync stmt summary failed", zap.Error(err))
	}
}

// GetMoreThanCntBindableStmt is used to get bindable statements.
// Statements whose execution times exceed the threshold will be
// returned. Since the historical data has been persisted, we only
// refer to the statistics data of the current window in memory.
func (s *StmtSummary) GetMoreThanCntBindableStmt(cnt int64) []*stmtsummary.BindableStmt {
	s.windowLock.Lock()
	values := s.window.lru.Values()
	s.windowLock.Unlock()
	stmts := make([]*stmtsummary.BindableStmt, 0, len(values))
	for _, value := range values {
		record := value.(*lockedStmtRecord)
		func() {
			record.Lock()
			defer record.Unlock()
			if record.StmtType == "Select" ||
				record.StmtType == "Delete" ||
				record.StmtType == "Update" ||
				record.StmtType == "Insert" ||
				record.StmtType == "Replace" {
				if len(record.AuthUsers) > 0 && record.ExecCount > cnt {
					stmt := &stmtsummary.BindableStmt{
						Schema:    record.SchemaName,
						Query:     record.SampleSQL,
						PlanHint:  record.PlanHint,
						Charset:   record.Charset,
						Collation: record.Collation,
						Users:     make(map[string]struct{}),
					}
					maps.Copy(stmt.Users, record.AuthUsers)

					// If it is SQL command prepare / execute, the ssElement.sampleSQL
					// is `execute ...`, we should get the original select query.
					// If it is binary protocol prepare / execute, ssbd.normalizedSQL
					// should be same as ssElement.sampleSQL.
					if record.Prepared {
						stmt.Query = record.NormalizedSQL
					}
					stmts = append(stmts, stmt)
				}
			}
		}()
	}
	return stmts
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
			s.windowLock.Unlock()
		}
	}
}

func (s *StmtSummary) rotate(now time.Time) {
	w := s.window
	s.window = newStmtWindow(now, uint(s.MaxStmtCount()))
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

// stmtWindow represents a single statistical window, which has a begin
// time and an end time. Data within a single window is eliminated
// according to the LRU strategy. All evicted data will be aggregated
// into stmtEvicted.
type stmtWindow struct {
	begin   time.Time
	lru     *kvcache.SimpleLRUCache // *stmtKey => *lockedStmtRecord
	evicted *stmtEvicted
}

func newStmtWindow(begin time.Time, capacity uint) *stmtWindow {
	w := &stmtWindow{
		begin:   begin,
		lru:     kvcache.NewSimpleLRUCache(capacity, 0, 0),
		evicted: newStmtEvicted(),
	}
	w.lru.SetOnEvict(func(k kvcache.Key, v kvcache.Value) {
		r := v.(*lockedStmtRecord)
		r.Lock()
		defer r.Unlock()
		w.evicted.add(k.(*stmtKey), r.StmtRecord)
	})
	return w
}

func (w *stmtWindow) clear() {
	w.lru.DeleteAll()
	w.evicted = newStmtEvicted()
}

type stmtStorage interface {
	persist(w *stmtWindow, end time.Time)
	sync() error
}

// stmtKey defines key for stmtElement.
type stmtKey struct {
	// Same statements may appear in different schema, but they refer to different tables.
	schemaName string
	digest     string
	// The digest of the previous statement.
	prevDigest string
	// The digest of the plan of this SQL.
	planDigest string
	// `hash` is the hash value of this object.
	hash []byte
}

// Hash implements SimpleLRUCache.Key.
// Only when current SQL is `commit` do we record `prevSQL`. Otherwise, `prevSQL` is empty.
// `prevSQL` is included in the key To distinguish different transactions.
func (k *stmtKey) Hash() []byte {
	if len(k.hash) == 0 {
		k.hash = make([]byte, 0, len(k.schemaName)+len(k.digest)+len(k.prevDigest)+len(k.planDigest))
		k.hash = append(k.hash, hack.Slice(k.digest)...)
		k.hash = append(k.hash, hack.Slice(k.schemaName)...)
		k.hash = append(k.hash, hack.Slice(k.prevDigest)...)
		k.hash = append(k.hash, hack.Slice(k.planDigest)...)
	}
	return k.hash
}

type stmtEvicted struct {
	sync.Mutex
	keys  map[string]struct{}
	other *StmtRecord
}

func newStmtEvicted() *stmtEvicted {
	return &stmtEvicted{
		keys: make(map[string]struct{}),
		other: &StmtRecord{
			AuthUsers:    make(map[string]struct{}),
			MinLatency:   time.Duration(math.MaxInt64),
			BackoffTypes: make(map[string]int),
			FirstSeen:    time.Unix(math.MaxInt64, 0),
		},
	}
}

func (e *stmtEvicted) add(key *stmtKey, record *StmtRecord) {
	if key == nil || record == nil {
		return
	}
	e.Lock()
	defer e.Unlock()
	e.keys[string(key.Hash())] = struct{}{}
	e.other.Merge(record)
}

func (e *stmtEvicted) count() int {
	e.Lock()
	defer e.Unlock()
	return len(e.keys)
}

type lockedStmtRecord struct {
	sync.Mutex
	*StmtRecord
}

type mockStmtStorage struct {
	sync.Mutex
	windows []*stmtWindow
}

func (s *mockStmtStorage) persist(w *stmtWindow, _ time.Time) {
	s.Lock()
	s.windows = append(s.windows, w)
	s.Unlock()
}

func (*mockStmtStorage) sync() error {
	return nil
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

// GetMoreThanCntBindableStmt wraps GlobalStmtSummary.GetMoreThanCntBindableStmt and
// stmtsummary.StmtSummaryByDigestMap.GetMoreThanCntBindableStmt.
func GetMoreThanCntBindableStmt(frequency int64) []*stmtsummary.BindableStmt {
	if config.GetGlobalConfig().Instance.StmtSummaryEnablePersistent {
		return GlobalStmtSummary.GetMoreThanCntBindableStmt(frequency)
	}
	return stmtsummary.StmtSummaryByDigestMap.GetMoreThanCntBindableStmt(frequency)
}
