// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package gcworker

import (
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/terror"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/tidb/ddl/util"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/privilege"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	tidbutil "github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// GCWorker periodically triggers GC process on tikv server.
type GCWorker struct {
	uuid         string
	desc         string
	store        tikv.Storage
	pdClient     pd.Client
	gcIsRunning  bool
	lastFinish   time.Time
	cancel       context.CancelFunc
	done         chan error
	testingKnobs struct {
		scanLocks    func(key []byte) []*tikv.Lock
		resolveLocks func(regionID tikv.RegionVerID) (ok bool, err error)
	}
}

// NewGCWorker creates a GCWorker instance.
func NewGCWorker(store tikv.Storage, pdClient pd.Client) (tikv.GCHandler, error) {
	ver, err := store.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
	worker := &GCWorker{
		uuid:        strconv.FormatUint(ver.Ver, 16),
		desc:        fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		store:       store,
		pdClient:    pdClient,
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
	}
	return worker, nil
}

// Start starts the worker.
func (w *GCWorker) Start() {
	var ctx context.Context
	ctx, w.cancel = context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go w.start(ctx, &wg)
	wg.Wait() // Wait create session finish in worker, some test code depend on this to avoid race.
}

// Close stops background goroutines.
func (w *GCWorker) Close() {
	w.cancel()
}

const (
	booleanTrue  = "true"
	booleanFalse = "false"

	gcWorkerTickInterval = time.Minute
	gcWorkerLease        = time.Minute * 2
	gcLeaderUUIDKey      = "tikv_gc_leader_uuid"
	gcLeaderDescKey      = "tikv_gc_leader_desc"
	gcLeaderLeaseKey     = "tikv_gc_leader_lease"

	gcLastRunTimeKey       = "tikv_gc_last_run_time"
	gcRunIntervalKey       = "tikv_gc_run_interval"
	gcDefaultRunInterval   = time.Minute * 10
	gcWaitTime             = time.Minute * 1
	gcRedoDeleteRangeDelay = 24 * time.Hour

	gcLifeTimeKey        = "tikv_gc_life_time"
	gcDefaultLifeTime    = time.Minute * 10
	gcMinLifeTime        = time.Minute * 10
	gcSafePointKey       = "tikv_gc_safe_point"
	gcConcurrencyKey     = "tikv_gc_concurrency"
	gcDefaultConcurrency = 2
	gcMinConcurrency     = 1
	gcMaxConcurrency     = 128
	// We don't want gc to sweep out the cached info belong to other processes, like coprocessor.
	gcScanLockLimit = tikv.ResolvedCacheSize / 2

	gcEnableKey          = "tikv_gc_enable"
	gcDefaultEnableValue = true

	gcModeKey         = "tikv_gc_mode"
	gcModeCentral     = "central"
	gcModeDistributed = "distributed"
	gcModeDefault     = gcModeDistributed

	gcScanLockModeKey      = "tikv_gc_scan_lock_mode"
	gcScanLockModeLegacy   = "legacy"
	gcScanLockModePhysical = "physical"
	gcScanLockModeDefault  = gcScanLockModePhysical

	gcAutoConcurrencyKey     = "tikv_gc_auto_concurrency"
	gcDefaultAutoConcurrency = true

	gcWorkerServiceSafePointID = "gc_worker"
)

var gcSafePointCacheInterval = tikv.GcSafePointCacheInterval

var gcVariableComments = map[string]string{
	gcLeaderUUIDKey:      "Current GC worker leader UUID. (DO NOT EDIT)",
	gcLeaderDescKey:      "Host name and pid of current GC leader. (DO NOT EDIT)",
	gcLeaderLeaseKey:     "Current GC worker leader lease. (DO NOT EDIT)",
	gcLastRunTimeKey:     "The time when last GC starts. (DO NOT EDIT)",
	gcRunIntervalKey:     "GC run interval, at least 10m, in Go format.",
	gcLifeTimeKey:        "All versions within life time will not be collected by GC, at least 10m, in Go format.",
	gcSafePointKey:       "All versions after safe point can be accessed. (DO NOT EDIT)",
	gcConcurrencyKey:     "How many goroutines used to do GC parallel, [1, 128], default 2",
	gcEnableKey:          "Current GC enable status",
	gcModeKey:            "Mode of GC, \"central\" or \"distributed\"",
	gcAutoConcurrencyKey: "Let TiDB pick the concurrency automatically. If set false, tikv_gc_concurrency will be used",
	gcScanLockModeKey:    "Mode of scanning locks, \"physical\" or \"legacy\"",
}

func (w *GCWorker) start(ctx context.Context, wg *sync.WaitGroup) {
	logutil.Logger(ctx).Info("[gc worker] start",
		zap.String("uuid", w.uuid))

	w.tick(ctx) // Immediately tick once to initialize configs.
	wg.Done()

	ticker := time.NewTicker(gcWorkerTickInterval)
	defer ticker.Stop()
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("gcWorker",
				zap.Reflect("r", r),
				zap.Stack("stack"))
			metrics.PanicCounter.WithLabelValues(metrics.LabelGCWorker).Inc()
		}
	}()
	for {
		select {
		case <-ticker.C:
			w.tick(ctx)
		case err := <-w.done:
			w.gcIsRunning = false
			w.lastFinish = time.Now()
			if err != nil {
				logutil.Logger(ctx).Error("[gc worker] runGCJob", zap.Error(err))
			}
		case <-ctx.Done():
			logutil.Logger(ctx).Info("[gc worker] quit", zap.String("uuid", w.uuid))
			return
		}
	}
}

func createSession(store kv.Storage) session.Session {
	for {
		se, err := session.CreateSession(store)
		if err != nil {
			logutil.BgLogger().Warn("[gc worker] create session", zap.Error(err))
			continue
		}
		// Disable privilege check for gc worker session.
		privilege.BindPrivilegeManager(se, nil)
		se.GetSessionVars().InRestrictedSQL = true
		return se
	}
}

func (w *GCWorker) tick(ctx context.Context) {
	isLeader, err := w.checkLeader()
	if err != nil {
		logutil.Logger(ctx).Warn("[gc worker] check leader", zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("check_leader").Inc()
		return
	}
	if isLeader {
		err = w.leaderTick(ctx)
		if err != nil {
			logutil.Logger(ctx).Warn("[gc worker] leader tick", zap.Error(err))
		}
	} else {
		// Config metrics should always be updated by leader, set them to 0 when current instance is not leader.
		metrics.GCConfigGauge.WithLabelValues(gcRunIntervalKey).Set(0)
		metrics.GCConfigGauge.WithLabelValues(gcLifeTimeKey).Set(0)
	}
}

// leaderTick of GC worker checks if it should start a GC job every tick.
func (w *GCWorker) leaderTick(ctx context.Context) error {
	if w.gcIsRunning {
		logutil.Logger(ctx).Info("[gc worker] there's already a gc job running, skipped",
			zap.String("leaderTick on", w.uuid))
		return nil
	}

	ok, safePoint, err := w.prepare()
	if err != nil || !ok {
		if err != nil {
			metrics.GCJobFailureCounter.WithLabelValues("prepare").Inc()
		}
		return errors.Trace(err)
	}
	// When the worker is just started, or an old GC job has just finished,
	// wait a while before starting a new job.
	if time.Since(w.lastFinish) < gcWaitTime {
		logutil.Logger(ctx).Info("[gc worker] another gc job has just finished, skipped.",
			zap.String("leaderTick on ", w.uuid))
		return nil
	}

	concurrency, err := w.getGCConcurrency(ctx)
	if err != nil {
		logutil.Logger(ctx).Info("[gc worker] failed to get gc concurrency.",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		return errors.Trace(err)
	}

	w.gcIsRunning = true
	logutil.Logger(ctx).Info("[gc worker] starts the whole job",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("concurrency", concurrency))
	go func() {
		w.done <- w.runGCJob(ctx, safePoint, concurrency)
	}()
	return nil
}

// prepare checks preconditions for starting a GC job. It returns a bool
// that indicates whether the GC job should start and the new safePoint.
func (w *GCWorker) prepare() (bool, uint64, error) {
	// Add a transaction here is to prevent following situations:
	// 1. GC check gcEnable is true, continue to do GC
	// 2. The user sets gcEnable to false
	// 3. The user gets `tikv_gc_safe_point` value is t1, then the user thinks the data after time t1 won't be clean by GC.
	// 4. GC update `tikv_gc_safe_point` value to t2, continue do GC in this round.
	// Then the data record that has been dropped between time t1 and t2, will be cleaned by GC, but the user thinks the data after t1 won't be clean by GC.
	ctx := context.Background()
	se := createSession(w.store)
	defer se.Close()
	_, err := se.Execute(ctx, "BEGIN")
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	doGC, safePoint, err := w.checkPrepare(ctx)
	if doGC {
		err = se.CommitTxn(ctx)
		if err != nil {
			return false, 0, errors.Trace(err)
		}
	} else {
		se.RollbackTxn(ctx)
	}
	return doGC, safePoint, errors.Trace(err)
}

func (w *GCWorker) checkPrepare(ctx context.Context) (bool, uint64, error) {
	enable, err := w.checkGCEnable()
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	if !enable {
		logutil.Logger(ctx).Warn("[gc worker] gc status is disabled.")
		return false, 0, nil
	}
	now, err := w.getOracleTime()
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	ok, err := w.checkGCInterval(now)
	if err != nil || !ok {
		return false, 0, errors.Trace(err)
	}
	newSafePoint, newSafePointValue, err := w.calculateNewSafePoint(ctx, now)
	if err != nil || newSafePoint == nil {
		return false, 0, errors.Trace(err)
	}
	err = w.saveTime(gcLastRunTimeKey, now)
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	err = w.saveTime(gcSafePointKey, *newSafePoint)
	if err != nil {
		return false, 0, errors.Trace(err)
	}
	return true, newSafePointValue, nil
}

// calculateNewSafePoint uses the current global transaction min start timestamp to calculate the new safe point.
func (w *GCWorker) calSafePointByMinStartTS(ctx context.Context, safePoint time.Time) time.Time {
	kvs, err := w.store.GetSafePointKV().GetWithPrefix(infosync.ServerMinStartTSPath)
	if err != nil {
		logutil.Logger(ctx).Warn("get all minStartTS failed", zap.Error(err))
		return safePoint
	}

	var globalMinStartTS uint64 = math.MaxUint64
	for _, v := range kvs {
		minStartTS, err := strconv.ParseUint(string(v.Value), 10, 64)
		if err != nil {
			logutil.Logger(ctx).Warn("parse minStartTS failed", zap.Error(err))
			continue
		}
		if minStartTS < globalMinStartTS {
			globalMinStartTS = minStartTS
		}
	}

	safePointTS := variable.GoTimeToTS(safePoint)
	if globalMinStartTS < safePointTS {
		safePoint = time.Unix(0, oracle.ExtractPhysical(globalMinStartTS)*1e6)
		logutil.Logger(ctx).Info("[gc worker] gc safepoint blocked by a running session",
			zap.String("uuid", w.uuid),
			zap.Uint64("globalMinStartTS", globalMinStartTS),
			zap.Time("safePoint", safePoint))
	}
	return safePoint
}

func (w *GCWorker) getOracleTime() (time.Time, error) {
	currentVer, err := w.store.CurrentVersion()
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	physical := oracle.ExtractPhysical(currentVer.Ver)
	sec, nsec := physical/1e3, (physical%1e3)*1e6
	return time.Unix(sec, nsec), nil
}

func (w *GCWorker) checkGCEnable() (bool, error) {
	return w.loadBooleanWithDefault(gcEnableKey, gcDefaultEnableValue)
}

func (w *GCWorker) checkUseAutoConcurrency() (bool, error) {
	return w.loadBooleanWithDefault(gcAutoConcurrencyKey, gcDefaultAutoConcurrency)
}

func (w *GCWorker) loadBooleanWithDefault(key string, defaultValue bool) (bool, error) {
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		return false, errors.Trace(err)
	}
	if str == "" {
		// Save default value for gc enable key. The default value is always true.
		defaultValueStr := booleanFalse
		if defaultValue {
			defaultValueStr = booleanTrue
		}
		err = w.saveValueToSysTable(key, defaultValueStr)
		if err != nil {
			return defaultValue, errors.Trace(err)
		}
		return defaultValue, nil
	}
	return strings.EqualFold(str, booleanTrue), nil
}

func (w *GCWorker) getGCConcurrency(ctx context.Context) (int, error) {
	useAutoConcurrency, err := w.checkUseAutoConcurrency()
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] failed to load config gc_auto_concurrency. use default value.",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		useAutoConcurrency = gcDefaultAutoConcurrency
	}
	if !useAutoConcurrency {
		return w.loadGCConcurrencyWithDefault()
	}

	stores, err := w.getStoresForGC(ctx)
	concurrency := len(stores)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] failed to get up stores to calculate concurrency. use config.",
			zap.String("uuid", w.uuid),
			zap.Error(err))

		concurrency, err = w.loadGCConcurrencyWithDefault()
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] failed to load gc concurrency from config. use default value.",
				zap.String("uuid", w.uuid),
				zap.Error(err))
			concurrency = gcDefaultConcurrency
		}
	}

	if concurrency == 0 {
		logutil.Logger(ctx).Error("[gc worker] no store is up",
			zap.String("uuid", w.uuid))
		return 0, errors.New("[gc worker] no store is up")
	}

	return concurrency, nil
}

func (w *GCWorker) checkGCInterval(now time.Time) (bool, error) {
	runInterval, err := w.loadDurationWithDefault(gcRunIntervalKey, gcDefaultRunInterval)
	if err != nil {
		return false, errors.Trace(err)
	}
	metrics.GCConfigGauge.WithLabelValues(gcRunIntervalKey).Set(runInterval.Seconds())
	lastRun, err := w.loadTime(gcLastRunTimeKey)
	if err != nil {
		return false, errors.Trace(err)
	}

	if lastRun != nil && lastRun.Add(*runInterval).After(now) {
		logutil.BgLogger().Debug("[gc worker] skipping garbage collection because gc interval hasn't elapsed since last run",
			zap.String("leaderTick on", w.uuid),
			zap.Duration("interval", *runInterval),
			zap.Time("last run", *lastRun))
		return false, nil
	}

	return true, nil
}

// validateGCLifeTime checks whether life time is small than min gc life time.
func (w *GCWorker) validateGCLifeTime(lifeTime time.Duration) (time.Duration, error) {
	if lifeTime >= gcMinLifeTime {
		return lifeTime, nil
	}

	logutil.BgLogger().Info("[gc worker] invalid gc life time",
		zap.Duration("get gc life time", lifeTime),
		zap.Duration("min gc life time", gcMinLifeTime))

	err := w.saveDuration(gcLifeTimeKey, gcMinLifeTime)
	return gcMinLifeTime, err
}

func (w *GCWorker) calculateNewSafePoint(ctx context.Context, now time.Time) (*time.Time, uint64, error) {
	lifeTime, err := w.loadDurationWithDefault(gcLifeTimeKey, gcDefaultLifeTime)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	*lifeTime, err = w.validateGCLifeTime(*lifeTime)
	if err != nil {
		return nil, 0, err
	}
	metrics.GCConfigGauge.WithLabelValues(gcLifeTimeKey).Set(lifeTime.Seconds())
	lastSafePoint, err := w.loadTime(gcSafePointKey)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	safePoint := w.calSafePointByMinStartTS(ctx, now.Add(-*lifeTime))

	safePointValue := oracle.ComposeTS(oracle.GetPhysical(safePoint), 0)
	safePointValue, err = w.setGCWorkerServiceSafePoint(ctx, safePointValue)
	safePoint = oracle.GetTimeFromTS(safePointValue)

	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	// We should never decrease safePoint.
	if lastSafePoint != nil && safePoint.Before(*lastSafePoint) {
		logutil.BgLogger().Info("[gc worker] last safe point is later than current one."+
			"No need to gc."+
			"This might be caused by manually enlarging gc lifetime",
			zap.String("leaderTick on", w.uuid),
			zap.Time("last safe point", *lastSafePoint),
			zap.Time("current safe point", safePoint))
		return nil, 0, nil
	}
	return &safePoint, safePointValue, nil
}

// setGCWorkerServiceSafePoint sets the given safePoint as TiDB's service safePoint to PD, and returns the current minimal
// service safePoint among all services.
func (w *GCWorker) setGCWorkerServiceSafePoint(ctx context.Context, safePoint uint64) (uint64, error) {
	// Sets TTL to MAX to make it permanently valid.
	minSafePoint, err := w.pdClient.UpdateServiceGCSafePoint(ctx, gcWorkerServiceSafePointID, math.MaxInt64, safePoint)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] failed to update service safe point",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("update_service_safe_point").Inc()
		return 0, errors.Trace(err)
	}
	if minSafePoint < safePoint {
		logutil.Logger(ctx).Info("[gc worker] there's another service in the cluster requires an earlier safe point. "+
			"gc will continue with the earlier one",
			zap.String("uuid", w.uuid),
			zap.Uint64("ourSafePoint", safePoint),
			zap.Uint64("minSafePoint", minSafePoint),
		)
		safePoint = minSafePoint
	}
	return safePoint, nil
}

func (w *GCWorker) runGCJob(ctx context.Context, safePoint uint64, concurrency int) error {
	failpoint.Inject("mockRunGCJobFail", func() {
		failpoint.Return(errors.New("mock failure of runGCJob"))
	})
	metrics.GCWorkerCounter.WithLabelValues("run_job").Inc()
	usePhysical, err := w.checkUsePhysicalScanLock()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = w.resolveLocks(ctx, safePoint, concurrency, usePhysical)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] resolve locks returns an error",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("resolve_lock").Inc()
		return errors.Trace(err)
	}

	// Save safe point to pd.
	err = w.saveSafePoint(w.store.GetSafePointKV(), safePoint)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] failed to save safe point to PD",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("save_safe_point").Inc()
		return errors.Trace(err)
	}
	// Sleep to wait for all other tidb instances update their safepoint cache.
	time.Sleep(gcSafePointCacheInterval)

	err = w.deleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] delete range returns an error",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("delete_range").Inc()
		return errors.Trace(err)
	}
	err = w.redoDeleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] redo-delete range returns an error",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("redo_delete_range").Inc()
		return errors.Trace(err)
	}

	useDistributedGC, err := w.checkUseDistributedGC()
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] failed to load gc mode, fall back to central mode.",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("check_gc_mode").Inc()
		useDistributedGC = false
	}

	if useDistributedGC {
		err = w.uploadSafePointToPD(ctx, safePoint)
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] failed to upload safe point to PD",
				zap.String("uuid", w.uuid),
				zap.Error(err))
			metrics.GCJobFailureCounter.WithLabelValues("upload_safe_point").Inc()
			return errors.Trace(err)
		}
	} else {
		err = w.doGC(ctx, safePoint, concurrency)
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] do GC returns an error",
				zap.String("uuid", w.uuid),
				zap.Error(err))
			metrics.GCJobFailureCounter.WithLabelValues("gc").Inc()
			return errors.Trace(err)
		}
	}

	return nil
}

// deleteRanges processes all delete range records whose ts < safePoint in table `gc_delete_range`
// `concurrency` specifies the concurrency to send NotifyDeleteRange.
func (w *GCWorker) deleteRanges(ctx context.Context, safePoint uint64, concurrency int) error {
	metrics.GCWorkerCounter.WithLabelValues("delete_range").Inc()

	se := createSession(w.store)
	ranges, err := util.LoadDeleteRanges(se, safePoint)
	se.Close()
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("[gc worker] start delete ranges",
		zap.String("uuid", w.uuid),
		zap.Int("ranges", len(ranges)))
	startTime := time.Now()
	for _, r := range ranges {
		startKey, endKey := r.Range()

		err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey, concurrency)
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] delete range failed on range",
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			continue
		}

		se := createSession(w.store)
		err = util.CompleteDeleteRange(se, r)
		se.Close()
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] failed to mark delete range task done",
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("save").Inc()
		}
	}
	logutil.Logger(ctx).Info("[gc worker] finish delete ranges",
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)),
		zap.Duration("cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

// redoDeleteRanges checks all deleted ranges whose ts is at least `lifetime + 24h` ago. See TiKV RFC #2.
// `concurrency` specifies the concurrency to send NotifyDeleteRange.
func (w *GCWorker) redoDeleteRanges(ctx context.Context, safePoint uint64, concurrency int) error {
	metrics.GCWorkerCounter.WithLabelValues("redo_delete_range").Inc()

	// We check delete range records that are deleted about 24 hours ago.
	redoDeleteRangesTs := safePoint - oracle.ComposeTS(int64(gcRedoDeleteRangeDelay.Seconds())*1000, 0)

	se := createSession(w.store)
	ranges, err := util.LoadDoneDeleteRanges(se, redoDeleteRangesTs)
	se.Close()
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("[gc worker] start redo-delete ranges",
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)))
	startTime := time.Now()
	for _, r := range ranges {
		startKey, endKey := r.Range()

		err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey, concurrency)
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] redo-delete range failed on range",
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			continue
		}

		se := createSession(w.store)
		err := util.DeleteDoneRecord(se, r)
		se.Close()
		if err != nil {
			logutil.Logger(ctx).Error("[gc worker] failed to remove delete_range_done record",
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("save_redo").Inc()
		}
	}
	logutil.Logger(ctx).Info("[gc worker] finish redo-delete ranges",
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)),
		zap.Duration("cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("redo_delete_ranges").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) doUnsafeDestroyRangeRequest(ctx context.Context, startKey []byte, endKey []byte, concurrency int) error {
	// Get all stores every time deleting a region. So the store list is less probably to be stale.
	stores, err := w.getStoresForGC(ctx)
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] delete ranges: got an error while trying to get store list from PD",
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("get_stores").Inc()
		return errors.Trace(err)
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	})

	var wg sync.WaitGroup
	errChan := make(chan error, len(stores))

	for _, store := range stores {
		address := store.Address
		storeID := store.Id
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err1 := w.store.GetTiKVClient().SendRequest(ctx, address, req, tikv.UnsafeDestroyRangeTimeout)
			if err1 == nil {
				if resp == nil || resp.Resp == nil {
					err1 = errors.Errorf("unsafe destroy range returns nil response from store %v", storeID)
				} else {
					errStr := (resp.Resp.(*kvrpcpb.UnsafeDestroyRangeResponse)).Error
					if len(errStr) > 0 {
						err1 = errors.Errorf("unsafe destroy range failed on store %v: %s", storeID, errStr)
					}
				}
			}

			if err1 != nil {
				metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("send").Inc()
			}
			errChan <- err1
		}()
	}

	var errs []string
	for range stores {
		err1 := <-errChan
		if err1 != nil {
			errs = append(errs, err1.Error())
		}
	}

	wg.Wait()

	if len(errs) > 0 {
		return errors.Errorf("[gc worker] destroy range finished with errors: %v", errs)
	}

	// Notify all affected regions in the range that UnsafeDestroyRange occurs.
	notifyTask := tikv.NewNotifyDeleteRangeTask(w.store, startKey, endKey, concurrency)
	err = notifyTask.Execute(ctx)
	if err != nil {
		return errors.Annotate(err, "[gc worker] failed notifying regions affected by UnsafeDestroyRange")
	}

	return nil
}

const (
	engineLabelKey     = "engine"
	engineLabelTiFlash = "tiflash"
	engineLabelTiKV    = "tikv"
)

// needsGCOperationForStore checks if the store-level requests related to GC needs to be sent to the store. The store-level
// requests includes UnsafeDestroyRange, PhysicalScanLock, etc.
func needsGCOperationForStore(store *metapb.Store) (bool, error) {
	// TombStone means the store has been removed from the cluster and there isn't any peer on the store, so needn't do GC for it.
	// Offline means the store is being removed from the cluster and it becomes tombstone after all peers are removed from it,
	// so we need to do GC for it.
	if store.State == metapb.StoreState_Tombstone {
		return false, nil
	}

	engineLabel := ""
	for _, label := range store.GetLabels() {
		if label.GetKey() == engineLabelKey {
			engineLabel = label.GetValue()
			break
		}
	}

	switch engineLabel {
	case engineLabelTiFlash:
		// For a TiFlash node, it uses other approach to delete dropped tables, so it's safe to skip sending
		// UnsafeDestroyRange requests; it has only learner peers and their data must exist in TiKV, so it's safe to
		// skip physical resolve locks for it.
		return false, nil

	case "":
		// If no engine label is set, it should be a TiKV node.
		fallthrough
	case engineLabelTiKV:
		return true, nil

	default:
		return true, errors.Errorf("unsupported store engine \"%v\" with storeID %v, addr %v",
			engineLabel,
			store.GetId(),
			store.GetAddress())
	}
}

// getStoresForGC gets the list of stores that needs to be processed during GC.
func (w *GCWorker) getStoresForGC(ctx context.Context) ([]*metapb.Store, error) {
	stores, err := w.pdClient.GetAllStores(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	upStores := make([]*metapb.Store, 0, len(stores))
	for _, store := range stores {
		needsGCOp, err := needsGCOperationForStore(store)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if needsGCOp {
			upStores = append(upStores, store)
		}
	}
	return upStores, nil
}

func (w *GCWorker) getStoresMapForGC(ctx context.Context) (map[uint64]*metapb.Store, error) {
	stores, err := w.getStoresForGC(ctx)
	if err != nil {
		return nil, err
	}

	storesMap := make(map[uint64]*metapb.Store, len(stores))
	for _, store := range stores {
		storesMap[store.Id] = store
	}

	return storesMap, nil
}

func (w *GCWorker) loadGCConcurrencyWithDefault() (int, error) {
	str, err := w.loadValueFromSysTable(gcConcurrencyKey)
	if err != nil {
		return gcDefaultConcurrency, errors.Trace(err)
	}
	if str == "" {
		err = w.saveValueToSysTable(gcConcurrencyKey, strconv.Itoa(gcDefaultConcurrency))
		if err != nil {
			return gcDefaultConcurrency, errors.Trace(err)
		}
		return gcDefaultConcurrency, nil
	}

	jobConcurrency, err := strconv.Atoi(str)
	if err != nil {
		return gcDefaultConcurrency, err
	}

	if jobConcurrency < gcMinConcurrency {
		jobConcurrency = gcMinConcurrency
	}

	if jobConcurrency > gcMaxConcurrency {
		jobConcurrency = gcMaxConcurrency
	}

	return jobConcurrency, nil
}

func (w *GCWorker) checkUseDistributedGC() (bool, error) {
	str, err := w.loadValueFromSysTable(gcModeKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	if str == "" {
		err = w.saveValueToSysTable(gcModeKey, gcModeDefault)
		if err != nil {
			return false, errors.Trace(err)
		}
		str = gcModeDefault
	}
	if strings.EqualFold(str, gcModeDistributed) {
		return true, nil
	}
	if strings.EqualFold(str, gcModeCentral) {
		return false, nil
	}
	logutil.BgLogger().Warn("[gc worker] distributed mode will be used",
		zap.String("invalid gc mode", str))
	return true, nil
}

func (w *GCWorker) checkUsePhysicalScanLock() (bool, error) {
	str, err := w.loadValueFromSysTable(gcScanLockModeKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	if str == "" {
		err = w.saveValueToSysTable(gcScanLockModeKey, gcScanLockModeDefault)
		if err != nil {
			return false, errors.Trace(err)
		}
		str = gcScanLockModeDefault
	}
	if strings.EqualFold(str, gcScanLockModePhysical) {
		return true, nil
	}
	if strings.EqualFold(str, gcScanLockModeLegacy) {
		return false, nil
	}
	logutil.BgLogger().Warn("[gc worker] legacy scan lock mode will be used",
		zap.String("invalid scan lock mode", str))
	return false, nil
}

func (w *GCWorker) resolveLocks(ctx context.Context, safePoint uint64, concurrency int, usePhysical bool) (bool, error) {
	if !usePhysical {
		return false, w.legacyResolveLocks(ctx, safePoint, concurrency)
	}

	// First try resolve locks with physical scan
	err := w.resolveLocksPhysical(ctx, safePoint)
	if err == nil {
		return true, nil
	}

	logutil.Logger(ctx).Error("[gc worker] resolve locks with physical scan failed, trying fallback to legacy resolve lock",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Error(err))

	return false, w.legacyResolveLocks(ctx, safePoint, concurrency)
}

func (w *GCWorker) legacyResolveLocks(ctx context.Context, safePoint uint64, concurrency int) error {
	metrics.GCWorkerCounter.WithLabelValues("resolve_locks").Inc()
	logutil.Logger(ctx).Info("[gc worker] start resolve locks",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("concurrency", concurrency))
	startTime := time.Now()

	handler := func(ctx context.Context, r kv.KeyRange) (tikv.RangeTaskStat, error) {
		return w.resolveLocksForRange(ctx, safePoint, r.StartKey, r.EndKey)
	}

	runner := tikv.NewRangeTaskRunner("resolve-locks-runner", w.store, concurrency, handler)
	// Run resolve lock on the whole TiKV cluster. Empty keys means the range is unbounded.
	err := runner.RunOnRange(ctx, []byte(""), []byte(""))
	if err != nil {
		logutil.Logger(ctx).Error("[gc worker] resolve locks failed",
			zap.String("uuid", w.uuid),
			zap.Uint64("safePoint", safePoint),
			zap.Error(err))
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("[gc worker] finish resolve locks",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("regions", runner.CompletedRegions()))
	metrics.GCHistogram.WithLabelValues("resolve_locks").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) resolveLocksForRange(ctx context.Context, safePoint uint64, startKey []byte, endKey []byte) (tikv.RangeTaskStat, error) {
	// for scan lock request, we must return all locks even if they are generated
	// by the same transaction. because gc worker need to make sure all locks have been
	// cleaned.
	req := tikvrpc.NewRequest(tikvrpc.CmdScanLock, &kvrpcpb.ScanLockRequest{
		MaxVersion: safePoint,
		Limit:      gcScanLockLimit,
	})

	var stat tikv.RangeTaskStat
	key := startKey
	bo := tikv.NewBackofferWithVars(ctx, tikv.GcResolveLockMaxBackoff, nil)
	failpoint.Inject("setGcResolveMaxBackoff", func(v failpoint.Value) {
		sleep := v.(int)
		// cooperate with github.com/pingcap/tidb/store/tikv/invalidCacheAndRetry
		ctx = context.WithValue(ctx, "injectedBackoff", struct{}{})
		bo = tikv.NewBackofferWithVars(ctx, sleep, nil)
	})
retryScanAndResolve:
	for {
		select {
		case <-ctx.Done():
			return stat, errors.New("[gc worker] gc job canceled")
		default:
		}

		req.ScanLock().StartKey = key
		loc, err := w.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return stat, errors.Trace(err)
		}
		resp, err := w.store.SendReq(bo, req, loc.Region, tikv.ReadTimeoutMedium)
		if err != nil {
			return stat, errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return stat, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return stat, errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return stat, errors.Trace(tikv.ErrBodyMissing)
		}
		locksResp := resp.Resp.(*kvrpcpb.ScanLockResponse)
		if locksResp.GetError() != nil {
			return stat, errors.Errorf("unexpected scanlock error: %s", locksResp)
		}
		locksInfo := locksResp.GetLocks()
		locks := make([]*tikv.Lock, len(locksInfo))
		for i := range locksInfo {
			locks[i] = tikv.NewLock(locksInfo[i])
		}
		if w.testingKnobs.scanLocks != nil {
			locks = append(locks, w.testingKnobs.scanLocks(key)...)
		}
		for {
			ok, err1 := w.store.GetLockResolver().BatchResolveLocks(bo, locks, loc.Region)
			if w.testingKnobs.resolveLocks != nil {
				ok, err1 = w.testingKnobs.resolveLocks(loc.Region)
			}
			if err1 != nil {
				return stat, errors.Trace(err1)
			}
			if !ok {
				err = bo.Backoff(tikv.BoTxnLock, errors.Errorf("remain locks: %d", len(locks)))
				if err != nil {
					return stat, errors.Trace(err)
				}
				stillInSame, refreshedLoc, err := w.tryRelocateLocksRegion(bo, locks)
				if err != nil {
					return stat, errors.Trace(err)
				}
				if stillInSame {
					loc = refreshedLoc
					continue
				}
				continue retryScanAndResolve
			}
			break
		}
		if len(locks) < gcScanLockLimit {
			stat.CompletedRegions++
			key = loc.EndKey
		} else {
			logutil.Logger(ctx).Info("[gc worker] region has more than limit locks",
				zap.String("uuid", w.uuid),
				zap.Uint64("region", loc.Region.GetID()),
				zap.Int("scan lock limit", gcScanLockLimit))
			metrics.GCRegionTooManyLocksCounter.Inc()
			key = locks[len(locks)-1].Key
		}

		if len(key) == 0 || (len(endKey) != 0 && bytes.Compare(key, endKey) >= 0) {
			break
		}
		bo = tikv.NewBackofferWithVars(ctx, tikv.GcResolveLockMaxBackoff, nil)
		failpoint.Inject("setGcResolveMaxBackoff", func(v failpoint.Value) {
			sleep := v.(int)
			bo = tikv.NewBackofferWithVars(ctx, sleep, nil)
		})
	}
	return stat, nil
}

func (w *GCWorker) tryRelocateLocksRegion(bo *tikv.Backoffer, locks []*tikv.Lock) (stillInSameRegion bool, refreshedLoc *tikv.KeyLocation, err error) {
	if len(locks) == 0 {
		return
	}
	refreshedLoc, err = w.store.GetRegionCache().LocateKey(bo, locks[0].Key)
	if err != nil {
		return
	}
	stillInSameRegion = refreshedLoc.Contains(locks[len(locks)-1].Key)
	return
}

// resolveLocksPhysical uses TiKV's `PhysicalScanLock` to scan stale locks in the cluster and resolve them. It tries to
// ensure no lock whose ts <= safePoint is left.
func (w *GCWorker) resolveLocksPhysical(ctx context.Context, safePoint uint64) error {
	metrics.GCWorkerCounter.WithLabelValues("resolve_locks_physical").Inc()
	logutil.Logger(ctx).Info("[gc worker] start resolve locks with physical scan locks",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint))
	startTime := time.Now()

	registeredStores := make(map[uint64]*metapb.Store)
	defer w.removeLockObservers(ctx, safePoint, registeredStores)

	dirtyStores, err := w.getStoresMapForGC(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	for retry := 0; retry < 3; retry++ {
		err = w.registerLockObservers(ctx, safePoint, dirtyStores)
		if err != nil {
			return errors.Trace(err)
		}
		for id, store := range dirtyStores {
			registeredStores[id] = store
		}

		resolvedStores, err := w.physicalScanAndResolveLocks(ctx, safePoint, dirtyStores)
		if err != nil {
			return errors.Trace(err)
		}

		failpoint.Inject("beforeCheckLockObservers", func() {})

		stores, err := w.getStoresMapForGC(ctx)
		if err != nil {
			return errors.Trace(err)
		}

		checkedStores, err := w.checkLockObservers(ctx, safePoint, stores)
		if err != nil {
			return errors.Trace(err)
		}

		for store := range stores {
			if _, ok := checkedStores[store]; ok {
				// The store is resolved and checked.
				if _, ok := resolvedStores[store]; ok {
					delete(stores, store)
				}
				// The store is checked and has been resolved before.
				if _, ok := dirtyStores[store]; !ok {
					delete(stores, store)
				}
				// If the store is checked and not resolved, we can retry to resolve it again, so leave it in dirtyStores.
			} else if _, ok := registeredStores[store]; ok {
				// The store has been registered and it's dirty due to too many collected locks. Fall back to legacy mode.
				// We can't remove the lock observer from the store and retry the whole procedure because if the store
				// receives duplicated remove and register requests during resolving locks, the store will be cleaned
				// when checking but the lock observer drops some locks. It may results in missing locks.
				return errors.Errorf("store %v is dirty", store)
			}
		}
		dirtyStores = stores

		// If there are still dirty stores, continue the loop to clean them again.
		// Only dirty stores will be scanned in the next loop.
		if len(dirtyStores) == 0 {
			break
		}
	}

	if len(dirtyStores) != 0 {
		return errors.Errorf("still has %d dirty stores after physical resolve locks", len(dirtyStores))
	}

	logutil.Logger(ctx).Info("[gc worker] finish resolve locks with physical scan locks",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Duration("takes", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("resolve_locks").Observe(time.Since(startTime).Seconds())
	return nil
}

func (w *GCWorker) registerLockObservers(ctx context.Context, safePoint uint64, stores map[uint64]*metapb.Store) error {
	logutil.Logger(ctx).Info("[gc worker] registering lock observers to tikv",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint))

	req := tikvrpc.NewRequest(tikvrpc.CmdRegisterLockObserver, &kvrpcpb.RegisterLockObserverRequest{
		MaxTs: safePoint,
	})

	for _, store := range stores {
		address := store.Address

		resp, err := w.store.GetTiKVClient().SendRequest(ctx, address, req, tikv.AccessLockObserverTimeout)
		if err != nil {
			return errors.Trace(err)
		}
		if resp.Resp == nil {
			return errors.Trace(tikv.ErrBodyMissing)
		}
		errStr := resp.Resp.(*kvrpcpb.RegisterLockObserverResponse).Error
		if len(errStr) > 0 {
			return errors.Errorf("register lock observer on store %v returns error: %v", store.Id, errStr)
		}
	}

	return nil
}

// checkLockObservers checks the state of each store's lock observer. If any lock collected by the observers, resolve
// them. Returns ids of clean stores.
func (w *GCWorker) checkLockObservers(ctx context.Context, safePoint uint64, stores map[uint64]*metapb.Store) (map[uint64]interface{}, error) {
	logutil.Logger(ctx).Info("[gc worker] checking lock observers",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint))

	req := tikvrpc.NewRequest(tikvrpc.CmdCheckLockObserver, &kvrpcpb.CheckLockObserverRequest{
		MaxTs: safePoint,
	})
	cleanStores := make(map[uint64]interface{}, len(stores))

	logError := func(store *metapb.Store, err error) {
		logutil.Logger(ctx).Error("[gc worker] failed to check lock observer for store",
			zap.String("uuid", w.uuid),
			zap.Any("store", store),
			zap.Error(err))
	}

	// When error occurs, this function doesn't fail immediately, but continues without adding the failed store to
	// cleanStores set.
	for _, store := range stores {
		address := store.Address

		resp, err := w.store.GetTiKVClient().SendRequest(ctx, address, req, tikv.AccessLockObserverTimeout)
		if err != nil {
			logError(store, err)
			continue
		}
		if resp.Resp == nil {
			logError(store, tikv.ErrBodyMissing)
			continue
		}
		respInner := resp.Resp.(*kvrpcpb.CheckLockObserverResponse)
		if len(respInner.Error) > 0 {
			err = errors.Errorf("check lock observer on store %v returns error: %v", store.Id, respInner.Error)
			logError(store, err)
			continue
		}

		// No need to resolve observed locks on uncleaned stores.
		if !respInner.IsClean {
			logutil.Logger(ctx).Warn("[gc worker] check lock observer: store is not clean",
				zap.String("uuid", w.uuid),
				zap.Any("store", store))
			continue
		}

		if len(respInner.Locks) > 0 {
			// Resolve the observed locks.
			locks := make([]*tikv.Lock, len(respInner.Locks))
			for i, lockInfo := range respInner.Locks {
				locks[i] = tikv.NewLock(lockInfo)
			}
			sort.Slice(locks, func(i, j int) bool {
				return bytes.Compare(locks[i].Key, locks[j].Key) < 0
			})
			err = w.resolveLocksAcrossRegions(ctx, locks)

			if err != nil {
				err = errors.Errorf("check lock observer on store %v returns error: %v", store.Id, respInner.Error)
				logError(store, err)
				continue
			}
		}
		cleanStores[store.Id] = nil
	}

	return cleanStores, nil
}

func (w *GCWorker) removeLockObservers(ctx context.Context, safePoint uint64, stores map[uint64]*metapb.Store) {
	logutil.Logger(ctx).Info("[gc worker] removing lock observers",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint))

	req := tikvrpc.NewRequest(tikvrpc.CmdRemoveLockObserver, &kvrpcpb.RemoveLockObserverRequest{
		MaxTs: safePoint,
	})

	logError := func(store *metapb.Store, err error) {
		logutil.Logger(ctx).Warn("[gc worker] failed to remove lock observer from store",
			zap.String("uuid", w.uuid),
			zap.Any("store", store),
			zap.Error(err))
	}

	for _, store := range stores {
		address := store.Address

		resp, err := w.store.GetTiKVClient().SendRequest(ctx, address, req, tikv.AccessLockObserverTimeout)
		if err != nil {
			logError(store, err)
			continue
		}
		if resp.Resp == nil {
			logError(store, tikv.ErrBodyMissing)
			continue
		}
		errStr := resp.Resp.(*kvrpcpb.RemoveLockObserverResponse).Error
		if len(errStr) > 0 {
			err = errors.Errorf("remove lock observer on store %v returns error: %v", store.Id, errStr)
			logError(store, err)
		}
	}
}

// physicalScanAndResolveLocks performs physical scan lock and resolves these locks. Returns successful stores
func (w *GCWorker) physicalScanAndResolveLocks(ctx context.Context, safePoint uint64, stores map[uint64]*metapb.Store) (map[uint64]interface{}, error) {
	ctx, cancel := context.WithCancel(ctx)
	// Cancel all spawned goroutines for lock scanning and resolving.
	defer cancel()

	scanner := newMergeLockScanner(safePoint, w.store.GetTiKVClient(), stores)
	err := scanner.Start(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	taskCh := make(chan []*tikv.Lock, len(stores))
	errCh := make(chan error, len(stores))

	wg := &sync.WaitGroup{}
	for range stores {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case locks, ok := <-taskCh:
					if !ok {
						// All locks have been resolved.
						return
					}
					err := w.resolveLocksAcrossRegions(ctx, locks)
					if err != nil {
						logutil.Logger(ctx).Error("resolve locks failed", zap.Error(err))
						errCh <- err
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	for {
		locks := scanner.NextBatch(128)
		if len(locks) == 0 {
			break
		}

		select {
		case taskCh <- locks:
		case err := <-errCh:
			return nil, errors.Trace(err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	close(taskCh)
	// Wait for all locks resolved.
	wg.Wait()

	select {
	case err := <-errCh:
		return nil, errors.Trace(err)
	default:
	}

	return scanner.GetSucceededStores(), nil
}

func (w *GCWorker) resolveLocksAcrossRegions(ctx context.Context, locks []*tikv.Lock) error {
	failpoint.Inject("resolveLocksAcrossRegionsErr", func(v failpoint.Value) {
		ms := v.(int)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		failpoint.Return(errors.New("injectedError"))
	})

	bo := tikv.NewBackofferWithVars(ctx, tikv.GcResolveLockMaxBackoff, nil)

	for {
		if len(locks) == 0 {
			break
		}

		key := locks[0].Key
		loc, err := w.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return errors.Trace(err)
		}

		locksInRegion := make([]*tikv.Lock, 0)

		for _, lock := range locks {
			if loc.Contains(lock.Key) {
				locksInRegion = append(locksInRegion, lock)
			} else {
				break
			}
		}

		ok, err := w.store.GetLockResolver().BatchResolveLocks(bo, locksInRegion, loc.Region)
		if err != nil {
			return errors.Trace(err)
		}
		if !ok {
			err = bo.Backoff(tikv.BoTxnLock, errors.Errorf("remain locks: %d", len(locks)))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}

		// Recreate backoffer for next region
		bo = tikv.NewBackofferWithVars(ctx, tikv.GcResolveLockMaxBackoff, nil)
		locks = locks[len(locksInRegion):]
	}

	return nil
}

func (w *GCWorker) uploadSafePointToPD(ctx context.Context, safePoint uint64) error {
	var newSafePoint uint64
	var err error

	bo := tikv.NewBackofferWithVars(ctx, tikv.GcOneRegionMaxBackoff, nil)
	for {
		newSafePoint, err = w.pdClient.UpdateGCSafePoint(ctx, safePoint)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return errors.Trace(err)
			}
			err = bo.Backoff(tikv.BoPDRPC, errors.Errorf("failed to upload safe point to PD, err: %v", err))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		break
	}

	if newSafePoint != safePoint {
		logutil.Logger(ctx).Warn("[gc worker] PD rejected safe point",
			zap.String("uuid", w.uuid),
			zap.Uint64("our safe point", safePoint),
			zap.Uint64("using another safe point", newSafePoint))
		return errors.Errorf("PD rejected our safe point %v but is using another safe point %v", safePoint, newSafePoint)
	}
	logutil.Logger(ctx).Info("[gc worker] sent safe point to PD",
		zap.String("uuid", w.uuid),
		zap.Uint64("safe point", safePoint))
	return nil
}

func (w *GCWorker) doGCForRange(ctx context.Context, startKey []byte, endKey []byte, safePoint uint64) (tikv.RangeTaskStat, error) {
	var stat tikv.RangeTaskStat
	defer func() {
		metrics.GCActionRegionResultCounter.WithLabelValues("success").Add(float64(stat.CompletedRegions))
		metrics.GCActionRegionResultCounter.WithLabelValues("fail").Add(float64(stat.FailedRegions))
	}()
	key := startKey
	for {
		bo := tikv.NewBackofferWithVars(ctx, tikv.GcOneRegionMaxBackoff, nil)
		loc, err := w.store.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return stat, errors.Trace(err)
		}

		var regionErr *errorpb.Error
		regionErr, err = w.doGCForRegion(bo, safePoint, loc.Region)

		// we check regionErr here first, because we know 'regionErr' and 'err' should not return together, to keep it to
		// make the process correct.
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss, errors.New(regionErr.String()))
			if err == nil {
				continue
			}
		}

		if err != nil {
			logutil.BgLogger().Warn("[gc worker]",
				zap.String("uuid", w.uuid),
				zap.String("gc for range", fmt.Sprintf("[%d, %d)", startKey, endKey)),
				zap.Uint64("safePoint", safePoint),
				zap.Error(err))
			stat.FailedRegions++
		} else {
			stat.CompletedRegions++
		}

		key = loc.EndKey
		if len(key) == 0 || bytes.Compare(key, endKey) >= 0 {
			break
		}
	}

	return stat, nil
}

// doGCForRegion used for gc for region.
// these two errors should not return together, for more, see the func 'doGC'
func (w *GCWorker) doGCForRegion(bo *tikv.Backoffer, safePoint uint64, region tikv.RegionVerID) (*errorpb.Error, error) {
	req := tikvrpc.NewRequest(tikvrpc.CmdGC, &kvrpcpb.GCRequest{
		SafePoint: safePoint,
	})

	resp, err := w.store.SendReq(bo, req, region, tikv.GCTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionErr, err := resp.GetRegionError()
	if err != nil {
		return nil, errors.Trace(err)
	}
	if regionErr != nil {
		return regionErr, nil
	}

	if resp.Resp == nil {
		return nil, errors.Trace(tikv.ErrBodyMissing)
	}
	gcResp := resp.Resp.(*kvrpcpb.GCResponse)
	if gcResp.GetError() != nil {
		return nil, errors.Errorf("unexpected gc error: %s", gcResp.GetError())
	}

	return nil, nil
}

func (w *GCWorker) doGC(ctx context.Context, safePoint uint64, concurrency int) error {
	metrics.GCWorkerCounter.WithLabelValues("do_gc").Inc()
	logutil.Logger(ctx).Info("[gc worker] start doing gc for all keys",
		zap.String("uuid", w.uuid),
		zap.Int("concurrency", concurrency),
		zap.Uint64("safePoint", safePoint))
	startTime := time.Now()

	runner := tikv.NewRangeTaskRunner(
		"gc-runner",
		w.store,
		concurrency,
		func(ctx context.Context, r kv.KeyRange) (tikv.RangeTaskStat, error) {
			return w.doGCForRange(ctx, r.StartKey, r.EndKey, safePoint)
		})

	err := runner.RunOnRange(ctx, []byte(""), []byte(""))
	if err != nil {
		logutil.Logger(ctx).Warn("[gc worker] failed to do gc for all keys",
			zap.String("uuid", w.uuid),
			zap.Int("concurrency", concurrency),
			zap.Error(err))
		return errors.Trace(err)
	}

	successRegions := runner.CompletedRegions()
	failedRegions := runner.FailedRegions()

	logutil.Logger(ctx).Info("[gc worker] finished doing gc for all keys",
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("successful regions", successRegions),
		zap.Int("failed regions", failedRegions),
		zap.Duration("total cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("do_gc").Observe(time.Since(startTime).Seconds())

	return nil
}

func (w *GCWorker) checkLeader() (bool, error) {
	metrics.GCWorkerCounter.WithLabelValues("check_leader").Inc()
	se := createSession(w.store)
	defer se.Close()

	ctx := context.Background()
	_, err := se.Execute(ctx, "BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	leader, err := w.loadValueFromSysTable(gcLeaderUUIDKey)
	if err != nil {
		se.RollbackTxn(ctx)
		return false, errors.Trace(err)
	}
	logutil.BgLogger().Debug("[gc worker] got leader", zap.String("uuid", leader))
	if leader == w.uuid {
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = se.CommitTxn(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}

	se.RollbackTxn(ctx)

	_, err = se.Execute(ctx, "BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	lease, err := w.loadTime(gcLeaderLeaseKey)
	if err != nil {
		se.RollbackTxn(ctx)
		return false, errors.Trace(err)
	}
	if lease == nil || lease.Before(time.Now()) {
		logutil.BgLogger().Debug("[gc worker] register as leader",
			zap.String("uuid", w.uuid))
		metrics.GCWorkerCounter.WithLabelValues("register_leader").Inc()

		err = w.saveValueToSysTable(gcLeaderUUIDKey, w.uuid)
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = w.saveValueToSysTable(gcLeaderDescKey, w.desc)
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = w.saveTime(gcLeaderLeaseKey, time.Now().Add(gcWorkerLease))
		if err != nil {
			se.RollbackTxn(ctx)
			return false, errors.Trace(err)
		}
		err = se.CommitTxn(ctx)
		if err != nil {
			return false, errors.Trace(err)
		}
		return true, nil
	}
	se.RollbackTxn(ctx)
	return false, nil
}

func (w *GCWorker) saveSafePoint(kv tikv.SafePointKV, t uint64) error {
	s := strconv.FormatUint(t, 10)
	err := kv.Put(tikv.GcSavedSafePoint, s)
	if err != nil {
		logutil.BgLogger().Error("save safepoint failed", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func (w *GCWorker) saveTime(key string, t time.Time) error {
	err := w.saveValueToSysTable(key, t.Format(tidbutil.GCTimeFormat))
	return errors.Trace(err)
}

func (w *GCWorker) loadTime(key string) (*time.Time, error) {
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if str == "" {
		return nil, nil
	}
	t, err := tidbutil.CompatibleParseGCTime(str)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &t, nil
}

func (w *GCWorker) saveDuration(key string, d time.Duration) error {
	err := w.saveValueToSysTable(key, d.String())
	return errors.Trace(err)
}

func (w *GCWorker) loadDuration(key string) (*time.Duration, error) {
	str, err := w.loadValueFromSysTable(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if str == "" {
		return nil, nil
	}
	d, err := time.ParseDuration(str)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &d, nil
}

func (w *GCWorker) loadDurationWithDefault(key string, def time.Duration) (*time.Duration, error) {
	d, err := w.loadDuration(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if d == nil {
		err = w.saveDuration(key, def)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return &def, nil
	}
	return d, nil
}

func (w *GCWorker) loadValueFromSysTable(key string) (string, error) {
	ctx := context.Background()
	se := createSession(w.store)
	defer se.Close()
	stmt := fmt.Sprintf(`SELECT HIGH_PRIORITY (variable_value) FROM mysql.tidb WHERE variable_name='%s' FOR UPDATE`, key)
	rs, err := se.Execute(ctx, stmt)
	if len(rs) > 0 {
		defer terror.Call(rs[0].Close)
	}
	if err != nil {
		return "", errors.Trace(err)
	}
	req := rs[0].NewChunk()
	err = rs[0].Next(ctx, req)
	if err != nil {
		return "", errors.Trace(err)
	}
	if req.NumRows() == 0 {
		logutil.BgLogger().Debug("[gc worker] load kv",
			zap.String("key", key))
		return "", nil
	}
	value := req.GetRow(0).GetString(0)
	logutil.BgLogger().Debug("[gc worker] load kv",
		zap.String("key", key),
		zap.String("value", value))
	return value, nil
}

func (w *GCWorker) saveValueToSysTable(key, value string) error {
	stmt := fmt.Sprintf(`INSERT HIGH_PRIORITY INTO mysql.tidb VALUES ('%[1]s', '%[2]s', '%[3]s')
			       ON DUPLICATE KEY
			       UPDATE variable_value = '%[2]s', comment = '%[3]s'`,
		key, value, gcVariableComments[key])
	se := createSession(w.store)
	defer se.Close()
	_, err := se.Execute(context.Background(), stmt)
	logutil.BgLogger().Debug("[gc worker] save kv",
		zap.String("key", key),
		zap.String("value", value),
		zap.Error(err))
	return errors.Trace(err)
}

// RunGCJob sends GC command to KV. It is exported for kv api, do not use it with GCWorker at the same time.
func RunGCJob(ctx context.Context, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		store:    s,
		uuid:     identifier,
		pdClient: pd,
	}

	if concurrency <= 0 {
		return errors.Errorf("[gc worker] gc concurrency should greater than 0, current concurrency: %v", concurrency)
	}

	safePoint, err := gcWorker.setGCWorkerServiceSafePoint(ctx, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = gcWorker.resolveLocks(ctx, safePoint, concurrency, false)
	if err != nil {
		return errors.Trace(err)
	}

	err = gcWorker.saveSafePoint(gcWorker.store.GetSafePointKV(), safePoint)
	if err != nil {
		return errors.Trace(err)
	}
	// Sleep to wait for all other tidb instances update their safepoint cache.
	time.Sleep(gcSafePointCacheInterval)
	err = gcWorker.doGC(ctx, safePoint, concurrency)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunDistributedGCJob notifies TiKVs to do GC. It is exported for kv api, do not use it with GCWorker at the same time.
// This function may not finish immediately because it may take some time to do resolveLocks.
// Param concurrency specifies the concurrency of resolveLocks phase.
func RunDistributedGCJob(ctx context.Context, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		store:    s,
		uuid:     identifier,
		pdClient: pd,
	}

	safePoint, err := gcWorker.setGCWorkerServiceSafePoint(ctx, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	_, err = gcWorker.resolveLocks(ctx, safePoint, concurrency, false)
	if err != nil {
		return errors.Trace(err)
	}

	// Save safe point to pd.
	err = gcWorker.saveSafePoint(gcWorker.store.GetSafePointKV(), safePoint)
	if err != nil {
		return errors.Trace(err)
	}
	// Sleep to wait for all other tidb instances update their safepoint cache.
	time.Sleep(gcSafePointCacheInterval)

	err = gcWorker.uploadSafePointToPD(ctx, safePoint)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RunResolveLocks resolves all locks before the safePoint and returns whether the physical scan mode is used.
// It is exported only for test, do not use it in the production environment.
func RunResolveLocks(ctx context.Context, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int, usePhysical bool) (bool, error) {
	gcWorker := &GCWorker{
		store:    s,
		uuid:     identifier,
		pdClient: pd,
	}
	return gcWorker.resolveLocks(ctx, safePoint, concurrency, usePhysical)
}

// MockGCWorker is for test.
type MockGCWorker struct {
	worker *GCWorker
}

// NewMockGCWorker creates a MockGCWorker instance ONLY for test.
func NewMockGCWorker(store tikv.Storage) (*MockGCWorker, error) {
	ver, err := store.CurrentVersion()
	if err != nil {
		return nil, errors.Trace(err)
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
	worker := &GCWorker{
		uuid:        strconv.FormatUint(ver.Ver, 16),
		desc:        fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		store:       store,
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
	}
	return &MockGCWorker{worker: worker}, nil
}

// DeleteRanges calls deleteRanges internally, just for test.
func (w *MockGCWorker) DeleteRanges(ctx context.Context, safePoint uint64) error {
	logutil.Logger(ctx).Error("deleteRanges is called")
	return w.worker.deleteRanges(ctx, safePoint, 1)
}

const scanLockResultBufferSize = 128

// mergeLockScanner is used to scan specified stores by using PhysicalScanLock. For multiple stores, the scanner will
// merge the scan results of each store, and remove the duplicating items from different stores.
type mergeLockScanner struct {
	safePoint     uint64
	client        tikv.Client
	stores        map[uint64]*metapb.Store
	receivers     mergeReceiver
	currentLock   *tikv.Lock
	scanLockLimit uint32
}

type receiver struct {
	Ch       <-chan scanLockResult
	StoreID  uint64
	NextLock *tikv.Lock
	Err      error
}

func (r *receiver) PeekNextLock() *tikv.Lock {
	if r.NextLock != nil {
		return r.NextLock
	}
	result, ok := <-r.Ch
	if !ok {
		return nil
	}
	r.Err = result.Err
	r.NextLock = result.Lock
	return r.NextLock
}

func (r *receiver) TakeNextLock() *tikv.Lock {
	lock := r.PeekNextLock()
	r.NextLock = nil
	return lock
}

// mergeReceiver is a list of receivers
type mergeReceiver []*receiver

func (r mergeReceiver) Len() int {
	return len(r)
}

func (r mergeReceiver) Less(i, j int) bool {
	lhs := r[i].PeekNextLock()
	rhs := r[j].PeekNextLock()
	// nil which means the receiver has finished should be the greatest one.
	if lhs == nil {
		// lhs >= rhs
		return false
	}
	if rhs == nil {
		// lhs != nil, so lhs < rhs
		return true
	}
	ord := bytes.Compare(lhs.Key, rhs.Key)
	return ord < 0 || (ord == 0 && lhs.TxnID < rhs.TxnID)
}

func (r mergeReceiver) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r *mergeReceiver) Push(x interface{}) {
	*r = append(*r, x.(*receiver))
}

func (r *mergeReceiver) Pop() interface{} {
	receivers := *r
	res := receivers[len(receivers)-1]
	*r = receivers[:len(receivers)-1]
	return res
}

type scanLockResult struct {
	Lock *tikv.Lock
	Err  error
}

func newMergeLockScanner(safePoint uint64, client tikv.Client, stores map[uint64]*metapb.Store) *mergeLockScanner {
	return &mergeLockScanner{
		safePoint:     safePoint,
		client:        client,
		stores:        stores,
		scanLockLimit: gcScanLockLimit,
	}
}

// Start initializes the scanner and enables retrieving items from the scanner.
func (s *mergeLockScanner) Start(ctx context.Context) error {
	receivers := make([]*receiver, 0, len(s.stores))

	for storeID, store := range s.stores {
		ch := make(chan scanLockResult, scanLockResultBufferSize)
		store1 := store
		go func() {
			defer close(ch)

			err := s.physicalScanLocksForStore(ctx, s.safePoint, store1, ch)
			if err != nil {
				logutil.Logger(ctx).Error("physical scan lock for store encountered error",
					zap.Uint64("safePoint", s.safePoint),
					zap.Any("store", store1),
					zap.Error(err))

				select {
				case ch <- scanLockResult{Err: err}:
				case <-ctx.Done():
				}
			}
		}()
		receivers = append(receivers, &receiver{Ch: ch, StoreID: storeID})
	}

	s.startWithReceivers(receivers)

	return nil
}

func (s *mergeLockScanner) startWithReceivers(receivers []*receiver) {
	s.receivers = receivers
	heap.Init(&s.receivers)
}

func (s *mergeLockScanner) Next() *tikv.Lock {
	for {
		nextReceiver := s.receivers[0]
		nextLock := nextReceiver.TakeNextLock()
		heap.Fix(&s.receivers, 0)

		if nextLock == nil {
			return nil
		}
		if s.currentLock == nil || !bytes.Equal(s.currentLock.Key, nextLock.Key) || s.currentLock.TxnID != nextLock.TxnID {
			s.currentLock = nextLock
			return nextLock
		}
	}
}

func (s *mergeLockScanner) NextBatch(batchSize int) []*tikv.Lock {
	result := make([]*tikv.Lock, 0, batchSize)
	for len(result) < batchSize {
		lock := s.Next()
		if lock == nil {
			break
		}
		result = append(result, lock)
	}
	return result
}

// GetSucceededStores gets a set of successfully scanned stores. Only call this after finishing scanning all locks.
func (s *mergeLockScanner) GetSucceededStores() map[uint64]interface{} {
	stores := make(map[uint64]interface{}, len(s.receivers))
	for _, receiver := range s.receivers {
		if receiver.Err == nil {
			stores[receiver.StoreID] = nil
		}
	}
	return stores
}

func (s *mergeLockScanner) physicalScanLocksForStore(ctx context.Context, safePoint uint64, store *metapb.Store, lockCh chan<- scanLockResult) error {
	address := store.Address
	req := tikvrpc.NewRequest(tikvrpc.CmdPhysicalScanLock, &kvrpcpb.PhysicalScanLockRequest{
		MaxTs: safePoint,
		Limit: s.scanLockLimit,
	})

	nextKey := make([]byte, 0)

	for {
		req.PhysicalScanLock().StartKey = nextKey

		response, err := s.client.SendRequest(ctx, address, req, tikv.ReadTimeoutMedium)
		if err != nil {
			return errors.Trace(err)
		}
		if response.Resp == nil {
			return errors.Trace(tikv.ErrBodyMissing)
		}
		resp := response.Resp.(*kvrpcpb.PhysicalScanLockResponse)
		if len(resp.Error) > 0 {
			return errors.Errorf("physical scan lock received error from store: %v", resp.Error)
		}

		if len(resp.Locks) == 0 {
			break
		}

		nextKey = resp.Locks[len(resp.Locks)-1].Key
		nextKey = append(nextKey, 0)

		for _, lockInfo := range resp.Locks {
			select {
			case lockCh <- scanLockResult{Lock: tikv.NewLock(lockInfo)}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		if len(resp.Locks) < int(s.scanLockLimit) {
			break
		}
	}

	return nil
}
