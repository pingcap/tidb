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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gcworker

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/label"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/ddl/util"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/session"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/logutil"
	tikverr "github.com/tikv/client-go/v2/error"
	tikvstore "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"github.com/tikv/client-go/v2/txnkv/rangetask"
	tikvutil "github.com/tikv/client-go/v2/util"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

// GCWorker periodically triggers GC process on tikv server.
type GCWorker struct {
	uuid               string
	desc               string
	store              kv.Storage
	tikvStore          tikv.Storage
	pdClient           pd.Client
	gcIsRunning        bool
	lastFinish         time.Time
	cancel             context.CancelFunc
	done               chan error
	regionLockResolver tikv.RegionLockResolver
}

// NewGCWorker creates a GCWorker instance.
func NewGCWorker(store kv.Storage, pdClient pd.Client) (*GCWorker, error) {
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return nil, errors.Trace(err)
	}
	hostName, err := os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return nil, errors.New("GC should run against TiKV storage")
	}
	uuid := strconv.FormatUint(ver.Ver, 16)
	resolverIdentifier := fmt.Sprintf("gc-worker-%s", uuid)
	worker := &GCWorker{
		uuid:               uuid,
		desc:               fmt.Sprintf("host:%s, pid:%d, start at %s", hostName, os.Getpid(), time.Now()),
		store:              store,
		tikvStore:          tikvStore,
		pdClient:           pdClient,
		gcIsRunning:        false,
		lastFinish:         time.Now(),
		regionLockResolver: tikv.NewRegionLockResolver(resolverIdentifier, tikvStore),
		done:               make(chan error),
	}
	variable.RegisterStatistics(worker)
	return worker, nil
}

// Start starts the worker.
func (w *GCWorker) Start() {
	var ctx context.Context
	ctx, w.cancel = context.WithCancel(context.Background())
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnGC)
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

	gcEnableKey          = "tikv_gc_enable"
	gcDefaultEnableValue = true

	gcModeKey         = "tikv_gc_mode"
	gcModeCentral     = "central"
	gcModeDistributed = "distributed"
	gcModeDefault     = gcModeDistributed

	gcScanLockModeKey = "tikv_gc_scan_lock_mode"

	gcAutoConcurrencyKey     = "tikv_gc_auto_concurrency"
	gcDefaultAutoConcurrency = true

	gcWorkerServiceSafePointID = "gc_worker"

	// Status var names start with tidb_%
	tidbGCLastRunTime = "tidb_gc_last_run_time"
	tidbGCLeaderDesc  = "tidb_gc_leader_desc"
	tidbGCLeaderLease = "tidb_gc_leader_lease"
	tidbGCLeaderUUID  = "tidb_gc_leader_uuid"
	tidbGCSafePoint   = "tidb_gc_safe_point"
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
	gcScanLockModeKey:    "Mode of scanning locks, \"physical\" or \"legacy\".(Deprecated)",
}

const (
	unsafeDestroyRangeTimeout = 5 * time.Minute
	gcTimeout                 = 5 * time.Minute
)

func (w *GCWorker) start(ctx context.Context, wg *sync.WaitGroup) {
	logutil.Logger(ctx).Info("start", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid))

	w.tick(ctx) // Immediately tick once to initialize configs.
	wg.Done()

	ticker := time.NewTicker(gcWorkerTickInterval)
	defer ticker.Stop()
	defer func() {
		r := recover()
		if r != nil {
			logutil.Logger(ctx).Error("gcWorker",
				zap.Any("r", r),
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
				logutil.Logger(ctx).Error("runGCJob", zap.String("category", "gc worker"), zap.Error(err))
			}
		case <-ctx.Done():
			logutil.Logger(ctx).Info("quit", zap.String("category", "gc worker"), zap.String("uuid", w.uuid))
			return
		}
	}
}

func createSession(store kv.Storage) sessiontypes.Session {
	for {
		se, err := session.CreateSession(store)
		if err != nil {
			logutil.BgLogger().Warn("create session", zap.String("category", "gc worker"), zap.Error(err))
			continue
		}
		// Disable privilege check for gc worker session.
		privilege.BindPrivilegeManager(se, nil)
		se.GetSessionVars().CommonGlobalLoaded = true
		se.GetSessionVars().InRestrictedSQL = true
		se.GetSessionVars().SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
		return se
	}
}

// GetScope gets the status variables scope.
func (w *GCWorker) GetScope(status string) variable.ScopeFlag {
	return variable.DefaultStatusVarScopeFlag
}

// Stats returns the server statistics.
func (w *GCWorker) Stats(vars *variable.SessionVars) (map[string]any, error) {
	m := make(map[string]any)
	if v, err := w.loadValueFromSysTable(gcLeaderUUIDKey); err == nil {
		m[tidbGCLeaderUUID] = v
	}
	if v, err := w.loadValueFromSysTable(gcLeaderDescKey); err == nil {
		m[tidbGCLeaderDesc] = v
	}
	if v, err := w.loadValueFromSysTable(gcLeaderLeaseKey); err == nil {
		m[tidbGCLeaderLease] = v
	}
	if v, err := w.loadValueFromSysTable(gcLastRunTimeKey); err == nil {
		m[tidbGCLastRunTime] = v
	}
	if v, err := w.loadValueFromSysTable(gcSafePointKey); err == nil {
		m[tidbGCSafePoint] = v
	}
	return m, nil
}

func (w *GCWorker) tick(ctx context.Context) {
	isLeader, err := w.checkLeader(ctx)
	if err != nil {
		logutil.Logger(ctx).Warn("check leader", zap.String("category", "gc worker"), zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("check_leader").Inc()
		return
	}
	if isLeader {
		err = w.leaderTick(ctx)
		if err != nil {
			logutil.Logger(ctx).Warn("leader tick", zap.String("category", "gc worker"), zap.Error(err))
		}
	} else {
		// Config metrics should always be updated by leader, set them to 0 when current instance is not leader.
		metrics.GCConfigGauge.WithLabelValues(gcRunIntervalKey).Set(0)
		metrics.GCConfigGauge.WithLabelValues(gcLifeTimeKey).Set(0)
	}
}

// getGCSafePoint returns the current gc safe point.
func getGCSafePoint(ctx context.Context, pdClient pd.Client) (uint64, error) {
	// If there is try to set gc safepoint is 0, the interface will not set gc safepoint to 0,
	// it will return current gc safepoint.
	safePoint, err := pdClient.UpdateGCSafePoint(ctx, 0)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return safePoint, nil
}

func (w *GCWorker) logIsGCSafePointTooEarly(ctx context.Context, safePoint uint64) error {
	now, err := w.getOracleTime()
	if err != nil {
		return errors.Trace(err)
	}

	checkTs := oracle.GoTimeToTS(now.Add(-gcDefaultLifeTime * 2))
	if checkTs > safePoint {
		logutil.Logger(ctx).Info("gc safepoint is too early. "+
			"Maybe there is a bit BR/Lightning/CDC task, "+
			"or a long transaction is running "+
			"or need a tidb without setting keyspace-name to calculate and update gc safe point.",
			zap.String("category", "gc worker"))
	}
	return nil
}

func (w *GCWorker) runKeyspaceDeleteRange(ctx context.Context, concurrency int) error {
	// Get safe point from PD.
	// The GC safe point is updated only after the global GC have done resolveLocks phase globally.
	// So, in the following code, resolveLocks must have been done by the global GC on the ranges to be deleted,
	// so its safe to delete the ranges.
	safePoint, err := getGCSafePoint(ctx, w.pdClient)
	if err != nil {
		logutil.Logger(ctx).Info("get gc safe point error", zap.String("category", "gc worker"), zap.Error(errors.Trace(err)))
		return nil
	}

	if safePoint == 0 {
		logutil.Logger(ctx).Info("skip keyspace delete range, because gc safe point is 0", zap.String("category", "gc worker"))
		return nil
	}

	err = w.logIsGCSafePointTooEarly(ctx, safePoint)
	if err != nil {
		logutil.Logger(ctx).Info("log is gc safe point is too early error", zap.String("category", "gc worker"), zap.Error(errors.Trace(err)))
		return nil
	}

	keyspaceID := w.store.GetCodec().GetKeyspaceID()
	logutil.Logger(ctx).Info("start keyspace delete range", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("concurrency", concurrency),
		zap.Uint32("keyspaceID", uint32(keyspaceID)),
		zap.Uint64("GCSafepoint", safePoint))

	// Do deleteRanges.
	err = w.deleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("delete range returns an error", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("delete_range").Inc()
		return errors.Trace(err)
	}

	// Do redoDeleteRanges.
	err = w.redoDeleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("redo-delete range returns an error", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("redo_delete_range").Inc()
		return errors.Trace(err)
	}

	return nil
}

// leaderTick of GC worker checks if it should start a GC job every tick.
func (w *GCWorker) leaderTick(ctx context.Context) error {
	if w.gcIsRunning {
		logutil.Logger(ctx).Info("there's already a gc job running, skipped", zap.String("category", "gc worker"),
			zap.String("leaderTick on", w.uuid))
		return nil
	}

	concurrency, err := w.getGCConcurrency(ctx)
	if err != nil {
		logutil.Logger(ctx).Info("failed to get gc concurrency.", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		return errors.Trace(err)
	}

	// Gc safe point is not separated by keyspace now. The whole cluster has only one global gc safe point.
	// So at least one TiDB with `keyspace-name` not set is required in the whole cluster to calculate and update gc safe point.
	// If `keyspace-name` is set, the TiDB node will only do its own delete range, and will not calculate gc safe point and resolve locks.
	// Note that when `keyspace-name` is set, `checkLeader` will be done within the key space.
	// Therefore only one TiDB node in each key space will be responsible to do delete range.
	if w.store.GetCodec().GetKeyspace() != nil {
		err = w.runKeyspaceGCJob(ctx, concurrency)
		if err != nil {
			return errors.Trace(err)
		}
		return nil
	}

	ok, safePoint, err := w.prepare(ctx)
	if err != nil {
		metrics.GCJobFailureCounter.WithLabelValues("prepare").Inc()
		return errors.Trace(err)
	} else if !ok {
		return nil
	}
	// When the worker is just started, or an old GC job has just finished,
	// wait a while before starting a new job.
	if time.Since(w.lastFinish) < gcWaitTime {
		logutil.Logger(ctx).Info("another gc job has just finished, skipped.", zap.String("category", "gc worker"),
			zap.String("leaderTick on ", w.uuid))
		return nil
	}

	w.gcIsRunning = true
	logutil.Logger(ctx).Info("starts the whole job", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("concurrency", concurrency))
	go func() {
		w.done <- w.runGCJob(ctx, safePoint, concurrency)
	}()
	return nil
}

func (w *GCWorker) runKeyspaceGCJob(ctx context.Context, concurrency int) error {
	// When the worker is just started, or an old GC job has just finished,
	// wait a while before starting a new job.
	if time.Since(w.lastFinish) < gcWaitTime {
		logutil.Logger(ctx).Info("another keyspace gc job has just finished, skipped.", zap.String("category", "gc worker"),
			zap.String("leaderTick on ", w.uuid))
		return nil
	}

	now, err := w.getOracleTime()
	if err != nil {
		return errors.Trace(err)
	}
	ok, err := w.checkGCInterval(now)
	if err != nil || !ok {
		return errors.Trace(err)
	}

	go func() {
		w.done <- w.runKeyspaceDeleteRange(ctx, concurrency)
	}()

	err = w.saveTime(gcLastRunTimeKey, now)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// prepare checks preconditions for starting a GC job. It returns a bool
// that indicates whether the GC job should start and the new safePoint.
func (w *GCWorker) prepare(ctx context.Context) (bool, uint64, error) {
	// Add a transaction here is to prevent following situations:
	// 1. GC check gcEnable is true, continue to do GC
	// 2. The user sets gcEnable to false
	// 3. The user gets `tikv_gc_safe_point` value is t1, then the user thinks the data after time t1 won't be clean by GC.
	// 4. GC update `tikv_gc_safe_point` value to t2, continue do GC in this round.
	// Then the data record that has been dropped between time t1 and t2, will be cleaned by GC, but the user thinks the data after t1 won't be clean by GC.
	se := createSession(w.store)
	defer se.Close()
	_, err := se.ExecuteInternal(ctx, "BEGIN")
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
		logutil.Logger(ctx).Warn("gc status is disabled.", zap.String("category", "gc worker"))
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
	newSafePoint, newSafePointValue, err := w.calcNewSafePoint(ctx, now)
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

func (w *GCWorker) calcGlobalMinStartTS(ctx context.Context) (uint64, error) {
	kvs, err := w.tikvStore.GetSafePointKV().GetWithPrefix(infosync.ServerMinStartTSPath)
	if err != nil {
		return 0, err
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
	return globalMinStartTS, nil
}

// calcNewSafePoint uses the current global transaction min start timestamp to calculate the new safe point.
func (w *GCWorker) calcSafePointByMinStartTS(ctx context.Context, safePoint uint64) uint64 {
	globalMinStartTS, err := w.calcGlobalMinStartTS(ctx)
	if err != nil {
		logutil.Logger(ctx).Warn("get all minStartTS failed", zap.Error(err))
		return safePoint
	}

	// If the lock.ts <= max_ts(safePoint), it will be collected and resolved by the gc worker,
	// the locks of ongoing pessimistic transactions could be resolved by the gc worker and then
	// the transaction is aborted, decrement the value by 1 to avoid this.
	globalMinStartAllowedTS := globalMinStartTS
	if globalMinStartTS > 0 {
		globalMinStartAllowedTS = globalMinStartTS - 1
	}

	if globalMinStartAllowedTS < safePoint {
		logutil.Logger(ctx).Info("gc safepoint blocked by a running session", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("globalMinStartTS", globalMinStartTS),
			zap.Uint64("globalMinStartAllowedTS", globalMinStartAllowedTS),
			zap.Uint64("safePoint", safePoint))
		safePoint = globalMinStartAllowedTS
	}
	return safePoint
}

func (w *GCWorker) getOracleTime() (time.Time, error) {
	currentVer, err := w.store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return time.Time{}, errors.Trace(err)
	}
	return oracle.GetTimeFromTS(currentVer.Ver), nil
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
		logutil.Logger(ctx).Error("failed to load config gc_auto_concurrency. use default value.", zap.String("category", "gc worker"),
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
		logutil.Logger(ctx).Error("failed to get up stores to calculate concurrency. use config.", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))

		concurrency, err = w.loadGCConcurrencyWithDefault()
		if err != nil {
			logutil.Logger(ctx).Error("failed to load gc concurrency from config. use default value.", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Error(err))
			concurrency = gcDefaultConcurrency
		}
	}

	if concurrency == 0 {
		logutil.Logger(ctx).Error("no store is up", zap.String("category", "gc worker"),
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
		logutil.BgLogger().Debug("skipping garbage collection because gc interval hasn't elapsed since last run", zap.String("category", "gc worker"),
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

	logutil.BgLogger().Info("invalid gc life time", zap.String("category", "gc worker"),
		zap.Duration("get gc life time", lifeTime),
		zap.Duration("min gc life time", gcMinLifeTime))

	err := w.saveDuration(gcLifeTimeKey, gcMinLifeTime)
	return gcMinLifeTime, err
}

func (w *GCWorker) calcNewSafePoint(ctx context.Context, now time.Time) (*time.Time, uint64, error) {
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

	safePointValue := w.calcSafePointByMinStartTS(ctx, oracle.GoTimeToTS(now.Add(-*lifeTime)))
	safePointValue, err = w.setGCWorkerServiceSafePoint(ctx, safePointValue)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	// safepoint is recorded in time.Time format which strips the logical part of the timestamp.
	// To prevent the GC worker from keeping working due to the loss of logical part when the
	// safe point isn't changed, we should compare them in time.Time format.
	safePoint := oracle.GetTimeFromTS(safePointValue)
	// We should never decrease safePoint.
	if lastSafePoint != nil && !safePoint.After(*lastSafePoint) {
		logutil.BgLogger().Info("last safe point is later than current one."+
			"No need to gc."+
			"This might be caused by manually enlarging gc lifetime",
			zap.String("category", "gc worker"),
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
		logutil.Logger(ctx).Error("failed to update service safe point", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("update_service_safe_point").Inc()
		return 0, errors.Trace(err)
	}
	if minSafePoint < safePoint {
		logutil.Logger(ctx).Info("there's another service in the cluster requires an earlier safe point. "+
			"gc will continue with the earlier one",
			zap.String("category", "gc worker"),
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
		failpoint.Return(errors.New("mock failure of runGCJoB"))
	})
	metrics.GCWorkerCounter.WithLabelValues("run_job").Inc()

	err := w.resolveLocks(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("resolve locks returns an error", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("resolve_lock").Inc()
		return errors.Trace(err)
	}

	// Save safe point to pd.
	err = w.saveSafePoint(w.tikvStore.GetSafePointKV(), safePoint)
	if err != nil {
		logutil.Logger(ctx).Error("failed to save safe point to PD", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("save_safe_point").Inc()
		return errors.Trace(err)
	}
	// Sleep to wait for all other tidb instances update their safepoint cache.
	time.Sleep(gcSafePointCacheInterval)

	err = w.deleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("delete range returns an error", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("delete_range").Inc()
		return errors.Trace(err)
	}
	err = w.redoDeleteRanges(ctx, safePoint, concurrency)
	if err != nil {
		logutil.Logger(ctx).Error("redo-delete range returns an error", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("redo_delete_range").Inc()
		return errors.Trace(err)
	}

	if w.checkUseDistributedGC() {
		err = w.uploadSafePointToPD(ctx, safePoint)
		if err != nil {
			logutil.Logger(ctx).Error("failed to upload safe point to PD", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Error(err))
			metrics.GCJobFailureCounter.WithLabelValues("upload_safe_point").Inc()
			return errors.Trace(err)
		}
	} else {
		err = w.doGC(ctx, safePoint, concurrency)
		if err != nil {
			logutil.Logger(ctx).Error("do GC returns an error", zap.String("category", "gc worker"),
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
	defer se.Close()
	ranges, err := util.LoadDeleteRanges(ctx, se, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	v2, err := util.IsRaftKv2(ctx, se)
	if err != nil {
		return errors.Trace(err)
	}
	// Cache table ids on which placement rules have been GC-ed, to avoid redundantly GC the same table id multiple times.
	gcPlacementRuleCache := make(map[int64]any, len(ranges))

	logutil.Logger(ctx).Info("start delete ranges", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("ranges", len(ranges)))
	startTime := time.Now()
	for _, r := range ranges {
		startKey, endKey := r.Range()
		if v2 {
			// In raftstore-v2, we use delete range instead to avoid deletion omission
			task := rangetask.NewDeleteRangeTask(w.tikvStore, startKey, endKey, concurrency)
			err = task.Execute(ctx)
		} else {
			err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey, concurrency)
		}
		failpoint.Inject("ignoreDeleteRangeFailed", func() {
			err = nil
		})

		if err != nil {
			logutil.Logger(ctx).Error("delete range failed on range", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			continue
		}

		err = util.CompleteDeleteRange(se, r, !v2)
		if err != nil {
			logutil.Logger(ctx).Error("failed to mark delete range task done", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("save").Inc()
		}

		if err := w.doGCPlacementRules(se, safePoint, r, gcPlacementRuleCache); err != nil {
			logutil.Logger(ctx).Error("gc placement rules failed on range", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Int64("jobID", r.JobID),
				zap.Int64("elementID", r.ElementID),
				zap.Error(err))
			continue
		}
		if err := w.doGCLabelRules(r); err != nil {
			logutil.Logger(ctx).Error("gc label rules failed on range", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Int64("jobID", r.JobID),
				zap.Int64("elementID", r.ElementID),
				zap.Error(err))
			continue
		}
	}
	logutil.Logger(ctx).Info("finish delete ranges", zap.String("category", "gc worker"),
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
	ranges, err := util.LoadDoneDeleteRanges(ctx, se, redoDeleteRangesTs)
	se.Close()
	if err != nil {
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("start redo-delete ranges", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("num of ranges", len(ranges)))
	startTime := time.Now()
	for _, r := range ranges {
		startKey, endKey := r.Range()

		err = w.doUnsafeDestroyRangeRequest(ctx, startKey, endKey, concurrency)
		if err != nil {
			logutil.Logger(ctx).Error("redo-delete range failed on range", zap.String("category", "gc worker"),
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
			logutil.Logger(ctx).Error("failed to remove delete_range_done record", zap.String("category", "gc worker"),
				zap.String("uuid", w.uuid),
				zap.Stringer("startKey", startKey),
				zap.Stringer("endKey", endKey),
				zap.Error(err))
			metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("save_redo").Inc()
		}
	}
	logutil.Logger(ctx).Info("finish redo-delete ranges", zap.String("category", "gc worker"),
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
		logutil.Logger(ctx).Error("delete ranges: got an error while trying to get store list from PD", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCUnsafeDestroyRangeFailuresCounterVec.WithLabelValues("get_stores").Inc()
		return errors.Trace(err)
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdUnsafeDestroyRange, &kvrpcpb.UnsafeDestroyRangeRequest{
		StartKey: startKey,
		EndKey:   endKey,
	}, kvrpcpb.Context{DiskFullOpt: kvrpcpb.DiskFullOpt_AllowedOnAlmostFull})

	var wg sync.WaitGroup
	errChan := make(chan error, len(stores))

	for _, store := range stores {
		address := store.Address
		storeID := store.Id
		wg.Add(1)
		go func() {
			defer wg.Done()

			resp, err1 := w.tikvStore.GetTiKVClient().SendRequest(ctx, address, req, unsafeDestroyRangeTimeout)
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

	return nil
}

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
		if label.GetKey() == placement.EngineLabelKey {
			engineLabel = label.GetValue()
			break
		}
	}

	switch engineLabel {
	case placement.EngineLabelTiFlash:
		// For a TiFlash node, it uses other approach to delete dropped tables, so it's safe to skip sending
		// UnsafeDestroyRange requests; it has only learner peers and their data must exist in TiKV, so it's safe to
		// skip physical resolve locks for it.
		return false, nil

	case placement.EngineLabelTiFlashCompute:
		logutil.BgLogger().Debug("will ignore gc tiflash_compute node", zap.String("category", "gc worker"))
		return false, nil

	case placement.EngineLabelTiKV, "":
		// If no engine label is set, it should be a TiKV node.
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

// Central mode is deprecated in v5.0. This function will always return true.
func (w *GCWorker) checkUseDistributedGC() bool {
	mode, err := w.loadValueFromSysTable(gcModeKey)
	if err == nil && mode == "" {
		err = w.saveValueToSysTable(gcModeKey, gcModeDefault)
	}
	if err != nil {
		logutil.BgLogger().Error("failed to load gc mode, fall back to distributed mode", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Error(err))
		metrics.GCJobFailureCounter.WithLabelValues("check_gc_mode").Inc()
	} else if strings.EqualFold(mode, gcModeCentral) {
		logutil.BgLogger().Warn("distributed mode will be used as central mode is deprecated", zap.String("category", "gc worker"))
	} else if !strings.EqualFold(mode, gcModeDistributed) {
		logutil.BgLogger().Warn("distributed mode will be used", zap.String("category", "gc worker"),
			zap.String("invalid gc mode", mode))
	}
	return true
}

func (w *GCWorker) resolveLocks(
	ctx context.Context,
	safePoint uint64,
	concurrency int,
) error {
	metrics.GCWorkerCounter.WithLabelValues("resolve_locks").Inc()
	logutil.Logger(ctx).Info("start resolve locks", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("concurrency", concurrency))
	startTime := time.Now()

	handler := func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
		scanLimit := uint32(tikv.GCScanLockLimit)
		failpoint.Inject("lowScanLockLimit", func() {
			scanLimit = 3
		})
		return tikv.ResolveLocksForRange(ctx, w.regionLockResolver, safePoint, r.StartKey, r.EndKey, tikv.NewGcResolveLockMaxBackoffer, scanLimit)
	}

	runner := rangetask.NewRangeTaskRunner("resolve-locks-runner", w.tikvStore, concurrency, handler)
	// Run resolve lock on the whole TiKV cluster. Empty keys means the range is unbounded.
	err := runner.RunOnRange(ctx, []byte(""), []byte(""))
	if err != nil {
		logutil.Logger(ctx).Error("resolve locks failed", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("safePoint", safePoint),
			zap.Error(err))
		return errors.Trace(err)
	}

	logutil.Logger(ctx).Info("finish resolve locks", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("regions", runner.CompletedRegions()))
	metrics.GCHistogram.WithLabelValues("resolve_locks").Observe(time.Since(startTime).Seconds())
	return nil
}

const gcOneRegionMaxBackoff = 20000

func (w *GCWorker) uploadSafePointToPD(ctx context.Context, safePoint uint64) error {
	var newSafePoint uint64
	var err error

	bo := tikv.NewBackofferWithVars(ctx, gcOneRegionMaxBackoff, nil)
	for {
		newSafePoint, err = w.pdClient.UpdateGCSafePoint(ctx, safePoint)
		if err != nil {
			if errors.Cause(err) == context.Canceled {
				return errors.Trace(err)
			}
			err = bo.Backoff(tikv.BoPDRPC(), errors.Errorf("failed to upload safe point to PD, err: %v", err))
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		break
	}

	if newSafePoint != safePoint {
		logutil.Logger(ctx).Warn("PD rejected safe point", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Uint64("our safe point", safePoint),
			zap.Uint64("using another safe point", newSafePoint))
		return errors.Errorf("PD rejected our safe point %v but is using another safe point %v", safePoint, newSafePoint)
	}
	logutil.Logger(ctx).Info("sent safe point to PD", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Uint64("safe point", safePoint))
	return nil
}

func (w *GCWorker) doGCForRange(ctx context.Context, startKey []byte, endKey []byte, safePoint uint64) (rangetask.TaskStat, error) {
	var stat rangetask.TaskStat
	defer func() {
		metrics.GCActionRegionResultCounter.WithLabelValues("success").Add(float64(stat.CompletedRegions))
		metrics.GCActionRegionResultCounter.WithLabelValues("fail").Add(float64(stat.FailedRegions))
	}()
	key := startKey
	for {
		bo := tikv.NewBackofferWithVars(ctx, gcOneRegionMaxBackoff, nil)
		loc, err := w.tikvStore.GetRegionCache().LocateKey(bo, key)
		if err != nil {
			return stat, errors.Trace(err)
		}

		var regionErr *errorpb.Error
		regionErr, err = w.doGCForRegion(bo, safePoint, loc.Region)

		// we check regionErr here first, because we know 'regionErr' and 'err' should not return together, to keep it to
		// make the process correct.
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss(), errors.New(regionErr.String()))
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

	resp, err := w.tikvStore.SendReq(bo, req, region, gcTimeout)
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
		return nil, errors.Trace(tikverr.ErrBodyMissing)
	}
	gcResp := resp.Resp.(*kvrpcpb.GCResponse)
	if gcResp.GetError() != nil {
		return nil, errors.Errorf("unexpected gc error: %s", gcResp.GetError())
	}

	return nil, nil
}

func (w *GCWorker) doGC(ctx context.Context, safePoint uint64, concurrency int) error {
	metrics.GCWorkerCounter.WithLabelValues("do_gc").Inc()
	logutil.Logger(ctx).Info("start doing gc for all keys", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Int("concurrency", concurrency),
		zap.Uint64("safePoint", safePoint))
	startTime := time.Now()

	runner := rangetask.NewRangeTaskRunner(
		"gc-runner",
		w.tikvStore,
		concurrency,
		func(ctx context.Context, r tikvstore.KeyRange) (rangetask.TaskStat, error) {
			return w.doGCForRange(ctx, r.StartKey, r.EndKey, safePoint)
		})

	err := runner.RunOnRange(ctx, []byte(""), []byte(""))
	if err != nil {
		logutil.Logger(ctx).Warn("failed to do gc for all keys", zap.String("category", "gc worker"),
			zap.String("uuid", w.uuid),
			zap.Int("concurrency", concurrency),
			zap.Error(err))
		return errors.Trace(err)
	}

	successRegions := runner.CompletedRegions()
	failedRegions := runner.FailedRegions()

	logutil.Logger(ctx).Info("finished doing gc for all keys", zap.String("category", "gc worker"),
		zap.String("uuid", w.uuid),
		zap.Uint64("safePoint", safePoint),
		zap.Int("successful regions", successRegions),
		zap.Int("failed regions", failedRegions),
		zap.Duration("total cost time", time.Since(startTime)))
	metrics.GCHistogram.WithLabelValues("do_gc").Observe(time.Since(startTime).Seconds())

	return nil
}

func (w *GCWorker) checkLeader(ctx context.Context) (bool, error) {
	metrics.GCWorkerCounter.WithLabelValues("check_leader").Inc()
	se := createSession(w.store)
	defer se.Close()

	_, err := se.ExecuteInternal(ctx, "BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	leader, err := w.loadValueFromSysTable(gcLeaderUUIDKey)
	if err != nil {
		se.RollbackTxn(ctx)
		return false, errors.Trace(err)
	}
	logutil.BgLogger().Debug("got leader", zap.String("category", "gc worker"), zap.String("uuid", leader))
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

	_, err = se.ExecuteInternal(ctx, "BEGIN")
	if err != nil {
		return false, errors.Trace(err)
	}
	lease, err := w.loadTime(gcLeaderLeaseKey)
	if err != nil {
		se.RollbackTxn(ctx)
		return false, errors.Trace(err)
	}
	if lease == nil || lease.Before(time.Now()) {
		logutil.BgLogger().Debug("register as leader", zap.String("category", "gc worker"),
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
	err := w.saveValueToSysTable(key, t.Format(tikvutil.GCTimeFormat))
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
	t, err := tikvutil.CompatibleParseGCTime(str)
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
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnGC)
	se := createSession(w.store)
	defer se.Close()
	rs, err := se.ExecuteInternal(ctx, `SELECT HIGH_PRIORITY (variable_value) FROM mysql.tidb WHERE variable_name=%? FOR UPDATE`, key)
	if rs != nil {
		defer terror.Call(rs.Close)
	}
	if err != nil {
		return "", errors.Trace(err)
	}
	req := rs.NewChunk(nil)
	err = rs.Next(ctx, req)
	if err != nil {
		return "", errors.Trace(err)
	}
	if req.NumRows() == 0 {
		logutil.BgLogger().Debug("load kv", zap.String("category", "gc worker"),
			zap.String("key", key))
		return "", nil
	}
	value := req.GetRow(0).GetString(0)
	logutil.BgLogger().Debug("load kv", zap.String("category", "gc worker"),
		zap.String("key", key),
		zap.String("value", value))
	return value, nil
}

func (w *GCWorker) saveValueToSysTable(key, value string) error {
	const stmt = `INSERT HIGH_PRIORITY INTO mysql.tidb VALUES (%?, %?, %?)
			       ON DUPLICATE KEY
			       UPDATE variable_value = %?, comment = %?`
	se := createSession(w.store)
	defer se.Close()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnGC)
	_, err := se.ExecuteInternal(ctx, stmt,
		key, value, gcVariableComments[key],
		value, gcVariableComments[key])
	logutil.BgLogger().Debug("save kv", zap.String("category", "gc worker"),
		zap.String("key", key),
		zap.String("value", value),
		zap.Error(err))
	return errors.Trace(err)
}

// GC placement rules when the partitions are removed by the GC worker.
// Placement rules cannot be removed immediately after drop table / truncate table,
// because the tables can be flashed back or recovered.
func (w *GCWorker) doGCPlacementRules(se sessiontypes.Session, safePoint uint64, dr util.DelRangeTask, gcPlacementRuleCache map[int64]any) (err error) {
	// Get the job from the job history
	var historyJob *model.Job
	failpoint.Inject("mockHistoryJobForGC", func(v failpoint.Value) {
		args, err1 := json.Marshal([]any{kv.Key{}, []int64{int64(v.(int))}})
		if err1 != nil {
			return
		}
		historyJob = &model.Job{
			ID:      dr.JobID,
			Type:    model.ActionDropTable,
			TableID: int64(v.(int)),
			RawArgs: args,
		}
	})
	if historyJob == nil {
		historyJob, err = ddl.GetHistoryJobByID(se, dr.JobID)
		if err != nil {
			return
		}
		if historyJob == nil {
			return dbterror.ErrDDLJobNotFound.GenWithStackByArgs(dr.JobID)
		}
	}

	// Notify PD to drop the placement rules of partition-ids and table-id, even if there may be no placement rules.
	var physicalTableIDs []int64
	switch historyJob.Type {
	case model.ActionDropTable, model.ActionTruncateTable:
		var startKey kv.Key
		if err = historyJob.DecodeArgs(&startKey, &physicalTableIDs); err != nil {
			return
		}
		physicalTableIDs = append(physicalTableIDs, historyJob.TableID)
	case model.ActionDropSchema, model.ActionDropTablePartition, model.ActionTruncateTablePartition,
		model.ActionReorganizePartition, model.ActionRemovePartitioning,
		model.ActionAlterTablePartitioning:
		if err = historyJob.DecodeArgs(&physicalTableIDs); err != nil {
			return
		}
	}

	// Skip table ids that's already successfully handled.
	tmp := physicalTableIDs[:0]
	for _, id := range physicalTableIDs {
		if _, ok := gcPlacementRuleCache[id]; !ok {
			tmp = append(tmp, id)
		}
	}
	physicalTableIDs = tmp

	if len(physicalTableIDs) == 0 {
		return
	}

	for _, id := range physicalTableIDs {
		// Delete pd rule
		failpoint.Inject("gcDeletePlacementRuleCounter", func() {})
		logutil.BgLogger().Info("try delete TiFlash pd rule",
			zap.Int64("tableID", id), zap.String("endKey", string(dr.EndKey)), zap.Uint64("safePoint", safePoint))
		ruleID := infosync.MakeRuleID(id)
		if err := infosync.DeleteTiFlashPlacementRule(context.Background(), "tiflash", ruleID); err != nil {
			logutil.BgLogger().Error("delete TiFlash pd rule failed when gc",
				zap.Error(err), zap.String("ruleID", ruleID), zap.Uint64("safePoint", safePoint))
		}
	}
	bundles := make([]*placement.Bundle, 0, len(physicalTableIDs))
	for _, id := range physicalTableIDs {
		bundles = append(bundles, placement.NewBundle(id))
	}
	err = infosync.PutRuleBundlesWithDefaultRetry(context.TODO(), bundles)
	if err != nil {
		return
	}

	// Cache the table id if its related rule are deleted successfully.
	for _, id := range physicalTableIDs {
		gcPlacementRuleCache[id] = struct{}{}
	}
	return nil
}

func (w *GCWorker) doGCLabelRules(dr util.DelRangeTask) (err error) {
	// Get the job from the job history
	var historyJob *model.Job
	failpoint.Inject("mockHistoryJob", func(v failpoint.Value) {
		args, err1 := json.Marshal([]any{kv.Key{}, []int64{}, []string{v.(string)}})
		if err1 != nil {
			return
		}
		historyJob = &model.Job{
			ID:      dr.JobID,
			Type:    model.ActionDropTable,
			RawArgs: args,
		}
	})
	if historyJob == nil {
		se := createSession(w.store)
		historyJob, err = ddl.GetHistoryJobByID(se, dr.JobID)
		se.Close()
		if err != nil {
			return
		}
		if historyJob == nil {
			return dbterror.ErrDDLJobNotFound.GenWithStackByArgs(dr.JobID)
		}
	}

	if historyJob.Type == model.ActionDropTable {
		var (
			startKey         kv.Key
			physicalTableIDs []int64
			ruleIDs          []string
			rules            map[string]*label.Rule
		)
		if err = historyJob.DecodeArgs(&startKey, &physicalTableIDs, &ruleIDs); err != nil {
			return
		}

		// TODO: Here we need to get rules from PD and filter the rules which is not elegant. We should find a better way.
		rules, err = infosync.GetLabelRules(context.TODO(), ruleIDs)
		if err != nil {
			return
		}

		ruleIDs = getGCRules(append(physicalTableIDs, historyJob.TableID), rules)
		patch := label.NewRulePatch([]*label.Rule{}, ruleIDs)
		err = infosync.UpdateLabelRules(context.TODO(), patch)
	}
	return
}

func getGCRules(ids []int64, rules map[string]*label.Rule) []string {
	oldRange := make(map[string]struct{})
	for _, id := range ids {
		startKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(id)))
		endKey := hex.EncodeToString(codec.EncodeBytes(nil, tablecodec.GenTablePrefix(id+1)))
		oldRange[startKey+endKey] = struct{}{}
	}

	var gcRules []string
	for _, rule := range rules {
		find := false
		for _, d := range rule.Data.([]any) {
			if r, ok := d.(map[string]any); ok {
				nowRange := fmt.Sprintf("%s%s", r["start_key"], r["end_key"])
				if _, ok := oldRange[nowRange]; ok {
					find = true
				}
			}
		}
		if find {
			gcRules = append(gcRules, rule.ID)
		}
	}
	return gcRules
}

// RunGCJob sends GC command to KV. It is exported for kv api, do not use it with GCWorker at the same time.
// only use for test
func RunGCJob(ctx context.Context, regionLockResolver tikv.RegionLockResolver, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		tikvStore:          s,
		uuid:               identifier,
		pdClient:           pd,
		regionLockResolver: regionLockResolver,
	}

	if concurrency <= 0 {
		return errors.Errorf("[gc worker] gc concurrency should greater than 0, current concurrency: %v", concurrency)
	}

	safePoint, err := gcWorker.setGCWorkerServiceSafePoint(ctx, safePoint)
	if err != nil {
		return errors.Trace(err)
	}

	err = gcWorker.resolveLocks(ctx, safePoint, concurrency)
	if err != nil {
		return errors.Trace(err)
	}

	err = gcWorker.saveSafePoint(gcWorker.tikvStore.GetSafePointKV(), safePoint)
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
func RunDistributedGCJob(ctx context.Context, regionLockResolver tikv.RegionLockResolver, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		tikvStore:          s,
		uuid:               identifier,
		pdClient:           pd,
		regionLockResolver: regionLockResolver,
	}

	safePoint, err := gcWorker.setGCWorkerServiceSafePoint(ctx, safePoint)
	if err != nil {
		return errors.Trace(err)
	}
	err = gcWorker.resolveLocks(ctx, safePoint, concurrency)
	if err != nil {
		return errors.Trace(err)
	}

	// Save safe point to pd.
	err = gcWorker.saveSafePoint(gcWorker.tikvStore.GetSafePointKV(), safePoint)
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

// RunResolveLocks resolves all locks before the safePoint.
// It is exported only for test, do not use it in the production environment.
func RunResolveLocks(ctx context.Context, s tikv.Storage, pd pd.Client, safePoint uint64, identifier string, concurrency int) error {
	gcWorker := &GCWorker{
		tikvStore:          s,
		uuid:               identifier,
		pdClient:           pd,
		regionLockResolver: tikv.NewRegionLockResolver("test-resolver", s),
	}
	return gcWorker.resolveLocks(ctx, safePoint, concurrency)
}

// MockGCWorker is for test.
type MockGCWorker struct {
	worker *GCWorker
}

// NewMockGCWorker creates a MockGCWorker instance ONLY for test.
func NewMockGCWorker(store kv.Storage) (*MockGCWorker, error) {
	ver, err := store.CurrentVersion(kv.GlobalTxnScope)
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
		tikvStore:   store.(tikv.Storage),
		gcIsRunning: false,
		lastFinish:  time.Now(),
		done:        make(chan error),
		pdClient:    store.(tikv.Storage).GetRegionCache().PDClient(),
	}
	return &MockGCWorker{worker: worker}, nil
}

// DeleteRanges calls deleteRanges internally, just for test.
func (w *MockGCWorker) DeleteRanges(ctx context.Context, safePoint uint64) error {
	logutil.Logger(ctx).Error("deleteRanges is called")
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnGC)
	return w.worker.deleteRanges(ctx, safePoint, 1)
}
