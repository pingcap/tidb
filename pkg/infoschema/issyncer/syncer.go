// Copyright 2025 PingCAP, Inc.
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

package issyncer

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/isvalidator"
	infoschema_metrics "github.com/pingcap/tidb/pkg/infoschema/metrics"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/domainutil"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.uber.org/zap"
)

var (
	mdlCheckLookDuration = 50 * time.Millisecond
	// LoadSchemaDiffVersionGapThreshold is the threshold for version gap to reload domain by loading schema diffs
	LoadSchemaDiffVersionGapThreshold int64 = 10000
)

func init() {
	if intest.InTest {
		// In test we can set duration lower to make test faster.
		mdlCheckLookDuration = 2 * time.Millisecond
	}
}

// Syncer is the main structure for syncing the info schema.
type Syncer struct {
	m               syncutil.Mutex
	store           kv.Storage
	infoCache       *infoschema.InfoCache
	schemaLease     time.Duration
	SchemaValidator validatorapi.Validator
	// Note: If you no longer need the session, you must call Destroy to release it.
	// Otherwise, the session will be leaked. Because there is a strong reference from the domain to the session.
	// Deprecated: Use `advancedSysSessionPool` instead.
	sysSessionPool util.DestroyableSessionPool
	// deferFn is used to release infoschema object lazily during v1 and v2 switch
	deferFn           deferFn
	mdlCheckCh        chan struct{}
	mdlCheckTableInfo *mdlCheckTableInfo

	// below fields are set when running background routines
	info              *infosync.InfoSyncer
	schemaVerSyncer   schemaver.Syncer
	minJobIDRefresher *systable.MinJobIDRefresher
	// autoidClient is used when there are tables with AUTO_ID_CACHE=1, it is the client to the autoid service.
	autoidClient       *autoid.ClientDiscover
	sysExecutorFactory func() (pools.Resource, error)
}

// New creates a new Syncer instance.
func New(
	store kv.Storage,
	infoCache *infoschema.InfoCache,
	schemaLease time.Duration,
	sysSessionPool util.DestroyableSessionPool,
) *Syncer {
	do := &Syncer{
		store:          store,
		infoCache:      infoCache,
		schemaLease:    schemaLease,
		sysSessionPool: sysSessionPool,

		mdlCheckCh: make(chan struct{}),
		mdlCheckTableInfo: &mdlCheckTableInfo{
			mu:         sync.Mutex{},
			jobsVerMap: make(map[int64]int64),
			jobsIDsMap: make(map[int64]string),
		},
	}
	do.SchemaValidator = isvalidator.New(schemaLease)

	return do
}

// InitSomeFields initializes some fields of the Syncer.
func (s *Syncer) InitSomeFields(
	info *infosync.InfoSyncer,
	schemaVerSyncer schemaver.Syncer,
	autoidClient *autoid.ClientDiscover,
	sysExecutorFactory func() (pools.Resource, error),
) {
	s.info = info
	s.schemaVerSyncer = schemaVerSyncer
	s.autoidClient = autoidClient
	s.sysExecutorFactory = sysExecutorFactory
}

// SetMinJobIDRefresher sets the MinJobIDRefresher for the Syncer.
func (s *Syncer) SetMinJobIDRefresher(minJobIDRefresher *systable.MinJobIDRefresher) {
	s.minJobIDRefresher = minJobIDRefresher
}

func (s *Syncer) refreshMDLCheckTableInfo(ctx context.Context) {
	se, err := s.sysSessionPool.Get()

	if err != nil {
		logutil.BgLogger().Warn("get system session failed", zap.Error(err))
		return
	}
	// Make sure the session is new.
	sctx := se.(sessionctx.Context)
	ctx = kv.WithInternalSourceType(ctx, kv.InternalTxnMeta)
	if _, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, "rollback"); err != nil {
		se.Close()
		return
	}
	defer s.sysSessionPool.Put(se)
	exec := sctx.GetRestrictedSQLExecutor()
	domainSchemaVer := s.InfoSchema().SchemaMetaVersion()
	// the job must stay inside tidb_ddl_job if we need to wait schema version for it.
	sql := fmt.Sprintf(`select job_id, version, table_ids from mysql.tidb_mdl_info
		where job_id >= %d and version <= %d`, s.minJobIDRefresher.GetCurrMinJobID(), domainSchemaVer)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql)
	if err != nil {
		logutil.BgLogger().Warn("get mdl info from tidb_mdl_info failed", zap.Error(err))
		return
	}
	s.mdlCheckTableInfo.mu.Lock()
	defer s.mdlCheckTableInfo.mu.Unlock()

	s.mdlCheckTableInfo.newestVer = domainSchemaVer
	s.mdlCheckTableInfo.jobsVerMap = make(map[int64]int64, len(rows))
	s.mdlCheckTableInfo.jobsIDsMap = make(map[int64]string, len(rows))
	for i := range rows {
		s.mdlCheckTableInfo.jobsVerMap[rows[i].GetInt64(0)] = rows[i].GetInt64(1)
		s.mdlCheckTableInfo.jobsIDsMap[rows[i].GetInt64(0)] = rows[i].GetString(2)
	}
}

// MDLCheckLoop is a loop that checks the MDL locks periodically.
func (s *Syncer) MDLCheckLoop(ctx context.Context) {
	ticker := time.Tick(mdlCheckLookDuration)
	var saveMaxSchemaVersion int64
	jobNeedToSync := false
	jobCache := make(map[int64]int64, 1000)

	for {
		// Wait for channels
		select {
		case <-s.mdlCheckCh:
		case <-ticker:
		case <-ctx.Done():
			return
		}

		if !vardef.EnableMDL.Load() {
			continue
		}

		s.mdlCheckTableInfo.mu.Lock()
		maxVer := s.mdlCheckTableInfo.newestVer
		if maxVer > saveMaxSchemaVersion {
			saveMaxSchemaVersion = maxVer
		} else if !jobNeedToSync {
			// Schema doesn't change, and no job to check in the last run.
			s.mdlCheckTableInfo.mu.Unlock()
			continue
		}

		jobNeedToCheckCnt := len(s.mdlCheckTableInfo.jobsVerMap)
		if jobNeedToCheckCnt == 0 {
			jobNeedToSync = false
			s.mdlCheckTableInfo.mu.Unlock()
			continue
		}

		jobsVerMap := make(map[int64]int64, len(s.mdlCheckTableInfo.jobsVerMap))
		jobsIDsMap := make(map[int64]string, len(s.mdlCheckTableInfo.jobsIDsMap))
		maps.Copy(jobsVerMap, s.mdlCheckTableInfo.jobsVerMap)
		maps.Copy(jobsIDsMap, s.mdlCheckTableInfo.jobsIDsMap)
		s.mdlCheckTableInfo.mu.Unlock()

		jobNeedToSync = true

		sm := s.info.GetSessionManager()
		if sm == nil {
			logutil.BgLogger().Info("session manager is nil")
		} else {
			sm.CheckOldRunningTxn(jobsVerMap, jobsIDsMap)
		}

		if len(jobsVerMap) == jobNeedToCheckCnt {
			jobNeedToSync = false
		}

		// Try to gc jobCache.
		if len(jobCache) > 1000 {
			jobCache = make(map[int64]int64, 1000)
		}

		for jobID, ver := range jobsVerMap {
			if cver, ok := jobCache[jobID]; ok && cver >= ver {
				// Already update, skip it.
				continue
			}
			logutil.BgLogger().Info("mdl gets lock, update self version to owner", zap.Int64("jobID", jobID), zap.Int64("version", ver))
			err := s.schemaVerSyncer.UpdateSelfVersion(context.Background(), jobID, ver)
			if err != nil {
				jobNeedToSync = true
				logutil.BgLogger().Warn("mdl gets lock, update self version to owner failed",
					zap.Int64("jobID", jobID), zap.Int64("version", ver), zap.Error(err))
			} else {
				jobCache[jobID] = ver
			}
		}
	}
}

// SyncLoop is the main loop for syncing the info schema.
func (s *Syncer) SyncLoop(ctx context.Context) {
	defer util.Recover(metrics.LabelDomain, "SyncLoop", nil, true)
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	ticker := time.NewTicker(s.schemaLease / 2)
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("info schema sync loop exited.")
	}()
	syncer := s.schemaVerSyncer

	for {
		select {
		case <-ticker.C:
			failpoint.Inject("disableOnTickReload", func() {
				failpoint.Continue()
			})
			err := s.Reload()
			if err != nil {
				logutil.BgLogger().Error("reload schema in loop failed", zap.Error(err))
			}
			s.deferFn.check()
		case _, ok := <-syncer.GlobalVersionCh():
			err := s.Reload()
			if err != nil {
				logutil.BgLogger().Error("reload schema in loop failed", zap.Error(err))
			}
			if !ok {
				logutil.BgLogger().Warn("reload schema in loop, schema syncer need rewatch")
				// Make sure the rewatch doesn't affect load schema, so we watch the global schema version asynchronously.
				syncer.WatchGlobalSchemaVer(context.Background())
			}
		case <-syncer.Done():
			// The schema syncer stops, we need stop the schema validator to synchronize the schema version.
			logutil.BgLogger().Info("reload schema in loop, schema syncer need restart")
			// The etcd is responsible for schema synchronization, we should ensure there is at most two different schema version
			// in the TiDB cluster, to make the data/schema be consistent. If we lost connection/session to etcd, the cluster
			// will treats this TiDB as a down instance, and etcd will remove the key of `/tidb/ddl/all_schema_versions/tidb-id`.
			// Say the schema version now is 1, the owner is changing the schema version to 2, it will not wait for this down TiDB syncing the schema,
			// then continue to change the TiDB schema to version 3. Unfortunately, this down TiDB schema version will still be version 1.
			// And version 1 is not consistent to version 3. So we need to stop the schema validator to prohibit the DML executing.
			s.SchemaValidator.Stop()
			err := s.mustRestartSyncer(ctx)
			if err != nil {
				logutil.BgLogger().Error("reload schema in loop, schema syncer restart failed", zap.Error(err))
				break
			}
			// The schema maybe changed, must reload schema then the schema validator can restart.
			exitLoop := s.mustReload(ctx)
			// domain is closed.
			if exitLoop {
				logutil.BgLogger().Error("domain is closed, exit info schema sync loop")
				return
			}
			s.SchemaValidator.Restart(s.InfoSchema().SchemaMetaVersion())
			logutil.BgLogger().Info("schema syncer restarted")
		case <-ctx.Done():
			return
		}
		s.refreshMDLCheckTableInfo(ctx)
		select {
		case s.mdlCheckCh <- struct{}{}:
		default:
		}
	}
}

// mustRestartSyncer tries to restart the SchemaSyncer.
// It returns until it's successful or the domain is stopped.
func (s *Syncer) mustRestartSyncer(ctx context.Context) error {
	syncer := s.schemaVerSyncer

	for {
		err := syncer.Restart(ctx)
		if err == nil {
			return nil
		}
		// If the domain has stopped, we return an error immediately.
		if ctx.Err() != nil {
			return err
		}
		logutil.BgLogger().Error("restart the schema syncer failed", zap.Error(err))
		time.Sleep(time.Second)
	}
}

// mustReload tries to Reload the schema, it returns until it's successful or the domain is closed.
// it returns false when it is successful, returns true when the domain is closed.
func (s *Syncer) mustReload(ctx context.Context) (exitLoop bool) {
	for {
		err := s.Reload()
		if err == nil {
			logutil.BgLogger().Info("mustReload succeed")
			return false
		}

		// If the domain is closed, we returns immediately.
		logutil.BgLogger().Info("reload the schema failed", zap.Error(err))
		if ctx.Err() != nil {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// Reload reloads InfoSchema.
// It's public in order to do the test.
func (s *Syncer) Reload() error {
	failpoint.Inject("ErrorMockReloadFailed", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("mock reload failed"))
		}
	})

	// Lock here for only once at the same time.
	s.m.Lock()
	defer s.m.Unlock()

	startTime := time.Now()
	ver, err := s.store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return err
	}

	version := ver.Ver
	is, hitCache, oldSchemaVersion, changes, err := s.LoadWithTS(version, false)
	if err != nil {
		if version = getFlashbackStartTSFromErrorMsg(err); version != 0 {
			// use the latest available version to create domain
			version--
			is, hitCache, oldSchemaVersion, changes, err = s.LoadWithTS(version, false)
		}
	}
	if err != nil {
		metrics.LoadSchemaCounter.WithLabelValues("failed").Inc()
		return err
	}
	metrics.LoadSchemaCounter.WithLabelValues("succ").Inc()

	// only update if it is not from cache
	if !hitCache {
		// loaded newer schema
		if oldSchemaVersion < is.SchemaMetaVersion() {
			// Update self schema version to etcd.
			err = s.schemaVerSyncer.UpdateSelfVersion(context.Background(), 0, is.SchemaMetaVersion())
			if err != nil {
				logutil.BgLogger().Info("update self version failed",
					zap.Int64("oldSchemaVersion", oldSchemaVersion),
					zap.Int64("neededSchemaVersion", is.SchemaMetaVersion()), zap.Error(err))
			}
		}

		// it is full load
		if changes == nil {
			logutil.BgLogger().Info("full load and reset schema validator")
			s.SchemaValidator.Reset()
		}
	}

	lease := s.schemaLease
	sub := time.Since(startTime)
	// Reload interval is lease / 2, if load schema time elapses more than this interval,
	// some query maybe responded by ErrInfoSchemaExpired error.
	if sub > (lease/2) && lease > 0 {
		// If it is a full load and there are a lot of tables, this is likely to happen.
		logutil.BgLogger().Warn("loading schema takes a long time", zap.Duration("take time", sub))

		// We can optimize the case by updating the TS to a new value, as long as the schema version is the same.
		// For example, lease is 45s, and the load process takes 2min, after the load process finish, this
		// loaded infoschema because stale immediately.
		// But if we re-check the schema version again and verify that it's still the newest, it is safe to use it.
		var latestSchemaVer int64
		var currentTS uint64
		ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnMeta)
		err := kv.RunInNewTxn(ctx, s.store, false, func(_ context.Context, txn kv.Transaction) error {
			var err error
			m := meta.NewReader(txn)
			latestSchemaVer, err = m.GetSchemaVersion()
			if err != nil {
				return errors.Trace(err)
			}
			currentTS = txn.StartTS()
			return nil
		})
		if err == nil && latestSchemaVer == is.SchemaMetaVersion() {
			version = currentTS
			logutil.BgLogger().Info("use this schema and update ts",
				zap.Int64("schema ver", latestSchemaVer),
				zap.Uint64("reload ts", currentTS))
		}
	}
	// lease renew, so it must be executed despite it is cache or not
	s.SchemaValidator.Update(version, oldSchemaVersion, is.SchemaMetaVersion(), changes)
	s.postReload(oldSchemaVersion, is.SchemaMetaVersion(), changes)

	return nil
}

func (s *Syncer) postReload(oldVer, currVer int64, change *transaction.RelatedSchemaChange) {
	if oldVer == currVer || change == nil {
		return
	}
	for idx, ac := range change.ActionTypes {
		if ac == uint64(model.ActionUnlockTable) {
			s.store.GetMemCache().Delete(change.PhyTblIDS[idx])
		}
		if ac == uint64(model.ActionFlashbackCluster) {
			if s.info != nil && s.info.GetSessionManager() != nil {
				s.info.GetSessionManager().KillNonFlashbackClusterConn()
			}
		}
	}
}

// LoadWithTS loads info schema at startTS.
// It returns:
// 1. the needed info schema
// 2. cache hit indicator
// 3. currentSchemaVersion(before loading)
// 4. the changed table IDs if it is not full load
// 5. an error if any
func (s *Syncer) LoadWithTS(startTS uint64, isSnapshot bool) (infoschema.InfoSchema, bool, int64, *transaction.RelatedSchemaChange, error) {
	beginTime := time.Now()
	defer func() {
		infoschema_metrics.LoadSchemaDurationTotal.Observe(time.Since(beginTime).Seconds())
	}()
	snapshot := s.store.GetSnapshot(kv.NewVersion(startTS))
	// Using the KV timeout read feature to address the issue of potential DDL lease expiration when
	// the meta region leader is slow.
	snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms.
	m := meta.NewReader(snapshot)
	neededSchemaVersion, err := m.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		return nil, false, 0, nil, err
	}
	// fetch the commit timestamp of the schema diff
	schemaTs, err := s.getTimestampForSchemaVersionWithNonEmptyDiff(m, neededSchemaVersion, startTS)
	if err != nil {
		logutil.BgLogger().Warn("failed to get schema version", zap.Error(err), zap.Int64("version", neededSchemaVersion))
		schemaTs = 0
	}

	enableV2 := vardef.SchemaCacheSize.Load() > 0
	currentSchemaVersion := int64(0)
	oldInfoSchema := s.infoCache.GetLatest()
	if oldInfoSchema != nil {
		currentSchemaVersion = oldInfoSchema.SchemaMetaVersion()
	}
	useV2, isV1V2Switch := shouldUseV2(enableV2, oldInfoSchema, isSnapshot)
	if is := s.infoCache.GetByVersion(neededSchemaVersion); is != nil {
		isV2, raw := infoschema.IsV2(is)
		if isV2 {
			// Copy the infoschema V2 instance and update its ts.
			// For example, the DDL run 30 minutes ago, GC happened 10 minutes ago. If we use
			// that infoschema it would get error "GC life time is shorter than transaction
			// duration" when visiting TiKV.
			// So we keep updating the ts of the infoschema v2.
			is = raw.CloneAndUpdateTS(startTS)
		}

		// try to insert here as well to correct the schemaTs if previous is wrong
		// the insert method check if schemaTs is zero
		s.infoCache.Insert(is, schemaTs)

		if !isV1V2Switch {
			return is, true, 0, nil, nil
		}
	}

	// TODO: tryLoadSchemaDiffs has potential risks of failure. And it becomes worse in history reading cases.
	// It is only kept because there is no alternative diff/partial loading solution.
	// And it is only used to diff upgrading the current latest infoschema, if:
	// 1. Not first time bootstrap loading, which needs a full load.
	// 2. It is newer than the current one, so it will be "the current one" after this function call.
	// 3. There are less 100 diffs.
	// 4. No regenerated schema diff.
	startTime := time.Now()
	if !isV1V2Switch && currentSchemaVersion != 0 && neededSchemaVersion > currentSchemaVersion && neededSchemaVersion-currentSchemaVersion < LoadSchemaDiffVersionGapThreshold {
		is, relatedChanges, diffTypes, err := s.tryLoadSchemaDiffs(useV2, m, currentSchemaVersion, neededSchemaVersion, startTS)
		if err == nil {
			infoschema_metrics.LoadSchemaDurationLoadDiff.Observe(time.Since(startTime).Seconds())
			isV2, _ := infoschema.IsV2(is)
			s.infoCache.Insert(is, schemaTs)
			logutil.BgLogger().Info("diff load InfoSchema success",
				zap.Bool("isV2", isV2),
				zap.Int64("currentSchemaVersion", currentSchemaVersion),
				zap.Int64("neededSchemaVersion", neededSchemaVersion),
				zap.Duration("elapsed time", time.Since(startTime)),
				zap.Int64("gotSchemaVersion", is.SchemaMetaVersion()),
				zap.Int64s("phyTblIDs", relatedChanges.PhyTblIDS),
				zap.Uint64s("actionTypes", relatedChanges.ActionTypes),
				zap.Strings("diffTypes", diffTypes))
			return is, false, currentSchemaVersion, relatedChanges, nil
		}
		// We can fall back to full load, don't need to return the error.
		logutil.BgLogger().Error("failed to load schema diff", zap.Error(err))
	}

	// add failpoint to simulate long-running schema loading scenario
	failpoint.Inject("mock-load-schema-long-time", func(val failpoint.Value) {
		if val.(bool) {
			// not ideal to use sleep, but not sure if there is a better way
			logutil.BgLogger().Error("sleep before doing a full load")
			time.Sleep(15 * time.Second)
		}
	})

	// full load.
	schemas, err := s.FetchAllSchemasWithTables(m)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}

	policies, err := s.fetchPolicies(m)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}

	resourceGroups, err := s.fetchResourceGroups(m)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}
	infoschema_metrics.LoadSchemaDurationLoadAll.Observe(time.Since(startTime).Seconds())

	data := s.infoCache.Data
	if isSnapshot {
		// Use a NewData() to avoid adding the snapshot schema to the infoschema history.
		// Why? imagine that the current schema version is [103 104 105 ...]
		// Then a snapshot read require infoschem version 53, and it's added
		// Now the history becomes [53,  ... 103, 104, 105 ...]
		// Then if a query ask for version 74, we'll mistakenly use 53!
		// Not adding snapshot schema to history can avoid such cases.
		data = infoschema.NewData()
	}
	builder := infoschema.NewBuilder(s, s.sysExecutorFactory, data, useV2)
	err = builder.InitWithDBInfos(schemas, policies, resourceGroups, neededSchemaVersion)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}
	is := builder.Build(startTS)
	isV2, _ := infoschema.IsV2(is)
	logutil.BgLogger().Info("full load InfoSchema success",
		zap.Bool("isV2", isV2),
		zap.Int64("currentSchemaVersion", currentSchemaVersion),
		zap.Int64("neededSchemaVersion", neededSchemaVersion),
		zap.Duration("elapsed time", time.Since(startTime)))

	if isV1V2Switch && schemaTs > 0 {
		// Reset the whole info cache to avoid co-existing of both v1 and v2, causing the memory usage doubled.
		fn := s.infoCache.Upsert(is, schemaTs)
		s.deferFn.add(fn, time.Now().Add(10*time.Minute))
		logutil.BgLogger().Info("infoschema v1/v2 switch")
	} else {
		s.infoCache.Insert(is, schemaTs)
	}
	return is, false, currentSchemaVersion, nil, nil
}

// tryLoadSchemaDiffs tries to only load latest schema changes.
// Return true if the schema is loaded successfully.
// Return false if the schema can not be loaded by schema diff, then we need to do full load.
// The second returned value is the delta updated table and partition IDs.
func (s *Syncer) tryLoadSchemaDiffs(useV2 bool, m meta.Reader, usedVersion, newVersion int64, startTS uint64) (infoschema.InfoSchema, *transaction.RelatedSchemaChange, []string, error) {
	var diffs []*model.SchemaDiff
	for usedVersion < newVersion {
		usedVersion++
		diff, err := m.GetSchemaDiff(usedVersion)
		if err != nil {
			return nil, nil, nil, err
		}
		if diff == nil {
			// Empty diff means the txn of generating schema version is committed, but the txn of `runDDLJob` is not or fail.
			// It is safe to skip the empty diff because the infoschema is new enough and consistent.
			logutil.BgLogger().Info("diff load InfoSchema get empty schema diff", zap.Int64("version", usedVersion))
			s.infoCache.InsertEmptySchemaVersion(usedVersion)
			continue
		}
		diffs = append(diffs, diff)
	}

	failpoint.Inject("MockTryLoadDiffError", func(val failpoint.Value) {
		switch val.(string) {
		case "exchangepartition":
			if diffs[0].Type == model.ActionExchangeTablePartition {
				failpoint.Return(nil, nil, nil, errors.New("mock error"))
			}
		case "renametable":
			if diffs[0].Type == model.ActionRenameTable {
				failpoint.Return(nil, nil, nil, errors.New("mock error"))
			}
		case "dropdatabase":
			if diffs[0].Type == model.ActionDropSchema {
				failpoint.Return(nil, nil, nil, errors.New("mock error"))
			}
		}
	})

	builder := infoschema.NewBuilder(s, s.sysExecutorFactory, s.infoCache.Data, useV2)
	err := builder.InitWithOldInfoSchema(s.infoCache.GetLatest())
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	builder.WithStore(s.store).SetDeltaUpdateBundles()
	phyTblIDs := make([]int64, 0, len(diffs))
	actions := make([]uint64, 0, len(diffs))
	diffTypes := make([]string, 0, len(diffs))
	for _, diff := range diffs {
		if diff.RegenerateSchemaMap {
			return nil, nil, nil, errors.Errorf("Meets a schema diff with RegenerateSchemaMap flag")
		}
		ids, err := builder.ApplyDiff(m, diff)
		if err != nil {
			return nil, nil, nil, err
		}
		if canSkipSchemaCheckerDDL(diff.Type) {
			continue
		}
		diffTypes = append(diffTypes, diff.Type.String())
		phyTblIDs = append(phyTblIDs, ids...)
		for range ids {
			actions = append(actions, uint64(diff.Type))
		}
	}

	is := builder.Build(startTS)
	relatedChange := transaction.RelatedSchemaChange{}
	relatedChange.PhyTblIDS = phyTblIDs
	relatedChange.ActionTypes = actions
	return is, &relatedChange, diffTypes, nil
}

// Returns the timestamp of a schema version, which is the commit timestamp of the schema diff
func (s *Syncer) getTimestampForSchemaVersionWithNonEmptyDiff(m meta.Reader, version int64, startTS uint64) (uint64, error) {
	tikvStore, ok := s.store.(helper.Storage)
	if ok {
		newHelper := helper.NewHelper(tikvStore)
		mvccResp, err := newHelper.GetMvccByEncodedKeyWithTS(m.EncodeSchemaDiffKey(version), startTS)
		if err != nil {
			return 0, err
		}
		if mvccResp == nil || mvccResp.Info == nil || len(mvccResp.Info.Writes) == 0 {
			return 0, errors.Errorf("There is no Write MVCC info for the schema version")
		}
		return mvccResp.Info.Writes[0].CommitTs, nil
	}
	return 0, errors.Errorf("cannot get store from domain")
}

// FetchAllSchemasWithTables fetches all schemas with their tables.
func (s *Syncer) FetchAllSchemasWithTables(m meta.Reader) ([]*model.DBInfo, error) {
	allSchemas, err := m.ListDatabases()
	if err != nil {
		return nil, err
	}
	if len(allSchemas) == 0 {
		return nil, nil
	}

	splittedSchemas := s.splitForConcurrentFetch(allSchemas)
	concurrency := min(len(splittedSchemas), 128)

	eg, ectx := util.NewErrorGroupWithRecoverWithCtx(context.Background())
	eg.SetLimit(concurrency)
	for _, schemas := range splittedSchemas {
		ss := schemas
		eg.Go(func() error {
			return s.fetchSchemasWithTables(ectx, ss, m)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return allSchemas, nil
}

func (*Syncer) fetchPolicies(m meta.Reader) ([]*model.PolicyInfo, error) {
	allPolicies, err := m.ListPolicies()
	if err != nil {
		return nil, err
	}
	return allPolicies, nil
}

func (*Syncer) fetchResourceGroups(m meta.Reader) ([]*model.ResourceGroupInfo, error) {
	allResourceGroups, err := m.ListResourceGroups()
	if err != nil {
		return nil, err
	}
	return allResourceGroups, nil
}

func (*Syncer) fetchSchemasWithTables(ctx context.Context, schemas []*model.DBInfo, m meta.Reader) error {
	failpoint.Inject("failed-fetch-schemas-with-tables", func() {
		failpoint.Return(errors.New("failpoint: failed to fetch schemas with tables"))
	})

	for _, di := range schemas {
		// if the ctx has been canceled, stop fetching schemas.
		if err := ctx.Err(); err != nil {
			return err
		}
		var tables []*model.TableInfo
		var err error
		if vardef.SchemaCacheSize.Load() > 0 && !infoschema.IsSpecialDB(di.Name.L) {
			name2ID, specialTableInfos, err := m.GetAllNameToIDAndTheMustLoadedTableInfo(di.ID)
			if err != nil {
				return err
			}
			di.TableName2ID = name2ID
			tables = specialTableInfos
			if domainutil.RepairInfo.InRepairMode() && len(domainutil.RepairInfo.GetRepairTableList()) > 0 {
				mustLoadReapirTableIDs := domainutil.RepairInfo.GetMustLoadRepairTableListByDB(di.Name.L, name2ID)
				for _, id := range mustLoadReapirTableIDs {
					tblInfo, err := m.GetTable(di.ID, id)
					if err != nil {
						return err
					}
					tables = append(tables, tblInfo)
				}
			}
		} else {
			tables, err = m.ListTables(ctx, di.ID)
			if err != nil {
				return err
			}
		}
		// If TreatOldVersionUTF8AsUTF8MB4 was enable, need to convert the old version schema UTF8 charset to UTF8MB4.
		if config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 {
			for _, tbInfo := range tables {
				infoschema.ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbInfo)
			}
		}
		diTables := make([]*model.TableInfo, 0, len(tables))
		for _, tbl := range tables {
			infoschema.ConvertCharsetCollateToLowerCaseIfNeed(tbl)
			// Check whether the table is in repair mode.
			if domainutil.RepairInfo.InRepairMode() && domainutil.RepairInfo.CheckAndFetchRepairedTable(di, tbl) {
				if tbl.State != model.StatePublic {
					// Do not load it because we are reparing the table and the table info could be `bad`
					// before repair is done.
					continue
				}
				// If the state is public, it means that the DDL job is done, but the table
				// haven't been deleted from the repair table list.
				// Since the repairment is done and table is visible, we should load it.
			}
			diTables = append(diTables, tbl)
		}
		di.Deprecated.Tables = diTables
	}
	return nil
}

// fetchSchemaConcurrency controls the goroutines to load schemas, but more goroutines
// increase the memory usage when calling json.Unmarshal(), which would cause OOM,
// so we decrease the concurrency.
const fetchSchemaConcurrency = 1

func (*Syncer) splitForConcurrentFetch(schemas []*model.DBInfo) [][]*model.DBInfo {
	groupCnt := fetchSchemaConcurrency
	schemaCnt := len(schemas)
	if vardef.SchemaCacheSize.Load() > 0 && schemaCnt > 1000 {
		// TODO: Temporary solution to speed up when too many databases, will refactor it later.
		groupCnt = 8
	}

	splitted := make([][]*model.DBInfo, 0, groupCnt)
	groupSizes := mathutil.Divide2Batches(schemaCnt, groupCnt)

	start := 0
	for _, groupSize := range groupSizes {
		splitted = append(splitted, schemas[start:start+groupSize])
		start += groupSize
	}

	return splitted
}

// Store gets KV store from domain.
func (s *Syncer) Store() kv.Storage {
	return s.store
}

// AutoIDClient returns the autoid client.
func (s *Syncer) AutoIDClient() *autoid.ClientDiscover {
	return s.autoidClient
}

// InfoSchema gets the latest information schema from domain.
func (s *Syncer) InfoSchema() infoschema.InfoSchema {
	return s.infoCache.GetLatest()
}

// GetInfoSyncer returns the InfoSyncer.
func (s *Syncer) GetInfoSyncer() *infosync.InfoSyncer {
	return s.info
}

// ChangeSchemaCacheSize changes the schema cache size.
func (s *Syncer) ChangeSchemaCacheSize(ctx context.Context, size uint64) error {
	err := kv.RunInNewTxn(kv.WithInternalSourceType(ctx, kv.InternalTxnDDL), s.store, true, func(_ context.Context, txn kv.Transaction) error {
		t := meta.NewMutator(txn)
		return t.SetSchemaCacheSize(size)
	})
	if err != nil {
		return err
	}
	if size > 0 {
		// Note: change the value to 0 is changing from infoschema v2 to v1.
		// What we do is change the implementation rather than set the cache capacity.
		// The change will not take effect until a schema reload happen.
		s.infoCache.Data.SetCacheCapacity(size)
	}
	return nil
}

// shouldUseV2 decides whether to use infoschema v2.
// When loading snapshot, infoschema should keep the same as before to avoid v1/v2 switch.
// Otherwise, it is decided by enabledV2.
func shouldUseV2(enableV2 bool, old infoschema.InfoSchema, isSnapshot bool) (useV2 bool, isV1V2Switch bool) {
	// case 1: no information about old
	if old == nil {
		return enableV2, false
	}
	// case 2: snapshot load should keep the same as old
	oldIsV2, _ := infoschema.IsV2(old)
	if isSnapshot {
		return oldIsV2, false
	}
	// case 3: the most general case
	return enableV2, oldIsV2 != enableV2
}

func canSkipSchemaCheckerDDL(tp model.ActionType) bool {
	switch tp {
	case model.ActionUpdateTiFlashReplicaStatus, model.ActionSetTiFlashReplica:
		return true
	}
	return false
}

func getFlashbackStartTSFromErrorMsg(err error) uint64 {
	slices := strings.Split(err.Error(), "is in flashback progress, FlashbackStartTS is ")
	if len(slices) != 2 {
		return 0
	}
	version, err := strconv.ParseUint(slices[1], 10, 0)
	if err != nil {
		return 0
	}
	return version
}
