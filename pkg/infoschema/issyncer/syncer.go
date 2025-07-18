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
	"github.com/pingcap/tidb/pkg/ddl/schemaver"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/isvalidator"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/session/sessionapi"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.uber.org/zap"
)

var (
	mdlCheckLookDuration = 50 * time.Millisecond
)

func init() {
	if intest.InTest {
		// In test we can set duration lower to make test faster.
		mdlCheckLookDuration = 2 * time.Millisecond
	}
}

// Syncer is the main structure for syncing the info schema.
type Syncer struct {
	m           syncutil.Mutex
	store       kv.Storage
	schemaLease time.Duration
	// Note: If you no longer need the session, you must call Destroy to release it.
	// Otherwise, the session will be leaked. Because there is a strong reference from the domain to the session.
	// Deprecated: Use `advancedSysSessionPool` instead.
	sysSessionPool util.DestroyableSessionPool
	// deferFn is used to release infoschema object lazily during v1 and v2 switch
	deferFn           deferFn
	mdlCheckCh        chan struct{}
	mdlCheckTableInfo *mdlCheckTableInfo
	schemaValidator   validatorapi.Validator
	loader            *Loader

	// below fields are set when running background routines
	info              *infosync.InfoSyncer
	schemaVerSyncer   schemaver.Syncer
	minJobIDRefresher *systable.MinJobIDRefresher
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
		schemaLease:    schemaLease,
		sysSessionPool: sysSessionPool,

		mdlCheckCh: make(chan struct{}),
		mdlCheckTableInfo: &mdlCheckTableInfo{
			mu:         sync.Mutex{},
			jobsVerMap: make(map[int64]int64),
			jobsIDsMap: make(map[int64]string),
		},
	}
	do.schemaValidator = isvalidator.New(schemaLease)
	mode := LoadModeAuto
	do.loader = &Loader{
		mode:      mode,
		store:     store,
		infoCache: infoCache,
		deferFn:   &do.deferFn,
		logger:    logutil.BgLogger().With(zap.Stringer("mode", mode)),
	}

	return do
}

// InitRequiredFields initializes some fields of the Syncer.
func (s *Syncer) InitRequiredFields(
	info *infosync.InfoSyncer,
	schemaVerSyncer schemaver.Syncer,
	autoidClient *autoid.ClientDiscover,
	sysExecutorFactory func() (pools.Resource, error),
) {
	s.info = info
	s.schemaVerSyncer = schemaVerSyncer
	s.loader.initFields(autoidClient, sysExecutorFactory)
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
	sctx := se.(sessionapi.Context)
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
			s.schemaValidator.Stop()
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
			s.schemaValidator.Restart(s.InfoSchema().SchemaMetaVersion())
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

// LoadWithTS loads the InfoSchema with a specific timestamp.
func (s *Syncer) LoadWithTS(startTS uint64, isSnapshot bool) (infoschema.InfoSchema, bool, int64, *transaction.RelatedSchemaChange, error) {
	return s.loader.LoadWithTS(startTS, isSnapshot)
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
	is, hitCache, oldSchemaVersion, changes, err := s.loader.LoadWithTS(version, false)
	if err != nil {
		if version = getFlashbackStartTSFromErrorMsg(err); version != 0 {
			// use the latest available version to create domain
			version--
			is, hitCache, oldSchemaVersion, changes, err = s.loader.LoadWithTS(version, false)
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
			s.schemaValidator.Reset()
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
	s.schemaValidator.Update(version, oldSchemaVersion, is.SchemaMetaVersion(), changes)
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

// InfoSchema gets the latest information schema from domain.
func (s *Syncer) InfoSchema() infoschema.InfoSchema {
	return s.loader.infoCache.GetLatest()
}

// GetInfoSyncer returns the InfoSyncer.
func (s *Syncer) GetInfoSyncer() *infosync.InfoSyncer {
	return s.info
}

// GetSchemaValidator returns the schema validator.
func (s *Syncer) GetSchemaValidator() validatorapi.Validator {
	return s.schemaValidator
}

// FetchAllSchemasWithTables fetches all schemas with their tables.
func (s *Syncer) FetchAllSchemasWithTables(m meta.Reader) ([]*model.DBInfo, error) {
	schemaCacheSize := vardef.SchemaCacheSize.Load()
	return s.loader.fetchAllSchemasWithTables(m, schemaCacheSize)
}

// ChangeSchemaCacheSize changes the schema cache size.
func (s *Syncer) ChangeSchemaCacheSize(ctx context.Context, size uint64) error {
	return s.loader.changeSchemaCacheSize(ctx, size)
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
