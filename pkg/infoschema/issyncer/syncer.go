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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/infoschema/issyncer/mdldef"
	"github.com/pingcap/tidb/pkg/infoschema/validatorapi"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/session/sessmgr"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
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
	logger            *zap.Logger
	crossKS           bool

	// below fields are set when running background routines
	isCoordinatorGetter func() sessmgr.InfoSchemaCoordinator
	schemaVerSyncer     schemaver.Syncer
	minJobIDRefresher   *systable.MinJobIDRefresher
}

// New creates a new Syncer instance.
func New(
	store kv.Storage,
	infoCache *infoschema.InfoCache,
	schemaLease time.Duration,
	sysSessionPool util.DestroyableSessionPool,
	isValidator validatorapi.Validator,
	filter Filter,
) *Syncer {
	s := newSyncer(store, logutil.BgLogger(), schemaLease, sysSessionPool, isValidator)
	s.loader = newLoader(store, infoCache, &s.deferFn, filter)
	return s
}

// NewCrossKSSyncer creates a new Syncer instance for cross keyspace.
func NewCrossKSSyncer(
	store kv.Storage,
	infoCache *infoschema.InfoCache,
	schemaLease time.Duration,
	sysSessionPool util.DestroyableSessionPool,
	isValidator validatorapi.Validator,
	targetKS string,
) *Syncer {
	logger := logutil.BgLogger().With(zap.String("targetKS", targetKS))
	s := newSyncer(store, logger, schemaLease, sysSessionPool, isValidator)
	s.loader = NewLoaderForCrossKS(store, infoCache)
	s.crossKS = true
	return s
}

func newSyncer(
	store kv.Storage,
	logger *zap.Logger,
	schemaLease time.Duration,
	sysSessionPool util.DestroyableSessionPool,
	isValidator validatorapi.Validator,
) *Syncer {
	s := &Syncer{
		store:          store,
		schemaLease:    schemaLease,
		sysSessionPool: sysSessionPool,

		mdlCheckCh: make(chan struct{}),
		mdlCheckTableInfo: &mdlCheckTableInfo{
			mu:   sync.Mutex{},
			jobs: make(map[int64]*mdldef.JobMDL),
		},
		logger: logger,
	}
	s.schemaValidator = isValidator
	return s
}

// InitRequiredFields initializes some fields of the Syncer.
func (s *Syncer) InitRequiredFields(
	isCoordinatorGetter func() sessmgr.InfoSchemaCoordinator,
	schemaVerSyncer schemaver.Syncer,
	autoidClient *autoid.ClientDiscover,
	sysExecutorFactory func() (pools.Resource, error),
) {
	s.isCoordinatorGetter = isCoordinatorGetter
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
		s.logger.Warn("get system session failed", zap.Error(err))
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
	domainSchemaVer := s.InfoSchema().SchemaMetaVersion()
	// the job must stay inside tidb_ddl_job if we need to wait schema version for it.
	sql := fmt.Sprintf(`select job_id, version, table_ids from mysql.tidb_mdl_info
		where job_id >= %d and version <= %d`, s.minJobIDRefresher.GetCurrMinJobID(), domainSchemaVer)
	rows, err := sqlexec.ExecSQL(ctx, sctx.GetSQLExecutor(), sql)
	if err != nil {
		s.logger.Warn("get mdl info from tidb_mdl_info failed", zap.Error(err))
		return
	}
	s.mdlCheckTableInfo.mu.Lock()
	defer s.mdlCheckTableInfo.mu.Unlock()

	s.mdlCheckTableInfo.newestVer = domainSchemaVer
	s.mdlCheckTableInfo.jobs = make(map[int64]*mdldef.JobMDL, len(rows))
	for i := range rows {
		jobID := rows[i].GetInt64(0)
		tableIDs := util.Str2Int64Map(rows[i].GetString(2))
		if s.skipMDLCheck(tableIDs) {
			continue
		}
		s.mdlCheckTableInfo.jobs[jobID] = &mdldef.JobMDL{
			Ver:      rows[i].GetInt64(1),
			TableIDs: tableIDs,
		}
	}
}

func (s *Syncer) skipMDLCheck(tableIDs map[int64]struct{}) bool {
	if !s.crossKS {
		return false
	}

	// for cross keyspace syncer, we only care about the system tables, so we can
	// skip the MDL check for user tables.
	for id := range tableIDs {
		if metadef.IsReservedID(id) {
			return false
		}
	}
	return true
}

// MDLCheckLoop is a loop that checks the MDL locks periodically.
func (s *Syncer) MDLCheckLoop(ctx context.Context) {
	ticker := time.Tick(mdlCheckLookDuration)
	var lastCheckedVersion int64
	haveJobToCheck := false
	jobCache := make(map[int64]int64, 1000)

	for {
		// Wait for channels
		select {
		case <-s.mdlCheckCh:
		case <-ticker:
		case <-ctx.Done():
			return
		}

		if !vardef.IsMDLEnabled() {
			continue
		}

		s.mdlCheckTableInfo.mu.Lock()
		maxVer := s.mdlCheckTableInfo.newestVer
		if maxVer > lastCheckedVersion {
			lastCheckedVersion = maxVer
		} else if !haveJobToCheck {
			// Schema doesn't change, and no job to check in the last run.
			s.mdlCheckTableInfo.mu.Unlock()
			continue
		}

		checkingJobCnt := len(s.mdlCheckTableInfo.jobs)
		if checkingJobCnt == 0 {
			haveJobToCheck = false
			s.mdlCheckTableInfo.mu.Unlock()
			continue
		}

		jobs := maps.Clone(s.mdlCheckTableInfo.jobs)
		s.mdlCheckTableInfo.mu.Unlock()

		haveJobToCheck = true

		coordinator := s.isCoordinatorGetter()
		if coordinator == nil {
			s.logger.Info("session manager is nil")
		} else {
			coordinator.CheckOldRunningTxn(jobs)
		}

		// if there are sessions using older schema version to access tables
		// involved in a DDL job, CheckOldRunningTxn will remove it from 'jobs',
		// so the remaining items are the jobs that can proceed to the next step.
		if len(jobs) == checkingJobCnt {
			haveJobToCheck = false
		}

		// Try to gc jobCache.
		if len(jobCache) > 1000 {
			jobCache = make(map[int64]int64, 1000)
		}

		for jobID, jMDL := range jobs {
			ver := jMDL.Ver
			if cver, ok := jobCache[jobID]; ok && cver >= ver {
				// Already update, skip it.
				continue
			}
			s.logger.Info("mdl gets lock, update self version to owner",
				zap.Int64("jobID", jobID), zap.Int64("version", ver))
			err := s.schemaVerSyncer.UpdateSelfVersion(context.Background(), jobID, ver)
			if err != nil {
				haveJobToCheck = true
				s.logger.Warn("mdl gets lock, update self version to owner failed",
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
		s.logger.Info("info schema sync loop exited.")
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
				s.logger.Error("reload schema in loop failed", zap.Error(err))
			}
			s.deferFn.check()
		case _, ok := <-syncer.GlobalVersionCh():
			err := s.Reload()
			if err != nil {
				s.logger.Error("reload schema in loop failed", zap.Error(err))
			}
			if !ok {
				s.logger.Warn("reload schema in loop, schema syncer need rewatch")
				// Make sure the rewatch doesn't affect load schema, so we watch the global schema version asynchronously.
				syncer.WatchGlobalSchemaVer(context.Background())
			}
		case <-syncer.Done():
			// The schema syncer stops, we need stop the schema validator to synchronize the schema version.
			s.logger.Info("reload schema in loop, schema syncer need restart")
			// The etcd is responsible for schema synchronization, we should ensure there is at most two different schema version
			// in the TiDB cluster, to make the data/schema be consistent. If we lost connection/session to etcd, the cluster
			// will treats this TiDB as a down instance, and etcd will remove the key of `/tidb/ddl/all_schema_versions/tidb-id`.
			// Say the schema version now is 1, the owner is changing the schema version to 2, it will not wait for this down TiDB syncing the schema,
			// then continue to change the TiDB schema to version 3. Unfortunately, this down TiDB schema version will still be version 1.
			// And version 1 is not consistent to version 3. So we need to stop the schema validator to prohibit the DML executing.
			s.schemaValidator.Stop()
			err := s.mustRestartSyncer(ctx)
			if err != nil {
				s.logger.Error("reload schema in loop, schema syncer restart failed", zap.Error(err))
				break
			}
			// The schema maybe changed, must reload schema then the schema validator can restart.
			exitLoop := s.mustReload(ctx)
			// domain is closed.
			if exitLoop {
				s.logger.Error("domain is closed, exit info schema sync loop")
				return
			}
			s.schemaValidator.Restart(s.InfoSchema().SchemaMetaVersion())
			s.logger.Info("schema syncer restarted")
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
		s.logger.Error("restart the schema syncer failed", zap.Error(err))
		time.Sleep(time.Second)
	}
}

// mustReload tries to Reload the schema, it returns until it's successful or the domain is closed.
// it returns false when it is successful, returns true when the domain is closed.
func (s *Syncer) mustReload(ctx context.Context) (exitLoop bool) {
	for {
		err := s.Reload()
		if err == nil {
			s.logger.Info("mustReload succeed")
			return false
		}

		// If the domain is closed, we returns immediately.
		s.logger.Info("reload the schema failed", zap.Error(err))
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
				s.logger.Info("update self version failed",
					zap.Int64("oldSchemaVersion", oldSchemaVersion),
					zap.Int64("neededSchemaVersion", is.SchemaMetaVersion()), zap.Error(err))
			}
		}

		// it is full load
		if changes == nil {
			s.logger.Info("full load and reset schema validator")
			s.schemaValidator.Reset()
		}
	}

	lease := s.schemaLease
	sub := time.Since(startTime)
	// Reload interval is lease / 2, if load schema time elapses more than this interval,
	// some query maybe responded by ErrInfoSchemaExpired error.
	if sub > (lease/2) && lease > 0 {
		// If it is a full load and there are a lot of tables, this is likely to happen.
		s.logger.Warn("loading schema takes a long time", zap.Duration("take time", sub))

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
			s.logger.Info("use this schema and update ts",
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
			coordinator := s.isCoordinatorGetter()
			if coordinator != nil {
				coordinator.KillNonFlashbackClusterConn()
			}
		}
	}
}

// InfoSchema gets the latest information schema from domain.
func (s *Syncer) InfoSchema() infoschema.InfoSchema {
	return s.loader.infoCache.GetLatest()
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
