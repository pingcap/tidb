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
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschema_metrics "github.com/pingcap/tidb/pkg/infoschema/metrics"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/domainutil"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	"go.uber.org/zap"
)

var (
	// LoadSchemaDiffVersionGapThreshold is the threshold for version gap to reload domain by loading schema diffs
	LoadSchemaDiffVersionGapThreshold int64 = 10000
)

// Loader is the main structure for syncing the info schema.
type Loader struct {
	store     kv.Storage
	infoCache *infoschema.InfoCache
	// deferFn is used to release infoschema object lazily during v1 and v2 switch
	deferFn *deferFn

	// below fields are set when running background routines
	// autoidClient is used when there are tables with AUTO_ID_CACHE=1, it is the client to the autoid service.
	autoidClient *autoid.ClientDiscover
	// CachedTable need internal session to access some system tables, such as
	// mysql.table_cache_meta
	sysExecutorFactory func() (pools.Resource, error)
}

// NewLoader creates a new Loader instance.
func NewLoader(store kv.Storage, infoCache *infoschema.InfoCache) *Loader {
	return &Loader{
		store:     store,
		infoCache: infoCache,
		deferFn:   &deferFn{},
	}
}

// initFields initializes some fields of the Loader.
// below fields are required for accessing user tables, if you only load system tables,
// you can skip this step.
func (l *Loader) initFields(
	autoidClient *autoid.ClientDiscover,
	sysExecutorFactory func() (pools.Resource, error),
) {
	l.autoidClient = autoidClient
	l.sysExecutorFactory = sysExecutorFactory
}

// LoadWithTS loads info schema at startTS.
// It returns:
// 1. the needed info schema
// 2. cache hit indicator
// 3. currentSchemaVersion(before loading)
// 4. the changed table IDs if it is not full load
// 5. an error if any
func (l *Loader) LoadWithTS(startTS uint64, isSnapshot bool) (infoschema.InfoSchema, bool, int64, *transaction.RelatedSchemaChange, error) {
	beginTime := time.Now()
	defer func() {
		infoschema_metrics.LoadSchemaDurationTotal.Observe(time.Since(beginTime).Seconds())
	}()
	snapshot := l.store.GetSnapshot(kv.NewVersion(startTS))
	// Using the KV timeout read feature to address the issue of potential DDL lease expiration when
	// the meta region leader is slow.
	snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms.
	m := meta.NewReader(snapshot)
	neededSchemaVersion, err := m.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		return nil, false, 0, nil, err
	}
	// fetch the commit timestamp of the schema diff
	schemaTs, err := l.getTimestampForSchemaVersionWithNonEmptyDiff(m, neededSchemaVersion, startTS)
	if err != nil {
		logutil.BgLogger().Warn("failed to get schema version", zap.Error(err), zap.Int64("version", neededSchemaVersion))
		schemaTs = 0
	}

	enableV2 := vardef.SchemaCacheSize.Load() > 0
	currentSchemaVersion := int64(0)
	oldInfoSchema := l.infoCache.GetLatest()
	if oldInfoSchema != nil {
		currentSchemaVersion = oldInfoSchema.SchemaMetaVersion()
	}
	useV2, isV1V2Switch := shouldUseV2(enableV2, oldInfoSchema, isSnapshot)
	if is := l.infoCache.GetByVersion(neededSchemaVersion); is != nil {
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
		l.infoCache.Insert(is, schemaTs)

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
		is, relatedChanges, diffTypes, err := l.tryLoadSchemaDiffs(useV2, m, currentSchemaVersion, neededSchemaVersion, startTS)
		if err == nil {
			infoschema_metrics.LoadSchemaDurationLoadDiff.Observe(time.Since(startTime).Seconds())
			isV2, _ := infoschema.IsV2(is)
			l.infoCache.Insert(is, schemaTs)
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
	schemas, err := l.fetchAllSchemasWithTables(m)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}

	policies, err := l.fetchPolicies(m)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}

	resourceGroups, err := l.fetchResourceGroups(m)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}
	infoschema_metrics.LoadSchemaDurationLoadAll.Observe(time.Since(startTime).Seconds())

	data := l.infoCache.Data
	if isSnapshot {
		// Use a NewData() to avoid adding the snapshot schema to the infoschema history.
		// Why? imagine that the current schema version is [103 104 105 ...]
		// Then a snapshot read require infoschem version 53, and it's added
		// Now the history becomes [53,  ... 103, 104, 105 ...]
		// Then if a query ask for version 74, we'll mistakenly use 53!
		// Not adding snapshot schema to history can avoid such cases.
		data = infoschema.NewData()
	}
	builder := infoschema.NewBuilder(l, l.sysExecutorFactory, data, useV2)
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
		fn := l.infoCache.Upsert(is, schemaTs)
		l.deferFn.add(fn, time.Now().Add(10*time.Minute))
		logutil.BgLogger().Info("infoschema v1/v2 switch")
	} else {
		l.infoCache.Insert(is, schemaTs)
	}
	return is, false, currentSchemaVersion, nil, nil
}

// tryLoadSchemaDiffs tries to only load latest schema changes.
// Return true if the schema is loaded successfully.
// Return false if the schema can not be loaded by schema diff, then we need to do full load.
// The second returned value is the delta updated table and partition IDs.
func (l *Loader) tryLoadSchemaDiffs(useV2 bool, m meta.Reader, usedVersion, newVersion int64, startTS uint64) (infoschema.InfoSchema, *transaction.RelatedSchemaChange, []string, error) {
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
			l.infoCache.InsertEmptySchemaVersion(usedVersion)
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

	builder := infoschema.NewBuilder(l, l.sysExecutorFactory, l.infoCache.Data, useV2)
	err := builder.InitWithOldInfoSchema(l.infoCache.GetLatest())
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	builder.WithStore(l.store).SetDeltaUpdateBundles()
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
func (l *Loader) getTimestampForSchemaVersionWithNonEmptyDiff(m meta.Reader, version int64, startTS uint64) (uint64, error) {
	tikvStore, ok := l.store.(helper.Storage)
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

// fetchAllSchemasWithTables fetches all schemas with their tables.
func (l *Loader) fetchAllSchemasWithTables(m meta.Reader) ([]*model.DBInfo, error) {
	allSchemas, err := m.ListDatabases()
	if err != nil {
		return nil, err
	}
	if len(allSchemas) == 0 {
		return nil, nil
	}

	splittedSchemas := l.splitForConcurrentFetch(allSchemas)
	concurrency := min(len(splittedSchemas), 128)

	eg, ectx := util.NewErrorGroupWithRecoverWithCtx(context.Background())
	eg.SetLimit(concurrency)
	for _, schemas := range splittedSchemas {
		ss := schemas
		eg.Go(func() error {
			return l.fetchSchemasWithTables(ectx, ss, m)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return allSchemas, nil
}

func (*Loader) fetchPolicies(m meta.Reader) ([]*model.PolicyInfo, error) {
	allPolicies, err := m.ListPolicies()
	if err != nil {
		return nil, err
	}
	return allPolicies, nil
}

func (*Loader) fetchResourceGroups(m meta.Reader) ([]*model.ResourceGroupInfo, error) {
	allResourceGroups, err := m.ListResourceGroups()
	if err != nil {
		return nil, err
	}
	return allResourceGroups, nil
}

func (*Loader) fetchSchemasWithTables(ctx context.Context, schemas []*model.DBInfo, m meta.Reader) error {
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
				mustLoadRepairTableIDs := domainutil.RepairInfo.GetMustLoadRepairTableListByDB(di.Name.L, name2ID)
				for _, id := range mustLoadRepairTableIDs {
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

func (*Loader) splitForConcurrentFetch(schemas []*model.DBInfo) [][]*model.DBInfo {
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
func (l *Loader) Store() kv.Storage {
	return l.store
}

// AutoIDClient returns the autoid client.
func (l *Loader) AutoIDClient() *autoid.ClientDiscover {
	return l.autoidClient
}

// changeSchemaCacheSize changes the schema cache size.
func (l *Loader) changeSchemaCacheSize(ctx context.Context, size uint64) error {
	err := kv.RunInNewTxn(kv.WithInternalSourceType(ctx, kv.InternalTxnDDL), l.store, true, func(_ context.Context, txn kv.Transaction) error {
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
		l.infoCache.Data.SetCacheCapacity(size)
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
