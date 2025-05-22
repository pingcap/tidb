// Copyright 2015 PingCAP, Inc.
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

package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"math"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/docker/go-units"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/streamhelper/daemon"
	"github.com/pingcap/tidb/pkg/bindinfo"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/ddl/notifier"
	"github.com/pingcap/tidb/pkg/ddl/placement"
	"github.com/pingcap/tidb/pkg/ddl/schematracker"
	"github.com/pingcap/tidb/pkg/ddl/systable"
	ddlutil "github.com/pingcap/tidb/pkg/ddl/util"
	disthandle "github.com/pingcap/tidb/pkg/disttask/framework/handle"
	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/pingcap/tidb/pkg/disttask/framework/scheduler"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/disttask/framework/taskexecutor"
	"github.com/pingcap/tidb/pkg/domain/globalconfigsync"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/infoschema"
	infoschema_metrics "github.com/pingcap/tidb/pkg/infoschema/metrics"
	"github.com/pingcap/tidb/pkg/infoschema/perfschema"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/kv"
	lcom "github.com/pingcap/tidb/pkg/lightning/common"
	"github.com/pingcap/tidb/pkg/meta"
	"github.com/pingcap/tidb/pkg/meta/autoid"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/owner"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	metrics2 "github.com/pingcap/tidb/pkg/planner/core/metrics"
	"github.com/pingcap/tidb/pkg/privilege/privileges"
	"github.com/pingcap/tidb/pkg/resourcegroup/runaway"
	"github.com/pingcap/tidb/pkg/session/syssession"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/sessionstates"
	"github.com/pingcap/tidb/pkg/sessionctx/sysproctrack"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/statistics/handle"
	"github.com/pingcap/tidb/pkg/statistics/handle/autoanalyze"
	"github.com/pingcap/tidb/pkg/statistics/handle/initstats"
	statslogutil "github.com/pingcap/tidb/pkg/statistics/handle/logutil"
	handleutil "github.com/pingcap/tidb/pkg/statistics/handle/util"
	"github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/ttl/ttlworker"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/cgroup"
	"github.com/pingcap/tidb/pkg/util/cpu"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	disttaskutil "github.com/pingcap/tidb/pkg/util/disttask"
	"github.com/pingcap/tidb/pkg/util/domainutil"
	"github.com/pingcap/tidb/pkg/util/engine"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/pingcap/tidb/pkg/util/expensivequery"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/globalconn"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/memoryusagealarm"
	"github.com/pingcap/tidb/pkg/util/replayer"
	"github.com/pingcap/tidb/pkg/util/servermemorylimit"
	"github.com/pingcap/tidb/pkg/util/size"
	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/pingcap/tidb/pkg/util/syncutil"
	"github.com/pingcap/tidb/pkg/workloadlearning"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	pd "github.com/tikv/pd/client"
	pdhttp "github.com/tikv/pd/client/http"
	"github.com/tikv/pd/client/opt"
	rmclient "github.com/tikv/pd/client/resource_group/controller"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	atomicutil "go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	mdlCheckLookDuration = 50 * time.Millisecond

	// LoadSchemaDiffVersionGapThreshold is the threshold for version gap to reload domain by loading schema diffs
	LoadSchemaDiffVersionGapThreshold int64 = 10000

	// NewInstancePlanCache creates a new instance level plan cache, this function is designed to avoid cycle-import.
	NewInstancePlanCache func(softMemLimit, hardMemLimit int64) sessionctx.InstancePlanCache
)

const (
	indexUsageGCDuration = 30 * time.Minute
)

func init() {
	if intest.InTest {
		// In test we can set duration lower to make test faster.
		mdlCheckLookDuration = 2 * time.Millisecond
	}
}

// NewMockDomain is only used for test
func NewMockDomain() *Domain {
	do := &Domain{}
	do.infoCache = infoschema.NewCache(do, 1)
	do.infoCache.Insert(infoschema.MockInfoSchema(nil), 0)
	return do
}

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store           kv.Storage
	infoCache       *infoschema.InfoCache
	privHandle      *privileges.Handle
	bindHandle      atomic.Value
	statsHandle     atomic.Pointer[handle.Handle]
	statsLease      time.Duration
	ddl             ddl.DDL
	ddlExecutor     ddl.Executor
	ddlNotifier     *notifier.DDLNotifier
	info            *infosync.InfoSyncer
	globalCfgSyncer *globalconfigsync.GlobalConfigSyncer
	m               syncutil.Mutex
	SchemaValidator SchemaValidator
	schemaLease     time.Duration
	// advancedSysSessionPool is a more powerful session pool that returns a wrapped session which can detect
	// some misuse of the session to avoid potential bugs.
	// It is recommended to use this pool instead of `sysSessionPool`.
	advancedSysSessionPool *syssession.AdvancedSessionPool
	// Note: If you no longer need the session, you must call Destroy to release it.
	// Otherwise, the session will be leaked. Because there is a strong reference from the domain to the session.
	// Deprecated: Use `advancedSysSessionPool` instead.
	sysSessionPool util.DestroyableSessionPool
	exit           chan struct{}
	// `etcdClient` must be used when keyspace is not set, or when the logic to each etcd path needs to be separated by keyspace.
	etcdClient *clientv3.Client
	// autoidClient is used when there are tables with AUTO_ID_CACHE=1, it is the client to the autoid service.
	autoidClient *autoid.ClientDiscover
	// `unprefixedEtcdCli` will never set the etcd namespace prefix by keyspace.
	// It is only used in storeMinStartTS and RemoveMinStartTS now.
	// It must be used when the etcd path isn't needed to separate by keyspace.
	// See keyspace RFC: https://github.com/pingcap/tidb/pull/39685
	unprefixedEtcdCli       *clientv3.Client
	sysVarCache             sysVarCache // replaces GlobalVariableCache
	slowQuery               *topNSlowQueries
	expensiveQueryHandle    *expensivequery.Handle
	memoryUsageAlarmHandle  *memoryusagealarm.Handle
	serverMemoryLimitHandle *servermemorylimit.Handle
	// TODO: use Run for each process in future pr
	wg            *util.WaitGroupEnhancedWrapper
	statsUpdating atomicutil.Int32
	// this is the parent context of DDL, and also used by other loops such as closestReplicaReadCheckLoop.
	// there are other top level contexts in the domain, such as the ones used in
	// InitDistTaskLoop and loadStatsWorker, domain only stores the cancelFns of them.
	// TODO unify top level context.
	ctx       context.Context
	cancelFns struct {
		mu  sync.Mutex
		fns []context.CancelFunc
	}
	dumpFileGcChecker   *dumpFileGcChecker
	planReplayerHandle  *planReplayerHandle
	extractTaskHandle   *ExtractHandle
	expiredTimeStamp4PC struct {
		// let `expiredTimeStamp4PC` use its own lock to avoid any block across domain.Reload()
		// and compiler.Compile(), see issue https://github.com/pingcap/tidb/issues/45400
		sync.RWMutex
		expiredTimeStamp types.Time
	}

	brOwnerMgr               owner.Manager
	logBackupAdvancer        *daemon.OwnerDaemon
	historicalStatsWorker    *HistoricalStatsWorker
	ttlJobManager            atomic.Pointer[ttlworker.JobManager]
	runawayManager           *runaway.Manager
	resourceGroupsController *rmclient.ResourceGroupsController

	serverID             uint64
	serverIDSession      *concurrency.Session
	isLostConnectionToPD atomicutil.Int32 // !0: true, 0: false.
	connIDAllocator      globalconn.Allocator

	onClose            func()
	sysExecutorFactory func(*Domain) (pools.Resource, error)

	sysProcesses SysProcesses

	mdlCheckTableInfo *mdlCheckTableInfo

	mdlCheckCh        chan struct{}
	stopAutoAnalyze   atomicutil.Bool
	minJobIDRefresher *systable.MinJobIDRefresher

	instancePlanCache sessionctx.InstancePlanCache // the instance level plan cache

	statsOwner owner.Manager
	// deferFn is used to release infoschema object lazily during v1 and v2 switch
	deferFn
}

type deferFn struct {
	sync.Mutex
	data []deferFnRecord
}

type deferFnRecord struct {
	fn   func()
	fire time.Time
}

func (df *deferFn) add(fn func(), fire time.Time) {
	df.Lock()
	defer df.Unlock()
	df.data = append(df.data, deferFnRecord{fn: fn, fire: fire})
}

func (df *deferFn) check() {
	now := time.Now()
	df.Lock()
	defer df.Unlock()

	// iterate the slice, call the defer function and remove it.
	rm := 0
	for i := range df.data {
		record := &df.data[i]
		if now.After(record.fire) {
			record.fn()
			rm++
		} else {
			df.data[i-rm] = df.data[i]
		}
	}
	df.data = df.data[:len(df.data)-rm]
}

type mdlCheckTableInfo struct {
	mu         sync.Mutex
	newestVer  int64
	jobsVerMap map[int64]int64
	jobsIDsMap map[int64]string
}

// InfoCache export for test.
func (do *Domain) InfoCache() *infoschema.InfoCache {
	return do.infoCache
}

// EtcdClient export for test.
func (do *Domain) EtcdClient() *clientv3.Client {
	return do.etcdClient
}

// UnprefixedEtcdCli export for test.
func (do *Domain) UnprefixedEtcdCli() *clientv3.Client {
	return do.unprefixedEtcdCli
}

// loadInfoSchema loads infoschema at startTS.
// It returns:
// 1. the needed infoschema
// 2. cache hit indicator
// 3. currentSchemaVersion(before loading)
// 4. the changed table IDs if it is not full load
// 5. an error if any
func (do *Domain) loadInfoSchema(startTS uint64, isSnapshot bool) (infoschema.InfoSchema, bool, int64, *transaction.RelatedSchemaChange, error) {
	beginTime := time.Now()
	defer func() {
		infoschema_metrics.LoadSchemaDurationTotal.Observe(time.Since(beginTime).Seconds())
	}()
	snapshot := do.store.GetSnapshot(kv.NewVersion(startTS))
	// Using the KV timeout read feature to address the issue of potential DDL lease expiration when
	// the meta region leader is slow.
	snapshot.SetOption(kv.TiKVClientReadTimeout, uint64(3000)) // 3000ms.
	m := meta.NewReader(snapshot)
	neededSchemaVersion, err := m.GetSchemaVersionWithNonEmptyDiff()
	if err != nil {
		return nil, false, 0, nil, err
	}
	// fetch the commit timestamp of the schema diff
	schemaTs, err := do.getTimestampForSchemaVersionWithNonEmptyDiff(m, neededSchemaVersion, startTS)
	if err != nil {
		logutil.BgLogger().Warn("failed to get schema version", zap.Error(err), zap.Int64("version", neededSchemaVersion))
		schemaTs = 0
	}

	enableV2 := vardef.SchemaCacheSize.Load() > 0
	currentSchemaVersion := int64(0)
	oldInfoSchema := do.infoCache.GetLatest()
	if oldInfoSchema != nil {
		currentSchemaVersion = oldInfoSchema.SchemaMetaVersion()
	}
	useV2, isV1V2Switch := shouldUseV2(enableV2, oldInfoSchema, isSnapshot)
	if is := do.infoCache.GetByVersion(neededSchemaVersion); is != nil {
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
		do.infoCache.Insert(is, schemaTs)

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
		is, relatedChanges, diffTypes, err := do.tryLoadSchemaDiffs(useV2, m, currentSchemaVersion, neededSchemaVersion, startTS)
		if err == nil {
			infoschema_metrics.LoadSchemaDurationLoadDiff.Observe(time.Since(startTime).Seconds())
			isV2, _ := infoschema.IsV2(is)
			do.infoCache.Insert(is, schemaTs)
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
	schemas, err := do.fetchAllSchemasWithTables(m)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}

	policies, err := do.fetchPolicies(m)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}

	resourceGroups, err := do.fetchResourceGroups(m)
	if err != nil {
		return nil, false, currentSchemaVersion, nil, err
	}
	infoschema_metrics.LoadSchemaDurationLoadAll.Observe(time.Since(startTime).Seconds())

	data := do.infoCache.Data
	if isSnapshot {
		// Use a NewData() to avoid adding the snapshot schema to the infoschema history.
		// Why? imagine that the current schema version is [103 104 105 ...]
		// Then a snapshot read require infoschem version 53, and it's added
		// Now the history becomes [53,  ... 103, 104, 105 ...]
		// Then if a query ask for version 74, we'll mistakenly use 53!
		// Not adding snapshot schema to history can avoid such cases.
		data = infoschema.NewData()
	}
	builder := infoschema.NewBuilder(do, do.sysFacHack, data, useV2)
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
		fn := do.infoCache.Upsert(is, schemaTs)
		do.deferFn.add(fn, time.Now().Add(10*time.Minute))
		logutil.BgLogger().Info("infoschema v1/v2 switch")
	} else {
		do.infoCache.Insert(is, schemaTs)
	}
	return is, false, currentSchemaVersion, nil, nil
}

// Returns the timestamp of a schema version, which is the commit timestamp of the schema diff
func (do *Domain) getTimestampForSchemaVersionWithNonEmptyDiff(m meta.Reader, version int64, startTS uint64) (uint64, error) {
	tikvStore, ok := do.Store().(helper.Storage)
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

func (do *Domain) sysFacHack() (pools.Resource, error) {
	// TODO: Here we create new sessions with sysFac in DDL,
	// which will use `do` as Domain instead of call `domap.Get`.
	// That's because `domap.Get` requires a lock, but before
	// we initialize Domain finish, we can't require that again.
	// After we remove the lazy logic of creating Domain, we
	// can simplify code here.
	return do.sysExecutorFactory(do)
}

func (*Domain) fetchPolicies(m meta.Reader) ([]*model.PolicyInfo, error) {
	allPolicies, err := m.ListPolicies()
	if err != nil {
		return nil, err
	}
	return allPolicies, nil
}

func (*Domain) fetchResourceGroups(m meta.Reader) ([]*model.ResourceGroupInfo, error) {
	allResourceGroups, err := m.ListResourceGroups()
	if err != nil {
		return nil, err
	}
	return allResourceGroups, nil
}

func (do *Domain) fetchAllSchemasWithTables(m meta.Reader) ([]*model.DBInfo, error) {
	allSchemas, err := m.ListDatabases()
	if err != nil {
		return nil, err
	}
	if len(allSchemas) == 0 {
		return nil, nil
	}

	splittedSchemas := do.splitForConcurrentFetch(allSchemas)
	concurrency := min(len(splittedSchemas), 128)

	eg, ectx := util.NewErrorGroupWithRecoverWithCtx(context.Background())
	eg.SetLimit(concurrency)
	for _, schemas := range splittedSchemas {
		ss := schemas
		eg.Go(func() error {
			return do.fetchSchemasWithTables(ectx, ss, m)
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return allSchemas, nil
}

// fetchSchemaConcurrency controls the goroutines to load schemas, but more goroutines
// increase the memory usage when calling json.Unmarshal(), which would cause OOM,
// so we decrease the concurrency.
const fetchSchemaConcurrency = 1

func (*Domain) splitForConcurrentFetch(schemas []*model.DBInfo) [][]*model.DBInfo {
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

func (*Domain) fetchSchemasWithTables(ctx context.Context, schemas []*model.DBInfo, m meta.Reader) error {
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

// tryLoadSchemaDiffs tries to only load latest schema changes.
// Return true if the schema is loaded successfully.
// Return false if the schema can not be loaded by schema diff, then we need to do full load.
// The second returned value is the delta updated table and partition IDs.
func (do *Domain) tryLoadSchemaDiffs(useV2 bool, m meta.Reader, usedVersion, newVersion int64, startTS uint64) (infoschema.InfoSchema, *transaction.RelatedSchemaChange, []string, error) {
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
			do.infoCache.InsertEmptySchemaVersion(usedVersion)
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

	builder := infoschema.NewBuilder(do, do.sysFacHack, do.infoCache.Data, useV2)
	err := builder.InitWithOldInfoSchema(do.infoCache.GetLatest())
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	builder.WithStore(do.store).SetDeltaUpdateBundles()
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

func canSkipSchemaCheckerDDL(tp model.ActionType) bool {
	switch tp {
	case model.ActionUpdateTiFlashReplicaStatus, model.ActionSetTiFlashReplica:
		return true
	}
	return false
}

// InfoSchema gets the latest information schema from domain.
func (do *Domain) InfoSchema() infoschema.InfoSchema {
	return do.infoCache.GetLatest()
}

// GetSnapshotInfoSchema gets a snapshot information schema.
func (do *Domain) GetSnapshotInfoSchema(snapshotTS uint64) (infoschema.InfoSchema, error) {
	// if the snapshotTS is new enough, we can get infoschema directly through snapshotTS.
	if is := do.infoCache.GetBySnapshotTS(snapshotTS); is != nil {
		return is, nil
	}
	is, _, _, _, err := do.loadInfoSchema(snapshotTS, true)
	infoschema_metrics.LoadSchemaCounterSnapshot.Inc()
	return is, err
}

// GetSnapshotMeta gets a new snapshot meta at startTS.
func (do *Domain) GetSnapshotMeta(startTS uint64) meta.Reader {
	snapshot := do.store.GetSnapshot(kv.NewVersion(startTS))
	return meta.NewReader(snapshot)
}

// ExpiredTimeStamp4PC gets expiredTimeStamp4PC from domain.
func (do *Domain) ExpiredTimeStamp4PC() types.Time {
	do.expiredTimeStamp4PC.RLock()
	defer do.expiredTimeStamp4PC.RUnlock()

	return do.expiredTimeStamp4PC.expiredTimeStamp
}

// SetExpiredTimeStamp4PC sets the expiredTimeStamp4PC from domain.
func (do *Domain) SetExpiredTimeStamp4PC(time types.Time) {
	do.expiredTimeStamp4PC.Lock()
	defer do.expiredTimeStamp4PC.Unlock()

	do.expiredTimeStamp4PC.expiredTimeStamp = time
}

// DDL gets DDL from domain.
func (do *Domain) DDL() ddl.DDL {
	return do.ddl
}

// DDLExecutor gets the ddl executor from domain.
func (do *Domain) DDLExecutor() ddl.Executor {
	return do.ddlExecutor
}

// SetDDL sets DDL to domain, it's only used in tests.
func (do *Domain) SetDDL(d ddl.DDL, executor ddl.Executor) {
	do.ddl = d
	do.ddlExecutor = executor
}

// InfoSyncer gets infoSyncer from domain.
func (do *Domain) InfoSyncer() *infosync.InfoSyncer {
	return do.info
}

// NotifyGlobalConfigChange notify global config syncer to store the global config into PD.
func (do *Domain) NotifyGlobalConfigChange(name, value string) {
	do.globalCfgSyncer.Notify(pd.GlobalConfigItem{Name: name, Value: value, EventType: pdpb.EventType_PUT})
}

// GetGlobalConfigSyncer exports for testing.
func (do *Domain) GetGlobalConfigSyncer() *globalconfigsync.GlobalConfigSyncer {
	return do.globalCfgSyncer
}

// Store gets KV store from domain.
func (do *Domain) Store() kv.Storage {
	return do.store
}

// GetScope gets the status variables scope.
func (*Domain) GetScope(string) vardef.ScopeFlag {
	// Now domain status variables scope are all default scope.
	return variable.DefaultStatusVarScopeFlag
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

// Reload reloads InfoSchema.
// It's public in order to do the test.
func (do *Domain) Reload() error {
	failpoint.Inject("ErrorMockReloadFailed", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(errors.New("mock reload failed"))
		}
	})

	// Lock here for only once at the same time.
	do.m.Lock()
	defer do.m.Unlock()

	startTime := time.Now()
	ver, err := do.store.CurrentVersion(kv.GlobalTxnScope)
	if err != nil {
		return err
	}

	version := ver.Ver
	is, hitCache, oldSchemaVersion, changes, err := do.loadInfoSchema(version, false)
	if err != nil {
		if version = getFlashbackStartTSFromErrorMsg(err); version != 0 {
			// use the latest available version to create domain
			version--
			is, hitCache, oldSchemaVersion, changes, err = do.loadInfoSchema(version, false)
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
			err = do.ddl.SchemaSyncer().UpdateSelfVersion(context.Background(), 0, is.SchemaMetaVersion())
			if err != nil {
				logutil.BgLogger().Info("update self version failed",
					zap.Int64("oldSchemaVersion", oldSchemaVersion),
					zap.Int64("neededSchemaVersion", is.SchemaMetaVersion()), zap.Error(err))
			}
		}

		// it is full load
		if changes == nil {
			logutil.BgLogger().Info("full load and reset schema validator")
			do.SchemaValidator.Reset()
		}
	}

	lease := do.GetSchemaLease()
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
		err := kv.RunInNewTxn(ctx, do.store, false, func(_ context.Context, txn kv.Transaction) error {
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
	do.SchemaValidator.Update(version, oldSchemaVersion, is.SchemaMetaVersion(), changes)

	return nil
}

// LogSlowQuery keeps topN recent slow queries in domain.
func (do *Domain) LogSlowQuery(query *SlowQueryInfo) {
	do.slowQuery.mu.RLock()
	defer do.slowQuery.mu.RUnlock()
	if do.slowQuery.mu.closed {
		return
	}

	select {
	case do.slowQuery.ch <- query:
	default:
	}
}

// ShowSlowQuery returns the slow queries.
func (do *Domain) ShowSlowQuery(showSlow *ast.ShowSlow) []*SlowQueryInfo {
	msg := &showSlowMessage{
		request: showSlow,
	}
	msg.Add(1)
	do.slowQuery.msgCh <- msg
	msg.Wait()
	return msg.result
}

func (do *Domain) topNSlowQueryLoop() {
	defer util.Recover(metrics.LabelDomain, "topNSlowQueryLoop", nil, false)
	ticker := time.NewTicker(time.Minute * 10)
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("topNSlowQueryLoop exited.")
	}()
	for {
		select {
		case now := <-ticker.C:
			do.slowQuery.RemoveExpired(now)
		case info, ok := <-do.slowQuery.ch:
			if !ok {
				return
			}
			do.slowQuery.Append(info)
		case msg := <-do.slowQuery.msgCh:
			req := msg.request
			switch req.Tp {
			case ast.ShowSlowTop:
				msg.result = do.slowQuery.QueryTop(int(req.Count), req.Kind)
			case ast.ShowSlowRecent:
				msg.result = do.slowQuery.QueryRecent(int(req.Count))
			default:
				msg.result = do.slowQuery.QueryAll()
			}
			msg.Done()
		}
	}
}

func (do *Domain) infoSyncerKeeper() {
	defer func() {
		logutil.BgLogger().Info("infoSyncerKeeper exited.")
	}()

	defer util.Recover(metrics.LabelDomain, "infoSyncerKeeper", nil, false)

	ticker := time.NewTicker(infosync.ReportInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			do.info.ReportMinStartTS(do.Store())
		case <-do.info.Done():
			logutil.BgLogger().Info("server info syncer need to restart")
			if err := do.info.Restart(context.Background()); err != nil {
				logutil.BgLogger().Error("server info syncer restart failed", zap.Error(err))
			} else {
				logutil.BgLogger().Info("server info syncer restarted")
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) globalConfigSyncerKeeper() {
	defer func() {
		logutil.BgLogger().Info("globalConfigSyncerKeeper exited.")
	}()

	defer util.Recover(metrics.LabelDomain, "globalConfigSyncerKeeper", nil, false)

	for {
		select {
		case entry := <-do.globalCfgSyncer.NotifyCh:
			err := do.globalCfgSyncer.StoreGlobalConfig(context.Background(), entry)
			if err != nil {
				logutil.BgLogger().Error("global config syncer store failed", zap.Error(err))
			}
		// TODO(crazycs520): Add owner to maintain global config is consistency with global variable.
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) topologySyncerKeeper() {
	defer util.Recover(metrics.LabelDomain, "topologySyncerKeeper", nil, false)
	ticker := time.NewTicker(infosync.TopologyTimeToRefresh)
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("topologySyncerKeeper exited.")
	}()

	for {
		select {
		case <-ticker.C:
			err := do.info.StoreTopologyInfo(context.Background())
			if err != nil {
				logutil.BgLogger().Error("refresh topology in loop failed", zap.Error(err))
			}
		case <-do.info.TopologyDone():
			logutil.BgLogger().Info("server topology syncer need to restart")
			if err := do.info.RestartTopology(context.Background()); err != nil {
				logutil.BgLogger().Error("server topology syncer restart failed", zap.Error(err))
			} else {
				logutil.BgLogger().Info("server topology syncer restarted")
			}
		case <-do.exit:
			return
		}
	}
}

// CheckAutoAnalyzeWindows checks the auto analyze windows and kill the auto analyze process if it is not in the window.
func (do *Domain) CheckAutoAnalyzeWindows() {
	se, err := do.sysSessionPool.Get()

	if err != nil {
		logutil.BgLogger().Warn("get system session failed", zap.Error(err))
		return
	}
	// Make sure the session is new.
	sctx := se.(sessionctx.Context)
	defer do.sysSessionPool.Put(se)
	if !autoanalyze.CheckAutoAnalyzeWindow(sctx) {
		for _, id := range handleutil.GlobalAutoAnalyzeProcessList.All() {
			do.SysProcTracker().KillSysProcess(id)
		}
	}
}

func (do *Domain) refreshMDLCheckTableInfo(ctx context.Context) {
	se, err := do.sysSessionPool.Get()

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
	defer do.sysSessionPool.Put(se)
	exec := sctx.GetRestrictedSQLExecutor()
	domainSchemaVer := do.InfoSchema().SchemaMetaVersion()
	// the job must stay inside tidb_ddl_job if we need to wait schema version for it.
	sql := fmt.Sprintf(`select job_id, version, table_ids from mysql.tidb_mdl_info
		where job_id >= %d and version <= %d`, do.minJobIDRefresher.GetCurrMinJobID(), domainSchemaVer)
	rows, _, err := exec.ExecRestrictedSQL(ctx, nil, sql)
	if err != nil {
		logutil.BgLogger().Warn("get mdl info from tidb_mdl_info failed", zap.Error(err))
		return
	}
	do.mdlCheckTableInfo.mu.Lock()
	defer do.mdlCheckTableInfo.mu.Unlock()

	do.mdlCheckTableInfo.newestVer = domainSchemaVer
	do.mdlCheckTableInfo.jobsVerMap = make(map[int64]int64, len(rows))
	do.mdlCheckTableInfo.jobsIDsMap = make(map[int64]string, len(rows))
	for i := range rows {
		do.mdlCheckTableInfo.jobsVerMap[rows[i].GetInt64(0)] = rows[i].GetInt64(1)
		do.mdlCheckTableInfo.jobsIDsMap[rows[i].GetInt64(0)] = rows[i].GetString(2)
	}
}

func (do *Domain) mdlCheckLoop() {
	ticker := time.Tick(mdlCheckLookDuration)
	var saveMaxSchemaVersion int64
	jobNeedToSync := false
	jobCache := make(map[int64]int64, 1000)

	for {
		// Wait for channels
		select {
		case <-do.mdlCheckCh:
		case <-ticker:
		case <-do.exit:
			return
		}

		if !vardef.EnableMDL.Load() {
			continue
		}

		do.mdlCheckTableInfo.mu.Lock()
		maxVer := do.mdlCheckTableInfo.newestVer
		if maxVer > saveMaxSchemaVersion {
			saveMaxSchemaVersion = maxVer
		} else if !jobNeedToSync {
			// Schema doesn't change, and no job to check in the last run.
			do.mdlCheckTableInfo.mu.Unlock()
			continue
		}

		jobNeedToCheckCnt := len(do.mdlCheckTableInfo.jobsVerMap)
		if jobNeedToCheckCnt == 0 {
			jobNeedToSync = false
			do.mdlCheckTableInfo.mu.Unlock()
			continue
		}

		jobsVerMap := make(map[int64]int64, len(do.mdlCheckTableInfo.jobsVerMap))
		jobsIDsMap := make(map[int64]string, len(do.mdlCheckTableInfo.jobsIDsMap))
		maps.Copy(jobsVerMap, do.mdlCheckTableInfo.jobsVerMap)
		maps.Copy(jobsIDsMap, do.mdlCheckTableInfo.jobsIDsMap)
		do.mdlCheckTableInfo.mu.Unlock()

		jobNeedToSync = true

		sm := do.InfoSyncer().GetSessionManager()
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
			err := do.ddl.SchemaSyncer().UpdateSelfVersion(context.Background(), jobID, ver)
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

func (do *Domain) loadSchemaInLoop(ctx context.Context) {
	defer util.Recover(metrics.LabelDomain, "loadSchemaInLoop", nil, true)
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	ticker := time.NewTicker(do.schemaLease / 2)
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("loadSchemaInLoop exited.")
	}()
	syncer := do.ddl.SchemaSyncer()

	for {
		select {
		case <-ticker.C:
			failpoint.Inject("disableOnTickReload", func() {
				failpoint.Continue()
			})
			err := do.Reload()
			if err != nil {
				logutil.BgLogger().Error("reload schema in loop failed", zap.Error(err))
			}
			do.deferFn.check()
		case _, ok := <-syncer.GlobalVersionCh():
			err := do.Reload()
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
			do.SchemaValidator.Stop()
			err := do.mustRestartSyncer(ctx)
			if err != nil {
				logutil.BgLogger().Error("reload schema in loop, schema syncer restart failed", zap.Error(err))
				break
			}
			// The schema maybe changed, must reload schema then the schema validator can restart.
			exitLoop := do.mustReload()
			// domain is closed.
			if exitLoop {
				logutil.BgLogger().Error("domain is closed, exit loadSchemaInLoop")
				return
			}
			do.SchemaValidator.Restart()
			logutil.BgLogger().Info("schema syncer restarted")
		case <-do.exit:
			return
		}
		do.refreshMDLCheckTableInfo(ctx)
		select {
		case do.mdlCheckCh <- struct{}{}:
		default:
		}
	}
}

// mustRestartSyncer tries to restart the SchemaSyncer.
// It returns until it's successful or the domain is stopped.
func (do *Domain) mustRestartSyncer(ctx context.Context) error {
	syncer := do.ddl.SchemaSyncer()

	for {
		err := syncer.Restart(ctx)
		if err == nil {
			return nil
		}
		// If the domain has stopped, we return an error immediately.
		if do.isClose() {
			return err
		}
		logutil.BgLogger().Error("restart the schema syncer failed", zap.Error(err))
		time.Sleep(time.Second)
	}
}

// mustReload tries to Reload the schema, it returns until it's successful or the domain is closed.
// it returns false when it is successful, returns true when the domain is closed.
func (do *Domain) mustReload() (exitLoop bool) {
	for {
		err := do.Reload()
		if err == nil {
			logutil.BgLogger().Info("mustReload succeed")
			return false
		}

		// If the domain is closed, we returns immediately.
		logutil.BgLogger().Info("reload the schema failed", zap.Error(err))
		if do.isClose() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (do *Domain) isClose() bool {
	select {
	case <-do.exit:
		logutil.BgLogger().Info("domain is closed")
		return true
	default:
	}
	return false
}

// Close closes the Domain and release its resource.
func (do *Domain) Close() {
	if do == nil {
		return
	}
	startTime := time.Now()
	if do.ddl != nil {
		terror.Log(do.ddl.Stop())
	}
	ttlJobManager := do.ttlJobManager.Load()
	if ttlJobManager != nil {
		logutil.BgLogger().Info("stopping ttlJobManager")
		ttlJobManager.Stop()
		err := ttlJobManager.WaitStopped(context.Background(), func() time.Duration {
			if intest.InTest {
				return 10 * time.Second
			}
			return 30 * time.Second
		}())
		if err != nil {
			logutil.BgLogger().Warn("fail to wait until the ttl job manager stop", zap.Error(err))
		} else {
			logutil.BgLogger().Info("ttlJobManager exited.")
		}
	}
	do.releaseServerID(context.Background())
	close(do.exit)
	if do.brOwnerMgr != nil {
		do.brOwnerMgr.Close()
	}

	do.runawayManager.Stop()

	do.slowQuery.Close()

	do.cancelFns.mu.Lock()
	for _, f := range do.cancelFns.fns {
		f()
	}
	do.cancelFns.mu.Unlock()

	// Clean etcd session and close the clients.
	// We should wait all the etcd keys keeper to exit
	// in case the keeper rewrite the key after the cleaning.
	do.wg.Wait()
	if do.info != nil {
		do.info.RemoveServerInfo()
		do.info.RemoveMinStartTS()
		do.info.RemoveTopologyInfo()
	}
	if do.unprefixedEtcdCli != nil {
		terror.Log(errors.Trace(do.unprefixedEtcdCli.Close()))
	}
	if do.etcdClient != nil {
		terror.Log(errors.Trace(do.etcdClient.Close()))
	}

	do.sysSessionPool.Close()
	do.advancedSysSessionPool.Close()
	variable.UnregisterStatistics(do.BindingHandle())
	if do.onClose != nil {
		do.onClose()
	}
	gctuner.WaitMemoryLimitTunerExitInTest()
	close(do.mdlCheckCh)

	// close MockGlobalServerInfoManagerEntry in order to refresh mock server info.
	if intest.InTest {
		infosync.MockGlobalServerInfoManagerEntry.Close()
	}
	if handle := do.statsHandle.Load(); handle != nil {
		handle.Close()
	}

	logutil.BgLogger().Info("domain closed", zap.Duration("take time", time.Since(startTime)))
}

const resourceIdleTimeout = 3 * time.Minute // resources in the ResourcePool will be recycled after idleTimeout

// NewDomain creates a new domain. Should not create multiple domains for the same store.
func NewDomain(store kv.Storage, schemaLease time.Duration, statsLease time.Duration, dumpFileGcLease time.Duration, factory pools.Factory) *Domain {
	return NewDomainWithEtcdClient(store, schemaLease, statsLease, dumpFileGcLease, factory, nil)
}

// NewDomainWithEtcdClient creates a new domain with etcd client. Should not create multiple domains for the same store.
func NewDomainWithEtcdClient(store kv.Storage, schemaLease time.Duration, statsLease time.Duration, dumpFileGcLease time.Duration, factory pools.Factory, etcdClient *clientv3.Client) *Domain {
	intest.Assert(schemaLease > 0, "schema lease should be a positive duration")
	capacity := 200 // capacity of the sysSessionPool size
	do := &Domain{
		store: store,
		exit:  make(chan struct{}),
		sysSessionPool: util.NewSessionPool(
			capacity, factory,
			func(r pools.Resource) {
				_, ok := r.(sessionctx.Context)
				intest.Assert(ok)
				infosync.StoreInternalSession(r)
			},
			func(r pools.Resource) {
				sctx, ok := r.(sessionctx.Context)
				intest.Assert(ok)
				intest.AssertFunc(func() bool {
					txn, _ := sctx.Txn(false)
					return txn == nil || !txn.Valid()
				})
				infosync.DeleteInternalSession(r)
			},
			func(r pools.Resource) {
				intest.Assert(r != nil)
				infosync.DeleteInternalSession(r)
			},
		),
		statsLease:        statsLease,
		schemaLease:       schemaLease,
		slowQuery:         newTopNSlowQueries(config.GetGlobalConfig().InMemSlowQueryTopNNum, time.Hour*24*7, config.GetGlobalConfig().InMemSlowQueryRecentNum),
		dumpFileGcChecker: &dumpFileGcChecker{gcLease: dumpFileGcLease, paths: []string{replayer.GetPlanReplayerDirName(), GetOptimizerTraceDirName(), GetExtractTaskDirName()}},
		mdlCheckTableInfo: &mdlCheckTableInfo{
			mu:         sync.Mutex{},
			jobsVerMap: make(map[int64]int64),
			jobsIDsMap: make(map[int64]string),
		},
		mdlCheckCh: make(chan struct{}),
	}

	do.advancedSysSessionPool = syssession.NewAdvancedSessionPool(capacity, func() (syssession.SessionContext, error) {
		r, err := factory()
		if err != nil {
			return nil, err
		}
		sctx, ok := r.(syssession.SessionContext)
		intest.Assert(ok, "type: %T should be cast to syssession.SessionContext", r)
		if !ok {
			return nil, errors.Errorf("type: %T cannot be cast to syssession.SessionContext", r)
		}
		return sctx, nil
	})
	do.infoCache = infoschema.NewCache(do, int(vardef.SchemaVersionCacheLimit.Load()))
	do.stopAutoAnalyze.Store(false)
	do.wg = util.NewWaitGroupEnhancedWrapper("domain", do.exit, config.GetGlobalConfig().TiDBEnableExitCheck)
	do.SchemaValidator = NewSchemaValidator(schemaLease, do)
	do.expensiveQueryHandle = expensivequery.NewExpensiveQueryHandle(do.exit)
	do.memoryUsageAlarmHandle = memoryusagealarm.NewMemoryUsageAlarmHandle(do.exit,
		&memoryusagealarm.TiDBConfigProvider{})
	do.serverMemoryLimitHandle = servermemorylimit.NewServerMemoryLimitHandle(do.exit)
	do.sysProcesses = SysProcesses{mu: &sync.RWMutex{}, procMap: make(map[uint64]sysproctrack.TrackProc)}
	do.initDomainSysVars()
	do.expiredTimeStamp4PC.expiredTimeStamp = types.NewTime(types.ZeroCoreTime, mysql.TypeTimestamp, types.DefaultFsp)
	do.etcdClient = etcdClient
	return do
}

const serverIDForStandalone = 1 // serverID for standalone deployment.

// Init initializes a domain. after return, session can be used to do DMLs but not
// DDLs which can be used after domain Start.
func (do *Domain) Init(
	sysExecutorFactory func(*Domain) (pools.Resource, error),
	ddlInjector func(ddl.DDL, ddl.Executor, *infoschema.InfoCache) *schematracker.Checker,
) error {
	do.sysExecutorFactory = sysExecutorFactory
	perfschema.Init()
	etcdStore, addrs, err := store.GetEtcdAddrs(do.store)
	if err != nil {
		return errors.Trace(err)
	}
	if len(addrs) > 0 {
		cli, err2 := store.NewEtcdCliWithAddrs(addrs, etcdStore)
		if err2 != nil {
			return errors.Trace(err2)
		}
		etcd.SetEtcdCliByNamespace(cli, keyspace.MakeKeyspaceEtcdNamespace(do.store.GetCodec()))

		do.etcdClient = cli

		do.autoidClient = autoid.NewClientDiscover(cli)

		unprefixedEtcdCli, err2 := store.NewEtcdCliWithAddrs(addrs, etcdStore)
		if err2 != nil {
			return errors.Trace(err2)
		}
		do.unprefixedEtcdCli = unprefixedEtcdCli
	}

	ctx, cancelFunc := context.WithCancel(context.Background())
	do.ctx = ctx
	do.cancelFns.mu.Lock()
	do.cancelFns.fns = append(do.cancelFns.fns, cancelFunc)
	do.cancelFns.mu.Unlock()

	ddlNotifierStore := notifier.OpenTableStore("mysql", ddl.NotifierTableName)
	do.ddlNotifier = notifier.NewDDLNotifier(
		do.sysSessionPool,
		ddlNotifierStore,
		time.Second,
	)
	// TODO(lance6716): find a more representative place for subscriber
	failpoint.InjectCall("afterDDLNotifierCreated", do.ddlNotifier)

	d := do.ddl
	eBak := do.ddlExecutor
	do.ddl, do.ddlExecutor = ddl.NewDDL(
		ctx,
		ddl.WithEtcdClient(do.etcdClient),
		ddl.WithStore(do.store),
		ddl.WithAutoIDClient(do.autoidClient),
		ddl.WithInfoCache(do.infoCache),
		ddl.WithLease(do.schemaLease),
		ddl.WithSchemaLoader(do),
		ddl.WithEventPublishStore(ddlNotifierStore),
	)

	failpoint.Inject("MockReplaceDDL", func(val failpoint.Value) {
		if val.(bool) {
			do.ddl = d
			do.ddlExecutor = eBak
		}
	})
	if ddlInjector != nil {
		checker := ddlInjector(do.ddl, do.ddlExecutor, do.infoCache)
		checker.CreateTestDB(nil)
		do.ddl = checker
		do.ddlExecutor = checker
	}

	// step 1: prepare the info/schema syncer which domain reload needed.
	pdCli, pdHTTPCli := do.GetPDClient(), do.GetPDHTTPClient()
	skipRegisterToDashboard := config.GetGlobalConfig().SkipRegisterToDashboard
	do.info, err = infosync.GlobalInfoSyncerInit(ctx, do.ddl.GetID(), do.ServerID,
		do.etcdClient, do.unprefixedEtcdCli, pdCli, pdHTTPCli,
		do.Store().GetCodec(), skipRegisterToDashboard, do.infoCache)
	if err != nil {
		return err
	}
	do.globalCfgSyncer = globalconfigsync.NewGlobalConfigSyncer(pdCli)
	err = do.ddl.SchemaSyncer().Init(ctx)
	if err != nil {
		return err
	}

	// step 2: initialize the global kill, which depends on `globalInfoSyncer`.`
	if config.GetGlobalConfig().EnableGlobalKill {
		do.connIDAllocator = globalconn.NewGlobalAllocator(do.ServerID, config.GetGlobalConfig().Enable32BitsConnectionID)

		if do.etcdClient != nil {
			err := do.acquireServerID(ctx)
			if err != nil {
				logutil.BgLogger().Error("acquire serverID failed", zap.Error(err))
				do.isLostConnectionToPD.Store(1) // will retry in `do.serverIDKeeper`
			} else {
				if err := do.info.StoreServerInfo(context.Background()); err != nil {
					return errors.Trace(err)
				}
				do.isLostConnectionToPD.Store(0)
			}
		} else {
			// set serverID for standalone deployment to enable 'KILL'.
			atomic.StoreUint64(&do.serverID, serverIDForStandalone)
		}
	} else {
		do.connIDAllocator = globalconn.NewSimpleAllocator()
	}

	// should put `initResourceGroupsController` after fetching server ID
	err = do.initResourceGroupsController(ctx, pdCli, do.ServerID())
	if err != nil {
		return err
	}

	// step 3: domain reload the infoSchema.
	return do.Reload()
}

// Start starts the domain. After start, DDLs can be executed using session, see
// Init also.
func (do *Domain) Start(startMode ddl.StartMode) error {
	gCfg := config.GetGlobalConfig()
	if gCfg.EnableGlobalKill && do.etcdClient != nil {
		do.wg.Add(1)
		go do.serverIDKeeper()
	}

	// TODO: Here we create new sessions with sysFac in DDL,
	// which will use `do` as Domain instead of call `domap.Get`.
	// That's because `domap.Get` requires a lock, but before
	// we initialize Domain finish, we can't require that again.
	// After we remove the lazy logic of creating Domain, we
	// can simplify code here.
	sysFac := func() (pools.Resource, error) {
		return do.sysExecutorFactory(do)
	}
	sysCtxPool := pools.NewResourcePool(sysFac, 512, 512, resourceIdleTimeout)

	// start the ddl after the domain reload, avoiding some internal sql running before infoSchema construction.
	err := do.ddl.Start(startMode, sysCtxPool)
	if err != nil {
		return err
	}
	do.minJobIDRefresher = do.ddl.GetMinJobIDRefresher()

	// Local store needs to get the change information for every DDL state in each session.
	do.wg.Run(func() {
		do.loadSchemaInLoop(do.ctx)
	}, "loadSchemaInLoop")
	do.wg.Run(do.mdlCheckLoop, "mdlCheckLoop")
	do.wg.Run(do.topNSlowQueryLoop, "topNSlowQueryLoop")
	do.wg.Run(do.infoSyncerKeeper, "infoSyncerKeeper")
	do.wg.Run(do.globalConfigSyncerKeeper, "globalConfigSyncerKeeper")
	do.wg.Run(do.runawayStartLoop, "runawayStartLoop")
	do.wg.Run(do.requestUnitsWriterLoop, "requestUnitsWriterLoop")
	skipRegisterToDashboard := gCfg.SkipRegisterToDashboard
	if !skipRegisterToDashboard {
		do.wg.Run(do.topologySyncerKeeper, "topologySyncerKeeper")
	}
	pdCli := do.GetPDClient()
	if pdCli != nil {
		do.wg.Run(func() {
			do.closestReplicaReadCheckLoop(do.ctx, pdCli)
		}, "closestReplicaReadCheckLoop")
	}

	err = do.initLogBackup(do.ctx, pdCli)
	if err != nil {
		return err
	}

	return nil
}

// GetSchemaLease return the schema lease.
func (do *Domain) GetSchemaLease() time.Duration {
	return do.schemaLease
}

// IsLeaseExpired returns whether lease has expired
func (do *Domain) IsLeaseExpired() bool {
	return do.SchemaValidator.IsLeaseExpired()
}

// InitInfo4Test init infosync for distributed execution test.
func (do *Domain) InitInfo4Test() {
	infosync.MockGlobalServerInfoManagerEntry.Add(do.ddl.GetID(), do.ServerID)
}

// SetOnClose used to set do.onClose func.
func (do *Domain) SetOnClose(onClose func()) {
	do.onClose = onClose
}

func (do *Domain) initLogBackup(ctx context.Context, pdClient pd.Client) error {
	cfg := config.GetGlobalConfig()
	if pdClient == nil || do.etcdClient == nil {
		log.Warn("pd / etcd client not provided, won't begin Advancer.")
		return nil
	}
	tikvStore, ok := do.Store().(tikv.Storage)
	if !ok {
		log.Warn("non tikv store, stop begin Advancer.")
		return nil
	}
	env, err := streamhelper.TiDBEnv(tikvStore, pdClient, do.etcdClient, cfg)
	if err != nil {
		return err
	}
	adv := streamhelper.NewCheckpointAdvancer(env)
	do.brOwnerMgr = streamhelper.OwnerManagerForLogBackup(ctx, do.etcdClient)
	do.logBackupAdvancer = daemon.New(adv, do.brOwnerMgr, adv.Config().TickDuration)
	loop, err := do.logBackupAdvancer.Begin(ctx)
	if err != nil {
		return err
	}
	do.wg.Run(loop, "logBackupAdvancer")
	return nil
}

// when tidb_replica_read = 'closest-adaptive', check tidb and tikv's zone label matches.
// if not match, disable replica_read to avoid uneven read traffic distribution.
func (do *Domain) closestReplicaReadCheckLoop(ctx context.Context, pdClient pd.Client) {
	defer util.Recover(metrics.LabelDomain, "closestReplicaReadCheckLoop", nil, false)

	// trigger check once instantly.
	if err := do.checkReplicaRead(ctx, pdClient); err != nil {
		logutil.BgLogger().Warn("refresh replicaRead flag failed", zap.Error(err))
	}

	ticker := time.NewTicker(time.Minute)
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("closestReplicaReadCheckLoop exited.")
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := do.checkReplicaRead(ctx, pdClient); err != nil {
				logutil.BgLogger().Warn("refresh replicaRead flag failed", zap.Error(err))
			}
		}
	}
}

// Periodically check and update the replica-read status when `tidb_replica_read` is set to "closest-adaptive"
// We disable "closest-adaptive" in following conditions to ensure the read traffic is evenly distributed across
// all AZs:
// - There are no TiKV servers in the AZ of this tidb instance
// - The AZ if this tidb contains more tidb than other AZ and this tidb's id is the bigger one.
func (do *Domain) checkReplicaRead(ctx context.Context, pdClient pd.Client) error {
	do.sysVarCache.RLock()
	replicaRead := do.sysVarCache.global[vardef.TiDBReplicaRead]
	do.sysVarCache.RUnlock()

	if !strings.EqualFold(replicaRead, "closest-adaptive") {
		logutil.BgLogger().Debug("closest replica read is not enabled, skip check!", zap.String("mode", replicaRead))
		return nil
	}

	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		return err
	}
	zone := ""
	for k, v := range serverInfo.Labels {
		if k == placement.DCLabelKey && v != "" {
			zone = v
			break
		}
	}
	if zone == "" {
		logutil.BgLogger().Debug("server contains no 'zone' label, disable closest replica read", zap.Any("labels", serverInfo.Labels))
		variable.SetEnableAdaptiveReplicaRead(false)
		return nil
	}

	stores, err := pdClient.GetAllStores(ctx, opt.WithExcludeTombstone())
	if err != nil {
		return err
	}

	storeZones := make(map[string]int)
	for _, s := range stores {
		// skip tumbstone stores or tiflash
		if s.NodeState == metapb.NodeState_Removing || s.NodeState == metapb.NodeState_Removed || engine.IsTiFlash(s) {
			continue
		}
		for _, label := range s.Labels {
			if label.Key == placement.DCLabelKey && label.Value != "" {
				storeZones[label.Value] = 0
				break
			}
		}
	}

	// no stores in this AZ
	if _, ok := storeZones[zone]; !ok {
		variable.SetEnableAdaptiveReplicaRead(false)
		return nil
	}

	servers, err := infosync.GetAllServerInfo(ctx)
	if err != nil {
		return err
	}
	svrIDsInThisZone := make([]string, 0)
	for _, s := range servers {
		if v, ok := s.Labels[placement.DCLabelKey]; ok && v != "" {
			if _, ok := storeZones[v]; ok {
				storeZones[v]++
				if v == zone {
					svrIDsInThisZone = append(svrIDsInThisZone, s.ID)
				}
			}
		}
	}
	enabledCount := math.MaxInt
	for _, count := range storeZones {
		if count < enabledCount {
			enabledCount = count
		}
	}
	// sort tidb in the same AZ by ID and disable the tidb with bigger ID
	// because ID is unchangeable, so this is a simple and stable algorithm to select
	// some instances across all tidb servers.
	if enabledCount < len(svrIDsInThisZone) {
		sort.Slice(svrIDsInThisZone, func(i, j int) bool {
			return strings.Compare(svrIDsInThisZone[i], svrIDsInThisZone[j]) < 0
		})
	}
	enabled := true
	if slices.Contains(svrIDsInThisZone[enabledCount:], serverInfo.ID) {
		enabled = false
	}

	if variable.SetEnableAdaptiveReplicaRead(enabled) {
		logutil.BgLogger().Info("tidb server adaptive closest replica read is changed", zap.Bool("enable", enabled))
	}
	return nil
}

// InitDistTaskLoop initializes the distributed task framework.
func (do *Domain) InitDistTaskLoop() error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalDistTask)
	failpoint.Inject("MockDisableDistTask", func(val failpoint.Value) {
		if val.(bool) {
			failpoint.Return(nil)
		}
	})

	taskManager := storage.NewTaskManager(do.sysSessionPool)
	var serverID string
	if intest.InTest {
		do.InitInfo4Test()
		serverID = disttaskutil.GenerateSubtaskExecID4Test(do.ddl.GetID())
	} else {
		serverID = disttaskutil.GenerateSubtaskExecID(ctx, do.ddl.GetID())
	}

	if serverID == "" {
		errMsg := fmt.Sprintf("TiDB node ID( = %s ) not found in available TiDB nodes list", do.ddl.GetID())
		return errors.New(errMsg)
	}
	managerCtx, cancel := context.WithCancel(ctx)
	do.cancelFns.mu.Lock()
	do.cancelFns.fns = append(do.cancelFns.fns, cancel)
	do.cancelFns.mu.Unlock()
	nodeRes, err := calculateNodeResource()
	if err != nil {
		return err
	}
	disthandle.SetNodeResource(nodeRes)
	executorManager, err := taskexecutor.NewManager(managerCtx, serverID, taskManager, nodeRes)
	if err != nil {
		return err
	}

	storage.SetTaskManager(taskManager)
	if err = executorManager.InitMeta(); err != nil {
		// executor manager loop will try to recover meta repeatedly, so we can
		// just log the error here.
		logutil.BgLogger().Warn("init task executor manager meta failed", zap.Error(err))
	}
	do.wg.Run(func() {
		defer func() {
			storage.SetTaskManager(nil)
		}()
		do.distTaskFrameworkLoop(ctx, taskManager, executorManager, serverID, nodeRes)
	}, "distTaskFrameworkLoop")
	return nil
}

func calculateNodeResource() (*proto.NodeResource, error) {
	logger := logutil.ErrVerboseLogger()
	totalMem, err := memory.MemTotal()
	if err != nil {
		// should not happen normally, as in main function of tidb-server, we assert
		// that memory.MemTotal() will not fail.
		return nil, err
	}
	totalCPU := cpu.GetCPUCount()
	if totalCPU <= 0 || totalMem <= 0 {
		return nil, errors.Errorf("invalid cpu or memory, cpu: %d, memory: %d", totalCPU, totalMem)
	}
	cgroupLimit, version, err := cgroup.GetCgroupMemLimit()
	// ignore the error of cgroup.GetCgroupMemLimit, as it's not a must-success step.
	if err == nil && version == cgroup.V2 {
		// see cgroup.detectMemLimitInV2 for more details.
		// below are some real memory limits tested on GCP:
		// node-spec  real-limit  percent
		// 16c32g        27.83Gi    87%
		// 32c64g        57.36Gi    89.6%
		// we use 'limit', not totalMem for adjust, as totalMem = min(physical-mem, 'limit')
		// content of 'memory.max' might be 'max', so we use the min of them.
		adjustedMem := min(totalMem, uint64(float64(cgroupLimit)*0.88))
		logger.Info("adjust memory limit for cgroup v2",
			zap.String("before", units.BytesSize(float64(totalMem))),
			zap.String("after", units.BytesSize(float64(adjustedMem))))
		totalMem = adjustedMem
	}
	var totalDisk uint64
	cfg := config.GetGlobalConfig()
	sz, err := lcom.GetStorageSize(cfg.TempDir)
	if err != nil {
		logger.Warn("get storage size failed, use tidb_ddl_disk_quota instead", zap.Error(err))
		totalDisk = vardef.DDLDiskQuota.Load()
	} else {
		totalDisk = sz.Capacity
	}
	logger.Info("initialize node resource",
		zap.Int("total-cpu", totalCPU),
		zap.String("total-mem", units.BytesSize(float64(totalMem))),
		zap.String("total-disk", units.BytesSize(float64(totalDisk))))
	return proto.NewNodeResource(totalCPU, int64(totalMem), totalDisk), nil
}

func (do *Domain) distTaskFrameworkLoop(ctx context.Context, taskManager *storage.TaskManager, executorManager *taskexecutor.Manager, serverID string, nodeRes *proto.NodeResource) {
	err := executorManager.Start()
	if err != nil {
		logutil.BgLogger().Error("dist task executor manager start failed", zap.Error(err))
		return
	}
	logutil.BgLogger().Info("dist task executor manager started")
	defer func() {
		logutil.BgLogger().Info("stopping dist task executor manager")
		executorManager.Stop()
		logutil.BgLogger().Info("dist task executor manager stopped")
	}()

	var schedulerManager *scheduler.Manager
	startSchedulerMgrIfNeeded := func() {
		if schedulerManager != nil && schedulerManager.Initialized() {
			return
		}
		schedulerManager = scheduler.NewManager(ctx, taskManager, serverID, nodeRes)
		schedulerManager.Start()
	}
	stopSchedulerMgrIfNeeded := func() {
		if schedulerManager != nil && schedulerManager.Initialized() {
			logutil.BgLogger().Info("stopping dist task scheduler manager because the current node is not DDL owner anymore", zap.String("id", do.ddl.GetID()))
			schedulerManager.Stop()
			logutil.BgLogger().Info("dist task scheduler manager stopped", zap.String("id", do.ddl.GetID()))
		}
	}

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case <-do.exit:
			stopSchedulerMgrIfNeeded()
			return
		case <-ticker.C:
			if do.ddl.OwnerManager().IsOwner() {
				startSchedulerMgrIfNeeded()
			} else {
				stopSchedulerMgrIfNeeded()
			}
		}
	}
}

// SysSessionPool returns the system session pool.
// Deprecated: Use AdvancedSysSessionPool instead.
func (do *Domain) SysSessionPool() util.DestroyableSessionPool {
	return do.sysSessionPool
}

// AdvancedSysSessionPool is a more powerful session pool that returns a wrapped session which can detect
// some misuse of the session to avoid potential bugs.
// It is recommended to use this pool instead of `sysSessionPool`.
func (do *Domain) AdvancedSysSessionPool() syssession.Pool {
	return do.advancedSysSessionPool
}

// SysProcTracker returns the system processes tracker.
func (do *Domain) SysProcTracker() sysproctrack.Tracker {
	return &do.sysProcesses
}

// DDLNotifier returns the DDL notifier.
func (do *Domain) DDLNotifier() *notifier.DDLNotifier {
	return do.ddlNotifier
}

// GetEtcdClient returns the etcd client.
func (do *Domain) GetEtcdClient() *clientv3.Client {
	return do.etcdClient
}

// AutoIDClient returns the autoid client.
func (do *Domain) AutoIDClient() *autoid.ClientDiscover {
	return do.autoidClient
}

// GetPDClient returns the PD client.
func (do *Domain) GetPDClient() pd.Client {
	if store, ok := do.store.(kv.StorageWithPD); ok {
		return store.GetPDClient()
	}
	return nil
}

// GetPDHTTPClient returns the PD HTTP client.
func (do *Domain) GetPDHTTPClient() pdhttp.Client {
	if store, ok := do.store.(kv.StorageWithPD); ok {
		return store.GetPDHTTPClient()
	}
	return nil
}

func (do *Domain) decodePrivilegeEvent(resp clientv3.WatchResponse) PrivilegeEvent {
	var msg PrivilegeEvent
	isNewVersionEvents := false
	for _, event := range resp.Events {
		if event.Kv != nil {
			val := event.Kv.Value
			if len(val) > 0 {
				var tmp PrivilegeEvent
				err := json.Unmarshal(val, &tmp)
				if err != nil {
					logutil.BgLogger().Warn("decodePrivilegeEvent unmarshal fail", zap.Error(err))
					break
				}
				isNewVersionEvents = true
				if do.ServerID() != 0 && tmp.ServerID == do.ServerID() {
					// Skip the events from this TiDB-Server
					continue
				}
				if tmp.All {
					msg.All = true
					break
				}
				// duplicated users in list is ok.
				msg.UserList = append(msg.UserList, tmp.UserList...)
			}
		}
	}

	// In case old version triggers the event, the event value is empty,
	// Then we fall back to the old way: reload all the users.
	if len(msg.UserList) == 0 && !isNewVersionEvents {
		msg.All = true
	}
	return msg
}

func (do *Domain) batchReadMoreData(ch clientv3.WatchChan, event PrivilegeEvent) PrivilegeEvent {
	timer := time.NewTimer(5 * time.Millisecond)
	defer timer.Stop()
	const maxBatchSize = 128
	for range maxBatchSize {
		select {
		case resp, ok := <-ch:
			if !ok {
				return event
			}
			tmp := do.decodePrivilegeEvent(resp)
			if tmp.All {
				event.All = true
			} else {
				if !event.All {
					event.UserList = append(event.UserList, tmp.UserList...)
				}
			}
			succ := timer.Reset(5 * time.Millisecond)
			if !succ {
				return event
			}
		case <-timer.C:
			return event
		}
	}
	return event
}

// LoadPrivilegeLoop create a goroutine loads privilege tables in a loop, it
// should be called only once in BootstrapSession.
func (do *Domain) LoadPrivilegeLoop(sctx sessionctx.Context) error {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnPrivilege)
	sctx.GetSessionVars().InRestrictedSQL = true
	_, err := sctx.GetSQLExecutor().ExecuteInternal(ctx, "set @@autocommit = 1")
	if err != nil {
		return err
	}
	do.privHandle = privileges.NewHandle(do.SysSessionPool(), sctx.GetSessionVars().GlobalVarsAccessor)

	var watchCh clientv3.WatchChan
	duration := 5 * time.Minute
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(do.ctx, privilegeKey)
		duration = 10 * time.Minute
	}

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("loadPrivilegeInLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "loadPrivilegeInLoop", nil, false)

		var count int
		for {
			var event PrivilegeEvent
			select {
			case <-do.exit:
				return
			case resp, ok := <-watchCh:
				if ok {
					count = 0
					event = do.decodePrivilegeEvent(resp)
					event = do.batchReadMoreData(watchCh, event)
				} else {
					if do.ctx.Err() == nil {
						logutil.BgLogger().Error("load privilege loop watch channel closed")
						watchCh = do.etcdClient.Watch(do.ctx, privilegeKey)
						count++
						if count > 10 {
							time.Sleep(time.Duration(count) * time.Second)
						}
						continue
					}
				}
			case <-time.After(duration):
				event.All = true
				event = do.batchReadMoreData(watchCh, event)
			}

			// All events are from this TiDB-Server, skip them
			if !event.All && len(event.UserList) == 0 {
				continue
			}

			err := privReloadEvent(do.privHandle, &event)
			metrics.LoadPrivilegeCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.BgLogger().Error("load privilege failed", zap.Error(err))
			}
		}
	}, "loadPrivilegeInLoop")
	return nil
}

func privReloadEvent(h *privileges.Handle, event *PrivilegeEvent) (err error) {
	switch {
	case !vardef.AccelerateUserCreationUpdate.Load():
		err = h.UpdateAll()
	case event.All:
		err = h.UpdateAllActive()
	default:
		err = h.Update(event.UserList)
	}
	return
}

// LoadSysVarCacheLoop create a goroutine loads sysvar cache in a loop,
// it should be called only once in BootstrapSession.
func (do *Domain) LoadSysVarCacheLoop(ctx sessionctx.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	err := do.rebuildSysVarCache(ctx)
	if err != nil {
		return err
	}
	var watchCh clientv3.WatchChan
	duration := 30 * time.Second
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), sysVarCacheKey)
	}

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("LoadSysVarCacheLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "LoadSysVarCacheLoop", nil, false)

		var count int
		for {
			ok := true
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
			case <-time.After(duration):
			}

			failpoint.Inject("skipLoadSysVarCacheLoop", func(val failpoint.Value) {
				// In some pkg integration test, there are many testSuite, and each testSuite has separate storage and
				// `LoadSysVarCacheLoop` background goroutine. Then each testSuite `RebuildSysVarCache` from it's
				// own storage.
				// Each testSuit will also call `checkEnableServerGlobalVar` to update some local variables.
				// That's the problem, each testSuit use different storage to update some same local variables.
				// So just skip `RebuildSysVarCache` in some integration testing.
				if val.(bool) {
					failpoint.Continue()
				}
			})

			if !ok {
				logutil.BgLogger().Error("LoadSysVarCacheLoop loop watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), sysVarCacheKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}
			count = 0
			logutil.BgLogger().Debug("Rebuilding sysvar cache from etcd watch event.")
			err := do.rebuildSysVarCache(ctx)
			metrics.LoadSysVarCacheCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.BgLogger().Error("LoadSysVarCacheLoop failed", zap.Error(err))
			}
		}
	}, "LoadSysVarCacheLoop")
	return nil
}

// WatchTiFlashComputeNodeChange create a routine to watch if the topology of tiflash_compute node is changed.
// TODO: tiflashComputeNodeKey is not put to etcd yet(finish this when AutoScaler is done)
//
//	store cache will only be invalidated every n seconds.
func (do *Domain) WatchTiFlashComputeNodeChange() error {
	var watchCh clientv3.WatchChan
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), tiflashComputeNodeKey)
	}
	duration := 10 * time.Second
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("WatchTiFlashComputeNodeChange exit")
		}()
		defer util.Recover(metrics.LabelDomain, "WatchTiFlashComputeNodeChange", nil, false)

		var count int
		var logCount int
		for {
			ok := true
			var watched bool
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
				watched = true
			case <-time.After(duration):
			}
			if !ok {
				logutil.BgLogger().Error("WatchTiFlashComputeNodeChange watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), tiflashComputeNodeKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}
			count = 0
			switch s := do.store.(type) {
			case tikv.Storage:
				logCount++
				s.GetRegionCache().InvalidateTiFlashComputeStores()
				if logCount == 6 {
					// Print log every 6*duration seconds.
					logutil.BgLogger().Debug("tiflash_compute store cache invalied, will update next query", zap.Bool("watched", watched))
					logCount = 0
				}
			default:
				logutil.BgLogger().Debug("No need to watch tiflash_compute store cache for non-tikv store")
				return
			}
		}
	}, "WatchTiFlashComputeNodeChange")
	return nil
}

// PrivilegeHandle returns the MySQLPrivilege.
func (do *Domain) PrivilegeHandle() *privileges.Handle {
	return do.privHandle
}

// BindingHandle returns domain's bindHandle.
func (do *Domain) BindingHandle() bindinfo.BindingHandle {
	v := do.bindHandle.Load()
	if v == nil {
		return nil
	}
	return v.(bindinfo.BindingHandle)
}

// InitBindingHandle create a goroutine loads BindInfo in a loop, it should
// be called only once in BootstrapSession.
func (do *Domain) InitBindingHandle() error {
	do.bindHandle.Store(bindinfo.NewBindingHandle(do.sysSessionPool))
	err := do.BindingHandle().LoadFromStorageToCache(true)
	if err != nil || bindinfo.Lease == 0 {
		return err
	}

	owner := do.NewOwnerManager(bindinfo.Prompt, bindinfo.OwnerKey)
	err = owner.CampaignOwner()
	if err != nil {
		logutil.BgLogger().Warn("campaign owner failed", zap.Error(err))
		return err
	}
	do.globalBindHandleWorkerLoop(owner)
	return nil
}

func (do *Domain) globalBindHandleWorkerLoop(owner owner.Manager) {
	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Info("globalBindHandleWorkerLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "globalBindHandleWorkerLoop", nil, false)

		bindWorkerTicker := time.NewTicker(bindinfo.Lease)
		gcBindTicker := time.NewTicker(100 * bindinfo.Lease)
		defer func() {
			bindWorkerTicker.Stop()
			gcBindTicker.Stop()
		}()
		for {
			select {
			case <-do.exit:
				do.BindingHandle().Close()
				owner.Close()
				return
			case <-bindWorkerTicker.C:
				bindHandle := do.BindingHandle()
				err := bindHandle.LoadFromStorageToCache(false)
				if err != nil {
					logutil.BgLogger().Error("update bindinfo failed", zap.Error(err))
				}
			case <-gcBindTicker.C:
				if !owner.IsOwner() {
					continue
				}
				err := do.BindingHandle().GCBinding()
				if err != nil {
					logutil.BgLogger().Error("GC bind record failed", zap.Error(err))
				}
			}
		}
	}, "globalBindHandleWorkerLoop")
}

// SetupPlanReplayerHandle setup plan replayer handle
func (do *Domain) SetupPlanReplayerHandle(collectorSctx sessionctx.Context, workersSctxs []sessionctx.Context) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnStats)
	do.planReplayerHandle = &planReplayerHandle{}
	do.planReplayerHandle.planReplayerTaskCollectorHandle = &planReplayerTaskCollectorHandle{
		ctx:  ctx,
		sctx: collectorSctx,
	}
	taskCH := make(chan *PlanReplayerDumpTask, 16)
	taskStatus := &planReplayerDumpTaskStatus{}
	taskStatus.finishedTaskMu.finishedTask = map[replayer.PlanReplayerTaskKey]struct{}{}
	taskStatus.runningTaskMu.runningTasks = map[replayer.PlanReplayerTaskKey]struct{}{}

	do.planReplayerHandle.planReplayerTaskDumpHandle = &planReplayerTaskDumpHandle{
		taskCH: taskCH,
		status: taskStatus,
	}
	do.planReplayerHandle.planReplayerTaskDumpHandle.workers = make([]*planReplayerTaskDumpWorker, 0)
	for i := range workersSctxs {
		worker := &planReplayerTaskDumpWorker{
			ctx:    ctx,
			sctx:   workersSctxs[i],
			taskCH: taskCH,
			status: taskStatus,
		}
		do.planReplayerHandle.planReplayerTaskDumpHandle.workers = append(do.planReplayerHandle.planReplayerTaskDumpHandle.workers, worker)
	}
}

// RunawayManager returns the runaway manager.
func (do *Domain) RunawayManager() *runaway.Manager {
	return do.runawayManager
}

// ResourceGroupsController returns the resource groups controller.
func (do *Domain) ResourceGroupsController() *rmclient.ResourceGroupsController {
	return do.resourceGroupsController
}

// SetResourceGroupsController is only used in test.
func (do *Domain) SetResourceGroupsController(controller *rmclient.ResourceGroupsController) {
	do.resourceGroupsController = controller
}

// SetupHistoricalStatsWorker setups worker
func (do *Domain) SetupHistoricalStatsWorker(ctx sessionctx.Context) {
	do.historicalStatsWorker = &HistoricalStatsWorker{
		tblCH: make(chan int64, 16),
		sctx:  ctx,
	}
}

// SetupDumpFileGCChecker setup sctx
func (do *Domain) SetupDumpFileGCChecker(ctx sessionctx.Context) {
	do.dumpFileGcChecker.setupSctx(ctx)
	do.dumpFileGcChecker.planReplayerTaskStatus = do.planReplayerHandle.status
}

// SetupExtractHandle setups extract handler
func (do *Domain) SetupExtractHandle(sctxs []sessionctx.Context) {
	do.extractTaskHandle = newExtractHandler(do.ctx, sctxs)
}

var planReplayerHandleLease atomic.Uint64

func init() {
	planReplayerHandleLease.Store(uint64(10 * time.Second))
	enableDumpHistoricalStats.Store(true)
}

// DisablePlanReplayerBackgroundJob4Test disable plan replayer handle for test
func DisablePlanReplayerBackgroundJob4Test() {
	planReplayerHandleLease.Store(0)
}

// DisableDumpHistoricalStats4Test disable historical dump worker for test
func DisableDumpHistoricalStats4Test() {
	enableDumpHistoricalStats.Store(false)
}

// StartPlanReplayerHandle start plan replayer handle job
func (do *Domain) StartPlanReplayerHandle() {
	lease := planReplayerHandleLease.Load()
	if lease < 1 {
		return
	}
	do.wg.Run(func() {
		logutil.BgLogger().Info("PlanReplayerTaskCollectHandle started")
		tikcer := time.NewTicker(time.Duration(lease))
		defer func() {
			tikcer.Stop()
			logutil.BgLogger().Info("PlanReplayerTaskCollectHandle exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "PlanReplayerTaskCollectHandle", nil, false)

		for {
			select {
			case <-do.exit:
				return
			case <-tikcer.C:
				err := do.planReplayerHandle.CollectPlanReplayerTask()
				if err != nil {
					logutil.BgLogger().Warn("plan replayer handle collect tasks failed", zap.Error(err))
				}
			}
		}
	}, "PlanReplayerTaskCollectHandle")

	do.wg.Run(func() {
		logutil.BgLogger().Info("PlanReplayerTaskDumpHandle started")
		defer func() {
			logutil.BgLogger().Info("PlanReplayerTaskDumpHandle exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "PlanReplayerTaskDumpHandle", nil, false)

		for _, worker := range do.planReplayerHandle.planReplayerTaskDumpHandle.workers {
			go worker.run()
		}
		<-do.exit
		do.planReplayerHandle.planReplayerTaskDumpHandle.Close()
	}, "PlanReplayerTaskDumpHandle")
}

// GetPlanReplayerHandle returns plan replayer handle
func (do *Domain) GetPlanReplayerHandle() *planReplayerHandle {
	return do.planReplayerHandle
}

// GetExtractHandle returns extract handle
func (do *Domain) GetExtractHandle() *ExtractHandle {
	return do.extractTaskHandle
}

// GetDumpFileGCChecker returns dump file GC checker for plan replayer and plan trace
func (do *Domain) GetDumpFileGCChecker() *dumpFileGcChecker {
	return do.dumpFileGcChecker
}

// DumpFileGcCheckerLoop creates a goroutine that handles `exit` and `gc`.
func (do *Domain) DumpFileGcCheckerLoop() {
	do.wg.Run(func() {
		logutil.BgLogger().Info("dumpFileGcChecker started")
		gcTicker := time.NewTicker(do.dumpFileGcChecker.gcLease)
		defer func() {
			gcTicker.Stop()
			logutil.BgLogger().Info("dumpFileGcChecker exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "dumpFileGcCheckerLoop", nil, false)

		for {
			select {
			case <-do.exit:
				return
			case <-gcTicker.C:
				do.dumpFileGcChecker.GCDumpFiles(time.Hour, time.Hour*24*7)
			}
		}
	}, "dumpFileGcChecker")
}

// GetHistoricalStatsWorker gets historical workers
func (do *Domain) GetHistoricalStatsWorker() *HistoricalStatsWorker {
	return do.historicalStatsWorker
}

// EnableDumpHistoricalStats used to control whether enable dump stats for unit test
var enableDumpHistoricalStats atomic.Bool

// StartHistoricalStatsWorker start historical workers running
func (do *Domain) StartHistoricalStatsWorker() {
	if !enableDumpHistoricalStats.Load() {
		return
	}
	do.wg.Run(func() {
		logutil.BgLogger().Info("HistoricalStatsWorker started")
		defer func() {
			logutil.BgLogger().Info("HistoricalStatsWorker exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "HistoricalStatsWorkerLoop", nil, false)

		for {
			select {
			case <-do.exit:
				close(do.historicalStatsWorker.tblCH)
				return
			case tblID, ok := <-do.historicalStatsWorker.tblCH:
				if !ok {
					return
				}
				err := do.historicalStatsWorker.DumpHistoricalStats(tblID, do.StatsHandle())
				if err != nil {
					logutil.BgLogger().Warn("dump historical stats failed", zap.Error(err), zap.Int64("tableID", tblID))
				}
			}
		}
	}, "HistoricalStatsWorker")
}

// StatsHandle returns the statistic handle.
func (do *Domain) StatsHandle() *handle.Handle {
	return do.statsHandle.Load()
}

// CreateStatsHandle is used only for test.
func (do *Domain) CreateStatsHandle(ctx context.Context, initStatsCtx sessionctx.Context) error {
	h, err := handle.NewHandle(
		ctx,
		initStatsCtx,
		do.statsLease,
		do.sysSessionPool,
		&do.sysProcesses,
		do.ddlNotifier,
		do.NextConnID,
		do.ReleaseConnID,
	)
	if err != nil {
		return err
	}
	h.StartWorker()
	do.statsHandle.Store(h)
	return nil
}

// StatsUpdating checks if the stats worker is updating.
func (do *Domain) StatsUpdating() bool {
	return do.statsUpdating.Load() > 0
}

// SetStatsUpdating sets the value of stats updating.
func (do *Domain) SetStatsUpdating(val bool) {
	if val {
		do.statsUpdating.Store(1)
	} else {
		do.statsUpdating.Store(0)
	}
}

// LoadAndUpdateStatsLoop loads and updates stats info.
func (do *Domain) LoadAndUpdateStatsLoop(concurrency int, initStatsCtx sessionctx.Context) error {
	if err := do.UpdateTableStatsLoop(initStatsCtx); err != nil {
		return err
	}
	do.StartLoadStatsSubWorkers(concurrency)
	return nil
}

// UpdateTableStatsLoop creates a goroutine loads stats info and updates stats info in a loop.
// It will also start a goroutine to analyze tables automatically.
// It should be called only once in BootstrapSession.
func (do *Domain) UpdateTableStatsLoop(initStatsCtx sessionctx.Context) error {
	statsHandle, err := handle.NewHandle(
		do.ctx,
		initStatsCtx,
		do.statsLease,
		do.sysSessionPool,
		&do.sysProcesses,
		do.ddlNotifier,
		do.NextConnID,
		do.ReleaseConnID,
	)
	if err != nil {
		return err
	}
	statsHandle.StartWorker()
	do.statsHandle.Store(statsHandle)
	do.ddl.RegisterStatsHandle(statsHandle)
	// Negative stats lease indicates that it is in test or in br binary mode, it does not need update.
	if do.statsLease >= 0 {
		do.wg.Run(do.loadStatsWorker, "loadStatsWorker")
	}
	variable.EnableStatsOwner = do.enableStatsOwner
	variable.DisableStatsOwner = do.disableStatsOwner
	do.statsOwner = do.NewOwnerManager(handle.StatsPrompt, handle.StatsOwnerKey)
	do.statsOwner.SetListener(owner.NewListenersWrapper(statsHandle, do.ddlNotifier))
	if config.GetGlobalConfig().Instance.TiDBEnableStatsOwner.Load() {
		err := do.statsOwner.CampaignOwner()
		if err != nil {
			logutil.BgLogger().Warn("campaign owner failed", zap.Error(err))
			return err
		}
	}
	do.wg.Run(func() {
		do.indexUsageWorker()
	}, "indexUsageWorker")
	if do.statsLease <= 0 {
		// For statsLease > 0, `gcStatsWorker` handles the quit of stats owner.
		do.wg.Run(func() { quitStatsOwner(do, do.statsOwner) }, "quitStatsOwner")
		return nil
	}
	waitStartTask := func(do *Domain, fn func()) {
		select {
		case <-do.StatsHandle().InitStatsDone:
		case <-do.exit: // It may happen that before initStatsDone, tidb receive Ctrl+C
			return
		}
		fn()
	}
	do.SetStatsUpdating(true)
	// The asyncLoadHistogram/dumpColStatsUsageWorker/deltaUpdateTickerWorker doesn't require the stats initialization to be completed.
	// This is because thos workers' primary responsibilities are to update the change delta and handle DDL operations.
	// These tasks need to be in work mod as soon as possible to avoid the problem.
	do.wg.Run(do.asyncLoadHistogram, "asyncLoadHistogram")
	do.wg.Run(do.deltaUpdateTickerWorker, "deltaUpdateTickerWorker")
	do.wg.Run(do.dumpColStatsUsageWorker, "dumpColStatsUsageWorker")
	do.wg.Run(func() { waitStartTask(do, do.gcStatsWorker) }, "gcStatsWorker")

	// Wait for the stats worker to finish the initialization.
	// Otherwise, we may start the auto analyze worker before the stats cache is initialized.
	do.wg.Run(func() { waitStartTask(do, do.autoAnalyzeWorker) }, "autoAnalyzeWorker")
	do.wg.Run(func() { waitStartTask(do, do.analyzeJobsCleanupWorker) }, "analyzeJobsCleanupWorker")
	do.wg.Run(
		func() {
			// The initStatsCtx is used to store the internal session for initializing stats,
			// so we need the gc min start ts calculation to track it as an internal session.
			// Since the session manager may not be ready at this moment, `infosync.StoreInternalSession` can fail.
			// we need to retry until the session manager is ready or the init stats completes.
			for !infosync.StoreInternalSession(initStatsCtx) {
				waitRetry := time.After(time.Second)
				select {
				case <-do.StatsHandle().InitStatsDone:
					return
				case <-waitRetry:
				}
			}
			select {
			case <-do.StatsHandle().InitStatsDone:
			case <-do.exit: // It may happen that before initStatsDone, tidb receive Ctrl+C
				return
			}
			infosync.DeleteInternalSession(initStatsCtx)
		},
		"RemoveInitStatsFromInternalSessions",
	)
	return nil
}

// enableStatsOwner enables this node to execute stats owner jobs.
// Since ownerManager.CampaignOwner will start a new goroutine to run ownerManager.campaignLoop,
// we should make sure that before invoking enableStatsOwner(), stats owner is DISABLE.
func (do *Domain) enableStatsOwner() error {
	if !do.statsOwner.IsOwner() {
		err := do.statsOwner.CampaignOwner()
		return errors.Trace(err)
	}
	return nil
}

// disableStatsOwner disable this node to execute stats owner.
// We should make sure that before invoking disableStatsOwner(), stats owner is ENABLE.
func (do *Domain) disableStatsOwner() error {
	// disable campaign by interrupting campaignLoop
	do.statsOwner.CampaignCancel()
	return nil
}

func quitStatsOwner(do *Domain, mgr owner.Manager) {
	<-do.exit
	mgr.Close()
}

// StartLoadStatsSubWorkers starts sub workers with new sessions to load stats concurrently.
func (do *Domain) StartLoadStatsSubWorkers(concurrency int) {
	statsHandle := do.StatsHandle()
	for range concurrency {
		do.wg.Add(1)
		go statsHandle.SubLoadWorker(do.exit, do.wg)
	}
	logutil.BgLogger().Info("start load stats sub workers", zap.Int("workerCount", concurrency))
}

// NewOwnerManager returns the owner manager for use outside of the domain.
func (do *Domain) NewOwnerManager(prompt, ownerKey string) owner.Manager {
	id := do.ddl.OwnerManager().ID()
	var statsOwner owner.Manager
	if do.etcdClient == nil {
		statsOwner = owner.NewMockManager(do.ctx, id, do.store, ownerKey)
	} else {
		statsOwner = owner.NewOwnerManager(do.ctx, do.etcdClient, prompt, id, ownerKey)
	}
	return statsOwner
}

func (do *Domain) initStats(ctx context.Context) {
	statsHandle := do.StatsHandle()
	defer func() {
		if r := recover(); r != nil {
			logutil.BgLogger().Error("panic when initiating stats", zap.Any("r", r),
				zap.Stack("stack"))
		}
		close(statsHandle.InitStatsDone)
	}()
	t := time.Now()
	liteInitStats := config.GetGlobalConfig().Performance.LiteInitStats
	initstats.InitStatsPercentage.Store(0)
	var err error
	if liteInitStats {
		err = statsHandle.InitStatsLite(ctx)
	} else {
		err = statsHandle.InitStats(ctx, do.InfoSchema())
	}
	initstats.InitStatsPercentage.Store(100)
	if err != nil {
		statslogutil.StatsLogger().Error("Init stats failed", zap.Bool("isLiteInitStats", liteInitStats), zap.Duration("duration", time.Since(t)), zap.Error(err))
	} else {
		statslogutil.StatsLogger().Info("Init stats succeed", zap.Bool("isLiteInitStats", liteInitStats), zap.Duration("duration", time.Since(t)))
	}
}

func (do *Domain) loadStatsWorker() {
	defer util.Recover(metrics.LabelDomain, "loadStatsWorker", nil, false)
	lease := do.statsLease
	if lease == 0 {
		lease = 3 * time.Second
	}
	loadTicker := time.NewTicker(lease)
	defer func() {
		loadTicker.Stop()
		logutil.BgLogger().Info("loadStatsWorker exited.")
	}()

	ctx, cancelFunc := context.WithCancel(context.Background())
	do.cancelFns.mu.Lock()
	do.cancelFns.fns = append(do.cancelFns.fns, cancelFunc)
	do.cancelFns.mu.Unlock()

	do.initStats(ctx)
	statsHandle := do.StatsHandle()
	var err error
	for {
		select {
		case <-loadTicker.C:
			err = statsHandle.Update(ctx, do.InfoSchema())
			if err != nil {
				logutil.BgLogger().Warn("update stats info failed", zap.Error(err))
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) asyncLoadHistogram() {
	defer util.Recover(metrics.LabelDomain, "asyncLoadStats", nil, false)
	lease := do.statsLease
	if lease == 0 {
		lease = 3 * time.Second
	}
	cleanupTicker := time.NewTicker(lease)
	defer func() {
		cleanupTicker.Stop()
		logutil.BgLogger().Info("asyncLoadStats exited.")
	}()
	select {
	case <-do.StatsHandle().InitStatsDone:
	case <-do.exit: // It may happen that before initStatsDone, tidb receive Ctrl+C
		return
	}
	statsHandle := do.StatsHandle()
	var err error
	for {
		select {
		case <-cleanupTicker.C:
			err = statsHandle.LoadNeededHistograms(do.InfoSchema())
			if err != nil {
				statslogutil.StatsErrVerboseSampleLogger().Warn("Load histograms failed", zap.Error(err))
			}
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) indexUsageWorker() {
	defer util.Recover(metrics.LabelDomain, "indexUsageWorker", nil, false)
	gcStatsTicker := time.NewTicker(indexUsageGCDuration)
	handle := do.StatsHandle()
	defer func() {
		logutil.BgLogger().Info("indexUsageWorker exited.")
	}()
	for {
		select {
		case <-do.exit:
			return
		case <-gcStatsTicker.C:
			if err := handle.GCIndexUsage(); err != nil {
				statslogutil.StatsLogger().Error("gc index usage failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) gcStatsWorkerExitPreprocessing() {
	ch := make(chan struct{}, 1)
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		logutil.BgLogger().Info("gcStatsWorker ready to release owner")
		do.statsOwner.Close()
		ch <- struct{}{}
	}()
	select {
	case <-ch:
		logutil.BgLogger().Info("gcStatsWorker exit preprocessing finished")
		return
	case <-timeout.Done():
		logutil.BgLogger().Warn("gcStatsWorker exit preprocessing timeout, force exiting")
		return
	}
}

func (*Domain) deltaUpdateTickerWorkerExitPreprocessing(statsHandle *handle.Handle) {
	ch := make(chan struct{}, 1)
	timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		logutil.BgLogger().Info("deltaUpdateTicker is going to exit, start to flush stats")
		statsHandle.FlushStats()
		ch <- struct{}{}
	}()
	select {
	case <-ch:
		logutil.BgLogger().Info("deltaUpdateTicker exit preprocessing finished")
		return
	case <-timeout.Done():
		logutil.BgLogger().Warn("deltaUpdateTicker exit preprocessing timeout, force exiting")
		return
	}
}

func (do *Domain) gcStatsWorker() {
	defer util.Recover(metrics.LabelDomain, "gcStatsWorker", nil, false)
	logutil.BgLogger().Info("gcStatsWorker started.")
	lease := do.statsLease
	gcStatsTicker := time.NewTicker(100 * lease)
	updateStatsHealthyTicker := time.NewTicker(20 * lease)
	readMemTicker := time.NewTicker(memory.ReadMemInterval)
	statsHandle := do.StatsHandle()
	defer func() {
		gcStatsTicker.Stop()
		readMemTicker.Stop()
		updateStatsHealthyTicker.Stop()
		do.SetStatsUpdating(false)
		logutil.BgLogger().Info("gcStatsWorker exited.")
	}()
	defer util.Recover(metrics.LabelDomain, "gcStatsWorker", nil, false)

	for {
		select {
		case <-do.exit:
			do.gcStatsWorkerExitPreprocessing()
			return
		case <-gcStatsTicker.C:
			if !do.statsOwner.IsOwner() {
				continue
			}
			err := statsHandle.GCStats(do.InfoSchema(), do.GetSchemaLease())
			if err != nil {
				logutil.BgLogger().Warn("GC stats failed", zap.Error(err))
			}
			do.CheckAutoAnalyzeWindows()
		case <-readMemTicker.C:
			memory.ForceReadMemStats()
			do.StatsHandle().StatsCache.TriggerEvict()
		case <-updateStatsHealthyTicker.C:
			statsHandle.UpdateStatsHealthyMetrics()
		}
	}
}

func (do *Domain) dumpColStatsUsageWorker() {
	logutil.BgLogger().Info("dumpColStatsUsageWorker started.")
	lease := do.statsLease
	dumpColStatsUsageTicker := time.NewTicker(100 * lease)
	statsHandle := do.StatsHandle()
	defer func() {
		dumpColStatsUsageTicker.Stop()
		logutil.BgLogger().Info("dumpColStatsUsageWorker exited.")
	}()
	defer util.Recover(metrics.LabelDomain, "dumpColStatsUsageWorker", nil, false)

	for {
		select {
		case <-do.exit:
			return
		case <-dumpColStatsUsageTicker.C:
			err := statsHandle.DumpColStatsUsageToKV()
			if err != nil {
				logutil.BgLogger().Warn("dump column stats usage failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) deltaUpdateTickerWorker() {
	defer util.Recover(metrics.LabelDomain, "deltaUpdateTickerWorker", nil, false)
	logutil.BgLogger().Info("deltaUpdateTickerWorker started.")
	lease := do.statsLease
	// We need to have different nodes trigger tasks at different times to avoid the herd effect.
	randDuration := time.Duration(rand.Int63n(int64(time.Minute)))
	deltaUpdateTicker := time.NewTicker(20*lease + randDuration)
	statsHandle := do.StatsHandle()
	for {
		select {
		case <-do.exit:
			do.deltaUpdateTickerWorkerExitPreprocessing(statsHandle)
			return
		case <-deltaUpdateTicker.C:
			err := statsHandle.DumpStatsDeltaToKV(false)
			if err != nil {
				logutil.BgLogger().Warn("dump stats delta failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) autoAnalyzeWorker() {
	defer util.Recover(metrics.LabelDomain, "autoAnalyzeWorker", nil, false)
	statsHandle := do.StatsHandle()
	analyzeTicker := time.NewTicker(do.statsLease)
	defer func() {
		analyzeTicker.Stop()
		statslogutil.StatsLogger().Info("autoAnalyzeWorker exited.")
	}()
	for {
		select {
		case <-analyzeTicker.C:
			// In order to prevent tidb from being blocked by the auto analyze task during shutdown,
			// a stopautoanalyze is added here for judgment.
			//
			// The reason for checking of stopAutoAnalyze is following:
			// According to the issue#41318, if we don't check stopAutoAnalyze here, the autoAnalyzeWorker will be tricker
			// again when domain.exit is true.
			// The "case <-analyzeTicker.C" condition and "case <-do.exit" condition are satisfied at the same time
			// when the system is already executing the shutdown task.
			// At this time, the Go language will randomly select a case that meets the conditions to execute,
			// and there is a probability that a new autoanalyze task will be started again
			// when the system has already executed the shutdown.
			// Because the time interval of statsLease is much smaller than the execution speed of auto analyze.
			// Therefore, when the current auto analyze is completed,
			// the probability of this happening is very high that the ticker condition and exist condition will be met
			// at the same time.
			// This causes the auto analyze task to be triggered all the time and block the shutdown of tidb.
			if vardef.RunAutoAnalyze.Load() && !do.stopAutoAnalyze.Load() && do.statsOwner.IsOwner() {
				statsHandle.HandleAutoAnalyze()
			}
		case <-do.exit:
			return
		}
	}
}

// analyzeJobsCleanupWorker is a background worker that periodically performs two main tasks:
//
//  1. Garbage Collection: It removes outdated analyze jobs from the statistics handle.
//     This operation is performed every hour and only if the current instance is the owner.
//     Analyze jobs older than 7 days are considered outdated and are removed.
//
//  2. Cleanup: It cleans up corrupted analyze jobs.
//     A corrupted analyze job is one that is in a 'pending' or 'running' state,
//     but is associated with a TiDB instance that is either not currently running or has been restarted.
//     Also, if the analyze job is killed by the user, it is considered corrupted.
//     This operation is performed every 100 stats leases.
//     It first retrieves the list of current analyze processes, then removes any analyze job
//     that is not associated with a current process. Additionally, if the current instance is the owner,
//     it also cleans up corrupted analyze jobs on dead instances.
func (do *Domain) analyzeJobsCleanupWorker() {
	defer util.Recover(metrics.LabelDomain, "analyzeJobsCleanupWorker", nil, false)
	// For GC.
	const gcInterval = time.Hour
	const daysToKeep = 7
	gcTicker := time.NewTicker(gcInterval)
	// For clean up.
	// Default stats lease is 3 * time.Second.
	// So cleanupInterval is 100 * 3 * time.Second = 5 * time.Minute.
	var cleanupInterval = do.statsLease * 100
	cleanupTicker := time.NewTicker(cleanupInterval)
	defer func() {
		gcTicker.Stop()
		cleanupTicker.Stop()
		logutil.BgLogger().Info("analyzeJobsCleanupWorker exited.")
	}()
	statsHandle := do.StatsHandle()
	for {
		select {
		case <-gcTicker.C:
			// Only the owner should perform this operation.
			if do.statsOwner.IsOwner() {
				updateTime := time.Now().AddDate(0, 0, -daysToKeep)
				err := statsHandle.DeleteAnalyzeJobs(updateTime)
				if err != nil {
					logutil.BgLogger().Warn("gc analyze history failed", zap.Error(err))
				}
			}
		case <-cleanupTicker.C:
			sm := do.InfoSyncer().GetSessionManager()
			if sm == nil {
				continue
			}
			analyzeProcessIDs := make(map[uint64]struct{}, 8)
			for _, process := range sm.ShowProcessList() {
				if isAnalyzeTableSQL(process.Info) {
					analyzeProcessIDs[process.ID] = struct{}{}
				}
			}

			err := statsHandle.CleanupCorruptedAnalyzeJobsOnCurrentInstance(analyzeProcessIDs)
			if err != nil {
				logutil.BgLogger().Warn("cleanup analyze jobs on current instance failed", zap.Error(err))
			}

			if do.statsOwner.IsOwner() {
				err = statsHandle.CleanupCorruptedAnalyzeJobsOnDeadInstances()
				if err != nil {
					logutil.BgLogger().Warn("cleanup analyze jobs on dead instances failed", zap.Error(err))
				}
			}
		case <-do.exit:
			return
		}
	}
}

func isAnalyzeTableSQL(sql string) bool {
	// Get rid of the comments.
	normalizedSQL := parser.Normalize(sql, "ON")
	return strings.HasPrefix(normalizedSQL, "analyze table")
}

// ExpensiveQueryHandle returns the expensive query handle.
func (do *Domain) ExpensiveQueryHandle() *expensivequery.Handle {
	return do.expensiveQueryHandle
}

// MemoryUsageAlarmHandle returns the memory usage alarm handle.
func (do *Domain) MemoryUsageAlarmHandle() *memoryusagealarm.Handle {
	return do.memoryUsageAlarmHandle
}

// ServerMemoryLimitHandle returns the expensive query handle.
func (do *Domain) ServerMemoryLimitHandle() *servermemorylimit.Handle {
	return do.serverMemoryLimitHandle
}

const (
	privilegeKey          = "/tidb/privilege"
	sysVarCacheKey        = "/tidb/sysvars"
	tiflashComputeNodeKey = "/tiflash/new_tiflash_compute_nodes"
)

// PrivilegeEvent is the message definition for NotifyUpdatePrivilege(), encoded in json.
// TiDB old version do not use no such message.
type PrivilegeEvent struct {
	All      bool
	ServerID uint64
	UserList []string
}

// NotifyUpdateAllUsersPrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdateAllUsersPrivilege() error {
	return do.notifyUpdatePrivilege(PrivilegeEvent{All: true})
}

// NotifyUpdatePrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdatePrivilege(userList []string) error {
	return do.notifyUpdatePrivilege(PrivilegeEvent{UserList: userList})
}

func (do *Domain) notifyUpdatePrivilege(event PrivilegeEvent) error {
	// No matter skip-grant-table is configured or not, sending an etcd message is required.
	// Because we need to tell other TiDB instances to update privilege data, say, we're changing the
	// password using a special TiDB instance and want the new password to take effect.
	if do.etcdClient != nil {
		event.ServerID = do.serverID
		data, err := json.Marshal(event)
		if err != nil {
			return errors.Trace(err)
		}
		if uint64(len(data)) > size.MB {
			logutil.BgLogger().Warn("notify update privilege message too large", zap.ByteString("value", data))
		}
		row := do.etcdClient.KV
		_, err = row.Put(do.ctx, privilegeKey, string(data))
		if err != nil {
			logutil.BgLogger().Warn("notify update privilege failed", zap.Error(err))
		}
	}

	// If skip-grant-table is configured, do not flush privileges.
	// Because LoadPrivilegeLoop does not run and the privilege Handle is nil,
	// the call to do.PrivilegeHandle().Update would panic.
	if config.GetGlobalConfig().Security.SkipGrantTable {
		return nil
	}

	return privReloadEvent(do.PrivilegeHandle(), &event)
}

// NotifyUpdateSysVarCache updates the sysvar cache key in etcd, which other TiDB
// clients are subscribed to for updates. For the caller, the cache is also built
// synchronously so that the effect is immediate.
func (do *Domain) NotifyUpdateSysVarCache(updateLocal bool) {
	if do.etcdClient != nil {
		row := do.etcdClient.KV
		_, err := row.Put(context.Background(), sysVarCacheKey, "")
		if err != nil {
			logutil.BgLogger().Warn("notify update sysvar cache failed", zap.Error(err))
		}
	}
	// update locally
	if updateLocal {
		if err := do.rebuildSysVarCache(nil); err != nil {
			logutil.BgLogger().Error("rebuilding sysvar cache failed", zap.Error(err))
		}
	}
}

// LoadSigningCertLoop loads the signing cert periodically to make sure it's fresh new.
func (do *Domain) LoadSigningCertLoop(signingCert, signingKey string) {
	sessionstates.SetCertPath(signingCert)
	sessionstates.SetKeyPath(signingKey)

	do.wg.Run(func() {
		defer func() {
			logutil.BgLogger().Debug("loadSigningCertLoop exited.")
		}()
		defer util.Recover(metrics.LabelDomain, "LoadSigningCertLoop", nil, false)

		for {
			select {
			case <-time.After(sessionstates.LoadCertInterval):
				sessionstates.ReloadSigningCert()
			case <-do.exit:
				return
			}
		}
	}, "loadSigningCertLoop")
}

// ServerID gets serverID.
func (do *Domain) ServerID() uint64 {
	return atomic.LoadUint64(&do.serverID)
}

// IsLostConnectionToPD indicates lost connection to PD or not.
func (do *Domain) IsLostConnectionToPD() bool {
	return do.isLostConnectionToPD.Load() != 0
}

// NextConnID return next connection ID.
func (do *Domain) NextConnID() uint64 {
	return do.connIDAllocator.NextID()
}

// ReleaseConnID releases connection ID.
func (do *Domain) ReleaseConnID(connID uint64) {
	do.connIDAllocator.Release(connID)
}

const (
	serverIDEtcdPath               = "/tidb/server_id"
	refreshServerIDRetryCnt        = 3
	acquireServerIDRetryInterval   = 300 * time.Millisecond
	acquireServerIDTimeout         = 10 * time.Second
	retrieveServerIDSessionTimeout = 10 * time.Second

	acquire32BitsServerIDRetryCnt = 3
)

var (
	// serverIDTTL should be LONG ENOUGH to avoid barbarically killing an on-going long-run SQL.
	serverIDTTL = 12 * time.Hour
	// serverIDTimeToKeepAlive is the interval that we keep serverID TTL alive periodically.
	serverIDTimeToKeepAlive = 5 * time.Minute
	// serverIDTimeToCheckPDConnectionRestored is the interval that we check connection to PD restored (after broken) periodically.
	serverIDTimeToCheckPDConnectionRestored = 10 * time.Second
	// lostConnectionToPDTimeout is the duration that when TiDB cannot connect to PD excceeds this limit,
	//   we realize the connection to PD is lost utterly, and server ID acquired before should be released.
	//   Must be SHORTER than `serverIDTTL`.
	lostConnectionToPDTimeout = 6 * time.Hour
)

var (
	ldflagIsGlobalKillTest                        = "0"  // 1:Yes, otherwise:No.
	ldflagServerIDTTL                             = "10" // in seconds.
	ldflagServerIDTimeToKeepAlive                 = "1"  // in seconds.
	ldflagServerIDTimeToCheckPDConnectionRestored = "1"  // in seconds.
	ldflagLostConnectionToPDTimeout               = "5"  // in seconds.
)

func initByLDFlagsForGlobalKill() {
	if ldflagIsGlobalKillTest == "1" {
		var (
			i   int
			err error
		)

		if i, err = strconv.Atoi(ldflagServerIDTTL); err != nil {
			panic("invalid ldflagServerIDTTL")
		}
		serverIDTTL = time.Duration(i) * time.Second

		if i, err = strconv.Atoi(ldflagServerIDTimeToKeepAlive); err != nil {
			panic("invalid ldflagServerIDTimeToKeepAlive")
		}
		serverIDTimeToKeepAlive = time.Duration(i) * time.Second

		if i, err = strconv.Atoi(ldflagServerIDTimeToCheckPDConnectionRestored); err != nil {
			panic("invalid ldflagServerIDTimeToCheckPDConnectionRestored")
		}
		serverIDTimeToCheckPDConnectionRestored = time.Duration(i) * time.Second

		if i, err = strconv.Atoi(ldflagLostConnectionToPDTimeout); err != nil {
			panic("invalid ldflagLostConnectionToPDTimeout")
		}
		lostConnectionToPDTimeout = time.Duration(i) * time.Second

		logutil.BgLogger().Info("global_kill_test is enabled", zap.Duration("serverIDTTL", serverIDTTL),
			zap.Duration("serverIDTimeToKeepAlive", serverIDTimeToKeepAlive),
			zap.Duration("serverIDTimeToCheckPDConnectionRestored", serverIDTimeToCheckPDConnectionRestored),
			zap.Duration("lostConnectionToPDTimeout", lostConnectionToPDTimeout))
	}
}

func (do *Domain) retrieveServerIDSession(ctx context.Context) (*concurrency.Session, error) {
	if do.serverIDSession != nil {
		return do.serverIDSession, nil
	}

	// `etcdClient.Grant` needs a shortterm timeout, to avoid blocking if connection to PD lost,
	//   while `etcdClient.KeepAlive` should be longterm.
	//   So we separately invoke `etcdClient.Grant` and `concurrency.NewSession` with leaseID.
	childCtx, cancel := context.WithTimeout(ctx, retrieveServerIDSessionTimeout)
	resp, err := do.etcdClient.Grant(childCtx, int64(serverIDTTL.Seconds()))
	cancel()
	if err != nil {
		logutil.BgLogger().Error("retrieveServerIDSession.Grant fail", zap.Error(err))
		return nil, err
	}
	leaseID := resp.ID

	session, err := concurrency.NewSession(do.etcdClient,
		concurrency.WithLease(leaseID), concurrency.WithContext(context.Background()))
	if err != nil {
		logutil.BgLogger().Error("retrieveServerIDSession.NewSession fail", zap.Error(err))
		return nil, err
	}
	do.serverIDSession = session
	return session, nil
}

func (do *Domain) acquireServerID(ctx context.Context) error {
	atomic.StoreUint64(&do.serverID, 0)

	session, err := do.retrieveServerIDSession(ctx)
	if err != nil {
		return err
	}

	conflictCnt := 0
	for {
		var proposeServerID uint64
		if config.GetGlobalConfig().Enable32BitsConnectionID {
			proposeServerID, err = do.proposeServerID(ctx, conflictCnt)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			// get a random serverID: [1, MaxServerID64]
			proposeServerID = uint64(rand.Int63n(int64(globalconn.MaxServerID64)) + 1) // #nosec G404
		}

		key := fmt.Sprintf("%s/%v", serverIDEtcdPath, proposeServerID)
		cmp := clientv3.Compare(clientv3.CreateRevision(key), "=", 0)
		value := "0"

		childCtx, cancel := context.WithTimeout(ctx, acquireServerIDTimeout)
		txn := do.etcdClient.Txn(childCtx)
		t := txn.If(cmp)
		resp, err := t.Then(clientv3.OpPut(key, value, clientv3.WithLease(session.Lease()))).Commit()
		cancel()
		if err != nil {
			return err
		}
		if !resp.Succeeded {
			logutil.BgLogger().Info("propose serverID exists, try again", zap.Uint64("proposeServerID", proposeServerID))
			time.Sleep(acquireServerIDRetryInterval)
			conflictCnt++
			continue
		}

		atomic.StoreUint64(&do.serverID, proposeServerID)
		logutil.BgLogger().Info("acquireServerID", zap.Uint64("serverID", do.ServerID()),
			zap.String("lease id", strconv.FormatInt(int64(session.Lease()), 16)))
		return nil
	}
}

func (do *Domain) releaseServerID(context.Context) {
	serverID := do.ServerID()
	if serverID == 0 {
		return
	}
	atomic.StoreUint64(&do.serverID, 0)

	if do.etcdClient == nil {
		return
	}
	key := fmt.Sprintf("%s/%v", serverIDEtcdPath, serverID)
	err := ddlutil.DeleteKeyFromEtcd(key, do.etcdClient, refreshServerIDRetryCnt, acquireServerIDTimeout)
	if err != nil {
		logutil.BgLogger().Error("releaseServerID fail", zap.Uint64("serverID", serverID), zap.Error(err))
	} else {
		logutil.BgLogger().Info("releaseServerID succeed", zap.Uint64("serverID", serverID))
	}
}

// propose server ID by random.
func (*Domain) proposeServerID(ctx context.Context, conflictCnt int) (uint64, error) {
	// get a random server ID in range [min, max]
	randomServerID := func(minv uint64, maxv uint64) uint64 {
		return uint64(rand.Int63n(int64(maxv-minv+1)) + int64(minv)) // #nosec G404
	}

	if conflictCnt < acquire32BitsServerIDRetryCnt {
		// get existing server IDs.
		allServerInfo, err := infosync.GetAllServerInfo(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		// `allServerInfo` contains current TiDB.
		if float32(len(allServerInfo)) <= 0.9*float32(globalconn.MaxServerID32) {
			serverIDs := make(map[uint64]struct{}, len(allServerInfo))
			for _, info := range allServerInfo {
				serverID := info.ServerIDGetter()
				if serverID <= globalconn.MaxServerID32 {
					serverIDs[serverID] = struct{}{}
				}
			}

			for range 15 {
				randServerID := randomServerID(1, globalconn.MaxServerID32)
				if _, ok := serverIDs[randServerID]; !ok {
					return randServerID, nil
				}
			}
		}
		logutil.BgLogger().Info("upgrade to 64 bits server ID due to used up", zap.Int("len(allServerInfo)", len(allServerInfo)))
	} else {
		logutil.BgLogger().Info("upgrade to 64 bits server ID due to conflict", zap.Int("conflictCnt", conflictCnt))
	}

	// upgrade to 64 bits.
	return randomServerID(globalconn.MaxServerID32+1, globalconn.MaxServerID64), nil
}

func (do *Domain) refreshServerIDTTL(ctx context.Context) error {
	session, err := do.retrieveServerIDSession(ctx)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%v", serverIDEtcdPath, do.ServerID())
	value := "0"
	err = ddlutil.PutKVToEtcd(ctx, do.etcdClient, refreshServerIDRetryCnt, key, value, clientv3.WithLease(session.Lease()))
	if err != nil {
		logutil.BgLogger().Error("refreshServerIDTTL fail", zap.Uint64("serverID", do.ServerID()), zap.Error(err))
	} else {
		logutil.BgLogger().Info("refreshServerIDTTL succeed", zap.Uint64("serverID", do.ServerID()),
			zap.String("lease id", strconv.FormatInt(int64(session.Lease()), 16)))
	}
	return err
}

func (do *Domain) serverIDKeeper() {
	defer func() {
		do.wg.Done()
		logutil.BgLogger().Info("serverIDKeeper exited.")
	}()
	defer util.Recover(metrics.LabelDomain, "serverIDKeeper", func() {
		logutil.BgLogger().Info("recover serverIDKeeper.")
		// should be called before `do.wg.Done()`, to ensure that Domain.Close() waits for the new `serverIDKeeper()` routine.
		do.wg.Add(1)
		go do.serverIDKeeper()
	}, false)

	tickerKeepAlive := time.NewTicker(serverIDTimeToKeepAlive)
	tickerCheckRestored := time.NewTicker(serverIDTimeToCheckPDConnectionRestored)
	defer func() {
		tickerKeepAlive.Stop()
		tickerCheckRestored.Stop()
	}()

	blocker := make(chan struct{}) // just used for blocking the sessionDone() when session is nil.
	sessionDone := func() <-chan struct{} {
		if do.serverIDSession == nil {
			return blocker
		}
		return do.serverIDSession.Done()
	}

	var lastSucceedTimestamp time.Time

	onConnectionToPDRestored := func() {
		logutil.BgLogger().Info("restored connection to PD")
		do.isLostConnectionToPD.Store(0)
		lastSucceedTimestamp = time.Now()

		if err := do.info.StoreServerInfo(context.Background()); err != nil {
			logutil.BgLogger().Error("StoreServerInfo failed", zap.Error(err))
		}
	}

	onConnectionToPDLost := func() {
		logutil.BgLogger().Warn("lost connection to PD")
		do.isLostConnectionToPD.Store(1)

		// Kill all connections when lost connection to PD,
		//   to avoid the possibility that another TiDB instance acquires the same serverID and generates a same connection ID,
		//   which will lead to a wrong connection killed.
		do.InfoSyncer().GetSessionManager().KillAllConnections()
	}

	for {
		select {
		case <-tickerKeepAlive.C:
			if !do.IsLostConnectionToPD() {
				if err := do.refreshServerIDTTL(context.Background()); err == nil {
					lastSucceedTimestamp = time.Now()
				} else {
					if lostConnectionToPDTimeout > 0 && time.Since(lastSucceedTimestamp) > lostConnectionToPDTimeout {
						onConnectionToPDLost()
					}
				}
			}
		case <-tickerCheckRestored.C:
			if do.IsLostConnectionToPD() {
				if err := do.acquireServerID(context.Background()); err == nil {
					onConnectionToPDRestored()
				}
			}
		case <-sessionDone():
			// inform that TTL of `serverID` is expired. See https://godoc.org/github.com/coreos/etcd/clientv3/concurrency#Session.Done
			//   Should be in `IsLostConnectionToPD` state, as `lostConnectionToPDTimeout` is shorter than `serverIDTTL`.
			//   So just set `do.serverIDSession = nil` to restart `serverID` session in `retrieveServerIDSession()`.
			logutil.BgLogger().Info("serverIDSession need restart")
			do.serverIDSession = nil
		case <-do.exit:
			return
		}
	}
}

// StartTTLJobManager creates and starts the ttl job manager
func (do *Domain) StartTTLJobManager() {
	ttlJobManager := ttlworker.NewJobManager(do.ddl.GetID(), do.advancedSysSessionPool, do.store, do.etcdClient, do.ddl.OwnerManager().IsOwner)
	do.ttlJobManager.Store(ttlJobManager)
	ttlJobManager.Start()
}

// TTLJobManager returns the ttl job manager on this domain
func (do *Domain) TTLJobManager() *ttlworker.JobManager {
	return do.ttlJobManager.Load()
}

// StopAutoAnalyze stops (*Domain).autoAnalyzeWorker to launch new auto analyze jobs.
func (do *Domain) StopAutoAnalyze() {
	do.stopAutoAnalyze.Store(true)
}

// InitInstancePlanCache initializes the instance level plan cache for this Domain.
func (do *Domain) InitInstancePlanCache() {
	hardLimit := vardef.InstancePlanCacheMaxMemSize.Load()
	softLimit := float64(hardLimit) * (1 - vardef.InstancePlanCacheReservedPercentage.Load())
	do.instancePlanCache = NewInstancePlanCache(int64(softLimit), hardLimit)
	// use a separate goroutine to avoid the eviction blocking other operations.
	do.wg.Run(do.planCacheEvictTrigger, "planCacheEvictTrigger")
	do.wg.Run(do.planCacheMetricsAndVars, "planCacheMetricsAndVars")
}

// GetInstancePlanCache returns the instance level plan cache in this Domain.
func (do *Domain) GetInstancePlanCache() sessionctx.InstancePlanCache {
	return do.instancePlanCache
}

// planCacheMetricsAndVars updates metrics and variables for Instance Plan Cache periodically.
func (do *Domain) planCacheMetricsAndVars() {
	defer util.Recover(metrics.LabelDomain, "planCacheMetricsAndVars", nil, false)
	ticker := time.NewTicker(time.Second * 15) // 15s by default
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("planCacheMetricsAndVars exited.")
	}()

	for {
		select {
		case <-ticker.C:
			// update limits
			hardLimit := vardef.InstancePlanCacheMaxMemSize.Load()
			softLimit := int64(float64(hardLimit) * (1 - vardef.InstancePlanCacheReservedPercentage.Load()))
			curSoft, curHard := do.instancePlanCache.GetLimits()
			if curSoft != softLimit || curHard != hardLimit {
				do.instancePlanCache.SetLimits(softLimit, hardLimit)
			}

			// update the metrics
			size := do.instancePlanCache.Size()
			memUsage := do.instancePlanCache.MemUsage()
			metrics2.GetPlanCacheInstanceNumCounter(true).Set(float64(size))
			metrics2.GetPlanCacheInstanceMemoryUsage(true).Set(float64(memUsage))
		case <-do.exit:
			return
		}
	}
}

// planCacheEvictTrigger triggers the plan cache eviction periodically.
func (do *Domain) planCacheEvictTrigger() {
	defer util.Recover(metrics.LabelDomain, "planCacheEvictTrigger", nil, false)
	ticker := time.NewTicker(time.Second * 30) // 30s by default
	defer func() {
		ticker.Stop()
		logutil.BgLogger().Info("planCacheEvictTrigger exited.")
	}()

	for {
		select {
		case <-ticker.C:
			// trigger the eviction
			begin := time.Now()
			enabled := vardef.EnableInstancePlanCache.Load()
			detailInfo, numEvicted := do.instancePlanCache.Evict(!enabled) // evict all if the plan cache is disabled
			metrics2.GetPlanCacheInstanceEvict().Set(float64(numEvicted))
			if numEvicted > 0 {
				logutil.BgLogger().Info("instance plan eviction",
					zap.String("detail", detailInfo),
					zap.Int64("num_evicted", int64(numEvicted)),
					zap.Duration("time_spent", time.Since(begin)))
			}
		case <-do.exit:
			return
		}
	}
}

// SetupWorkloadBasedLearningWorker sets up all of the workload based learning workers.
func (do *Domain) SetupWorkloadBasedLearningWorker() {
	wbLearningHandle := workloadlearning.NewWorkloadLearningHandle(do.sysSessionPool)
	wbCacheWorker := workloadlearning.NewWLCacheWorker(do.sysSessionPool)
	// Start the workload based learning worker to analyze the read workload by statement_summary.
	do.wg.Run(
		func() {
			do.readTableCostWorker(wbLearningHandle, wbCacheWorker)
		},
		"readTableCostWorker",
	)
	// TODO: Add more workers for other workload based learning tasks.
}

// readTableCostWorker is a background worker that periodically analyze the read path table cost by statement_summary.
func (do *Domain) readTableCostWorker(wbLearningHandle *workloadlearning.Handle, wbCacheWorker *workloadlearning.WLCacheWorker) {
	// Recover the panic and log the error when worker exit.
	defer util.Recover(metrics.LabelDomain, "readTableCostWorker", nil, false)
	readTableCostTicker := time.NewTicker(vardef.WorkloadBasedLearningInterval.Load())
	defer func() {
		readTableCostTicker.Stop()
		logutil.BgLogger().Info("readTableCostWorker exited.")
	}()
	for {
		select {
		case <-readTableCostTicker.C:
			if vardef.EnableWorkloadBasedLearning.Load() && do.statsOwner.IsOwner() {
				wbLearningHandle.HandleTableReadCost(do.InfoSchema())
				wbCacheWorker.UpdateTableReadCostCache()
			}
		case <-do.exit:
			return
		}
	}
}

func init() {
	initByLDFlagsForGlobalKill()
}

var (
	// ErrInfoSchemaExpired returns the error that information schema is out of date.
	ErrInfoSchemaExpired = dbterror.ClassDomain.NewStd(errno.ErrInfoSchemaExpired)
	// ErrInfoSchemaChanged returns the error that information schema is changed.
	ErrInfoSchemaChanged = dbterror.ClassDomain.NewStdErr(errno.ErrInfoSchemaChanged,
		mysql.Message(errno.MySQLErrName[errno.ErrInfoSchemaChanged].Raw+". "+kv.TxnRetryableMark, nil))
)

// SysProcesses holds the sys processes infos
type SysProcesses struct {
	mu      *sync.RWMutex
	procMap map[uint64]sysproctrack.TrackProc
}

// Track tracks the sys process into procMap
func (s *SysProcesses) Track(id uint64, proc sysproctrack.TrackProc) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if oldProc, ok := s.procMap[id]; ok && oldProc != proc {
		return errors.Errorf("The ID is in use: %v", id)
	}
	s.procMap[id] = proc
	proc.GetSessionVars().ConnectionID = id
	proc.GetSessionVars().SQLKiller.Reset()
	return nil
}

// UnTrack removes the sys process from procMap
func (s *SysProcesses) UnTrack(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if proc, ok := s.procMap[id]; ok {
		delete(s.procMap, id)
		proc.GetSessionVars().ConnectionID = 0
		proc.GetSessionVars().SQLKiller.Reset()
	}
}

// GetSysProcessList gets list of system ProcessInfo
func (s *SysProcesses) GetSysProcessList() map[uint64]*util.ProcessInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rs := make(map[uint64]*util.ProcessInfo)
	for connID, proc := range s.procMap {
		// if session is still tracked in this map, it's not returned to sysSessionPool yet
		if pi := proc.ShowProcess(); pi != nil && pi.ID == connID {
			rs[connID] = pi
		}
	}
	return rs
}

// KillSysProcess kills sys process with specified ID
func (s *SysProcesses) KillSysProcess(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if proc, ok := s.procMap[id]; ok {
		proc.GetSessionVars().SQLKiller.SendKillSignal(sqlkiller.QueryInterrupted)
	}
}
