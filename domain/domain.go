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
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"crypto/tls"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/etcd/clientv3"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/juju/errors"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/terror"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
	"google.golang.org/grpc"
)

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store           kv.Storage
	infoHandle      *infoschema.Handle
	privHandle      *privileges.Handle
	statsHandle     unsafe.Pointer
	statsLease      time.Duration
	ddl             ddl.DDL
	m               sync.Mutex
	SchemaValidator SchemaValidator
	sysSessionPool  *pools.ResourcePool
	exit            chan struct{}
	etcdClient      *clientv3.Client
	wg              sync.WaitGroup

	MockReloadFailed MockFailure // It mocks reload failed.
}

// loadInfoSchema loads infoschema at startTS into handle, usedSchemaVersion is the currently used
// infoschema version, if it is the same as the schema version at startTS, we don't need to reload again.
// It returns the latest schema version, the changed table IDs, whether it's a full load and an error.
func (do *Domain) loadInfoSchema(handle *infoschema.Handle, usedSchemaVersion int64, startTS uint64) (int64, []int64, bool, error) {
	var fullLoad bool
	snapshot, err := do.store.GetSnapshot(kv.NewVersion(startTS))
	if err != nil {
		return 0, nil, fullLoad, errors.Trace(err)
	}
	m := meta.NewSnapshotMeta(snapshot)
	latestSchemaVersion, err := m.GetSchemaVersion()
	if err != nil {
		return 0, nil, fullLoad, errors.Trace(err)
	}
	if usedSchemaVersion != 0 && usedSchemaVersion == latestSchemaVersion {
		return latestSchemaVersion, nil, fullLoad, nil
	}

	// Update self schema version to etcd.
	defer func() {
		if err != nil {
			log.Info("[ddl] not update self schema version to etcd")
			return
		}
		err = do.ddl.SchemaSyncer().UpdateSelfVersion(goctx.Background(), latestSchemaVersion)
		if err != nil {
			log.Infof("[ddl] update self version from %v to %v failed %v", usedSchemaVersion, latestSchemaVersion, err)
		}
	}()

	startTime := time.Now()
	ok, tblIDs, err := do.tryLoadSchemaDiffs(m, usedSchemaVersion, latestSchemaVersion)
	if err != nil {
		// We can fall back to full load, don't need to return the error.
		log.Errorf("[ddl] failed to load schema diff err %v", err)
	}
	if ok {
		log.Infof("[ddl] diff load InfoSchema from version %d to %d, in %v",
			usedSchemaVersion, latestSchemaVersion, time.Since(startTime))
		return latestSchemaVersion, tblIDs, fullLoad, nil
	}

	fullLoad = true
	schemas, err := do.fetchAllSchemasWithTables(m)
	if err != nil {
		return 0, nil, fullLoad, errors.Trace(err)
	}

	newISBuilder, err := infoschema.NewBuilder(handle).InitWithDBInfos(schemas, latestSchemaVersion)
	if err != nil {
		return 0, nil, fullLoad, errors.Trace(err)
	}
	log.Infof("[ddl] full load InfoSchema from version %d to %d, in %v",
		usedSchemaVersion, latestSchemaVersion, time.Since(startTime))
	newISBuilder.Build()
	return latestSchemaVersion, nil, fullLoad, nil
}

func (do *Domain) fetchAllSchemasWithTables(m *meta.Meta) ([]*model.DBInfo, error) {
	allSchemas, err := m.ListDatabases()
	if err != nil {
		return nil, errors.Trace(err)
	}
	splittedSchemas := do.splitForConcurrentFetch(allSchemas)
	doneCh := make(chan error, len(splittedSchemas))
	for _, schemas := range splittedSchemas {
		go do.fetchSchemasWithTables(schemas, m, doneCh)
	}
	for range splittedSchemas {
		err = <-doneCh
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return allSchemas, nil
}

const fetchSchemaConcurrency = 8

func (do *Domain) splitForConcurrentFetch(schemas []*model.DBInfo) [][]*model.DBInfo {
	groupSize := (len(schemas) + fetchSchemaConcurrency - 1) / fetchSchemaConcurrency
	splitted := make([][]*model.DBInfo, 0, fetchSchemaConcurrency)
	schemaCnt := len(schemas)
	for i := 0; i < schemaCnt; i += groupSize {
		end := i + groupSize
		if end > schemaCnt {
			end = schemaCnt
		}
		splitted = append(splitted, schemas[i:end])
	}
	return splitted
}

func (do *Domain) fetchSchemasWithTables(schemas []*model.DBInfo, m *meta.Meta, done chan error) {
	for _, di := range schemas {
		if di.State != model.StatePublic {
			// schema is not public, can't be used outside.
			continue
		}
		tables, err := m.ListTables(di.ID)
		if err != nil {
			done <- err
			return
		}
		di.Tables = make([]*model.TableInfo, 0, len(tables))
		for _, tbl := range tables {
			if tbl.State != model.StatePublic {
				// schema is not public, can't be used outside.
				continue
			}
			di.Tables = append(di.Tables, tbl)
		}
	}
	done <- nil
}

const (
	initialVersion         = 0
	maxNumberOfDiffsToLoad = 100
)

func isTooOldSchema(usedVersion, newVersion int64) bool {
	if usedVersion == initialVersion || newVersion-usedVersion > maxNumberOfDiffsToLoad {
		return true
	}
	return false
}

// tryLoadSchemaDiffs tries to only load latest schema changes.
// Return true if the schema is loaded successfully.
// Return false if the schema can not be loaded by schema diff, then we need to do full load.
// The second returned value is the delta updated table IDs.
func (do *Domain) tryLoadSchemaDiffs(m *meta.Meta, usedVersion, newVersion int64) (bool, []int64, error) {
	// If there isn't any used version, or used version is too old, we do full load.
	if isTooOldSchema(usedVersion, newVersion) {
		return false, nil, nil
	}
	if usedVersion > newVersion {
		// When user use History Read feature, history schema will be loaded.
		// usedVersion may be larger than newVersion, full load is needed.
		return false, nil, nil
	}
	var diffs []*model.SchemaDiff
	for usedVersion < newVersion {
		usedVersion++
		diff, err := m.GetSchemaDiff(usedVersion)
		if err != nil {
			return false, nil, errors.Trace(err)
		}
		if diff == nil {
			// If diff is missing for any version between used and new version, we fall back to full reload.
			return false, nil, nil
		}
		diffs = append(diffs, diff)
	}
	builder := infoschema.NewBuilder(do.infoHandle).InitWithOldInfoSchema()
	tblIDs := make([]int64, 0, len(diffs))
	for _, diff := range diffs {
		ids, err := builder.ApplyDiff(m, diff)
		if err != nil {
			return false, nil, errors.Trace(err)
		}
		tblIDs = append(tblIDs, ids...)
	}
	builder.Build()
	return true, tblIDs, nil
}

// InfoSchema gets information schema from domain.
func (do *Domain) InfoSchema() infoschema.InfoSchema {
	return do.infoHandle.Get()
}

// GetSnapshotInfoSchema gets a snapshot information schema.
func (do *Domain) GetSnapshotInfoSchema(snapshotTS uint64) (infoschema.InfoSchema, error) {
	snapHandle := do.infoHandle.EmptyClone()
	_, _, _, err := do.loadInfoSchema(snapHandle, do.infoHandle.Get().SchemaMetaVersion(), snapshotTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return snapHandle.Get(), nil
}

// DDL gets DDL from domain.
func (do *Domain) DDL() ddl.DDL {
	return do.ddl
}

// Store gets KV store from domain.
func (do *Domain) Store() kv.Storage {
	return do.store
}

// GetScope gets the status variables scope.
func (do *Domain) GetScope(status string) variable.ScopeFlag {
	// Now domain status variables scope are all default scope.
	return variable.DefaultStatusVarScopeFlag
}

func (do *Domain) mockReloadFailed() error {
	return errors.New("mock reload failed")
}

// Reload reloads InfoSchema.
// It's public in order to do the test.
func (do *Domain) Reload() error {
	// for test
	if do.MockReloadFailed.getValue() {
		return do.mockReloadFailed()
	}

	// Lock here for only once at the same time.
	do.m.Lock()
	defer do.m.Unlock()

	startTime := time.Now()

	var err error
	var latestSchemaVersion int64

	ver, err := do.store.CurrentVersion()
	if err != nil {
		return errors.Trace(err)
	}

	schemaVersion := int64(0)
	oldInfoSchema := do.infoHandle.Get()
	if oldInfoSchema != nil {
		schemaVersion = oldInfoSchema.SchemaMetaVersion()
	}

	var (
		fullLoad        bool
		changedTableIDs []int64
	)
	latestSchemaVersion, changedTableIDs, fullLoad, err = do.loadInfoSchema(do.infoHandle, schemaVersion, ver.Ver)
	loadSchemaDuration.Observe(time.Since(startTime).Seconds())
	if err != nil {
		loadSchemaCounter.WithLabelValues("failed").Inc()
		return errors.Trace(err)
	}
	loadSchemaCounter.WithLabelValues("succ").Inc()

	if fullLoad {
		log.Info("[ddl] full load and reset schema validator.")
		do.SchemaValidator.Reset()
	}
	do.SchemaValidator.Update(ver.Ver, schemaVersion, latestSchemaVersion, changedTableIDs)

	lease := do.DDL().GetLease()
	sub := time.Since(startTime)
	if sub > lease && lease > 0 {
		log.Warnf("[ddl] loading schema takes a long time %v", sub)
	}

	return nil
}

func (do *Domain) loadSchemaInLoop(lease time.Duration) {
	defer do.wg.Done()
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
	syncer := do.ddl.SchemaSyncer()

	for {
		select {
		case <-ticker.C:
			err := do.Reload()
			if err != nil {
				log.Errorf("[ddl] reload schema in loop err %v", errors.ErrorStack(err))
			}
		case <-syncer.GlobalVersionCh():
			err := do.Reload()
			if err != nil {
				log.Errorf("[ddl] reload schema in loop err %v", errors.ErrorStack(err))
			}
		case <-syncer.Done():
			// The schema syncer stops, we need stop the schema validator to synchronize the schema version.
			log.Info("[ddl] reload schema in loop, schema syncer need restart")
			do.SchemaValidator.Stop()
			err := do.mustRestartSyncer()
			if err != nil {
				log.Errorf("[ddl] reload schema in loop, schema syncer restart err %v", errors.ErrorStack(err))
				break
			}
			do.SchemaValidator.Restart()
		case <-do.exit:
			return
		}
	}
}

// mustRestartSyncer tries to restart the SchemaSyncer.
// It returns until it's successful or the domain is stoped.
func (do *Domain) mustRestartSyncer() error {
	ctx := goctx.Background()
	syncer := do.ddl.SchemaSyncer()

	for {
		err := syncer.Restart(ctx)
		if err == nil {
			return nil
		}
		// If the domain has stopped, we return an error immediately.
		select {
		case <-do.exit:
			return errors.Trace(err)
		default:
		}
		time.Sleep(time.Second)
		log.Infof("[ddl] restart the schema syncer failed %v", err)
	}
}

// Close closes the Domain and release its resource.
func (do *Domain) Close() {
	if do.ddl != nil {
		terror.Log(errors.Trace(do.ddl.Stop()))
	}
	close(do.exit)
	if do.etcdClient != nil {
		terror.Log(errors.Trace(do.etcdClient.Close()))
	}
	do.sysSessionPool.Close()
	do.wg.Wait()
}

type ddlCallback struct {
	ddl.BaseCallback
	do *Domain
}

func (c *ddlCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	log.Infof("[ddl] on DDL change, must reload")

	err = c.do.Reload()
	if err != nil {
		log.Errorf("[ddl] on DDL change reload err %v", err)
	}

	return nil
}

// MockFailure mocks reload failed.
// It's used for fixing data race in tests.
type MockFailure struct {
	sync.RWMutex
	val bool // val is true means we need to mock reload failed.
}

// SetValue sets whether we need to mock reload failed.
func (m *MockFailure) SetValue(isFailed bool) {
	m.Lock()
	defer m.Unlock()
	m.val = isFailed
}

func (m *MockFailure) getValue() bool {
	m.RLock()
	defer m.RUnlock()
	return m.val
}

// EtcdBackend is used for judging a storage is a real TiKV.
type EtcdBackend interface {
	EtcdAddrs() []string
	TLSConfig() *tls.Config
	StartGCWorker() error
}

const resourceIdleTimeout = 3 * time.Minute // resources in the ResourcePool will be recycled after idleTimeout

// NewDomain creates a new domain. Should not create multiple domains for the same store.
func NewDomain(store kv.Storage, ddlLease time.Duration, statsLease time.Duration, factory pools.Factory) *Domain {
	capacity := 200 // capacity of the sysSessionPool size
	return &Domain{
		store:           store,
		SchemaValidator: NewSchemaValidator(ddlLease),
		exit:            make(chan struct{}),
		sysSessionPool:  pools.NewResourcePool(factory, capacity, capacity, resourceIdleTimeout),
		statsLease:      statsLease,
		infoHandle:      infoschema.NewHandle(store),
	}
}

// Init initializes a domain.
func (do *Domain) Init(ddlLease time.Duration, sysFactory func(*Domain) (pools.Resource, error)) error {
	if ebd, ok := do.store.(EtcdBackend); ok {
		if addrs := ebd.EtcdAddrs(); addrs != nil {
			cli, err := clientv3.New(clientv3.Config{
				Endpoints:   addrs,
				DialTimeout: 5 * time.Second,
				DialOptions: []grpc.DialOption{
					grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
					grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
				},
				TLS: ebd.TLSConfig(),
			})
			if err != nil {
				return errors.Trace(err)
			}
			do.etcdClient = cli
		}
	}

	// TODO: Here we create new sessions with sysFac in DDL,
	// which will use `do` as Domain instead of call `domap.Get`.
	// That's because `domap.Get` requires a lock, but before
	// we initialize Domain finish, we can't require that again.
	// After we remove the lazy logic of creating Domain, we
	// can simplify code here.
	sysFac := func() (pools.Resource, error) {
		return sysFactory(do)
	}
	sysCtxPool := pools.NewResourcePool(sysFac, 2, 2, resourceIdleTimeout)
	ctx := goctx.Background()
	callback := &ddlCallback{do: do}
	do.ddl = ddl.NewDDL(ctx, do.etcdClient, do.store, do.infoHandle, callback, ddlLease, sysCtxPool)

	err := do.ddl.SchemaSyncer().Init(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	err = do.Reload()
	if err != nil {
		return errors.Trace(err)
	}

	// Only when the store is local that the lease value is 0.
	// If the store is local, it doesn't need loadSchemaInLoop.
	if ddlLease > 0 {
		do.wg.Add(1)
		// Local store needs to get the change information for every DDL state in each session.
		go do.loadSchemaInLoop(ddlLease)
	}

	return nil
}

// SysSessionPool returns the system session pool.
func (do *Domain) SysSessionPool() *pools.ResourcePool {
	return do.sysSessionPool
}

// LoadPrivilegeLoop create a goroutine loads privilege tables in a loop, it
// should be called only once in BootstrapSession.
func (do *Domain) LoadPrivilegeLoop(ctx context.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	do.privHandle = privileges.NewHandle()
	err := do.privHandle.Update(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	var watchCh clientv3.WatchChan
	duration := 5 * time.Minute
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(goctx.Background(), privilegeKey)
		duration = 10 * time.Minute
	}

	go func() {
		var count int
		for {
			ok := true
			select {
			case <-do.exit:
				return
			case _, ok = <-watchCh:
			case <-time.After(duration):
			}
			if !ok {
				log.Error("[domain] load privilege loop watch channel closed.")
				watchCh = do.etcdClient.Watch(goctx.Background(), privilegeKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}

			count = 0
			err := do.privHandle.Update(ctx)
			if err != nil {
				log.Error("[domain] load privilege fail:", errors.ErrorStack(err))
			} else {
				log.Info("[domain] reload privilege success.")
			}
		}
	}()
	return nil
}

// PrivilegeHandle returns the MySQLPrivilege.
func (do *Domain) PrivilegeHandle() *privileges.Handle {
	return do.privHandle
}

// StatsHandle returns the statistic handle.
func (do *Domain) StatsHandle() *statistics.Handle {
	return (*statistics.Handle)(atomic.LoadPointer(&do.statsHandle))
}

// CreateStatsHandle is used only for test.
func (do *Domain) CreateStatsHandle(ctx context.Context) {
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(statistics.NewHandle(ctx, do.statsLease)))
}

// RunAutoAnalyze indicates if this TiDB server starts auto analyze worker and can run auto analyze job.
var RunAutoAnalyze = true

// UpdateTableStatsLoop creates a goroutine loads stats info and updates stats info in a loop.
// It will also start a goroutine to analyze tables automatically.
// It should be called only once in BootstrapSession.
func (do *Domain) UpdateTableStatsLoop(ctx context.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	statsHandle := statistics.NewHandle(ctx, do.statsLease)
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(statsHandle))
	do.ddl.RegisterEventCh(statsHandle.DDLEventCh())
	if do.statsLease <= 0 {
		return nil
	}
	owner := do.newStatsOwner()
	do.wg.Add(1)
	go do.updateStatsWorker(ctx, owner)
	if RunAutoAnalyze {
		do.wg.Add(1)
		go do.autoAnalyzeWorker(owner)
	}
	return nil
}

func (do *Domain) newStatsOwner() owner.Manager {
	id := do.ddl.OwnerManager().ID()
	cancelCtx, cancelFunc := goctx.WithCancel(goctx.Background())
	var statsOwner owner.Manager
	if do.etcdClient == nil {
		statsOwner = owner.NewMockManager(id, cancelFunc)
	} else {
		statsOwner = owner.NewOwnerManager(do.etcdClient, statistics.StatsPrompt, id, statistics.StatsOwnerKey, cancelFunc)
	}
	// TODO: Need to do something when err is not nil.
	err := statsOwner.CampaignOwner(cancelCtx)
	if err != nil {
		log.Warnf("[stats] campaign owner fail:", errors.ErrorStack(err))
	}
	return statsOwner
}

func (do *Domain) updateStatsWorker(ctx context.Context, owner owner.Manager) {
	lease := do.statsLease
	deltaUpdateDuration := lease * 5
	loadTicker := time.NewTicker(lease)
	defer loadTicker.Stop()
	deltaUpdateTicker := time.NewTicker(deltaUpdateDuration)
	defer deltaUpdateTicker.Stop()
	loadHistogramTicker := time.NewTicker(lease)
	defer loadHistogramTicker.Stop()
	gcStatsTicker := time.NewTicker(100 * lease)
	defer gcStatsTicker.Stop()
	statsHandle := do.StatsHandle()
	t := time.Now()
	err := statsHandle.InitStats(do.InfoSchema())
	if err != nil {
		log.Error("[stats] init stats info failed: ", errors.ErrorStack(err))
	} else {
		log.Info("[stats] init stats info takes ", time.Now().Sub(t))
	}
	for {
		select {
		case <-loadTicker.C:
			err := statsHandle.Update(do.InfoSchema())
			if err != nil {
				log.Error("[stats] update stats info fail: ", errors.ErrorStack(err))
			}
		case <-do.exit:
			do.wg.Done()
			return
			// This channel is sent only by ddl owner or the drop stats executor.
		case t := <-statsHandle.DDLEventCh():
			err := statsHandle.HandleDDLEvent(t)
			if err != nil {
				log.Error("[stats] handle ddl event fail: ", errors.ErrorStack(err))
			}
		case t := <-statsHandle.AnalyzeResultCh():
			for i, hg := range t.Hist {
				err := statistics.SaveStatsToStorage(ctx, t.TableID, t.Count, t.IsIndex, hg, t.Cms[i])
				if err != nil {
					log.Error("[stats] save histogram to storage fail: ", errors.ErrorStack(err))
				}
			}
		case <-deltaUpdateTicker.C:
			err := statsHandle.DumpStatsDeltaToKV()
			if err != nil {
				log.Error("[stats] dump stats delta fail: ", errors.ErrorStack(err))
			}
		case <-loadHistogramTicker.C:
			err := statsHandle.LoadNeededHistograms()
			if err != nil {
				log.Error("[stats] load histograms fail: ", errors.ErrorStack(err))
			}
		case <-gcStatsTicker.C:
			if !owner.IsOwner() {
				continue
			}
			err := statsHandle.GCStats(do.InfoSchema(), do.DDL().GetLease())
			if err != nil {
				log.Error("[stats] gc stats fail: ", errors.ErrorStack(err))
			}
		}
	}
}

func (do *Domain) autoAnalyzeWorker(owner owner.Manager) {
	statsHandle := do.StatsHandle()
	analyzeTicker := time.NewTicker(do.statsLease)
	defer analyzeTicker.Stop()
	for {
		select {
		case <-analyzeTicker.C:
			if owner.IsOwner() {
				err := statsHandle.HandleAutoAnalyze(do.InfoSchema())
				if err != nil {
					log.Error("[stats] auto analyze fail:", errors.ErrorStack(err))
				}
			}
		case <-do.exit:
			do.wg.Done()
			return
		}
	}
}

const privilegeKey = "/tidb/privilege"

// NotifyUpdatePrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdatePrivilege(ctx context.Context) {
	if do.etcdClient != nil {
		row := do.etcdClient.KV
		_, err := row.Put(goctx.Background(), privilegeKey, "")
		if err != nil {
			log.Warn("notify update privilege failed:", err)
		}
	}
}

// Domain error codes.
const (
	codeInfoSchemaExpired terror.ErrCode = 1
	codeInfoSchemaChanged terror.ErrCode = 2
)

var (
	// ErrInfoSchemaExpired returns the error that information schema is out of date.
	ErrInfoSchemaExpired = terror.ClassDomain.New(codeInfoSchemaExpired, "Information schema is out of date.")
	// ErrInfoSchemaChanged returns the error that information schema is changed.
	ErrInfoSchemaChanged = terror.ClassDomain.New(codeInfoSchemaChanged, "Information schema is changed.")
)
