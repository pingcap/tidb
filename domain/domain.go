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
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/context"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/perfschema"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/terror"
	goctx "golang.org/x/net/context"
)

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store           kv.Storage
	infoHandle      *infoschema.Handle
	privHandle      *privileges.Handle
	statsHandle     *statistics.Handle
	statsLease      time.Duration
	ddl             ddl.DDL
	m               sync.Mutex
	SchemaValidator SchemaValidator
	sysSessionPool  *pools.ResourcePool
	exit            chan struct{}
	etcdClient      *clientv3.Client

	MockReloadFailed MockFailure // It mocks reload failed.
}

// loadInfoSchema loads infoschema at startTS into handle, usedSchemaVersion is the currently used
// infoschema version, if it is the same as the schema version at startTS, we don't need to reload again.
// It returns the latest schema version and an error.
func (do *Domain) loadInfoSchema(handle *infoschema.Handle, usedSchemaVersion int64, startTS uint64) (int64, error) {
	snapshot, err := do.store.GetSnapshot(kv.NewVersion(startTS))
	if err != nil {
		return 0, errors.Trace(err)
	}
	m := meta.NewSnapshotMeta(snapshot)
	latestSchemaVersion, err := m.GetSchemaVersion()
	if err != nil {
		return 0, errors.Trace(err)
	}
	if usedSchemaVersion != 0 && usedSchemaVersion == latestSchemaVersion {
		return latestSchemaVersion, nil
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
	ok, err := do.tryLoadSchemaDiffs(m, usedSchemaVersion, latestSchemaVersion)
	if err != nil {
		// We can fall back to full load, don't need to return the error.
		log.Errorf("[ddl] failed to load schema diff err %v", err)
	}
	if ok {
		log.Infof("[ddl] diff load InfoSchema from version %d to %d, in %v",
			usedSchemaVersion, latestSchemaVersion, time.Since(startTime))
		return latestSchemaVersion, nil
	}

	schemas, err := do.fetchAllSchemasWithTables(m)
	if err != nil {
		return 0, errors.Trace(err)
	}

	newISBuilder, err := infoschema.NewBuilder(handle).InitWithDBInfos(schemas, latestSchemaVersion)
	if err != nil {
		return 0, errors.Trace(err)
	}
	log.Infof("[ddl] full load InfoSchema from version %d to %d, in %v",
		usedSchemaVersion, latestSchemaVersion, time.Since(startTime))
	newISBuilder.Build()
	return latestSchemaVersion, nil
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

// tryLoadSchemaDiffs tries to only load latest schema changes.
// Returns true if the schema is loaded successfully.
// Returns false if the schema can not be loaded by schema diff, then we need to do full load.
func (do *Domain) tryLoadSchemaDiffs(m *meta.Meta, usedVersion, newVersion int64) (bool, error) {
	if usedVersion == initialVersion || newVersion-usedVersion > maxNumberOfDiffsToLoad {
		// If there isn't any used version, or used version is too old, we do full load.
		return false, nil
	}
	if usedVersion > newVersion {
		// When user use History Read feature, history schema will be loaded.
		// usedVersion may be larger than newVersion, full load is needed.
		return false, nil
	}
	var diffs []*model.SchemaDiff
	for usedVersion < newVersion {
		usedVersion++
		diff, err := m.GetSchemaDiff(usedVersion)
		if err != nil {
			return false, errors.Trace(err)
		}
		if diff == nil {
			// If diff is missing for any version between used and new version, we fall back to full reload.
			return false, nil
		}
		diffs = append(diffs, diff)
	}
	builder := infoschema.NewBuilder(do.infoHandle).InitWithOldInfoSchema()
	for _, diff := range diffs {
		err := builder.ApplyDiff(m, diff)
		if err != nil {
			return false, errors.Trace(err)
		}
	}
	builder.Build()
	return true, nil
}

// InfoSchema gets information schema from domain.
func (do *Domain) InfoSchema() infoschema.InfoSchema {
	return do.infoHandle.Get()
}

// GetSnapshotInfoSchema gets a snapshot information schema.
func (do *Domain) GetSnapshotInfoSchema(snapshotTS uint64) (infoschema.InfoSchema, error) {
	snapHandle := do.infoHandle.EmptyClone()
	_, err := do.loadInfoSchema(snapHandle, do.infoHandle.Get().SchemaMetaVersion(), snapshotTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return snapHandle.Get(), nil
}

// PerfSchema gets performance schema from domain.
func (do *Domain) PerfSchema() perfschema.PerfSchema {
	return do.infoHandle.GetPerfHandle()
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
	return variable.DefaultScopeFlag
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

	latestSchemaVersion, err = do.loadInfoSchema(do.infoHandle, schemaVersion, ver.Ver)
	loadSchemaDuration.Observe(time.Since(startTime).Seconds())
	if err != nil {
		loadSchemaCounter.WithLabelValues("failed").Inc()
		return errors.Trace(err)
	}
	loadSchemaCounter.WithLabelValues("succ").Inc()

	do.SchemaValidator.Update(ver.Ver, latestSchemaVersion)

	lease := do.DDL().GetLease()
	sub := time.Since(startTime)
	if sub > lease && lease > 0 {
		log.Warnf("[ddl] loading schema takes a long time %v", sub)
	}

	return nil
}

func (do *Domain) loadSchemaInLoop(lease time.Duration) {
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	// TODO: Reset ticker or make interval longer.
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
			do.SchemaValidator.Stop()
			err := syncer.Restart(goctx.Background())
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

// Close closes the Domain and release its resource.
func (do *Domain) Close() {
	do.ddl.Stop()
	close(do.exit)
	if do.etcdClient != nil {
		do.etcdClient.Close()
	}
	do.sysSessionPool.Close()
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

type etcdBackend interface {
	EtcdAddrs() []string
}

// NewDomain creates a new domain. Should not create multiple domains for the same store.
func NewDomain(store kv.Storage, ddlLease time.Duration, statsLease time.Duration, factory pools.Factory) (d *Domain, err error) {
	capacity := 200                // capacity of the sysSessionPool size
	idleTimeout := 3 * time.Minute // sessions in the sysSessionPool will be recycled after idleTimeout
	d = &Domain{
		store:           store,
		SchemaValidator: newSchemaValidator(ddlLease),
		exit:            make(chan struct{}),
		sysSessionPool:  pools.NewResourcePool(factory, capacity, capacity, idleTimeout),
		statsLease:      statsLease,
	}

	if ebd, ok := store.(etcdBackend); ok {
		if addrs := ebd.EtcdAddrs(); addrs != nil {
			cli, err := clientv3.New(clientv3.Config{
				Endpoints:   addrs,
				DialTimeout: 5 * time.Second,
			})
			if err != nil {
				return nil, errors.Trace(err)
			}
			d.etcdClient = cli
		}
	}

	d.infoHandle, err = infoschema.NewHandle(d.store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ctx := goctx.Background()
	callback := &ddlCallback{do: d}
	d.ddl = ddl.NewDDL(ctx, d.etcdClient, d.store, d.infoHandle, callback, ddlLease)
	if err = d.ddl.SchemaSyncer().Init(ctx); err != nil {
		return nil, errors.Trace(err)
	}
	if err = d.Reload(); err != nil {
		return nil, errors.Trace(err)
	}

	// Only when the store is local that the lease value is 0.
	// If the store is local, it doesn't need loadSchemaInLoop.
	if ddlLease > 0 {
		// Local store needs to get the change information for every DDL state in each session.
		go d.loadSchemaInLoop(ddlLease)
	}

	return d, nil
}

// SysSessionPool returns the system session pool.
func (do *Domain) SysSessionPool() *pools.ResourcePool {
	return do.sysSessionPool
}

// LoadPrivilegeLoop create a goroutine loads privilege tables in a loop, it
// should be called only once in BootstrapSession.
func (do *Domain) LoadPrivilegeLoop(ctx context.Context) error {
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
		for {
			select {
			case <-do.exit:
				return
			case <-watchCh:
			case <-time.After(duration):
			}
			err := do.privHandle.Update(ctx)
			if err != nil {
				log.Error("load privilege fail:", errors.ErrorStack(err))
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
	return do.statsHandle
}

// CreateStatsHandle is used only for test.
func (do *Domain) CreateStatsHandle(ctx context.Context) {
	do.statsHandle = statistics.NewHandle(ctx, do.statsLease)
}

// UpdateTableStatsLoop creates a goroutine loads stats info and updates stats info in a loop. It
// should be called only once in BootstrapSession.
func (do *Domain) UpdateTableStatsLoop(ctx context.Context) error {
	do.statsHandle = statistics.NewHandle(ctx, do.statsLease)
	do.ddl.RegisterEventCh(do.statsHandle.DDLEventCh())
	err := do.statsHandle.Update(do.InfoSchema())
	if err != nil {
		return errors.Trace(err)
	}
	lease := do.statsLease
	if lease <= 0 {
		return nil
	}
	deltaUpdateDuration := time.Minute
	go func(do *Domain) {
		loadTicker := time.NewTicker(lease)
		defer loadTicker.Stop()
		deltaUpdateTicker := time.NewTicker(deltaUpdateDuration)
		defer deltaUpdateTicker.Stop()

		for {
			select {
			case <-loadTicker.C:
				err := do.statsHandle.Update(do.InfoSchema())
				if err != nil {
					log.Error(errors.ErrorStack(err))
				}
			case <-do.exit:
				return
			// This channel is sent only by ddl owner. Only owner know if this ddl is done or not.
			case t := <-do.statsHandle.DDLEventCh():
				err := do.statsHandle.HandleDDLEvent(t)
				if err != nil {
					log.Error(errors.ErrorStack(err))
				}
			case <-deltaUpdateTicker.C:
				do.statsHandle.DumpStatsDeltaToKV()
			}
		}
	}(do)
	return nil
}

const privilegeKey = "/tidb/privilege"

// NotifyUpdatePrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdatePrivilege(ctx context.Context) {
	if do.etcdClient != nil {
		kv := do.etcdClient.KV
		_, err := kv.Put(goctx.Background(), privilegeKey, "")
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
