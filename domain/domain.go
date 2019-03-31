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
	"context"
	"crypto/tls"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/coreos/etcd/clientv3"
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/ngaut/pools"
	"github.com/ngaut/sync2"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/bindinfo"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/infoschema/perfschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/privilege/privileges"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/sqlexec"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store           kv.Storage
	infoHandle      *infoschema.Handle
	privHandle      *privileges.Handle
	bindHandle      *bindinfo.Handle
	statsHandle     unsafe.Pointer
	statsLease      time.Duration
	statsUpdating   sync2.AtomicInt32
	ddl             ddl.DDL
	info            *InfoSyncer
	m               sync.Mutex
	SchemaValidator SchemaValidator
	sysSessionPool  *sessionPool
	exit            chan struct{}
	etcdClient      *clientv3.Client
	wg              sync.WaitGroup
	gvc             GlobalVariableCache
	slowQuery       *topNSlowQueries
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
			logutil.Logger(context.Background()).Info("cannot update self schema version to etcd")
			return
		}
		err = do.ddl.SchemaSyncer().UpdateSelfVersion(context.Background(), latestSchemaVersion)
		if err != nil {
			logutil.Logger(context.Background()).Info("update self version failed", zap.Int64("usedSchemaVersion", usedSchemaVersion),
				zap.Int64("latestSchemaVersion", latestSchemaVersion), zap.Error(err))
		}
	}()

	startTime := time.Now()
	ok, tblIDs, err := do.tryLoadSchemaDiffs(m, usedSchemaVersion, latestSchemaVersion)
	if err != nil {
		// We can fall back to full load, don't need to return the error.
		logutil.Logger(context.Background()).Error("failed to load schema diff", zap.Error(err))
	}
	if ok {
		logutil.Logger(context.Background()).Info("diff load InfoSchema from version failed",
			zap.Int64("usedSchemaVersion", usedSchemaVersion),
			zap.Int64("latestSchemaVersion", latestSchemaVersion),
			zap.Duration("start time", time.Since(startTime)),
			zap.Int64s("tblIDs", tblIDs))
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
	logutil.Logger(context.Background()).Info("full load InfoSchema failed", zap.Int64("usedSchemaVersion", usedSchemaVersion),
		zap.Int64("latestSchemaVersion", latestSchemaVersion), zap.Duration("start time", time.Since(startTime)))
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
		// If TreatOldVersionUTF8AsUTF8MB4 was enable, need to convert the old version schema UTF8 charset to UTF8MB4.
		if config.GetGlobalConfig().TreatOldVersionUTF8AsUTF8MB4 {
			for _, tbInfo := range tables {
				infoschema.ConvertOldVersionUTF8ToUTF8MB4IfNeed(tbInfo)
			}
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

// GetSnapshotMeta gets a new snapshot meta at startTS.
func (do *Domain) GetSnapshotMeta(startTS uint64) (*meta.Meta, error) {
	snapshot, err := do.store.GetSnapshot(kv.NewVersion(startTS))
	if err != nil {
		return nil, errors.Trace(err)
	}
	return meta.NewSnapshotMeta(snapshot), nil
}

// DDL gets DDL from domain.
func (do *Domain) DDL() ddl.DDL {
	return do.ddl
}

// InfoSyncer gets infoSyncer from domain.
func (do *Domain) InfoSyncer() *InfoSyncer {
	return do.info
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

// Reload reloads InfoSchema.
// It's public in order to do the test.
func (do *Domain) Reload() error {
	// gofail: var ErrorMockReloadFailed bool
	// if ErrorMockReloadFailed {
	// 		return errors.New("mock reload failed")
	// }

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
	metrics.LoadSchemaDuration.Observe(time.Since(startTime).Seconds())
	if err != nil {
		metrics.LoadSchemaCounter.WithLabelValues("failed").Inc()
		return errors.Trace(err)
	}
	metrics.LoadSchemaCounter.WithLabelValues("succ").Inc()

	if fullLoad {
		logutil.Logger(context.Background()).Info("full load and reset schema validator")
		do.SchemaValidator.Reset()
	}
	do.SchemaValidator.Update(ver.Ver, schemaVersion, latestSchemaVersion, changedTableIDs)

	lease := do.DDL().GetLease()
	sub := time.Since(startTime)
	// Reload interval is lease / 2, if load schema time elapses more than this interval,
	// some query maybe responded by ErrInfoSchemaExpired error.
	if sub > (lease/2) && lease > 0 {
		logutil.Logger(context.Background()).Warn("loading schema takes a long time", zap.Duration("take time", sub))
	}

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
	defer recoverInDomain("topNSlowQueryLoop", false)
	defer do.wg.Done()
	ticker := time.NewTicker(time.Minute * 10)
	defer ticker.Stop()
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
	defer do.wg.Done()
	defer recoverInDomain("infoSyncerKeeper", false)
	for {
		select {
		case <-do.info.Done():
			logutil.Logger(context.Background()).Info("server info syncer need to restart")
			if err := do.info.Restart(context.Background()); err != nil {
				logutil.Logger(context.Background()).Error("server restart failed", zap.Error(err))
			}
			logutil.Logger(context.Background()).Info("server info syncer restarted")
		case <-do.exit:
			return
		}
	}
}

func (do *Domain) loadSchemaInLoop(lease time.Duration) {
	defer do.wg.Done()
	// Lease renewal can run at any frequency.
	// Use lease/2 here as recommend by paper.
	ticker := time.NewTicker(lease / 2)
	defer ticker.Stop()
	defer recoverInDomain("loadSchemaInLoop", true)
	syncer := do.ddl.SchemaSyncer()

	for {
		select {
		case <-ticker.C:
			err := do.Reload()
			if err != nil {
				logutil.Logger(context.Background()).Error("reload schema in loop failed", zap.Error(err))
			}
		case _, ok := <-syncer.GlobalVersionCh():
			err := do.Reload()
			if err != nil {
				logutil.Logger(context.Background()).Error("reload schema in loop failed", zap.Error(err))
			}
			if !ok {
				logutil.Logger(context.Background()).Warn("reload schema in loop, schema syncer need rewatch")
				// Make sure the rewatch doesn't affect load schema, so we watch the global schema version asynchronously.
				syncer.WatchGlobalSchemaVer(context.Background())
			}
		case <-syncer.Done():
			// The schema syncer stops, we need stop the schema validator to synchronize the schema version.
			logutil.Logger(context.Background()).Info("reload schema in loop, schema syncer need restart")
			// The etcd is responsible for schema synchronization, we should ensure there is at most two different schema version
			// in the TiDB cluster, to make the data/schema be consistent. If we lost connection/session to etcd, the cluster
			// will treats this TiDB as a down instance, and etcd will remove the key of `/tidb/ddl/all_schema_versions/tidb-id`.
			// Say the schema version now is 1, the owner is changing the schema version to 2, it will not wait for this down TiDB syncing the schema,
			// then continue to change the TiDB schema to version 3. Unfortunately, this down TiDB schema version will still be version 1.
			// And version 1 is not consistent to version 3. So we need to stop the schema validator to prohibit the DML executing.
			do.SchemaValidator.Stop()
			err := do.mustRestartSyncer()
			if err != nil {
				logutil.Logger(context.Background()).Error("reload schema in loop, schema syncer restart failed", zap.Error(err))
				break
			}
			// The schema maybe changed, must reload schema then the schema validator can restart.
			exitLoop := do.mustReload()
			if exitLoop {
				// domain is closed.
				logutil.Logger(context.Background()).Error("domain is closed, exit loadSchemaInLoop")
				return
			}
			do.SchemaValidator.Restart()
			logutil.Logger(context.Background()).Info("schema syncer restarted")
		case <-do.exit:
			return
		}
	}
}

// mustRestartSyncer tries to restart the SchemaSyncer.
// It returns until it's successful or the domain is stoped.
func (do *Domain) mustRestartSyncer() error {
	ctx := context.Background()
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
		logutil.Logger(context.Background()).Info("restart the schema syncer failed", zap.Error(err))
	}
}

// mustReload tries to Reload the schema, it returns until it's successful or the domain is closed.
// it returns false when it is successful, returns true when the domain is closed.
func (do *Domain) mustReload() (exitLoop bool) {
	for {
		err := do.Reload()
		if err == nil {
			logutil.Logger(context.Background()).Info("mustReload succeed")
			return false
		}

		logutil.Logger(context.Background()).Info("reload the schema failed", zap.Error(err))
		// If the domain is closed, we returns immediately.
		select {
		case <-do.exit:
			logutil.Logger(context.Background()).Info("domain is closed")
			return true
		default:
		}

		time.Sleep(200 * time.Millisecond)
	}
}

// Close closes the Domain and release its resource.
func (do *Domain) Close() {
	if do.ddl != nil {
		terror.Log(errors.Trace(do.ddl.Stop()))
	}
	if do.info != nil {
		do.info.RemoveServerInfo()
	}
	close(do.exit)
	if do.etcdClient != nil {
		terror.Log(errors.Trace(do.etcdClient.Close()))
	}
	do.sysSessionPool.Close()
	do.slowQuery.Close()
	do.wg.Wait()
	logutil.Logger(context.Background()).Info("domain closed")
}

type ddlCallback struct {
	ddl.BaseCallback
	do *Domain
}

func (c *ddlCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	logutil.Logger(context.Background()).Info("performing DDL change, must reload")

	err = c.do.Reload()
	if err != nil {
		logutil.Logger(context.Background()).Error("performing DDL change failed", zap.Error(err))
	}

	return nil
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
		sysSessionPool:  newSessionPool(capacity, factory),
		statsLease:      statsLease,
		infoHandle:      infoschema.NewHandle(store),
		slowQuery:       newTopNSlowQueries(30, time.Hour*24*7, 500),
	}
}

// Init initializes a domain.
func (do *Domain) Init(ddlLease time.Duration, sysFactory func(*Domain) (pools.Resource, error)) error {
	perfschema.Init()
	if ebd, ok := do.store.(EtcdBackend); ok {
		if addrs := ebd.EtcdAddrs(); addrs != nil {
			cfg := config.GetGlobalConfig()
			cli, err := clientv3.New(clientv3.Config{
				Endpoints:        addrs,
				AutoSyncInterval: 30 * time.Second,
				DialTimeout:      5 * time.Second,
				DialOptions: []grpc.DialOption{
					grpc.WithUnaryInterceptor(grpc_prometheus.UnaryClientInterceptor),
					grpc.WithStreamInterceptor(grpc_prometheus.StreamClientInterceptor),
					grpc.WithBackoffMaxDelay(time.Second * 3),
					grpc.WithKeepaliveParams(keepalive.ClientParameters{
						Time:                time.Duration(cfg.TiKVClient.GrpcKeepAliveTime) * time.Second,
						Timeout:             time.Duration(cfg.TiKVClient.GrpcKeepAliveTimeout) * time.Second,
						PermitWithoutStream: true,
					}),
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
	ctx := context.Background()
	callback := &ddlCallback{do: do}
	do.ddl = ddl.NewDDL(ctx, do.etcdClient, do.store, do.infoHandle, callback, ddlLease, sysCtxPool)

	err := do.ddl.SchemaSyncer().Init(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	do.info = NewInfoSyncer(do.ddl.GetID(), do.etcdClient)
	err = do.info.Init(ctx)
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
	do.wg.Add(1)
	go do.topNSlowQueryLoop()

	do.wg.Add(1)
	go do.infoSyncerKeeper()
	return nil
}

type sessionPool struct {
	resources chan pools.Resource
	factory   pools.Factory
	mu        struct {
		sync.RWMutex
		closed bool
	}
}

func newSessionPool(cap int, factory pools.Factory) *sessionPool {
	return &sessionPool{
		resources: make(chan pools.Resource, cap),
		factory:   factory,
	}
}

func (p *sessionPool) Get() (resource pools.Resource, err error) {
	var ok bool
	select {
	case resource, ok = <-p.resources:
		if !ok {
			err = errors.New("session pool closed")
		}
	default:
		resource, err = p.factory()
	}
	return
}

func (p *sessionPool) Put(resource pools.Resource) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.mu.closed {
		resource.Close()
		return
	}

	select {
	case p.resources <- resource:
	default:
		resource.Close()
	}
}
func (p *sessionPool) Close() {
	p.mu.Lock()
	if p.mu.closed {
		p.mu.Unlock()
		return
	}
	p.mu.closed = true
	close(p.resources)
	p.mu.Unlock()

	for r := range p.resources {
		r.Close()
	}
}

// SysSessionPool returns the system session pool.
func (do *Domain) SysSessionPool() *sessionPool {
	return do.sysSessionPool
}

// GetEtcdClient returns the etcd client.
func (do *Domain) GetEtcdClient() *clientv3.Client {
	return do.etcdClient
}

// LoadPrivilegeLoop create a goroutine loads privilege tables in a loop, it
// should be called only once in BootstrapSession.
func (do *Domain) LoadPrivilegeLoop(ctx sessionctx.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	do.privHandle = privileges.NewHandle()
	err := do.privHandle.Update(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	var watchCh clientv3.WatchChan
	duration := 5 * time.Minute
	if do.etcdClient != nil {
		watchCh = do.etcdClient.Watch(context.Background(), privilegeKey)
		duration = 10 * time.Minute
	}

	do.wg.Add(1)
	go func() {
		defer do.wg.Done()
		defer recoverInDomain("loadPrivilegeInLoop", false)
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
				logutil.Logger(context.Background()).Error("load privilege loop watch channel closed")
				watchCh = do.etcdClient.Watch(context.Background(), privilegeKey)
				count++
				if count > 10 {
					time.Sleep(time.Duration(count) * time.Second)
				}
				continue
			}

			count = 0
			err := do.privHandle.Update(ctx)
			metrics.LoadPrivilegeCounter.WithLabelValues(metrics.RetLabel(err)).Inc()
			if err != nil {
				logutil.Logger(context.Background()).Error("load privilege failed", zap.Error(err))
			} else {
				logutil.Logger(context.Background()).Debug("reload privilege success")
			}
		}
	}()
	return nil
}

// PrivilegeHandle returns the MySQLPrivilege.
func (do *Domain) PrivilegeHandle() *privileges.Handle {
	return do.privHandle
}

// BindHandle returns domain's bindHandle.
func (do *Domain) BindHandle() *bindinfo.Handle {
	return do.bindHandle
}

// LoadBindInfoLoop create a goroutine loads BindInfo in a loop, it should
// be called only once in BootstrapSession.
func (do *Domain) LoadBindInfoLoop(ctx sessionctx.Context, parser *parser.Parser) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	do.bindHandle = bindinfo.NewHandle()

	bindCacheUpdater := bindinfo.NewBindCacheUpdater(ctx, do.BindHandle(), parser)
	err := bindCacheUpdater.Update(true)
	if err != nil {
		return errors.Trace(err)
	}

	duration := 3 * time.Second
	do.wg.Add(1)
	go func() {
		defer do.wg.Done()
		defer recoverInDomain("loadBindInfoLoop", false)
		for {
			select {
			case <-do.exit:
				return
			case <-time.After(duration):
			}
			err = bindCacheUpdater.Update(false)
			if err != nil {
				logutil.Logger(context.Background()).Error("update bindinfo failed", zap.Error(err))
			}
		}
	}()
	return nil
}

// StatsHandle returns the statistic handle.
func (do *Domain) StatsHandle() *statistics.Handle {
	return (*statistics.Handle)(atomic.LoadPointer(&do.statsHandle))
}

// CreateStatsHandle is used only for test.
func (do *Domain) CreateStatsHandle(ctx sessionctx.Context) {
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(statistics.NewHandle(ctx, do.statsLease)))
}

// StatsUpdating checks if the stats worker is updating.
func (do *Domain) StatsUpdating() bool {
	return do.statsUpdating.Get() > 0
}

// SetStatsUpdating sets the value of stats updating.
func (do *Domain) SetStatsUpdating(val bool) {
	if val {
		do.statsUpdating.Set(1)
	} else {
		do.statsUpdating.Set(0)
	}
}

// RunAutoAnalyze indicates if this TiDB server starts auto analyze worker and can run auto analyze job.
var RunAutoAnalyze = true

// UpdateTableStatsLoop creates a goroutine loads stats info and updates stats info in a loop.
// It will also start a goroutine to analyze tables automatically.
// It should be called only once in BootstrapSession.
func (do *Domain) UpdateTableStatsLoop(ctx sessionctx.Context) error {
	ctx.GetSessionVars().InRestrictedSQL = true
	statsHandle := statistics.NewHandle(ctx, do.statsLease)
	atomic.StorePointer(&do.statsHandle, unsafe.Pointer(statsHandle))
	do.ddl.RegisterEventCh(statsHandle.DDLEventCh())
	if do.statsLease <= 0 {
		return nil
	}
	owner := do.newStatsOwner()
	do.wg.Add(1)
	do.SetStatsUpdating(true)
	go do.updateStatsWorker(ctx, owner)
	if RunAutoAnalyze {
		do.wg.Add(1)
		go do.autoAnalyzeWorker(owner)
	}
	return nil
}

func (do *Domain) newStatsOwner() owner.Manager {
	id := do.ddl.OwnerManager().ID()
	cancelCtx, cancelFunc := context.WithCancel(context.Background())
	var statsOwner owner.Manager
	if do.etcdClient == nil {
		statsOwner = owner.NewMockManager(id, cancelFunc)
	} else {
		statsOwner = owner.NewOwnerManager(do.etcdClient, statistics.StatsPrompt, id, statistics.StatsOwnerKey, cancelFunc)
	}
	// TODO: Need to do something when err is not nil.
	err := statsOwner.CampaignOwner(cancelCtx)
	if err != nil {
		logutil.Logger(context.Background()).Warn("campaign owner failed", zap.Error(err))
	}
	return statsOwner
}

func (do *Domain) updateStatsWorker(ctx sessionctx.Context, owner owner.Manager) {
	defer recoverInDomain("updateStatsWorker", false)
	lease := do.statsLease
	deltaUpdateDuration := lease * 20
	loadTicker := time.NewTicker(lease)
	defer loadTicker.Stop()
	deltaUpdateTicker := time.NewTicker(deltaUpdateDuration)
	defer deltaUpdateTicker.Stop()
	loadHistogramTicker := time.NewTicker(lease)
	defer loadHistogramTicker.Stop()
	gcStatsTicker := time.NewTicker(100 * lease)
	defer gcStatsTicker.Stop()
	dumpFeedbackTicker := time.NewTicker(200 * lease)
	defer dumpFeedbackTicker.Stop()
	loadFeedbackTicker := time.NewTicker(5 * lease)
	defer loadFeedbackTicker.Stop()
	statsHandle := do.StatsHandle()
	t := time.Now()
	err := statsHandle.InitStats(do.InfoSchema())
	if err != nil {
		logutil.Logger(context.Background()).Debug("init stats info failed", zap.Error(err))
	} else {
		logutil.Logger(context.Background()).Info("init stats info time", zap.Duration("take time", time.Since(t)))
	}
	defer func() {
		do.SetStatsUpdating(false)
		do.wg.Done()
	}()
	for {
		select {
		case <-loadTicker.C:
			err = statsHandle.Update(do.InfoSchema())
			if err != nil {
				logutil.Logger(context.Background()).Debug("update stats info failed", zap.Error(err))
			}
		case <-do.exit:
			statsHandle.FlushStats()
			return
			// This channel is sent only by ddl owner.
		case t := <-statsHandle.DDLEventCh():
			err = statsHandle.HandleDDLEvent(t)
			if err != nil {
				logutil.Logger(context.Background()).Debug("handle ddl event failed", zap.Error(err))
			}
		case <-deltaUpdateTicker.C:
			err = statsHandle.DumpStatsDeltaToKV(statistics.DumpDelta)
			if err != nil {
				logutil.Logger(context.Background()).Debug("dump stats delta failed", zap.Error(err))
			}
			statsHandle.UpdateErrorRate(do.InfoSchema())
		case <-loadHistogramTicker.C:
			err = statsHandle.LoadNeededHistograms()
			if err != nil {
				logutil.Logger(context.Background()).Debug("load histograms failed", zap.Error(err))
			}
		case <-loadFeedbackTicker.C:
			statsHandle.UpdateStatsByLocalFeedback(do.InfoSchema())
			if !owner.IsOwner() {
				continue
			}
			err = statsHandle.HandleUpdateStats(do.InfoSchema())
			if err != nil {
				logutil.Logger(context.Background()).Debug("update stats using feedback failed", zap.Error(err))
			}
		case <-dumpFeedbackTicker.C:
			err = statsHandle.DumpStatsFeedbackToKV()
			if err != nil {
				logutil.Logger(context.Background()).Debug("dump stats feedback failed", zap.Error(err))
			}
		case <-gcStatsTicker.C:
			if !owner.IsOwner() {
				continue
			}
			err = statsHandle.GCStats(do.InfoSchema(), do.DDL().GetLease())
			if err != nil {
				logutil.Logger(context.Background()).Debug("GC stats failed", zap.Error(err))
			}
		}
	}
}

func (do *Domain) autoAnalyzeWorker(owner owner.Manager) {
	defer recoverInDomain("autoAnalyzeWorker", false)
	statsHandle := do.StatsHandle()
	analyzeTicker := time.NewTicker(do.statsLease)
	defer func() {
		analyzeTicker.Stop()
		do.wg.Done()
	}()
	for {
		select {
		case <-analyzeTicker.C:
			if owner.IsOwner() {
				statsHandle.HandleAutoAnalyze(do.InfoSchema())
			}
		case <-do.exit:
			return
		}
	}
}

const privilegeKey = "/tidb/privilege"

// NotifyUpdatePrivilege updates privilege key in etcd, TiDB client that watches
// the key will get notification.
func (do *Domain) NotifyUpdatePrivilege(ctx sessionctx.Context) {
	if do.etcdClient != nil {
		row := do.etcdClient.KV
		_, err := row.Put(context.Background(), privilegeKey, "")
		if err != nil {
			logutil.Logger(context.Background()).Warn("notify update privilege failed", zap.Error(err))
		}
	}
	// update locally
	_, _, err := ctx.(sqlexec.RestrictedSQLExecutor).ExecRestrictedSQL(ctx, `FLUSH PRIVILEGES`)
	if err != nil {
		logutil.Logger(context.Background()).Error("unable to update privileges", zap.Error(err))
	}
}

func recoverInDomain(funcName string, quit bool) {
	r := recover()
	if r == nil {
		return
	}
	buf := util.GetStack()
	logutil.Logger(context.Background()).Error("recover in domain failed", zap.String("funcName", funcName),
		zap.Any("error", r), zap.String("buffer", string(buf)))
	metrics.PanicCounter.WithLabelValues(metrics.LabelDomain).Inc()
	if quit {
		// Wait for metrics to be pushed.
		time.Sleep(time.Second * 15)
		os.Exit(1)
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

func init() {
	// Map error codes to mysql error codes.
	terror.ErrClassToMySQLCodes[terror.ClassDomain] = make(map[terror.ErrCode]uint16)
}
