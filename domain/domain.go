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
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/perfschema"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/terror"
)

var ddlLastReloadSchemaTS = "ddl_last_reload_schema_ts"

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store          kv.Storage
	infoHandle     *infoschema.Handle
	ddl            ddl.DDL
	leaseCh        chan time.Duration
	lastLeaseTS    int64 // nano seconds
	m              sync.Mutex
	SchemaValidity *schemaValidityInfo
}

// loadInfoSchema loads infoschema at startTS into handle, usedSchemaVersion is the currently used
// infoschema version, if it is the same as the schema version at startTS, we don't need to reload again.
func (do *Domain) loadInfoSchema(handle *infoschema.Handle, usedSchemaVersion int64, startTS uint64) error {
	snapshot, err := do.store.GetSnapshot(kv.NewVersion(startTS))
	if err != nil {
		return errors.Trace(err)
	}
	m := meta.NewSnapshotMeta(snapshot)
	latestSchemaVersion, err := m.GetSchemaVersion()
	if err != nil {
		return errors.Trace(err)
	}
	if usedSchemaVersion != 0 && usedSchemaVersion == latestSchemaVersion {
		log.Debugf("[ddl] schema version is still %d, no need reload", usedSchemaVersion)
		return nil
	}
	ok, err := do.tryLoadSchemaDiffs(m, usedSchemaVersion, latestSchemaVersion)
	if err != nil {
		// We can fall back to full load, don't need to return the error.
		log.Errorf("[ddl] failed to load schema diff %v", err)
	}
	if ok {
		log.Infof("[ddl] diff load InfoSchema from version %d to %d", usedSchemaVersion, latestSchemaVersion)
		return nil
	}
	schemas, err := do.getAllSchemasWithTablesFromMeta(m)
	if err != nil {
		return errors.Trace(err)
	}

	newISBuilder, err := infoschema.NewBuilder(handle).InitWithDBInfos(schemas, latestSchemaVersion)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("[ddl] full load InfoSchema from version %d to %d", usedSchemaVersion, latestSchemaVersion)
	return newISBuilder.Build()
}

func (do *Domain) getAllSchemasWithTablesFromMeta(m *meta.Meta) ([]*model.DBInfo, error) {
	schemas, err := m.ListDatabases()
	if err != nil {
		return nil, errors.Trace(err)
	}

	for _, di := range schemas {
		if di.State != model.StatePublic {
			// schema is not public, can't be used outside.
			continue
		}

		tables, err1 := m.ListTables(di.ID)
		if err1 != nil {
			err = err1
			return nil, errors.Trace(err1)
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
	return schemas, nil
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
	err := builder.Build()
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, nil
}

// InfoSchema gets information schema from domain.
func (do *Domain) InfoSchema() infoschema.InfoSchema {
	// try reload if possible.
	do.tryReload()
	return do.infoHandle.Get()
}

// GetSnapshotInfoSchema gets a snapshot information schema.
func (do *Domain) GetSnapshotInfoSchema(snapshotTS uint64) (infoschema.InfoSchema, error) {
	snapHandle := do.infoHandle.EmptyClone()
	err := do.loadInfoSchema(snapHandle, do.infoHandle.Get().SchemaMetaVersion(), snapshotTS)
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

// SetLease will reset the lease time for online DDL change.
func (do *Domain) SetLease(lease time.Duration) {
	if lease <= 0 {
		log.Warnf("[ddl] set the current lease:%v into a new lease:%v failed, so do nothing",
			do.ddl.GetLease(), lease)
		return
	}

	if do.leaseCh == nil {
		log.Errorf("[ddl] set the current lease:%v into a new lease:%v failed, so do nothing",
			do.ddl.GetLease(), lease)
		return
	}

	do.leaseCh <- lease
	// let ddl to reset lease too.
	do.ddl.SetLease(lease)
}

// Stats returns the domain statistic.
func (do *Domain) Stats() (map[string]interface{}, error) {
	m := make(map[string]interface{})
	m[ddlLastReloadSchemaTS] = atomic.LoadInt64(&do.lastLeaseTS) / 1e9

	return m, nil
}

// GetScope gets the status variables scope.
func (do *Domain) GetScope(status string) variable.ScopeFlag {
	// Now domain status variables scope are all default scope.
	return variable.DefaultScopeFlag
}

func (do *Domain) tryReload() {
	// If we don't have update the schema for a long time > lease, we must force reloading it.
	// Although we try to reload schema every lease time in a goroutine, sometimes it may not run accurately.
	// e.g., the machine has a very high load, running the ticker is delayed.
	last := atomic.LoadInt64(&do.lastLeaseTS)
	lease := do.ddl.GetLease()

	// if lease is 0, we use the local store, so no need to reload.
	if lease > 0 && time.Now().UnixNano()-last > lease.Nanoseconds() {
		do.MustReload()
	}
}

var defaultMinReloadTimeout = 20 * time.Second

// Reload reloads InfoSchema.
func (do *Domain) Reload() error {
	// for test
	if do.SchemaValidity.MockReloadFailed {
		err := kv.RunInNewTxn(do.store, false, func(txn kv.Transaction) error {
			do.SchemaValidity.setLastFailedTS(txn.StartTS())
			return nil
		})
		if err != nil {
			log.Errorf("mock reload failed err:%v", err)
			return errors.Trace(err)
		}
		return errors.New("mock reload failed")
	}

	// lock here for only once at same time.
	do.m.Lock()
	defer do.m.Unlock()

	timeout := do.ddl.GetLease() / 2
	if timeout < defaultMinReloadTimeout {
		timeout = defaultMinReloadTimeout
	}

	exit := int32(0)
	done := make(chan error, 1)
	go func() {
		var err error
		for {
			var ver kv.Version
			ver, err = do.store.CurrentVersion()
			if err == nil {
				schemaVersion := int64(0)
				oldInfoSchema := do.infoHandle.Get()
				if oldInfoSchema != nil {
					schemaVersion = oldInfoSchema.SchemaMetaVersion()
				}
				err = do.loadInfoSchema(do.infoHandle, schemaVersion, ver.Ver)
			}
			if err == nil {
				atomic.StoreInt64(&do.lastLeaseTS, time.Now().UnixNano())
				break
			}
			do.SchemaValidity.setLastFailedTS(ver.Ver)
			log.Errorf("[ddl] load schema err %v, retry again", errors.ErrorStack(err))
			if atomic.LoadInt32(&exit) == 1 {
				return
			}
			// TODO: use a backoff algorithm.
			time.Sleep(500 * time.Millisecond)
			continue
		}

		done <- err
	}()

	select {
	case err := <-done:
		return errors.Trace(err)
	case <-time.After(timeout):
		atomic.StoreInt32(&exit, 1)
		return ErrLoadSchemaTimeOut
	}
}

// MustReload reloads the infoschema.
// If reload error, it will hold whole program to guarantee data safe.
// It's public in order to do the test.
func (do *Domain) MustReload() error {
	if err := do.Reload(); err != nil {
		log.Errorf("[ddl] reload schema err %v, txnTS:%v", errors.ErrorStack(err),
			do.SchemaValidity.getLastFailedTS())
		do.SchemaValidity.SetValidity(false)
		return errors.Trace(err)
	}
	do.SchemaValidity.SetValidity(true)
	return nil
}

func (do *Domain) loadSchemaInLoop(lease time.Duration) {
	ticker := time.NewTicker(lease)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := do.MustReload()
			if err != nil {
				log.Errorf("[ddl] reload schema in loop err %v", errors.ErrorStack(err))
			}
		case newLease := <-do.leaseCh:
			if lease == newLease {
				// nothing to do
				continue
			}

			lease = newLease
			// reset ticker too.
			ticker.Stop()
			ticker = time.NewTicker(lease)
		}
	}
}

type ddlCallback struct {
	ddl.BaseCallback
	do *Domain
}

func (c *ddlCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	log.Warnf("[ddl] on DDL change")

	return c.do.MustReload()
}

type schemaValidityInfo struct {
	isValid          bool
	lastInvalidTS    uint64 // It's used for recording the last txn TS of schema invalid.
	mux              sync.RWMutex
	lastFailedTS     uint64 // It's used for recording the last txn TS of loading schema failed.
	MockReloadFailed bool   // It mocks reload failed.
}

func (s *schemaValidityInfo) setLastFailedTS(ts uint64) {
	atomic.StoreUint64(&s.lastFailedTS, ts)
}

func (s *schemaValidityInfo) getLastFailedTS() uint64 {
	return atomic.LoadUint64(&s.lastFailedTS)
}

// SetValidity sets the schema validity value.
// It's public in order to do the test.
func (s *schemaValidityInfo) SetValidity(v bool) {
	s.mux.Lock()
	if !v {
		txnTS := s.getLastFailedTS()
		log.Errorf("[ddl] SetValidity, v:%v txnTS:%v lastInvalidTS:%v", v, txnTS, s.lastInvalidTS)
		if s.lastInvalidTS < txnTS {
			s.lastInvalidTS = txnTS
		}
	}
	if s.isValid != v {
		log.Infof("[ddl] SetValidity, original:%v current:%v", s.isValid, v)
		s.isValid = v
	}
	s.mux.Unlock()
}

func (s *schemaValidityInfo) Check(lastFailedTS uint64) error {
	s.mux.RLock()
	if s.isValid && (lastFailedTS == 0 || lastFailedTS > s.lastInvalidTS) {
		s.mux.RUnlock()
		return nil
	}
	s.mux.RUnlock()
	return ErrLoadSchemaTimeOut.Gen("InfomationSchema is out of date.")
}

// NewDomain creates a new domain.
func NewDomain(store kv.Storage, lease time.Duration) (d *Domain, err error) {
	d = &Domain{store: store,
		SchemaValidity: &schemaValidityInfo{}}

	d.infoHandle, err = infoschema.NewHandle(d.store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	d.ddl = ddl.NewDDL(d.store, d.infoHandle, &ddlCallback{do: d}, lease)
	if err = d.MustReload(); err != nil {
		return nil, errors.Trace(err)
	}

	variable.RegisterStatistics(d)

	// Only when the store is local that the lease value is 0.
	// If the store is local, it doesn't need loadSchemaInLoop.
	if lease > 0 {
		d.leaseCh = make(chan time.Duration, 1)
		go d.loadSchemaInLoop(lease)
	}

	return d, nil
}

// Domain error codes.
const (
	codeLoadSchemaTimeOut terror.ErrCode = 1
)

var (
	// ErrLoadSchemaTimeOut returns for loading schema time out.
	ErrLoadSchemaTimeOut = terror.ClassDomain.New(codeLoadSchemaTimeOut, "reload schema timeout")
)
