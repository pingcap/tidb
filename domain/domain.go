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
	lastLeaseTS    int64 // nano seconds
	m              sync.Mutex
	SchemaValidity *schemaValidityInfo
	exit           chan struct{}
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

func (do *Domain) mockReloadFailed() error {
	ver, err := do.store.CurrentVersion()
	if err != nil {
		log.Errorf("mock reload failed err:%v", err)
		return errors.Trace(err)
	}
	lease := do.DDL().GetLease()
	// Make sure that is timed out when checking validity.
	mockLastSuccTime := time.Now().UnixNano() - int64(lease)
	log.Warnf("mock lastSuccTS:%v, lease:%v", time.Now(), time.Duration(lease))
	do.SchemaValidity.updateTimeInfo(mockLastSuccTime, ver.Ver)
	return errors.New("mock reload failed")
}

const doReloadSleepTime = 500 * time.Millisecond
const loadRetryTimes = 5

// Reload reloads InfoSchema.
// It's public in order to do the test.
func (do *Domain) Reload() error {
	// for test
	if do.SchemaValidity.MockReloadFailed.getValue() {
		return do.mockReloadFailed()
	}

	// Lock here for only once at the same time.
	do.m.Lock()
	defer do.m.Unlock()

	var err error
	var latestSchemaVersion int64
	for i := 0; i < loadRetryTimes; i++ {
		startTime := time.Now()
		var ver kv.Version
		ver, err = do.store.CurrentVersion()
		if err == nil {
			schemaVersion := int64(0)
			oldInfoSchema := do.infoHandle.Get()
			if oldInfoSchema != nil {
				schemaVersion = oldInfoSchema.SchemaMetaVersion()
			}
			latestSchemaVersion, err = do.loadInfoSchema(do.infoHandle, schemaVersion, ver.Ver)
		}
		if err == nil {
			atomic.StoreInt64(&do.lastLeaseTS, time.Now().UnixNano())
			do.SchemaValidity.updateTimeInfo(startTime.UnixNano(), ver.Ver)
			do.SchemaValidity.updateSchemaVersion(latestSchemaVersion)
			sub := time.Since(startTime)
			lease := do.DDL().GetLease()
			if sub > lease && lease > 0 {
				log.Infof("[ddl] loading schema takes a long time %v", sub)
			}
			break
		}
		log.Errorf("[ddl] load schema err %v, ver:%v, retry again", errors.ErrorStack(err), ver.Ver)
		// TODO: Use a backoff algorithm.
		time.Sleep(doReloadSleepTime)
	}

	return errors.Trace(err)
}

func (do *Domain) checkValidityInLoop(lease time.Duration) {
	timer := time.NewTimer(lease)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			// TODO: Using the local time, it will affect the accuracy of the check when clock transition.
			lastReloadTime, lastSuccTS := do.SchemaValidity.getTimeInfo()
			sub := time.Duration(time.Now().UnixNano() - lastReloadTime)
			if sub > lease {
				// If sub is greater than a lease,
				// it means that the schema version hasn't update for a lease.
				do.SchemaValidity.SetExpireInfo(true, lastSuccTS)
			} else {
				do.SchemaValidity.SetExpireInfo(false, lastSuccTS)
			}

			waitTime := lease
			if sub > 0 {
				// If the schema is invalid (sub >= lease), it means reload schema will become frequent.
				// We need to reduce wait time to check the validity more frequently.
				if sub >= lease {
					waitTime = minInterval(lease)
					log.Warnf("[ddl] check validity in a loop, sub:%v, lease:%v, succ:%v, waitTime:%v",
						sub, lease, lastSuccTS, waitTime)
				} else {
					waitTime -= sub
				}
			}
			log.Debugf("[ddl] check validity in a loop, sub:%v, lease:%v, succ:%v, waitTime:%v",
				sub, lease, lastSuccTS, waitTime)
			timer.Reset(waitTime)
		case <-do.exit:
			return
		}
	}
}

// minInterval gets a minimal interval.
// It uses to reload schema and check schema validity after the schema is invalid.
// If lease is 0, it's used for local store and minimal interval is 5ms.
func minInterval(lease time.Duration) time.Duration {
	if lease > 0 {
		return lease / 4
	}
	return 5 * time.Millisecond
}

func (do *Domain) loadSchemaInLoop(lease time.Duration) {
	ticker := time.NewTicker(minInterval(lease))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := do.Reload()
			if err != nil {
				log.Errorf("[ddl] reload schema in loop err %v", errors.ErrorStack(err))
			}
		case <-do.exit:
			return
		}
	}
}

// Close closes the Domain and release its resource.
func (do *Domain) Close() {
	do.ddl.Stop()
	close(do.exit)
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

type schemaValidityInfo struct {
	mux         sync.RWMutex
	isExpired   bool   // Whether information schema is out of date.
	recoveredTS uint64 // It's used for recording the first txn TS of schema vaild.
	timeInfo    struct {
		mux            sync.RWMutex
		lastReloadTime int64  // It's used for recording the time of last reload schema.
		lastSuccTS     uint64 // It's used for recording the last txn TS of loading schema succeed.
	}
	lastSchemaVer    int64       // It's used for recording the last schema version.
	MockReloadFailed MockFailure // It mocks reload failed.
}

func (s *schemaValidityInfo) updateSchemaVersion(version int64) {
	atomic.StoreInt64(&s.lastSchemaVer, version)
}

func (s *schemaValidityInfo) updateTimeInfo(lastReloadTime int64, lastSuccTS uint64) {
	s.timeInfo.mux.Lock()
	defer s.timeInfo.mux.Unlock()

	s.timeInfo.lastReloadTime = lastReloadTime
	s.timeInfo.lastSuccTS = lastSuccTS
}

func (s *schemaValidityInfo) getTimeInfo() (int64, uint64) {
	s.timeInfo.mux.Lock()
	defer s.timeInfo.mux.Unlock()

	return s.timeInfo.lastReloadTime, s.timeInfo.lastSuccTS
}

// SetExpireInfo sets the information of whether information schema is out of date.
// It's public in order to do the test.
func (s *schemaValidityInfo) SetExpireInfo(expired bool, lastSuccTS uint64) {
	s.mux.Lock()
	if s.isExpired != expired {
		log.Infof("[ddl] SetExpireInfo, original:%v current:%v lastSuccTS:%v", s.isExpired, expired, lastSuccTS)
		if expired {
			log.Errorf("[ddl] SetExpireInfo, information schema is expired %v, lastSuccTS:%v", expired, lastSuccTS)
			s.recoveredTS = lastSuccTS
		}
		s.isExpired = expired
	}
	s.mux.Unlock()
}

// Check checks schema validity. It returns the current schema version and an error.
func (s *schemaValidityInfo) Check(txnTS uint64, schemaVer int64) (int64, error) {
	currVer := atomic.LoadInt64(&s.lastSchemaVer)
	s.mux.RLock()
	if s.isExpired {
		s.mux.RUnlock()
		return currVer, ErrInfoSchemaExpired
	}

	// txnTS != 0, it means the transition isn't nil.
	// txnTS <= s.recoveredTS, it means the transition begins before schema is recovered.
	// schemaVer != currVer, it means the schema version is changed.
	if txnTS != 0 && txnTS <= s.recoveredTS && schemaVer != currVer {
		s.mux.RUnlock()
		log.Warnf("check schema validity, txnTS:%v recordTS:%v schema version original:%v input:%v",
			txnTS, s.recoveredTS, currVer, schemaVer)
		return currVer, ErrInfoSchemaChanged
	}
	s.mux.RUnlock()
	return currVer, nil
}

// NewDomain creates a new domain. Should not create multiple domains for the same store.
func NewDomain(store kv.Storage, lease time.Duration) (d *Domain, err error) {
	d = &Domain{
		store:          store,
		SchemaValidity: &schemaValidityInfo{},
		exit:           make(chan struct{}),
	}

	d.infoHandle, err = infoschema.NewHandle(d.store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	d.ddl = ddl.NewDDL(d.store, d.infoHandle, &ddlCallback{do: d}, lease)
	if err = d.Reload(); err != nil {
		return nil, errors.Trace(err)
	}
	d.SchemaValidity.SetExpireInfo(false, 0)

	variable.RegisterStatistics(d)

	// Only when the store is local that the lease value is 0.
	// If the store is local, it doesn't need loadSchemaInLoop and checkValidityInLoop.
	if lease > 0 {
		go d.checkValidityInLoop(lease)
	}
	// Local store needs to get the change information for every DDL state in each session.
	go d.loadSchemaInLoop(lease)

	return d, nil
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
