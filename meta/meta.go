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

package meta

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/structure"
	"github.com/pingcap/tidb/terror"
)

var (
	globalIDMutex sync.Mutex
)

// Meta structure:
//	NextGlobalID -> int64
//	SchemaVersion -> int64
//	DBs -> {
//		DB:1 -> db meta data []byte
//		DB:2 -> db meta data []byte
//	}
//	DB:1 -> {
//		Table:1 -> table meta data []byte
//		Table:2 -> table meta data []byte
//		TID:1 -> int64
//		TID:2 -> int64
//	}
//

var (
	mMetaPrefix       = []byte("m")
	mNextGlobalIDKey  = []byte("NextGlobalID")
	mSchemaVersionKey = []byte("SchemaVersionKey")
	mDBs              = []byte("DBs")
	mDBPrefix         = "DB"
	mTablePrefix      = "Table"
	mTableIDPrefix    = "TID"
	mBootstrapKey     = []byte("BootstrapKey")
	mTableStatsPrefix = "TStats"
	mSchemaDiffPrefix = "Diff"
)

var (
	errInvalidTableKey = terror.ClassMeta.New(codeInvalidTableKey, "invalid table meta key")
	errInvalidDBKey    = terror.ClassMeta.New(codeInvalidDBKey, "invalid db key")

	// ErrDBExists is the error for db exists.
	ErrDBExists = terror.ClassMeta.New(codeDatabaseExists, "database already exists")
	// ErrDBNotExists is the error for db not exists.
	ErrDBNotExists = terror.ClassMeta.New(codeDatabaseNotExists, "database doesn't exist")
	// ErrTableExists is the error for table exists.
	ErrTableExists = terror.ClassMeta.New(codeTableExists, "table already exists")
	// ErrTableNotExists is the error for table not exists.
	ErrTableNotExists = terror.ClassMeta.New(codeTableNotExists, "table doesn't exist")
)

// Meta is for handling meta information in a transaction.
type Meta struct {
	txn     *structure.TxStructure
	StartTS uint64 // StartTS is the txn's start TS.
}

// NewMeta creates a Meta in transaction txn.
func NewMeta(txn kv.Transaction) *Meta {
	txn.SetOption(kv.Priority, kv.PriorityHigh)
	txn.SetOption(kv.SyncLog, true)
	t := structure.NewStructure(txn, txn, mMetaPrefix)
	return &Meta{txn: t, StartTS: txn.StartTS()}
}

// NewSnapshotMeta creates a Meta with snapshot.
func NewSnapshotMeta(snapshot kv.Snapshot) *Meta {
	t := structure.NewStructure(snapshot, nil, mMetaPrefix)
	return &Meta{txn: t}
}

// GenGlobalID generates next id globally.
func (m *Meta) GenGlobalID() (int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	return m.txn.Inc(mNextGlobalIDKey, 1)
}

// GetGlobalID gets current global id.
func (m *Meta) GetGlobalID() (int64, error) {
	return m.txn.GetInt64(mNextGlobalIDKey)
}

func (m *Meta) dbKey(dbID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mDBPrefix, dbID))
}

func (m *Meta) parseDatabaseID(key string) (int64, error) {
	seps := strings.Split(key, ":")
	if len(seps) != 2 {
		return 0, errors.Errorf("invalid db key %s", key)
	}

	n, err := strconv.ParseInt(seps[1], 10, 64)
	return n, errors.Trace(err)
}

func (m *Meta) autoTableIDKey(tableID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mTableIDPrefix, tableID))
}

func (m *Meta) tableKey(tableID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mTablePrefix, tableID))
}

func (m *Meta) parseTableID(key string) (int64, error) {
	seps := strings.Split(key, ":")
	if len(seps) != 2 {
		return 0, errors.Errorf("invalid table meta key %s", key)
	}

	n, err := strconv.ParseInt(seps[1], 10, 64)
	return n, errors.Trace(err)
}

// GenAutoTableIDIDKeyValue generate meta key by dbID, tableID and coresponding value by autoID.
func (m *Meta) GenAutoTableIDIDKeyValue(dbID, tableID, autoID int64) (key, value []byte) {
	dbKey := m.dbKey(dbID)
	autoTableIDKey := m.autoTableIDKey(tableID)
	return m.txn.EncodeHashAutoIDKeyValue(dbKey, autoTableIDKey, autoID)
}

// GenAutoTableID adds step to the auto ID of the table and returns the sum.
func (m *Meta) GenAutoTableID(dbID, tableID, step int64) (int64, error) {
	// Check if DB exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return 0, errors.Trace(err)
	}
	// Check if table exists.
	tableKey := m.tableKey(tableID)
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		return 0, errors.Trace(err)
	}

	return m.txn.HInc(dbKey, m.autoTableIDKey(tableID), step)
}

// GetAutoTableID gets current auto id with table id.
func (m *Meta) GetAutoTableID(dbID int64, tableID int64) (int64, error) {
	return m.txn.HGetInt64(m.dbKey(dbID), m.autoTableIDKey(tableID))
}

// GetSchemaVersion gets current global schema version.
func (m *Meta) GetSchemaVersion() (int64, error) {
	return m.txn.GetInt64(mSchemaVersionKey)
}

// GenSchemaVersion generates next schema version.
func (m *Meta) GenSchemaVersion() (int64, error) {
	return m.txn.Inc(mSchemaVersionKey, 1)
}

func (m *Meta) checkDBExists(dbKey []byte) error {
	v, err := m.txn.HGet(mDBs, dbKey)
	if err != nil {
		return errors.Trace(err)
	} else if v == nil {
		return ErrDBNotExists
	}

	return nil
}

func (m *Meta) checkDBNotExists(dbKey []byte) error {
	v, err := m.txn.HGet(mDBs, dbKey)
	if err != nil {
		return errors.Trace(err)
	}

	if v != nil {
		return ErrDBExists
	}

	return nil
}

func (m *Meta) checkTableExists(dbKey []byte, tableKey []byte) error {
	v, err := m.txn.HGet(dbKey, tableKey)
	if err != nil {
		return errors.Trace(err)
	}

	if v == nil {
		return ErrTableNotExists
	}

	return nil
}

func (m *Meta) checkTableNotExists(dbKey []byte, tableKey []byte) error {
	v, err := m.txn.HGet(dbKey, tableKey)
	if err != nil {
		return errors.Trace(err)
	}

	if v != nil {
		return ErrTableExists
	}

	return nil
}

// CreateDatabase creates a database with db info.
func (m *Meta) CreateDatabase(dbInfo *model.DBInfo) error {
	dbKey := m.dbKey(dbInfo.ID)

	if err := m.checkDBNotExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(dbInfo)
	if err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(mDBs, dbKey, data)
}

// UpdateDatabase updates a database with db info.
func (m *Meta) UpdateDatabase(dbInfo *model.DBInfo) error {
	dbKey := m.dbKey(dbInfo.ID)

	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(dbInfo)
	if err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(mDBs, dbKey, data)
}

// CreateTable creates a table with tableInfo in database.
func (m *Meta) CreateTable(dbID int64, tableInfo *model.TableInfo) error {
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	// Check if table exists.
	tableKey := m.tableKey(tableInfo.ID)
	if err := m.checkTableNotExists(dbKey, tableKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(tableInfo)
	if err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(dbKey, tableKey, data)
}

// DropDatabase drops whole database.
func (m *Meta) DropDatabase(dbID int64) error {
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.txn.HClear(dbKey); err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.HDel(mDBs, dbKey); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// DropTable drops table in database.
// If delAutoID is true, it will delete the auto_increment id key-value of the table.
// For rename table, we do not need to rename auto_increment id key-value.
func (m *Meta) DropTable(dbID int64, tblID int64, delAutoID bool) error {
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	// Check if table exists.
	tableKey := m.tableKey(tblID)
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.HDel(dbKey, tableKey); err != nil {
		return errors.Trace(err)
	}
	if delAutoID {
		if err := m.txn.HDel(dbKey, m.autoTableIDKey(tblID)); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// UpdateTable updates the table with table info.
func (m *Meta) UpdateTable(dbID int64, tableInfo *model.TableInfo) error {
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	// Check if table exists.
	tableKey := m.tableKey(tableInfo.ID)
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(tableInfo)
	if err != nil {
		return errors.Trace(err)
	}

	err = m.txn.HSet(dbKey, tableKey, data)
	return errors.Trace(err)
}

// ListTables shows all tables in database.
func (m *Meta) ListTables(dbID int64) ([]*model.TableInfo, error) {
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}

	res, err := m.txn.HGetAll(dbKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tables := make([]*model.TableInfo, 0, len(res)/2)
	for _, r := range res {
		// only handle table meta
		tableKey := string(r.Field)
		if !strings.HasPrefix(tableKey, mTablePrefix) {
			continue
		}

		tbInfo := &model.TableInfo{}
		err = json.Unmarshal(r.Value, tbInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}

		tables = append(tables, tbInfo)
	}

	return tables, nil
}

// ListDatabases shows all databases.
func (m *Meta) ListDatabases() ([]*model.DBInfo, error) {
	res, err := m.txn.HGetAll(mDBs)
	if err != nil {
		return nil, errors.Trace(err)
	}

	dbs := make([]*model.DBInfo, 0, len(res))
	for _, r := range res {
		dbInfo := &model.DBInfo{}
		err = json.Unmarshal(r.Value, dbInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dbs = append(dbs, dbInfo)
	}
	return dbs, nil
}

// GetDatabase gets the database value with ID.
func (m *Meta) GetDatabase(dbID int64) (*model.DBInfo, error) {
	dbKey := m.dbKey(dbID)
	value, err := m.txn.HGet(mDBs, dbKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	dbInfo := &model.DBInfo{}
	err = json.Unmarshal(value, dbInfo)
	return dbInfo, errors.Trace(err)
}

// GetTable gets the table value in database with tableID.
func (m *Meta) GetTable(dbID int64, tableID int64) (*model.TableInfo, error) {
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}

	tableKey := m.tableKey(tableID)
	value, err := m.txn.HGet(dbKey, tableKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	tableInfo := &model.TableInfo{}
	err = json.Unmarshal(value, tableInfo)
	return tableInfo, errors.Trace(err)
}

// DDL job structure
//	DDLOnwer: []byte
//	DDLJobList: list jobs
//	DDLJobHistory: hash
//	DDLJobReorg: hash
//
// for multi DDL workers, only one can become the owner
// to operate DDL jobs, and dispatch them to MR Jobs.

var (
	mDDLJobListKey    = []byte("DDLJobList")
	mDDLJobHistoryKey = []byte("DDLJobHistory")
	mDDLJobReorgKey   = []byte("DDLJobReorg")
)

func (m *Meta) enQueueDDLJob(key []byte, job *model.Job) error {
	b, err := job.Encode(true)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.RPush(key, b)
}

// EnQueueDDLJob adds a DDL job to the list.
func (m *Meta) EnQueueDDLJob(job *model.Job) error {
	return m.enQueueDDLJob(mDDLJobListKey, job)
}

func (m *Meta) deQueueDDLJob(key []byte) (*model.Job, error) {
	value, err := m.txn.LPop(key)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{}
	err = job.Decode(value)
	return job, errors.Trace(err)
}

// DeQueueDDLJob pops a DDL job from the list.
func (m *Meta) DeQueueDDLJob() (*model.Job, error) {
	return m.deQueueDDLJob(mDDLJobListKey)
}

func (m *Meta) getDDLJob(key []byte, index int64) (*model.Job, error) {
	value, err := m.txn.LIndex(key, index)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{}
	err = job.Decode(value)
	return job, errors.Trace(err)
}

// GetDDLJob returns the DDL job with index.
func (m *Meta) GetDDLJob(index int64) (*model.Job, error) {
	job, err := m.getDDLJob(mDDLJobListKey, index)
	return job, errors.Trace(err)
}

// updateDDLJob updates the DDL job with index and key.
// updateRawArgs is used to determine whether to update the raw args when encode the job.
func (m *Meta) updateDDLJob(index int64, job *model.Job, key []byte, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.LSet(key, index, b)
}

// UpdateDDLJob updates the DDL job with index.
// updateRawArgs is used to determine whether to update the raw args when encode the job.
func (m *Meta) UpdateDDLJob(index int64, job *model.Job, updateRawArgs bool) error {
	return m.updateDDLJob(index, job, mDDLJobListKey, updateRawArgs)
}

// DDLJobQueueLen returns the DDL job queue length.
func (m *Meta) DDLJobQueueLen() (int64, error) {
	return m.txn.LLen(mDDLJobListKey)
}

func (m *Meta) jobIDKey(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

func (m *Meta) addHistoryDDLJob(key []byte, job *model.Job) error {
	b, err := job.Encode(true)
	if err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(key, m.jobIDKey(job.ID), b)
}

// AddHistoryDDLJob adds DDL job to history.
func (m *Meta) AddHistoryDDLJob(job *model.Job) error {
	return m.addHistoryDDLJob(mDDLJobHistoryKey, job)
}

func (m *Meta) getHistoryDDLJob(key []byte, id int64) (*model.Job, error) {
	value, err := m.txn.HGet(key, m.jobIDKey(id))
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{}
	err = job.Decode(value)
	return job, errors.Trace(err)
}

// GetHistoryDDLJob gets a history DDL job.
func (m *Meta) GetHistoryDDLJob(id int64) (*model.Job, error) {
	return m.getHistoryDDLJob(mDDLJobHistoryKey, id)
}

// GetAllHistoryDDLJobs gets all history DDL jobs.
func (m *Meta) GetAllHistoryDDLJobs() ([]*model.Job, error) {
	pairs, err := m.txn.HGetAll(mDDLJobHistoryKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var jobs []*model.Job
	for _, pair := range pairs {
		job := &model.Job{}
		err = job.Decode(pair.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, job)
	}
	sorter := &jobsSorter{jobs: jobs}
	sort.Sort(sorter)
	return jobs, nil
}

// jobsSorter implements the sort.Interface interface.
type jobsSorter struct {
	jobs []*model.Job
}

func (s *jobsSorter) Swap(i, j int) {
	s.jobs[i], s.jobs[j] = s.jobs[j], s.jobs[i]
}

func (s *jobsSorter) Len() int {
	return len(s.jobs)
}

func (s *jobsSorter) Less(i, j int) bool {
	return s.jobs[i].ID < s.jobs[j].ID
}

// GetBootstrapVersion returns the version of the server which boostrap the store.
// If the store is not bootstraped, the version will be zero.
func (m *Meta) GetBootstrapVersion() (int64, error) {
	value, err := m.txn.GetInt64(mBootstrapKey)
	return value, errors.Trace(err)
}

// FinishBootstrap finishes bootstrap.
func (m *Meta) FinishBootstrap(version int64) error {
	err := m.txn.Set(mBootstrapKey, []byte(fmt.Sprintf("%d", version)))
	return errors.Trace(err)
}

// UpdateDDLReorgHandle saves the job reorganization latest processed handle for later resuming.
func (m *Meta) UpdateDDLReorgHandle(job *model.Job, handle int64) error {
	err := m.txn.HSet(mDDLJobReorgKey, m.jobIDKey(job.ID), []byte(strconv.FormatInt(handle, 10)))
	return errors.Trace(err)
}

// RemoveDDLReorgHandle removes the job reorganization handle.
func (m *Meta) RemoveDDLReorgHandle(job *model.Job) error {
	err := m.txn.HDel(mDDLJobReorgKey, m.jobIDKey(job.ID))
	return errors.Trace(err)
}

// GetDDLReorgHandle gets the latest processed handle.
func (m *Meta) GetDDLReorgHandle(job *model.Job) (int64, error) {
	value, err := m.txn.HGetInt64(mDDLJobReorgKey, m.jobIDKey(job.ID))
	return value, errors.Trace(err)
}

func (m *Meta) tableStatsKey(tableID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mTableStatsPrefix, tableID))
}

func (m *Meta) schemaDiffKey(schemaVersion int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mSchemaDiffPrefix, schemaVersion))
}

// GetSchemaDiff gets the modification information on a given schema version.
func (m *Meta) GetSchemaDiff(schemaVersion int64) (*model.SchemaDiff, error) {
	diffKey := m.schemaDiffKey(schemaVersion)
	data, err := m.txn.Get(diffKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(data) == 0 {
		return nil, nil
	}
	diff := &model.SchemaDiff{}
	err = json.Unmarshal(data, diff)
	return diff, errors.Trace(err)
}

// SetSchemaDiff sets the modification information on a given schema version.
func (m *Meta) SetSchemaDiff(diff *model.SchemaDiff) error {
	data, err := json.Marshal(diff)
	if err != nil {
		return errors.Trace(err)
	}
	diffKey := m.schemaDiffKey(diff.Version)
	err = m.txn.Set(diffKey, data)
	return errors.Trace(err)
}

// meta error codes.
const (
	codeInvalidTableKey terror.ErrCode = 1
	codeInvalidDBKey                   = 2

	codeDatabaseExists    = 1007
	codeDatabaseNotExists = 1049
	codeTableExists       = 1050
	codeTableNotExists    = 1146
)

func init() {
	metaMySQLErrCodes := map[terror.ErrCode]uint16{
		codeDatabaseExists:    mysql.ErrDBCreateExists,
		codeDatabaseNotExists: mysql.ErrBadDB,
		codeTableNotExists:    mysql.ErrNoSuchTable,
		codeTableExists:       mysql.ErrTableExists,
	}
	terror.ErrClassToMySQLCodes[terror.ClassMeta] = metaMySQLErrCodes
}
