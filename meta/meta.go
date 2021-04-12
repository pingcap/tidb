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
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/errno"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	tikvstore "github.com/pingcap/tidb/store/tikv/kv"
	"github.com/pingcap/tidb/structure"
	"github.com/pingcap/tidb/util/dbterror"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
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
	mSequencePrefix   = "SID"
	mSeqCyclePrefix   = "SequenceCycle"
	mTableIDPrefix    = "TID"
	mRandomIDPrefix   = "TARID"
	mBootstrapKey     = []byte("BootstrapKey")
	mSchemaDiffPrefix = "Diff"
)

var (
	// ErrDBExists is the error for db exists.
	ErrDBExists = dbterror.ClassMeta.NewStd(mysql.ErrDBCreateExists)
	// ErrDBNotExists is the error for db not exists.
	ErrDBNotExists = dbterror.ClassMeta.NewStd(mysql.ErrBadDB)
	// ErrTableExists is the error for table exists.
	ErrTableExists = dbterror.ClassMeta.NewStd(mysql.ErrTableExists)
	// ErrTableNotExists is the error for table not exists.
	ErrTableNotExists = dbterror.ClassMeta.NewStd(mysql.ErrNoSuchTable)
	// ErrDDLReorgElementNotExist is the error for reorg element not exists.
	ErrDDLReorgElementNotExist = dbterror.ClassMeta.NewStd(errno.ErrDDLReorgElementNotExist)
)

// Meta is for handling meta information in a transaction.
type Meta struct {
	txn        *structure.TxStructure
	StartTS    uint64 // StartTS is the txn's start TS.
	jobListKey JobListKeyType
}

// NewMeta creates a Meta in transaction txn.
// If the current Meta needs to handle a job, jobListKey is the type of the job's list.
func NewMeta(txn kv.Transaction, jobListKeys ...JobListKeyType) *Meta {
	txn.SetOption(tikvstore.Priority, tikvstore.PriorityHigh)
	txn.SetOption(tikvstore.SyncLog, true)
	t := structure.NewStructure(txn, txn, mMetaPrefix)
	listKey := DefaultJobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}
	return &Meta{txn: t,
		StartTS:    txn.StartTS(),
		jobListKey: listKey,
	}
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

// GenGlobalIDs generates the next n global IDs.
func (m *Meta) GenGlobalIDs(n int) ([]int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	newID, err := m.txn.Inc(mNextGlobalIDKey, int64(n))
	if err != nil {
		return nil, err
	}
	origID := newID - int64(n)
	ids := make([]int64, 0, n)
	for i := origID + 1; i <= newID; i++ {
		ids = append(ids, i)
	}
	return ids, nil
}

// GetGlobalID gets current global id.
func (m *Meta) GetGlobalID() (int64, error) {
	return m.txn.GetInt64(mNextGlobalIDKey)
}

func (m *Meta) dbKey(dbID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mDBPrefix, dbID))
}

func (m *Meta) autoTableIDKey(tableID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mTableIDPrefix, tableID))
}

func (m *Meta) autoRandomTableIDKey(tableID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mRandomIDPrefix, tableID))
}

func (m *Meta) tableKey(tableID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mTablePrefix, tableID))
}

func (m *Meta) sequenceKey(sequenceID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mSequencePrefix, sequenceID))
}

func (m *Meta) sequenceCycleKey(sequenceID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mSeqCyclePrefix, sequenceID))
}

// DDLJobHistoryKey is only used for testing.
func DDLJobHistoryKey(m *Meta, jobID int64) []byte {
	return m.txn.EncodeHashDataKey(mDDLJobHistoryKey, m.jobIDKey(jobID))
}

// GenAutoTableIDKeyValue generates meta key by dbID, tableID and corresponding value by autoID.
func (m *Meta) GenAutoTableIDKeyValue(dbID, tableID, autoID int64) (key, value []byte) {
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

// GenAutoRandomID adds step to the auto shard ID of the table and returns the sum.
func (m *Meta) GenAutoRandomID(dbID, tableID, step int64) (int64, error) {
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

	return m.txn.HInc(dbKey, m.autoRandomTableIDKey(tableID), step)
}

// GetAutoTableID gets current auto id with table id.
func (m *Meta) GetAutoTableID(dbID int64, tableID int64) (int64, error) {
	return m.txn.HGetInt64(m.dbKey(dbID), m.autoTableIDKey(tableID))
}

// GetAutoRandomID gets current auto random id with table id.
func (m *Meta) GetAutoRandomID(dbID int64, tableID int64) (int64, error) {
	return m.txn.HGetInt64(m.dbKey(dbID), m.autoRandomTableIDKey(tableID))
}

// GenSequenceValue adds step to the sequence value and returns the sum.
func (m *Meta) GenSequenceValue(dbID, sequenceID, step int64) (int64, error) {
	// Check if DB exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return 0, errors.Trace(err)
	}
	// Check if sequence exists.
	tableKey := m.tableKey(sequenceID)
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		return 0, errors.Trace(err)
	}
	return m.txn.HInc(dbKey, m.sequenceKey(sequenceID), step)
}

// GetSequenceValue gets current sequence value with sequence id.
func (m *Meta) GetSequenceValue(dbID int64, sequenceID int64) (int64, error) {
	return m.txn.HGetInt64(m.dbKey(dbID), m.sequenceKey(sequenceID))
}

// SetSequenceValue sets start value when sequence in cycle.
func (m *Meta) SetSequenceValue(dbID int64, sequenceID int64, start int64) error {
	return m.txn.HSet(m.dbKey(dbID), m.sequenceKey(sequenceID), []byte(strconv.FormatInt(start, 10)))
}

// GetSequenceCycle gets current sequence cycle times with sequence id.
func (m *Meta) GetSequenceCycle(dbID int64, sequenceID int64) (int64, error) {
	return m.txn.HGetInt64(m.dbKey(dbID), m.sequenceCycleKey(sequenceID))
}

// SetSequenceCycle sets cycle times value when sequence in cycle.
func (m *Meta) SetSequenceCycle(dbID int64, sequenceID int64, round int64) error {
	return m.txn.HSet(m.dbKey(dbID), m.sequenceCycleKey(sequenceID), []byte(strconv.FormatInt(round, 10)))
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
	if err == nil && v == nil {
		err = ErrDBNotExists.GenWithStack("database doesn't exist")
	}
	return errors.Trace(err)
}

func (m *Meta) checkDBNotExists(dbKey []byte) error {
	v, err := m.txn.HGet(mDBs, dbKey)
	if err == nil && v != nil {
		err = ErrDBExists.GenWithStack("database already exists")
	}
	return errors.Trace(err)
}

func (m *Meta) checkTableExists(dbKey []byte, tableKey []byte) error {
	v, err := m.txn.HGet(dbKey, tableKey)
	if err == nil && v == nil {
		err = ErrTableNotExists.GenWithStack("table doesn't exist")
	}
	return errors.Trace(err)
}

func (m *Meta) checkTableNotExists(dbKey []byte, tableKey []byte) error {
	v, err := m.txn.HGet(dbKey, tableKey)
	if err == nil && v != nil {
		err = ErrTableExists.GenWithStack("table already exists")
	}
	return errors.Trace(err)
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

// CreateTableOrView creates a table with tableInfo in database.
func (m *Meta) CreateTableOrView(dbID int64, tableInfo *model.TableInfo) error {
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

// CreateTableAndSetAutoID creates a table with tableInfo in database,
// and rebases the table autoID.
func (m *Meta) CreateTableAndSetAutoID(dbID int64, tableInfo *model.TableInfo, autoIncID, autoRandID int64) error {
	err := m.CreateTableOrView(dbID, tableInfo)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.txn.HInc(m.dbKey(dbID), m.autoTableIDKey(tableInfo.ID), autoIncID)
	if err != nil {
		return errors.Trace(err)
	}
	if tableInfo.AutoRandomBits > 0 {
		_, err = m.txn.HInc(m.dbKey(dbID), m.autoRandomTableIDKey(tableInfo.ID), autoRandID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// CreateSequenceAndSetSeqValue creates sequence with tableInfo in database, and rebase the sequence seqValue.
func (m *Meta) CreateSequenceAndSetSeqValue(dbID int64, tableInfo *model.TableInfo, seqValue int64) error {
	err := m.CreateTableOrView(dbID, tableInfo)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.txn.HInc(m.dbKey(dbID), m.sequenceKey(tableInfo.ID), seqValue)
	return errors.Trace(err)
}

// RestartSequenceValue resets the the sequence value.
func (m *Meta) RestartSequenceValue(dbID int64, tableInfo *model.TableInfo, seqValue int64) error {
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
	return errors.Trace(m.txn.HSet(m.dbKey(dbID), m.sequenceKey(tableInfo.ID), []byte(strconv.FormatInt(seqValue, 10))))
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

// DropSequence drops sequence in database.
// Sequence is made of table struct and kv value pair.
func (m *Meta) DropSequence(dbID int64, tblID int64, delAutoID bool) error {
	err := m.DropTableOrView(dbID, tblID, delAutoID)
	if err != nil {
		return err
	}
	err = m.txn.HDel(m.dbKey(dbID), m.sequenceKey(tblID))
	return errors.Trace(err)
}

// DropTableOrView drops table in database.
// If delAutoID is true, it will delete the auto_increment id key-value of the table.
// For rename table, we do not need to rename auto_increment id key-value.
func (m *Meta) DropTableOrView(dbID int64, tblID int64, delAutoID bool) error {
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
		if err := m.txn.HDel(dbKey, m.autoRandomTableIDKey(tblID)); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// CleanAutoID is used to delete the auto-id of specific table.
func (m *Meta) CleanAutoID(dbID, tblID int64) error {
	dbKey := m.dbKey(dbID)
	if err := m.txn.HDel(dbKey, m.autoTableIDKey(tblID)); err != nil {
		return errors.Trace(err)
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
//	DDLJobList: list jobs
//	DDLJobHistory: hash
//	DDLJobReorg: hash
//
// for multi DDL workers, only one can become the owner
// to operate DDL jobs, and dispatch them to MR Jobs.

var (
	mDDLJobListKey    = []byte("DDLJobList")
	mDDLJobAddIdxList = []byte("DDLJobAddIdxList")
	mDDLJobHistoryKey = []byte("DDLJobHistory")
	mDDLJobReorgKey   = []byte("DDLJobReorg")
)

// JobListKeyType is a key type of the DDL job queue.
type JobListKeyType []byte

var (
	// DefaultJobListKey keeps all actions of DDL jobs except "add index".
	DefaultJobListKey JobListKeyType = mDDLJobListKey
	// AddIndexJobListKey only keeps the action of adding index.
	AddIndexJobListKey JobListKeyType = mDDLJobAddIdxList
)

func (m *Meta) enQueueDDLJob(key []byte, job *model.Job) error {
	b, err := job.Encode(true)
	if err == nil {
		err = m.txn.RPush(key, b)
	}
	return errors.Trace(err)
}

// EnQueueDDLJob adds a DDL job to the list.
func (m *Meta) EnQueueDDLJob(job *model.Job, jobListKeys ...JobListKeyType) error {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	return m.enQueueDDLJob(listKey, job)
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
	return m.deQueueDDLJob(m.jobListKey)
}

func (m *Meta) getDDLJob(key []byte, index int64) (*model.Job, error) {
	value, err := m.txn.LIndex(key, index)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{
		// For compatibility, if the job is enqueued by old version TiDB and Priority field is omitted,
		// set the default priority to tikvstore.PriorityLow.
		Priority: tikvstore.PriorityLow,
	}
	err = job.Decode(value)
	// Check if the job.Priority is valid.
	if job.Priority < tikvstore.PriorityNormal || job.Priority > tikvstore.PriorityHigh {
		job.Priority = tikvstore.PriorityLow
	}
	return job, errors.Trace(err)
}

// GetDDLJobByIdx returns the corresponding DDL job by the index.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) GetDDLJobByIdx(index int64, jobListKeys ...JobListKeyType) (*model.Job, error) {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	startTime := time.Now()
	job, err := m.getDDLJob(listKey, index)
	metrics.MetaHistogram.WithLabelValues(metrics.GetDDLJobByIdx, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return job, errors.Trace(err)
}

// updateDDLJob updates the DDL job with index and key.
// updateRawArgs is used to determine whether to update the raw args when encode the job.
func (m *Meta) updateDDLJob(index int64, job *model.Job, key []byte, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err == nil {
		err = m.txn.LSet(key, index, b)
	}
	return errors.Trace(err)
}

// UpdateDDLJob updates the DDL job with index.
// updateRawArgs is used to determine whether to update the raw args when encode the job.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) UpdateDDLJob(index int64, job *model.Job, updateRawArgs bool, jobListKeys ...JobListKeyType) error {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	startTime := time.Now()
	err := m.updateDDLJob(index, job, listKey, updateRawArgs)
	metrics.MetaHistogram.WithLabelValues(metrics.UpdateDDLJob, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}

// DDLJobQueueLen returns the DDL job queue length.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) DDLJobQueueLen(jobListKeys ...JobListKeyType) (int64, error) {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}
	return m.txn.LLen(listKey)
}

// GetAllDDLJobsInQueue gets all DDL Jobs in the current queue.
// The length of jobListKeys can only be 1 or 0.
// If its length is 1, we need to replace m.jobListKey with jobListKeys[0].
// Otherwise, we use m.jobListKey directly.
func (m *Meta) GetAllDDLJobsInQueue(jobListKeys ...JobListKeyType) ([]*model.Job, error) {
	listKey := m.jobListKey
	if len(jobListKeys) != 0 {
		listKey = jobListKeys[0]
	}

	values, err := m.txn.LGetAll(listKey)
	if err != nil || values == nil {
		return nil, errors.Trace(err)
	}

	jobs := make([]*model.Job, 0, len(values))
	for _, val := range values {
		job := &model.Job{}
		err = job.Decode(val)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (m *Meta) jobIDKey(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

func (m *Meta) reorgJobCurrentElement(id int64) []byte {
	b := make([]byte, 0, 12)
	b = append(b, m.jobIDKey(id)...)
	b = append(b, "_ele"...)
	return b
}

func (m *Meta) reorgJobStartHandle(id int64, element *Element) []byte {
	b := make([]byte, 0, 16+len(element.TypeKey))
	b = append(b, m.jobIDKey(id)...)
	b = append(b, element.TypeKey...)
	eID := make([]byte, 8)
	binary.BigEndian.PutUint64(eID, uint64(element.ID))
	b = append(b, eID...)
	return b
}

func (m *Meta) reorgJobEndHandle(id int64, element *Element) []byte {
	b := make([]byte, 8, 25)
	binary.BigEndian.PutUint64(b, uint64(id))
	b = append(b, element.TypeKey...)
	eID := make([]byte, 8)
	binary.BigEndian.PutUint64(eID, uint64(element.ID))
	b = append(b, eID...)
	b = append(b, "_end"...)
	return b
}

func (m *Meta) reorgJobPhysicalTableID(id int64, element *Element) []byte {
	b := make([]byte, 8, 25)
	binary.BigEndian.PutUint64(b, uint64(id))
	b = append(b, element.TypeKey...)
	eID := make([]byte, 8)
	binary.BigEndian.PutUint64(eID, uint64(element.ID))
	b = append(b, eID...)
	b = append(b, "_pid"...)
	return b
}

func (m *Meta) addHistoryDDLJob(key []byte, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err == nil {
		err = m.txn.HSet(key, m.jobIDKey(job.ID), b)
	}
	return errors.Trace(err)
}

// AddHistoryDDLJob adds DDL job to history.
func (m *Meta) AddHistoryDDLJob(job *model.Job, updateRawArgs bool) error {
	return m.addHistoryDDLJob(mDDLJobHistoryKey, job, updateRawArgs)
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
	startTime := time.Now()
	job, err := m.getHistoryDDLJob(mDDLJobHistoryKey, id)
	metrics.MetaHistogram.WithLabelValues(metrics.GetHistoryDDLJob, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return job, errors.Trace(err)
}

// GetAllHistoryDDLJobs gets all history DDL jobs.
func (m *Meta) GetAllHistoryDDLJobs() ([]*model.Job, error) {
	pairs, err := m.txn.HGetAll(mDDLJobHistoryKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	jobs, err := decodeJob(pairs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// sort job.
	sorter := &jobsSorter{jobs: jobs}
	sort.Sort(sorter)
	return jobs, nil
}

// GetLastNHistoryDDLJobs gets latest N history ddl jobs.
func (m *Meta) GetLastNHistoryDDLJobs(num int) ([]*model.Job, error) {
	pairs, err := m.txn.HGetLastN(mDDLJobHistoryKey, num)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return decodeJob(pairs)
}

// LastJobIterator is the iterator for gets latest history.
type LastJobIterator struct {
	iter *structure.ReverseHashIterator
}

// GetLastHistoryDDLJobsIterator gets latest N history ddl jobs iterator.
func (m *Meta) GetLastHistoryDDLJobsIterator() (*LastJobIterator, error) {
	iter, err := structure.NewHashReverseIter(m.txn, mDDLJobHistoryKey)
	if err != nil {
		return nil, err
	}
	return &LastJobIterator{
		iter: iter,
	}, nil
}

// GetLastJobs gets last several jobs.
func (i *LastJobIterator) GetLastJobs(num int, jobs []*model.Job) ([]*model.Job, error) {
	if len(jobs) < num {
		jobs = make([]*model.Job, 0, num)
	}
	jobs = jobs[:0]
	iter := i.iter
	for iter.Valid() && len(jobs) < num {
		job := &model.Job{}
		err := job.Decode(iter.Value())
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, job)
		err = iter.Next()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return jobs, nil
}

func decodeJob(jobPairs []structure.HashPair) ([]*model.Job, error) {
	jobs := make([]*model.Job, 0, len(jobPairs))
	for _, pair := range jobPairs {
		job := &model.Job{}
		err := job.Decode(pair.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		jobs = append(jobs, job)
	}
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

// GetBootstrapVersion returns the version of the server which bootstrap the store.
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

// ElementKeyType is a key type of the element.
type ElementKeyType []byte

var (
	// ColumnElementKey is the key for column element.
	ColumnElementKey ElementKeyType = []byte("_col_")
	// IndexElementKey is the key for index element.
	IndexElementKey ElementKeyType = []byte("_idx_")
)

const elementKeyLen = 5

// Element has the information of the backfill job's type and ID.
type Element struct {
	ID      int64
	TypeKey []byte
}

// String defines a Stringer function for debugging and pretty printing.
func (e *Element) String() string {
	return "ID:" + strconv.FormatInt(e.ID, 10) + "," +
		"TypeKey:" + string(e.TypeKey)
}

// EncodeElement encodes an Element into a byte slice.
// It's exported for testing.
func (e *Element) EncodeElement() []byte {
	b := make([]byte, 13)
	copy(b[:elementKeyLen], e.TypeKey)
	binary.BigEndian.PutUint64(b[elementKeyLen:], uint64(e.ID))
	return b
}

// DecodeElement decodes values from a byte slice generated with an element.
// It's exported for testing.
func DecodeElement(b []byte) (*Element, error) {
	if len(b) < elementKeyLen+8 {
		return nil, errors.Errorf("invalid encoded element %q length %d", b, len(b))
	}

	var tp []byte
	prefix := b[:elementKeyLen]
	b = b[elementKeyLen:]
	switch string(prefix) {
	case string(IndexElementKey):
		tp = IndexElementKey
	case string(ColumnElementKey):
		tp = ColumnElementKey
	default:
		return nil, errors.Errorf("invalid encoded element key prefix %q", prefix)
	}

	id := binary.BigEndian.Uint64(b)
	return &Element{ID: int64(id), TypeKey: tp}, nil
}

// UpdateDDLReorgStartHandle saves the job reorganization latest processed element and start handle for later resuming.
func (m *Meta) UpdateDDLReorgStartHandle(job *model.Job, element *Element, startKey kv.Key) error {
	err := m.txn.HSet(mDDLJobReorgKey, m.reorgJobCurrentElement(job.ID), element.EncodeElement())
	if err != nil {
		return errors.Trace(err)
	}
	if startKey != nil {
		err = m.txn.HSet(mDDLJobReorgKey, m.reorgJobStartHandle(job.ID, element), startKey)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// UpdateDDLReorgHandle saves the job reorganization latest processed information for later resuming.
func (m *Meta) UpdateDDLReorgHandle(job *model.Job, startKey, endKey kv.Key, physicalTableID int64, element *Element) error {
	err := m.txn.HSet(mDDLJobReorgKey, m.reorgJobCurrentElement(job.ID), element.EncodeElement())
	if err != nil {
		return errors.Trace(err)
	}
	if startKey != nil {
		err = m.txn.HSet(mDDLJobReorgKey, m.reorgJobStartHandle(job.ID, element), startKey)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if endKey != nil {
		err = m.txn.HSet(mDDLJobReorgKey, m.reorgJobEndHandle(job.ID, element), endKey)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = m.txn.HSet(mDDLJobReorgKey, m.reorgJobPhysicalTableID(job.ID, element), []byte(strconv.FormatInt(physicalTableID, 10)))
	return errors.Trace(err)
}

// RemoveReorgElement removes the element of the reorganization information.
// It's used for testing.
func (m *Meta) RemoveReorgElement(job *model.Job) error {
	err := m.txn.HDel(mDDLJobReorgKey, m.reorgJobCurrentElement(job.ID))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// RemoveDDLReorgHandle removes the job reorganization related handles.
func (m *Meta) RemoveDDLReorgHandle(job *model.Job, elements []*Element) error {
	if len(elements) == 0 {
		return nil
	}

	err := m.txn.HDel(mDDLJobReorgKey, m.reorgJobCurrentElement(job.ID))
	if err != nil {
		return errors.Trace(err)
	}

	for _, element := range elements {
		err = m.txn.HDel(mDDLJobReorgKey, m.reorgJobStartHandle(job.ID, element))
		if err != nil {
			return errors.Trace(err)
		}
		if err = m.txn.HDel(mDDLJobReorgKey, m.reorgJobEndHandle(job.ID, element)); err != nil {
			logutil.BgLogger().Warn("remove DDL reorg end handle", zap.Error(err))
		}
		if err = m.txn.HDel(mDDLJobReorgKey, m.reorgJobPhysicalTableID(job.ID, element)); err != nil {
			logutil.BgLogger().Warn("remove DDL reorg physical ID", zap.Error(err))
		}
	}
	return nil
}

// GetDDLReorgHandle gets the latest processed DDL reorganize position.
func (m *Meta) GetDDLReorgHandle(job *model.Job) (element *Element, startKey, endKey kv.Key, physicalTableID int64, err error) {
	elementBytes, err := m.txn.HGet(mDDLJobReorgKey, m.reorgJobCurrentElement(job.ID))
	if err != nil {
		return nil, nil, nil, 0, errors.Trace(err)
	}
	if elementBytes == nil {
		return nil, nil, nil, 0, ErrDDLReorgElementNotExist
	}
	element, err = DecodeElement(elementBytes)
	if err != nil {
		return nil, nil, nil, 0, errors.Trace(err)
	}

	startKey, err = getReorgJobFieldHandle(m.txn, m.reorgJobStartHandle(job.ID, element))
	if err != nil {
		return nil, nil, nil, 0, errors.Trace(err)
	}
	endKey, err = getReorgJobFieldHandle(m.txn, m.reorgJobEndHandle(job.ID, element))
	if err != nil {
		return nil, nil, nil, 0, errors.Trace(err)
	}

	physicalTableID, err = m.txn.HGetInt64(mDDLJobReorgKey, m.reorgJobPhysicalTableID(job.ID, element))
	if err != nil {
		err = errors.Trace(err)
		return
	}

	// physicalTableID may be 0, because older version TiDB (without table partition) doesn't store them.
	// update them to table's in this case.
	if physicalTableID == 0 {
		if job.ReorgMeta != nil {
			endKey = kv.IntHandle(job.ReorgMeta.EndHandle).Encoded()
		} else {
			endKey = kv.IntHandle(math.MaxInt64).Encoded()
		}
		physicalTableID = job.TableID
		logutil.BgLogger().Warn("new TiDB binary running on old TiDB DDL reorg data",
			zap.Int64("partition ID", physicalTableID),
			zap.Stringer("startHandle", startKey),
			zap.Stringer("endHandle", endKey))
	}
	return
}

func getReorgJobFieldHandle(t *structure.TxStructure, reorgJobField []byte) (kv.Key, error) {
	bs, err := t.HGet(mDDLJobReorgKey, reorgJobField)
	if err != nil {
		return nil, errors.Trace(err)
	}
	keyNotFound := bs == nil
	if keyNotFound {
		return nil, nil
	}
	return bs, nil
}

func (m *Meta) schemaDiffKey(schemaVersion int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mSchemaDiffPrefix, schemaVersion))
}

// GetSchemaDiff gets the modification information on a given schema version.
func (m *Meta) GetSchemaDiff(schemaVersion int64) (*model.SchemaDiff, error) {
	diffKey := m.schemaDiffKey(schemaVersion)
	startTime := time.Now()
	data, err := m.txn.Get(diffKey)
	metrics.MetaHistogram.WithLabelValues(metrics.GetSchemaDiff, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	if err != nil || len(data) == 0 {
		return nil, errors.Trace(err)
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
	startTime := time.Now()
	err = m.txn.Set(diffKey, data)
	metrics.MetaHistogram.WithLabelValues(metrics.SetSchemaDiff, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return errors.Trace(err)
}
