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
	"strconv"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/structure"
)

var (
	globalIDMutex sync.Mutex
)

// Meta structure:
//	mNextGlobalID -> int64
//	mSchemaVersion -> int64
//	mDBs -> {
//		mDB:1 -> db meta data []byte
//		mDB:2 -> db meta data []byte
//	}
//	mDB:1 -> {
//		mTable:1 -> table meta data []byte
//		mTable:2 -> table meta data []byte
//		mTID:1 -> int64
//		mTID:2 -> int64
//	}
//

var (
	mNextGlobalIDKey  = []byte("mNextGlobalID")
	mSchemaVersionKey = []byte("mSchemaVersionKey")
	mDBs              = []byte("mDBs")
	mDBPrefix         = "mDB"
	mTablePrefix      = "mTable"
	mTableIDPrefix    = "mTID"
)

var (
	// ErrDBExists is the error for db exists.
	ErrDBExists = errors.New("database already exists")
	// ErrDBNotExists is the error for db not exists.
	ErrDBNotExists = errors.New("database doesn't exist")
	// ErrTableExists is the error for table exists.
	ErrTableExists = errors.New("table already exists")
	// ErrTableNotExists is the error for table not exists.
	ErrTableNotExists = errors.New("table doesn't exist")
)

// TMeta is for handling meta information in a transaction.
type TMeta struct {
	txn *structure.TxStructure
}

// NewMeta creates a TMeta in transaction txn.
func NewMeta(txn kv.Transaction) *TMeta {
	t := structure.NewStructure(txn, []byte{0x00})
	return &TMeta{txn: t}
}

// GenGlobalID generates next id globally.
func (m *TMeta) GenGlobalID() (int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	return m.txn.Inc(mNextGlobalIDKey, 1)
}

// GetGlobalID gets current global id.
func (m *TMeta) GetGlobalID() (int64, error) {
	return m.txn.GetInt64(mNextGlobalIDKey)
}

func (m *TMeta) dbKey(dbID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mDBPrefix, dbID))
}

func (m *TMeta) parseDatabaseID(key string) (int64, error) {
	seps := strings.Split(key, ":")
	if len(seps) != 2 {
		return 0, errors.Errorf("invalid db key %s", key)
	}

	n, err := strconv.ParseInt(seps[1], 10, 64)
	return n, errors.Trace(err)
}

func (m *TMeta) autoTalbeIDKey(tableID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mTableIDPrefix, tableID))
}

func (m *TMeta) tableKey(tableID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mTablePrefix, tableID))
}

func (m *TMeta) parseTableID(key string) (int64, error) {
	seps := strings.Split(key, ":")
	if len(seps) != 2 {
		return 0, errors.Errorf("invalid table meta key %s", key)
	}

	n, err := strconv.ParseInt(seps[1], 10, 64)
	return n, errors.Trace(err)
}

// GenAutoTableID adds step to the auto id of the table and returns the sum.
func (m *TMeta) GenAutoTableID(dbID int64, tableID int64, step int64) (int64, error) {
	// check db exists
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return 0, errors.Trace(err)
	}

	// check table exists
	tableKey := m.tableKey(tableID)
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		return 0, errors.Trace(err)
	}

	return m.txn.HInc(dbKey, m.autoTalbeIDKey(tableID), step)
}

// GetAutoTableID gets current auto id with table id.
func (m *TMeta) GetAutoTableID(dbID int64, tableID int64) (int64, error) {
	return m.txn.HGetInt64(m.dbKey(dbID), m.autoTalbeIDKey(tableID))
}

// GetSchemaVersion gets current global schema version.
func (m *TMeta) GetSchemaVersion() (int64, error) {
	return m.txn.GetInt64(mSchemaVersionKey)
}

// GenSchemaVersion generates next schema version.
func (m *TMeta) GenSchemaVersion() (int64, error) {
	return m.txn.Inc(mSchemaVersionKey, 1)
}

func (m *TMeta) checkDBExists(dbKey []byte) error {
	v, err := m.txn.HGet(mDBs, dbKey)
	if err != nil {
		return errors.Trace(err)
	} else if v == nil {
		return ErrDBNotExists
	}

	return nil
}

func (m *TMeta) checkDBNotExists(dbKey []byte) error {
	v, err := m.txn.HGet(mDBs, dbKey)
	if err != nil {
		return errors.Trace(err)
	} else if v != nil {
		return ErrDBExists
	}

	return nil
}

func (m *TMeta) checkTableExists(dbKey []byte, tableKey []byte) error {
	v, err := m.txn.HGet(dbKey, tableKey)
	if err != nil {
		return errors.Trace(err)
	} else if v == nil {
		return ErrTableNotExists
	}

	return nil
}

func (m *TMeta) checkTableNotExists(dbKey []byte, tableKey []byte) error {
	v, err := m.txn.HGet(dbKey, tableKey)
	if err != nil {
		return errors.Trace(err)
	} else if v != nil {
		return ErrTableExists
	}

	return nil
}

// CreateDatabase creates a database with db info.
func (m *TMeta) CreateDatabase(dbInfo *model.DBInfo) error {
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
func (m *TMeta) UpdateDatabase(dbInfo *model.DBInfo) error {
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
func (m *TMeta) CreateTable(dbID int64, tableInfo *model.TableInfo) error {
	// first check db exists or not.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	tableKey := m.tableKey(tableInfo.ID)
	// then check table exists or not
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
func (m *TMeta) DropDatabase(dbID int64) error {
	// first check db exists or not.
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
func (m *TMeta) DropTable(dbID int64, tableID int64) error {
	// first check db exists or not.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	tableKey := m.tableKey(tableID)

	// then check table exists or not
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.HDel(dbKey, tableKey); err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.HDel(dbKey, m.autoTalbeIDKey(tableID)); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// UpdateTable updates the table with table info.
func (m *TMeta) UpdateTable(dbID int64, tableInfo *model.TableInfo) error {
	// first check db exists or not.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	tableKey := m.tableKey(tableInfo.ID)

	// then check table exists or not
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
func (m *TMeta) ListTables(dbID int64) ([]*model.TableInfo, error) {
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
func (m *TMeta) ListDatabases() ([]*model.DBInfo, error) {
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
func (m *TMeta) GetDatabase(dbID int64) (*model.DBInfo, error) {
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
func (m *TMeta) GetTable(dbID int64, tableID int64) (*model.TableInfo, error) {
	// first check db exists or not.
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

// DDL structure
//	mDDLOnwer: []byte
//	mDDLJobList: list jobs
//	mDDLJobHistory: hash
//
// for multi DDL workers, only one can become the owner
// to operate DDL jobs, and dispatch them to MR Jobs.

var (
	mDDLOwnerKey      = []byte("mDDLOwner")
	mDDLJobListKey    = []byte("mDDLJobList")
	mDDLJobHistoryKey = []byte("mDDLJobHistory")
)

// GetDDLOwner gets the current owner for DDL.
func (m *TMeta) GetDDLOwner() (*model.Owner, error) {
	value, err := m.txn.Get(mDDLOwnerKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	owner := &model.Owner{}
	err = json.Unmarshal(value, owner)
	return owner, errors.Trace(err)
}

// SetDDLOwner sets the current owner for DDL.
func (m *TMeta) SetDDLOwner(o *model.Owner) error {
	b, err := json.Marshal(o)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.Set(mDDLOwnerKey, b)
}

// EnQueueDDLJob adds a DDL job to the list.
func (m *TMeta) EnQueueDDLJob(job *model.Job) error {
	b, err := json.Marshal(job)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.RPush(mDDLJobListKey, b)
}

// DeQueueDDLJob pops a DDL job from the list.
func (m *TMeta) DeQueueDDLJob() (*model.Job, error) {
	value, err := m.txn.LPop(mDDLJobListKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{}
	err = json.Unmarshal(value, job)
	return job, errors.Trace(err)
}

// GetDDLJob returns the DDL job with index.
func (m *TMeta) GetDDLJob(index int64) (*model.Job, error) {
	value, err := m.txn.LIndex(mDDLJobListKey, index)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{}
	err = json.Unmarshal(value, job)
	return job, errors.Trace(err)
}

// UpdateDDLJob updates the DDL job with index.
func (m *TMeta) UpdateDDLJob(index int64, job *model.Job) error {
	b, err := json.Marshal(job)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.LSet(mDDLJobListKey, index, b)
}

// DDLJobLength returns the DDL job length.
func (m *TMeta) DDLJobLength() (int64, error) {
	return m.txn.LLen(mDDLJobListKey)
}

func (m *TMeta) jobIDKey(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

// AddHistoryDDLJob adds DDL job to history.
func (m *TMeta) AddHistoryDDLJob(job *model.Job) error {
	b, err := json.Marshal(job)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.HSet(mDDLJobHistoryKey, m.jobIDKey(job.ID), b)
}

// GetHistoryDDLJob gets a history DDL job.
func (m *TMeta) GetHistoryDDLJob(id int64) (*model.Job, error) {
	value, err := m.txn.HGet(mDDLJobHistoryKey, m.jobIDKey(id))
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{}
	err = json.Unmarshal(value, job)
	return job, errors.Trace(err)
}
