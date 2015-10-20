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
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/structure"
)

var (
	// SchemaMetaPrefix is the prefix for database meta key prefix.
	SchemaMetaPrefix = string(MakeMetaKey("mDB:"))

	// TableMetaPrefix is the prefix for table meta key prefix.
	TableMetaPrefix = MakeMetaKey("mTable:")

	// SchemaMetaVersionKey is used as lock for changing schema.
	SchemaMetaVersionKey = MakeMetaKey("mSchemaVersion")

	// globalIDPrefix is used as the key of global ID.
	globalIDKey = MakeMetaKey("mNextGlobalID")

	// globalSchemaKey is used as the hash key for all schemas.
	globalSchemaKey = MakeMetaKey("mDBs")
)

var (
	globalIDMutex sync.Mutex
)

// MakeMetaKey creates meta key
func MakeMetaKey(key string) []byte {
	return append([]byte{0x0}, key...)
}

// GenID adds step to the value for key and returns the sum.
func GenID(txn kv.Transaction, key []byte, step int) (int64, error) {
	if len(key) == 0 {
		return 0, errors.New("Invalid key")
	}
	err := txn.LockKeys(key)
	if err != nil {
		return 0, err
	}
	id, err := txn.Inc(key, int64(step))
	if err != nil {
		return 0, errors.Trace(err)
	}

	return id, errors.Trace(err)
}

// DBMetaKey generates database meta key according to databaseID.
func DBMetaKey(databaseID int64) string {
	return fmt.Sprintf("%s:%d", SchemaMetaPrefix, databaseID)
}

func parseDBMetaKey(key string) (int64, error) {
	seps := strings.Split(key, "::")
	if len(seps) != 2 {
		return 0, errors.Errorf("invalid db meta key %s", key)
	}

	n, err := strconv.ParseInt(seps[1], 10, 64)
	return n, errors.Trace(err)
}

// TableMetaKey generates table meta key according to tableID.
func TableMetaKey(tableID int64) string {
	return fmt.Sprintf("%s:%d", TableMetaPrefix, tableID)
}

func parseTableMetaKey(key string) (int64, error) {
	seps := strings.Split(key, "::")
	if len(seps) != 2 {
		return 0, errors.Errorf("invalid db meta key %s", key)
	}

	n, err := strconv.ParseInt(seps[1], 10, 64)
	return n, errors.Trace(err)
}

// AutoIDKey generates table autoID meta key according to tableID.
func AutoIDKey(tableID int64) string {
	if tableID == 0 {
		log.Error("Invalid tableID")
	}
	return fmt.Sprintf("%s:%d_auto_id", TableMetaPrefix, tableID)
}

// GenGlobalID generates the next id in the store scope.
func GenGlobalID(store kv.Storage) (ID int64, err error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()
	err = kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		ID, err = GenID(txn, globalIDKey, 1)
		if err != nil {
			return errors.Trace(err)
		}

		log.Info("Generate global id", ID)

		return nil
	})

	return
}

var (
	ErrDBExists       = errors.New("database already exists")
	ErrDBNotExists    = errors.New("database doesn't exist")
	ErrTableExists    = errors.New("table already exists")
	ErrTableNotExists = errors.New("table doesn't exist")
)

// Meta is the structure saving meta information.
type Meta struct {
	store *structure.TStore
}

// TMeta is for handling meta information in a transaction.
type TMeta struct {
	txn *structure.TStructure
}

func NewMeta(store kv.Storage) *Meta {
	m := &Meta{}

	m.store = structure.NewStore(store, []byte{0x00})

	return m
}

// Begin creates a TMeta object and you can handle meta information in a transaction.
func (m *Meta) Begin() (*TMeta, error) {
	txn := &TMeta{}

	var err error
	txn.txn, err = m.store.Begin()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return txn, nil
}

// RunInNewTxn runs fn in a new transaction.
func (m *Meta) RunInNewTxn(retryable bool, fn func(t *TMeta) error) error {
	const maxRetryNum = 10
	for i := 0; i < maxRetryNum; i++ {
		txn, err := m.Begin()
		if err != nil {
			log.Errorf("RunInNewTxn error - %v", err)
			return errors.Trace(err)
		}

		err = fn(txn)
		if retryable && kv.IsRetryableError(err) {
			log.Warnf("Retry txn %v", txn)
			txn.Rollback()
			continue
		}
		if err != nil {
			return errors.Trace(err)
		}

		err = txn.Commit()
		if retryable && kv.IsRetryableError(err) {
			log.Warnf("Retry txn %v", txn)
			txn.Rollback()
			continue
		}

		return errors.Trace(err)
	}

	return errors.Errorf("retry too many times")
}

// GenGlobalID generates next id globally.
func (m *TMeta) GenGlobalID() (int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	return m.txn.Inc(globalIDKey, 1)
}

// GenAutoTableID adds step to the auto id of the table and returns the sum.
func (m *TMeta) GenAutoTableID(tableID int64, step int64) (int64, error) {
	return m.txn.Inc([]byte(AutoIDKey(tableID)), step)
}

// GetSchemaVersion gets current global schema version.
func (m *TMeta) GetSchemaVersion() (int64, error) {
	return m.txn.GetInt64(SchemaMetaVersionKey)
}

// GenSchemaVersion generates next schema version.
func (m *TMeta) GenSchemaVersion() (int64, error) {
	return m.txn.Inc(SchemaMetaVersionKey, 1)
}

func (m *TMeta) checkDBExists(dbKey []byte) error {
	v, err := m.txn.HGet(globalSchemaKey, dbKey)
	if err != nil {
		return err
	} else if v == nil {
		return ErrDBNotExists
	}

	return nil
}

func (m *TMeta) checkDBNotExists(dbKey []byte) error {
	v, err := m.txn.HGet(globalSchemaKey, dbKey)
	if err != nil {
		return err
	} else if v != nil {
		return ErrDBExists
	}

	return nil
}

func (m *TMeta) checkTableExists(dbKey []byte, tableKey []byte) error {
	v, err := m.txn.HGet(dbKey, tableKey)
	if err != nil {
		return err
	} else if v == nil {
		return ErrTableNotExists
	}

	return nil
}

func (m *TMeta) checkTableNotExists(dbKey []byte, tableKey []byte) error {
	v, err := m.txn.HGet(dbKey, tableKey)
	if err != nil {
		return err
	} else if v != nil {
		return ErrTableExists
	}

	return nil
}

// CreateDatabase creates a database with id and some information.
func (m *TMeta) CreateDatabase(dbID int64, data []byte) error {
	dbKey := []byte(DBMetaKey(dbID))

	if err := m.checkDBNotExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(globalSchemaKey, dbKey, data)
}

// UpdateDatabase updates a database with id and some information.
func (m *TMeta) UpdateDatabase(dbID int64, data []byte) error {
	dbKey := []byte(DBMetaKey(dbID))

	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(globalSchemaKey, dbKey, data)
}

// CreateTable creates a table with tableID in database.
func (m *TMeta) CreateTable(dbID int64, tableID int64, data []byte) error {
	// first check db exists or not.
	dbKey := []byte(DBMetaKey(dbID))
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	tableKey := []byte(TableMetaKey(tableID))
	// then check table exists or not
	if err := m.checkTableNotExists(dbKey, tableKey); err != nil {
		return errors.Trace(err)
	}

	return m.txn.HSet(dbKey, tableKey, data)
}

// DropDatabase drops whole database.
func (m *TMeta) DropDatabase(dbID int64) error {
	// first check db exists or not.
	dbKey := []byte(DBMetaKey(dbID))

	ids, err := m.ListTableIDs(dbID)
	if err != nil {
		return errors.Trace(err)
	}

	// remove auto id.
	for _, tableID := range ids {
		if err = m.txn.Clear([]byte(AutoIDKey(tableID))); err != nil {
			return errors.Trace(err)
		}
	}

	if err = m.txn.HClear(dbKey); err != nil {
		return errors.Trace(err)
	}

	if err = m.txn.HDel(globalSchemaKey, dbKey); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// DropTable drops table in database.
func (m *TMeta) DropTable(dbID int64, tableID int64) error {
	// first check db exists or not.
	dbKey := []byte(DBMetaKey(dbID))
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	tableKey := []byte(TableMetaKey(tableID))

	// then check table exists or not
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.HDel(dbKey, tableKey); err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.Clear([]byte(AutoIDKey(tableID))); err != nil {
		return errors.Trace(err)
	}

	return nil
}

// UpdateTable updates the table with tableID in database.
func (m *TMeta) UpdateTable(dbID int64, tableID int64, data []byte) error {
	// first check db exists or not.
	dbKey := []byte(DBMetaKey(dbID))
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	tableKey := []byte(TableMetaKey(tableID))

	// then check table exists or not
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		return errors.Trace(err)
	}

	err := m.txn.HSet(dbKey, tableKey, data)

	return errors.Trace(err)
}

// ListTables shows all table IDs in database.
func (m *TMeta) ListTableIDs(dbID int64) ([]int64, error) {
	dbKey := []byte(DBMetaKey(dbID))
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}

	keys, err := m.txn.HKeys(dbKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ids := make([]int64, 0, len(keys)/2)
	for _, key := range keys {
		var id int64
		id, err = parseTableMetaKey(string(key))
		if err != nil {
			return nil, errors.Trace(err)
		}

		ids = append(ids, id)
	}

	return ids, nil
}

// ListDatabases shows all database IDs.
func (m *TMeta) ListDatabaseIDs() ([]int64, error) {
	keys, err := m.txn.HKeys(globalSchemaKey)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ids := make([]int64, 0, len(keys))
	for _, key := range keys {
		var id int64
		id, err = parseDBMetaKey(string(key))
		if err != nil {
			return nil, errors.Trace(err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// GetDatabase gets the database value with ID.
func (m *TMeta) GetDatabase(dbID int64) ([]byte, error) {
	dbKey := []byte(DBMetaKey(dbID))
	return m.txn.HGet(globalSchemaKey, dbKey)
}

// GetTable gets the table value in database with tableID.
func (m *TMeta) GetTable(dbID int64, tableID int64) ([]byte, error) {
	// first check db exists or not.
	dbKey := []byte(DBMetaKey(dbID))
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}

	tableKey := []byte(TableMetaKey(tableID))

	return m.txn.HGet(dbKey, tableKey)
}

// Commit commits the transaction.
func (m *TMeta) Commit() error {
	return m.txn.Commit()
}

// Rollback rolls back the transaction.
func (m *TMeta) Rollback() error {
	return m.txn.Rollback()
}
