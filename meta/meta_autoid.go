// Copyright 2021 PingCAP, Inc.
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

package meta

import (
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/parser/model"
)

var _ AutoIDAccessor = &autoIDAccessor{}

// AutoIDAccessor represents the entry to retrieve/mutate auto IDs.
type AutoIDAccessor interface {
	Get() (int64, error)
	Put(val int64) error
	Inc(step int64) (int64, error)
	Del() error
}

type autoIDAccessor struct {
	m          *Meta
	databaseID int64
	tableID    int64

	idEncodeFn func(int64) []byte
}

// Get implements the interface AutoIDAccessor.
func (a *autoIDAccessor) Get() (int64, error) {
	m := a.m
	return m.txn.HGetInt64(m.dbKey(a.databaseID), a.idEncodeFn(a.tableID))
}

// Put implements the interface AutoIDAccessor.
func (a *autoIDAccessor) Put(val int64) error {
	m := a.m
	return m.txn.HSet(m.dbKey(a.databaseID), a.idEncodeFn(a.tableID), []byte(strconv.FormatInt(val, 10)))
}

// Inc implements the interface AutoIDAccessor.
func (a *autoIDAccessor) Inc(step int64) (int64, error) {
	m := a.m
	dbKey := m.dbKey(a.databaseID)
	if err := m.checkDBExists(dbKey); err != nil {
		return 0, errors.Trace(err)
	}
	// Check if table exists.
	tableKey := m.tableKey(a.tableID)
	if err := m.checkTableExists(dbKey, tableKey); err != nil {
		return 0, errors.Trace(err)
	}

	return m.txn.HInc(dbKey, a.idEncodeFn(a.tableID), step)
}

// Del implements the interface AutoIDAccessor.
func (a *autoIDAccessor) Del() error {
	m := a.m
	dbKey := m.dbKey(a.databaseID)
	if err := m.txn.HDel(dbKey, a.idEncodeFn(a.tableID)); err != nil {
		return errors.Trace(err)
	}
	return nil
}

var _ AutoIDAccessors = &autoIDAccessors{}

// AutoIDAccessors represents all the auto IDs of a table.
type AutoIDAccessors interface {
	Get() (AutoIDGroup, error)
	Put(autoIDs AutoIDGroup) error
	Del() error

	AccessorPicker
}

// AccessorPicker is used to pick a type of auto ID accessor.
type AccessorPicker interface {
	RowID() AutoIDAccessor
	RandomID() AutoIDAccessor
	IncrementID(tableVersion uint16) AutoIDAccessor

	SequenceValue() AutoIDAccessor
	SequenceCycle() AutoIDAccessor
}

type autoIDAccessors struct {
	access autoIDAccessor
}

const sepAutoIncVer = model.TableInfoVersion4 + 1

// Get implements the interface AutoIDAccessors.
func (a *autoIDAccessors) Get() (autoIDs AutoIDGroup, err error) {
	if autoIDs.RowID, err = a.RowID().Get(); err != nil {
		return autoIDs, err
	}
	if autoIDs.IncrementID, err = a.IncrementID(sepAutoIncVer).Get(); err != nil {
		return autoIDs, err
	}
	if autoIDs.RandomID, err = a.RandomID().Get(); err != nil {
		return autoIDs, err
	}
	return
}

// Put implements the interface AutoIDAccessors.
func (a *autoIDAccessors) Put(autoIDs AutoIDGroup) error {
	if err := a.RowID().Put(autoIDs.RowID); err != nil {
		return err
	}
	if err := a.IncrementID(sepAutoIncVer).Put(autoIDs.IncrementID); err != nil {
		return err
	}
	return a.RandomID().Put(autoIDs.RandomID)
}

// Del implements the interface AutoIDAccessors.
func (a *autoIDAccessors) Del() error {
	if err := a.RowID().Del(); err != nil {
		return err
	}
	if err := a.IncrementID(sepAutoIncVer).Del(); err != nil {
		return err
	}
	return a.RandomID().Del()
}

// RowID is used to get the _tidb_rowid meta key-value accessor.
func (a *autoIDAccessors) RowID() AutoIDAccessor {
	a.access.idEncodeFn = a.access.m.autoTableIDKey
	return &a.access
}

// IncrementID is used to get the auto_increment ID meta key-value accessor.
func (a *autoIDAccessors) IncrementID(tableVersion uint16) AutoIDAccessor {
	// _tidb_rowid and auto_increment ID in old version TiDB share the same meta key-value.
	if tableVersion < sepAutoIncVer {
		a.access.idEncodeFn = a.access.m.autoTableIDKey
	} else {
		a.access.idEncodeFn = a.access.m.autoIncrementIDKey
	}
	return &a.access
}

// RandomID is used to get the auto_random ID meta key-value accessor.
func (a *autoIDAccessors) RandomID() AutoIDAccessor {
	a.access.idEncodeFn = a.access.m.autoRandomTableIDKey
	return &a.access
}

// SequenceValue is used to get the sequence value key-value accessor.
func (a *autoIDAccessors) SequenceValue() AutoIDAccessor {
	a.access.idEncodeFn = a.access.m.sequenceKey
	return &a.access
}

// SequenceCircle is used to get the sequence circle key-value accessor.
func (a *autoIDAccessors) SequenceCycle() AutoIDAccessor {
	a.access.idEncodeFn = a.access.m.sequenceCycleKey
	return &a.access
}

// NewAutoIDAccessors creates a new AutoIDAccessors.
func NewAutoIDAccessors(m *Meta, databaseID, tableID int64) AutoIDAccessors {
	return &autoIDAccessors{
		autoIDAccessor{
			m:          m,
			databaseID: databaseID,
			tableID:    tableID,
		},
	}
}

// AutoIDGroup represents a group of auto IDs of a specific table.
type AutoIDGroup struct {
	RowID       int64
	IncrementID int64
	RandomID    int64
}

// BackupAndRestoreAutoIDs changes the meta key-values to fetch & delete
// all the auto IDs from an old table, and set them to a new table.
func BackupAndRestoreAutoIDs(m *Meta, databaseID, tableID int64, newDatabaseID, newTableID int64) (err error) {
	acc := NewAutoIDAccessors(m, databaseID, tableID)
	autoIDs, err := acc.Get()
	if err != nil {
		return errors.Trace(err)
	}
	overwriteIDs := databaseID == newDatabaseID && tableID == newTableID
	if !overwriteIDs {
		err = acc.Del()
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = NewAutoIDAccessors(m, newDatabaseID, newTableID).Put(autoIDs)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
