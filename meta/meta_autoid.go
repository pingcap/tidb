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
)

var _ AutoIDAccessor = autoIDAccessor{}

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

	tableIDEncFn func(int64) []byte
}

// Get implements the interface AutoIDAccessor.
func (a autoIDAccessor) Get() (int64, error) {
	m := a.m
	return m.txn.HGetInt64(m.dbKey(a.databaseID), a.tableIDEncFn(a.tableID))
}

// Put implements the interface AutoIDAccessor.
func (a autoIDAccessor) Put(val int64) error {
	m := a.m
	return m.txn.HSet(m.dbKey(a.databaseID), a.tableIDEncFn(a.tableID), []byte(strconv.FormatInt(val, 10)))
}

// Inc implements the interface AutoIDAccessor.
func (a autoIDAccessor) Inc(step int64) (int64, error) {
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

	return m.txn.HInc(dbKey, a.tableIDEncFn(a.tableID), step)
}

// Del implements the interface AutoIDAccessor.
func (a autoIDAccessor) Del() error {
	m := a.m
	dbKey := m.dbKey(a.databaseID)
	if err := m.txn.HDel(dbKey, a.tableIDEncFn(a.tableID)); err != nil {
		return errors.Trace(err)
	}
	return nil
}

var _ AutoIDAccessors = autoIDAccessors{}

// AutoIDAccessors represents all the auto IDs of a table.
type AutoIDAccessors interface {
	Get() (AutoIDs, error)
	Put(autoIDs AutoIDs) error
	Del() error
}

type autoIDAccessors struct {
	rowIDAcc  AutoIDAccessor
	randIDAcc AutoIDAccessor
}

// Get implements the interface AutoIDAccessors.
func (a autoIDAccessors) Get() (autoIDs AutoIDs, err error) {
	autoIDs.RowID, err = a.rowIDAcc.Get()
	if err != nil {
		return
	}
	autoIDs.RandomID, err = a.randIDAcc.Get()
	if err != nil {
		return
	}
	return
}

// Put implements the interface AutoIDAccessors.
func (a autoIDAccessors) Put(autoIDs AutoIDs) error {
	if err := a.rowIDAcc.Put(autoIDs.RowID); err != nil {
		return err
	}
	return a.randIDAcc.Put(autoIDs.RandomID)
}

// Del implements the interface AutoIDAccessors.
func (a autoIDAccessors) Del() error {
	if err := a.rowIDAcc.Del(); err != nil {
		return err
	}
	return a.randIDAcc.Del()
}

// AutoIDCtrl can be changed into rowid/auto_increment/auto_random ID accessor.
type AutoIDCtrl struct {
	access autoIDAccessor
}

// RowID is used to get the _tidb_rowid meta key-value accessor.
func (a *AutoIDCtrl) RowID() AutoIDAccessor {
	a.access.tableIDEncFn = a.access.m.autoTableIDKey
	return a.access
}

// IncrementID is used to get the auto_increment ID meta key-value accessor.
func (a *AutoIDCtrl) IncrementID() AutoIDAccessor {
	a.access.tableIDEncFn = a.access.m.autoIncrementIDKey
	return a.access
}

// RandomID is used to get the auto_random ID meta key-value accessor.
func (a *AutoIDCtrl) RandomID() AutoIDAccessor {
	a.access.tableIDEncFn = a.access.m.autoRandomTableIDKey
	return a.access
}

// All is used to get all auto IDs meta key-value accessor.
func (a *AutoIDCtrl) All() AutoIDAccessors {
	acc := autoIDAccessor{
		m:          a.access.m,
		databaseID: a.access.databaseID,
		tableID:    a.access.tableID,
	}
	randAcc := acc
	acc.tableIDEncFn = a.access.m.autoTableIDKey
	randAcc.tableIDEncFn = a.access.m.autoRandomTableIDKey
	return autoIDAccessors{
		rowIDAcc:  acc,
		randIDAcc: randAcc,
	}
}

// NewAutoIDCtrl creates a new AutoIDCtrl.
func NewAutoIDCtrl(m *Meta, databaseID, tableID int64) *AutoIDCtrl {
	return &AutoIDCtrl{
		autoIDAccessor{
			m:          m,
			databaseID: databaseID,
			tableID:    tableID,
		},
	}
}

// AutoIDs represents all auto IDs of a table.
type AutoIDs struct {
	RowID    int64
	RandomID int64
}

// BackupAndRestoreAutoIDs changes the meta key-values to fetch & delete
// all the auto IDs from a old table, and set them to a new table.
func BackupAndRestoreAutoIDs(m *Meta, databaseID, tableID int64,
	newDatabaseID, newTableID int64, transform func(AutoIDs) (AutoIDs, error)) (err error) {
	ctrl := NewAutoIDCtrl(m, databaseID, tableID).All()
	autoIDs, err := ctrl.Get()
	if err != nil {
		return errors.Trace(err)
	}
	overwriteIDs := databaseID == newDatabaseID && tableID == newTableID
	if !overwriteIDs {
		err = ctrl.Del()
		if err != nil {
			return errors.Trace(err)
		}
	}
	if transform != nil {
		autoIDs, err = transform(autoIDs)
		if err != nil {
			return errors.Trace(err)
		}
	}
	err = NewAutoIDCtrl(m, newDatabaseID, newTableID).All().Put(autoIDs)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
