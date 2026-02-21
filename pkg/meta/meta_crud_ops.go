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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package meta

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// CreatePolicy creates a policy.
func (m *Mutator) CreatePolicy(policy *model.PolicyInfo) error {
	if policy.ID == 0 {
		return errors.New("policy.ID is invalid")
	}

	policyKey := m.policyKey(policy.ID)
	if err := m.checkPolicyNotExists(policyKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(policy)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.HSet(mPolicies, policyKey, attachMagicByte(data))
}

// UpdatePolicy updates a policy.
func (m *Mutator) UpdatePolicy(policy *model.PolicyInfo) error {
	policyKey := m.policyKey(policy.ID)

	if err := m.checkPolicyExists(policyKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(policy)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.HSet(mPolicies, policyKey, attachMagicByte(data))
}

// AddResourceGroup creates a resource group.
func (m *Mutator) AddResourceGroup(group *model.ResourceGroupInfo) error {
	if group.ID == 0 {
		return errors.New("group.ID is invalid")
	}
	groupKey := m.resourceGroupKey(group.ID)
	if err := m.checkResourceGroupNotExists(groupKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(group)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.HSet(mResourceGroups, groupKey, attachMagicByte(data))
}

// UpdateResourceGroup updates a resource group.
func (m *Mutator) UpdateResourceGroup(group *model.ResourceGroupInfo) error {
	groupKey := m.resourceGroupKey(group.ID)
	// do not check the default because it may not be persisted.
	if group.ID != defaultGroupID {
		if err := m.checkResourceGroupExists(groupKey); err != nil {
			return errors.Trace(err)
		}
	}

	data, err := json.Marshal(group)
	if err != nil {
		return errors.Trace(err)
	}
	return m.txn.HSet(mResourceGroups, groupKey, attachMagicByte(data))
}

// DropResourceGroup drops a resource group.
func (m *Mutator) DropResourceGroup(groupID int64) error {
	// Check if group exists.
	groupKey := m.resourceGroupKey(groupID)
	if err := m.txn.HDel(mResourceGroups, groupKey); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// CreateDatabase creates a database with db info.
func (m *Mutator) CreateDatabase(dbInfo *model.DBInfo) error {
	dbKey := m.dbKey(dbInfo.ID)

	if err := m.checkDBNotExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	data, err := json.Marshal(dbInfo)
	if err != nil {
		return errors.Trace(err)
	}

	if err := m.txn.HSet(mDBs, dbKey, data); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// IsDatabaseExist checks whether a database exists by dbID.
// exported for testing.
func (m *Mutator) IsDatabaseExist(dbID int64) (bool, error) {
	dbKey := m.dbKey(dbID)
	v, err := m.txn.HGet(mDBs, dbKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	return v != nil, nil
}

// UpdateDatabase updates a database with db info.
func (m *Mutator) UpdateDatabase(dbInfo *model.DBInfo) error {
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
func (m *Mutator) CreateTableOrView(dbID int64, tableInfo *model.TableInfo) error {
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

	if err := m.txn.HSet(dbKey, tableKey, data); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// SetBDRRole write BDR role into storage.
func (m *Mutator) SetBDRRole(role string) error {
	return errors.Trace(m.txn.Set(mBDRRole, []byte(role)))
}

// GetBDRRole get BDR role from storage.
func (m *Mutator) GetBDRRole() (string, error) {
	v, err := m.txn.Get(mBDRRole)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(v), nil
}

// ClearBDRRole clear BDR role from storage.
func (m *Mutator) ClearBDRRole() error {
	return errors.Trace(m.txn.Clear(mBDRRole))
}

// SetDDLTableVersion write a key into storage.
func (m *Mutator) SetDDLTableVersion(ddlTableVersion DDLTableVersion) error {
	return m.setTableVersion(mDDLTableVersion, int(ddlTableVersion))
}

// SetNextGenBootTableVersion set the table version on initial bootstrap.
func (m *Mutator) SetNextGenBootTableVersion(version NextGenBootTableVersion) error {
	return m.setTableVersion(mBootTableVersion, int(version))
}

func (m *Mutator) setTableVersion(key []byte, version int) error {
	return errors.Trace(m.txn.Set(key, encodeIntVal(version)))
}

// GetDDLTableVersion check if the tables related to concurrent DDL exists.
func (m *Mutator) GetDDLTableVersion() (DDLTableVersion, error) {
	v, err := m.getTableVersion(mDDLTableVersion)
	return DDLTableVersion(v), err
}

// GetNextGenBootTableVersion checks the version of the bootstrapping tables.
func (m *Mutator) GetNextGenBootTableVersion() (NextGenBootTableVersion, error) {
	v, err := m.getTableVersion(mBootTableVersion)
	return NextGenBootTableVersion(v), err
}

func (m *Mutator) getTableVersion(key []byte) (int, error) {
	v, err := m.txn.Get(key)
	if err != nil {
		return -1, errors.Trace(err)
	}
	if string(v) == "" {
		return 0, nil
	}
	ver, err := strconv.Atoi(string(v))
	if err != nil {
		return -1, errors.Trace(err)
	}
	return ver, nil
}

// CreateMySQLDatabaseIfNotExists creates mysql schema and return its DB ID.
func (m *Mutator) CreateMySQLDatabaseIfNotExists() (int64, error) {
	if kerneltype.IsNextGen() {
		return metadef.SystemDatabaseID, m.CreateSysDatabaseByIDIfNotExists(mysql.SystemDB, metadef.SystemDatabaseID)
	}
	id, err := m.GetSystemDBID()
	if id != 0 || err != nil {
		return id, err
	}

	id, err = m.GenGlobalID()
	if err != nil {
		return 0, errors.Trace(err)
	}
	return id, m.CreateSysDatabaseByID(mysql.SystemDB, id)
}

// CreateSysDatabaseByIDIfNotExists creates a system database with the given name
// and ID if it does not already exist.
func (m *Mutator) CreateSysDatabaseByIDIfNotExists(name string, id int64) error {
	exist, err := m.IsDatabaseExist(id)
	if err != nil {
		return err
	}
	if exist {
		return nil
	}
	return m.CreateSysDatabaseByID(name, id)
}

// CreateSysDatabaseByID creates a system database with the given name and ID.
// exported for testing.
func (m *Mutator) CreateSysDatabaseByID(name string, id int64) error {
	db := model.DBInfo{
		ID:      id,
		Name:    ast.NewCIStr(name),
		Charset: mysql.UTF8MB4Charset,
		Collate: mysql.UTF8MB4DefaultCollation,
		State:   model.StatePublic,
	}
	return m.CreateDatabase(&db)
}

// GetSystemDBID gets the system DB ID. return (0, nil) indicates that the system DB does not exist.
func (m *Mutator) GetSystemDBID() (int64, error) {
	dbs, err := m.ListDatabases()
	if err != nil {
		return 0, err
	}
	for _, db := range dbs {
		if db.Name.L == mysql.SystemDB {
			return db.ID, nil
		}
	}
	return 0, nil
}

// SetMetadataLock sets the metadata lock.
func (m *Mutator) SetMetadataLock(b bool) error {
	var data []byte
	if b {
		data = []byte("1")
	} else {
		data = []byte("0")
	}
	return errors.Trace(m.txn.Set(mMetaDataLock, data))
}

// GetMetadataLock gets the metadata lock.
func (m *Mutator) GetMetadataLock() (enable bool, isNull bool, err error) {
	val, err := m.txn.Get(mMetaDataLock)
	if err != nil {
		return false, false, errors.Trace(err)
	}
	if len(val) == 0 {
		return false, true, nil
	}
	return bytes.Equal(val, []byte("1")), false, nil
}

// SetSchemaCacheSize sets the schema cache size.
func (m *Mutator) SetSchemaCacheSize(size uint64) error {
	return errors.Trace(m.txn.Set(mSchemaCacheSize, []byte(strconv.FormatUint(size, 10))))
}

// GetSchemaCacheSize gets the schema cache size.
func (m *Mutator) GetSchemaCacheSize() (size uint64, isNull bool, err error) {
	val, err := m.txn.Get(mSchemaCacheSize)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if len(val) == 0 {
		return 0, true, nil
	}
	size, err = strconv.ParseUint(string(val), 10, 64)
	return size, false, errors.Trace(err)
}

// CreateTableAndSetAutoID creates a table with tableInfo in database,
// and rebases the table autoID.
func (m *Mutator) CreateTableAndSetAutoID(dbID int64, tableInfo *model.TableInfo, autoIDs model.AutoIDGroup) error {
	err := m.CreateTableOrView(dbID, tableInfo)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.txn.HInc(m.dbKey(dbID), m.autoTableIDKey(tableInfo.ID), autoIDs.RowID)
	if err != nil {
		return errors.Trace(err)
	}
	if tableInfo.AutoRandomBits > 0 {
		_, err = m.txn.HInc(m.dbKey(dbID), m.autoRandomTableIDKey(tableInfo.ID), autoIDs.RandomID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if tableInfo.SepAutoInc() && tableInfo.GetAutoIncrementColInfo() != nil {
		_, err = m.txn.HInc(m.dbKey(dbID), m.autoIncrementIDKey(tableInfo.ID), autoIDs.IncrementID)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// CreateSequenceAndSetSeqValue creates sequence with tableInfo in database, and rebase the sequence seqValue.
func (m *Mutator) CreateSequenceAndSetSeqValue(dbID int64, tableInfo *model.TableInfo, seqValue int64) error {
	err := m.CreateTableOrView(dbID, tableInfo)
	if err != nil {
		return errors.Trace(err)
	}
	_, err = m.txn.HInc(m.dbKey(dbID), m.sequenceKey(tableInfo.ID), seqValue)
	return errors.Trace(err)
}

// RestartSequenceValue resets the the sequence value.
func (m *Mutator) RestartSequenceValue(dbID int64, tableInfo *model.TableInfo, seqValue int64) error {
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

// DropPolicy drops the specified policy.
func (m *Mutator) DropPolicy(policyID int64) error {
	// Check if policy exists.
	policyKey := m.policyKey(policyID)
	if err := m.txn.HClear(policyKey); err != nil {
		return errors.Trace(err)
	}
	if err := m.txn.HDel(mPolicies, policyKey); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// DropDatabase drops whole database.
func (m *Mutator) DropDatabase(dbID int64) error {
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
func (m *Mutator) DropSequence(dbID int64, tblID int64) error {
	err := m.DropTableOrView(dbID, tblID)
	if err != nil {
		return err
	}
	err = m.GetAutoIDAccessors(dbID, tblID).Del()
	if err != nil {
		return err
	}
	err = m.txn.HDel(m.dbKey(dbID), m.sequenceKey(tblID))
	return errors.Trace(err)
}

// DropTableOrView drops table in database.
// If delAutoID is true, it will delete the auto_increment id key-value of the table.
// For rename table, we do not need to rename auto_increment id key-value.
func (m *Mutator) DropTableOrView(dbID int64, tblID int64) error {
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
	return nil
}

// UpdateTable updates the table with table info.
func (m *Mutator) UpdateTable(dbID int64, tableInfo *model.TableInfo) error {
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

	tableInfo.Revision++

	data, err := json.Marshal(tableInfo)
	if err != nil {
		return errors.Trace(err)
	}

	err = m.txn.HSet(dbKey, tableKey, data)
	return errors.Trace(err)
}
