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
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

func (m *Mutator) GenGlobalID() (int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	newID, err := m.txn.Inc(mNextGlobalIDKey, 1)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if newID > metadef.MaxUserGlobalID {
		return 0, errors.Errorf("global id:%d exceeds the limit:%d", newID, metadef.MaxUserGlobalID)
	}
	return newID, err
}

// AdvanceGlobalIDs advances the global ID by n.
// return the old global ID.
func (m *Mutator) AdvanceGlobalIDs(n int) (int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	newID, err := m.txn.Inc(mNextGlobalIDKey, int64(n))
	if err != nil {
		return 0, err
	}
	if newID > metadef.MaxUserGlobalID {
		return 0, errors.Errorf("global id:%d exceeds the limit:%d", newID, metadef.MaxUserGlobalID)
	}
	origID := newID - int64(n)
	return origID, nil
}

// GenGlobalIDs generates the next n global IDs.
func (m *Mutator) GenGlobalIDs(n int) ([]int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	newID, err := m.txn.Inc(mNextGlobalIDKey, int64(n))
	if err != nil {
		return nil, err
	}
	if newID > metadef.MaxUserGlobalID {
		return nil, errors.Errorf("global id:%d exceeds the limit:%d", newID, metadef.MaxUserGlobalID)
	}
	origID := newID - int64(n)
	ids := make([]int64, 0, n)
	for i := origID + 1; i <= newID; i++ {
		ids = append(ids, i)
	}
	return ids, nil
}

// GlobalIDKey returns the key for the global ID.
func (m *Mutator) GlobalIDKey() []byte {
	return m.txn.EncodeStringDataKey(mNextGlobalIDKey)
}

// GenPlacementPolicyID generates next placement policy id globally.
func (m *Mutator) GenPlacementPolicyID() (int64, error) {
	policyIDMutex.Lock()
	defer policyIDMutex.Unlock()

	return m.txn.Inc(mPolicyGlobalID, 1)
}

// GetGlobalID gets current global id.
func (m *Mutator) GetGlobalID() (int64, error) {
	return m.txn.GetInt64(mNextGlobalIDKey)
}

// GetPolicyID gets current policy global id.
func (m *Mutator) GetPolicyID() (int64, error) {
	return m.txn.GetInt64(mPolicyGlobalID)
}

func (*Mutator) policyKey(policyID int64) []byte {
	return fmt.Appendf(nil, "%s:%d", mPolicyPrefix, policyID)
}

func (*Mutator) resourceGroupKey(groupID int64) []byte {
	return fmt.Appendf(nil, "%s:%d", mResourceGroupPrefix, groupID)
}

func (*Mutator) dbKey(dbID int64) []byte {
	return DBkey(dbID)
}

// DBkey encodes the dbID into dbKey.
func DBkey(dbID int64) []byte {
	return fmt.Appendf(nil, "%s:%d", mDBPrefix, dbID)
}

// ParseDBKey decodes the dbkey to get dbID.
func ParseDBKey(dbkey []byte) (int64, error) {
	if !IsDBkey(dbkey) {
		return 0, ErrInvalidString.GenWithStack("fail to parse dbKey")
	}

	dbID := strings.TrimPrefix(string(dbkey), mDBPrefix+":")
	id, err := strconv.Atoi(dbID)
	return int64(id), errors.Trace(err)
}

// IsDBkey checks whether the dbKey comes from DBKey().
func IsDBkey(dbKey []byte) bool {
	return strings.HasPrefix(string(dbKey), mDBPrefix+":")
}

func (*Mutator) autoTableIDKey(tableID int64) []byte {
	return AutoTableIDKey(tableID)
}

// AutoTableIDKey decodes the auto tableID key.
func AutoTableIDKey(tableID int64) []byte {
	return fmt.Appendf(nil, "%s:%d", mTableIDPrefix, tableID)
}

// IsAutoTableIDKey checks whether the key is auto tableID key.
func IsAutoTableIDKey(key []byte) bool {
	return strings.HasPrefix(string(key), mTableIDPrefix+":")
}

// ParseAutoTableIDKey decodes the tableID from the auto tableID key.
func ParseAutoTableIDKey(key []byte) (int64, error) {
	if !IsAutoTableIDKey(key) {
		return 0, ErrInvalidString.GenWithStack("fail to parse autoTableKey")
	}

	tableID := strings.TrimPrefix(string(key), mTableIDPrefix+":")
	id, err := strconv.Atoi(tableID)
	return int64(id), err
}

func (*Mutator) autoIncrementIDKey(tableID int64) []byte {
	return AutoIncrementIDKey(tableID)
}

// AutoIncrementIDKey decodes the auto inc table key.
func AutoIncrementIDKey(tableID int64) []byte {
	return fmt.Appendf(nil, "%s:%d", mIncIDPrefix, tableID)
}

// IsAutoIncrementIDKey checks whether the key is auto increment key.
func IsAutoIncrementIDKey(key []byte) bool {
	return strings.HasPrefix(string(key), mIncIDPrefix+":")
}

// ParseAutoIncrementIDKey decodes the tableID from the auto tableID key.
func ParseAutoIncrementIDKey(key []byte) (int64, error) {
	if !IsAutoIncrementIDKey(key) {
		return 0, ErrInvalidString.GenWithStack("fail to parse autoIncrementKey")
	}

	tableID := strings.TrimPrefix(string(key), mIncIDPrefix+":")
	id, err := strconv.Atoi(tableID)
	return int64(id), err
}

func (*Mutator) autoRandomTableIDKey(tableID int64) []byte {
	return AutoRandomTableIDKey(tableID)
}

// AutoRandomTableIDKey encodes the auto random tableID key.
func AutoRandomTableIDKey(tableID int64) []byte {
	return fmt.Appendf(nil, "%s:%d", mRandomIDPrefix, tableID)
}

// IsAutoRandomTableIDKey checks whether the key is auto random tableID key.
func IsAutoRandomTableIDKey(key []byte) bool {
	return strings.HasPrefix(string(key), mRandomIDPrefix+":")
}

// ParseAutoRandomTableIDKey decodes the tableID from the auto random tableID key.
func ParseAutoRandomTableIDKey(key []byte) (int64, error) {
	if !IsAutoRandomTableIDKey(key) {
		return 0, ErrInvalidString.GenWithStack("fail to parse AutoRandomTableIDKey")
	}

	tableID := strings.TrimPrefix(string(key), mRandomIDPrefix+":")
	id, err := strconv.Atoi(tableID)
	return int64(id), err
}

func (*Mutator) tableKey(tableID int64) []byte {
	return TableKey(tableID)
}

// TableKey encodes the tableID into tableKey.
func TableKey(tableID int64) []byte {
	return fmt.Appendf(nil, "%s:%d", mTablePrefix, tableID)
}

// IsTableKey checks whether the tableKey comes from TableKey().
func IsTableKey(tableKey []byte) bool {
	return strings.HasPrefix(string(tableKey), mTablePrefix+":")
}

// ParseTableKey decodes the tableKey to get tableID.
func ParseTableKey(tableKey []byte) (int64, error) {
	if !strings.HasPrefix(string(tableKey), mTablePrefix) {
		return 0, ErrInvalidString.GenWithStack("fail to parse tableKey")
	}

	tableID := strings.TrimPrefix(string(tableKey), mTablePrefix+":")
	id, err := strconv.Atoi(tableID)
	return int64(id), errors.Trace(err)
}

func (*Mutator) sequenceKey(sequenceID int64) []byte {
	return SequenceKey(sequenceID)
}

// SequenceKey encodes the sequence key.
func SequenceKey(sequenceID int64) []byte {
	return fmt.Appendf(nil, "%s:%d", mSequencePrefix, sequenceID)
}

// IsSequenceKey checks whether the key is sequence key.
func IsSequenceKey(key []byte) bool {
	return strings.HasPrefix(string(key), mSequencePrefix+":")
}

// ParseSequenceKey decodes the tableID from the sequence key.
func ParseSequenceKey(key []byte) (int64, error) {
	if !IsSequenceKey(key) {
		return 0, ErrInvalidString.GenWithStack("fail to parse sequence key")
	}

	sequenceID := strings.TrimPrefix(string(key), mSequencePrefix+":")
	id, err := strconv.Atoi(sequenceID)
	return int64(id), errors.Trace(err)
}

func (*Mutator) sequenceCycleKey(sequenceID int64) []byte {
	return fmt.Appendf(nil, "%s:%d", mSeqCyclePrefix, sequenceID)
}

// DDLJobHistoryKey is only used for testing.
func DDLJobHistoryKey(m *Mutator, jobID int64) []byte {
	return m.txn.EncodeHashDataKey(mDDLJobHistoryKey, m.jobIDKey(jobID))
}

// GenAutoTableIDKeyValue generates meta key by dbID, tableID and corresponding value by autoID.
func (m *Mutator) GenAutoTableIDKeyValue(dbID, tableID, autoID int64) (key, value []byte) {
	dbKey := m.dbKey(dbID)
	autoTableIDKey := m.autoTableIDKey(tableID)
	return m.txn.EncodeHashAutoIDKeyValue(dbKey, autoTableIDKey, autoID)
}

// GetAutoIDAccessors gets the controller for auto IDs.
func (m *Mutator) GetAutoIDAccessors(dbID, tableID int64) AutoIDAccessors {
	return NewAutoIDAccessors(m, dbID, tableID)
}

// GetSchemaVersionWithNonEmptyDiff gets current global schema version, if diff is nil, we should return version - 1.
// Consider the following scenario:
/*
//             t1            		t2			      t3             t4
//             |					|				   |
//    update schema version         |              set diff
//                             stale read ts
*/
// At the first time, t2 reads the schema version v10, but the v10's diff is not set yet, so it loads v9 infoSchema.
// But at t4 moment, v10's diff has been set and been cached in the memory, so stale read on t2 will get v10 schema from cache,
// and inconsistency happen.
// To solve this problem, we always check the schema diff at first, if the diff is empty, we know at t2 moment we can only see the v9 schema,
// so make neededSchemaVersion = neededSchemaVersion - 1.
// For `Reload`, we can also do this: if the newest version's diff is not set yet, it is ok to load the previous version's infoSchema, and wait for the next reload.
// if there are multiple consecutive jobs failed or cancelled after the schema version
// increased, the returned 'version - 1' might still not have diff.
func (m *Mutator) GetSchemaVersionWithNonEmptyDiff() (int64, error) {
	v, err := m.txn.GetInt64(mSchemaVersionKey)
	if err != nil {
		return 0, err
	}
	diff, err := m.GetSchemaDiff(v)
	if err != nil {
		return 0, err
	}

	if diff == nil && v > 0 {
		// Although the diff of v is undetermined, the last version's diff is deterministic(this is guaranteed by schemaVersionManager).
		v--
	}
	return v, err
}

// EncodeSchemaDiffKey returns the raw kv key for a schema diff
func (m *Mutator) EncodeSchemaDiffKey(schemaVersion int64) kv.Key {
	diffKey := m.schemaDiffKey(schemaVersion)
	return m.txn.EncodeStringDataKey(diffKey)
}

// GetSchemaVersion gets current global schema version.
func (m *Mutator) GetSchemaVersion() (int64, error) {
	return m.txn.GetInt64(mSchemaVersionKey)
}

// GenSchemaVersion generates next schema version.
func (m *Mutator) GenSchemaVersion() (int64, error) {
	return m.txn.Inc(mSchemaVersionKey, 1)
}

// GenSchemaVersions increases the schema version.
func (m *Mutator) GenSchemaVersions(count int64) (int64, error) {
	return m.txn.Inc(mSchemaVersionKey, count)
}

func (m *Mutator) checkPolicyExists(policyKey []byte) error {
	v, err := m.txn.HGet(mPolicies, policyKey)
	if err == nil && v == nil {
		err = ErrPolicyNotExists.GenWithStack("policy doesn't exist")
	}
	return errors.Trace(err)
}

func (m *Mutator) checkPolicyNotExists(policyKey []byte) error {
	v, err := m.txn.HGet(mPolicies, policyKey)
	if err == nil && v != nil {
		err = ErrPolicyExists.GenWithStack("policy already exists")
	}
	return errors.Trace(err)
}

func (m *Mutator) checkResourceGroupNotExists(groupKey []byte) error {
	v, err := m.txn.HGet(mResourceGroups, groupKey)
	if err == nil && v != nil {
		err = ErrResourceGroupExists.GenWithStack("group already exists")
	}
	return errors.Trace(err)
}

func (m *Mutator) checkResourceGroupExists(groupKey []byte) error {
	v, err := m.txn.HGet(mResourceGroups, groupKey)
	if err == nil && v == nil {
		err = ErrResourceGroupNotExists.GenWithStack("group doesn't exist")
	}
	return errors.Trace(err)
}

func (m *Mutator) checkDBExists(dbKey []byte) error {
	v, err := m.txn.HGet(mDBs, dbKey)
	if err == nil && v == nil {
		err = ErrDBNotExists.GenWithStack("database doesn't exist")
	}
	return errors.Trace(err)
}

func (m *Mutator) checkDBNotExists(dbKey []byte) error {
	v, err := m.txn.HGet(mDBs, dbKey)
	if err == nil && v != nil {
		err = ErrDBExists.GenWithStack("database already exists")
	}
	return errors.Trace(err)
}

func (m *Mutator) checkTableExists(dbKey []byte, tableKey []byte) error {
	v, err := m.txn.HGet(dbKey, tableKey)
	if err == nil && v == nil {
		err = ErrTableNotExists.GenWithStack("table doesn't exist")
	}
	return errors.Trace(err)
}

func (m *Mutator) checkTableNotExists(dbKey []byte, tableKey []byte) error {
	v, err := m.txn.HGet(dbKey, tableKey)
	if err == nil && v != nil {
		err = ErrTableExists.GenWithStack("table already exists")
	}
	return errors.Trace(err)
}

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
