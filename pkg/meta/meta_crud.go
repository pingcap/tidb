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
	"fmt"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/metadef"
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
