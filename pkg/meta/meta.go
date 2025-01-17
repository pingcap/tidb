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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	rmpb "github.com/pingcap/kvproto/pkg/resource_manager"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/resourcegroup"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/pingcap/tidb/pkg/structure"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/dbterror"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/pingcap/tidb/pkg/util/partialjson"
	"github.com/pingcap/tidb/pkg/util/set"
)

var (
	globalIDMutex sync.Mutex
	policyIDMutex sync.Mutex
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
// DDL version 2
// Names -> {
//		Name:DBname\x00tablename -> tableID
// }

var (
	mMetaPrefix = []byte("m")
	// the value inside it is actually the max current used ID, not next id.
	mNextGlobalIDKey     = []byte("NextGlobalID")
	mSchemaVersionKey    = []byte("SchemaVersionKey")
	mDBs                 = []byte("DBs")
	mDBPrefix            = "DB"
	mTablePrefix         = "Table"
	mSequencePrefix      = "SID"
	mSeqCyclePrefix      = "SequenceCycle"
	mTableIDPrefix       = "TID"
	mIncIDPrefix         = "IID"
	mRandomIDPrefix      = "TARID"
	mBootstrapKey        = []byte("BootstrapKey")
	mSchemaDiffPrefix    = "Diff"
	mPolicies            = []byte("Policies")
	mPolicyPrefix        = "Policy"
	mResourceGroups      = []byte("ResourceGroups")
	mResourceGroupPrefix = "RG"
	mPolicyGlobalID      = []byte("PolicyGlobalID")
	mPolicyMagicByte     = CurrentMagicByteVer
	mDDLTableVersion     = []byte("DDLTableVersion")
	mBDRRole             = []byte("BDRRole")
	mMetaDataLock        = []byte("metadataLock")
	mSchemaCacheSize     = []byte("SchemaCacheSize")
	mRequestUnitStats    = []byte("RequestUnitStats")
	// the id for 'default' group, the internal ddl can ensure
	// user created resource group won't duplicate with this id.
	defaultGroupID = int64(1)
	// the default meta of the `default` group
	defaultRGroupMeta = &model.ResourceGroupInfo{
		ResourceGroupSettings: &model.ResourceGroupSettings{
			RURate:     math.MaxInt32,
			BurstLimit: -1,
			Priority:   ast.MediumPriorityValue,
		},
		ID:    defaultGroupID,
		Name:  ast.NewCIStr(resourcegroup.DefaultResourceGroupName),
		State: model.StatePublic,
	}
)

const (
	// CurrentMagicByteVer is the current magic byte version, used for future meta compatibility.
	CurrentMagicByteVer byte = 0x00
	// PolicyMagicByte handler
	// 0x00 - 0x3F: Json Handler
	// 0x40 - 0x7F: Reserved
	// 0x80 - 0xBF: Reserved
	// 0xC0 - 0xFF: Reserved

	// type means how to handle the serialized data.
	typeUnknown int = 0
	typeJSON    int = 1
	// todo: customized handler.

	// MaxInt48 is the max value of int48.
	MaxInt48 = 0x0000FFFFFFFFFFFF
	// MaxGlobalID reserves 1000 IDs. Use MaxInt48 to reserves the high 2 bytes to compatible with Multi-tenancy.
	MaxGlobalID = MaxInt48 - 1000
)

var (
	// ErrDBExists is the error for db exists.
	ErrDBExists = dbterror.ClassMeta.NewStd(mysql.ErrDBCreateExists)
	// ErrDBNotExists is the error for db not exists.
	ErrDBNotExists = dbterror.ClassMeta.NewStd(mysql.ErrBadDB)
	// ErrPolicyExists is the error for policy exists.
	ErrPolicyExists = dbterror.ClassMeta.NewStd(errno.ErrPlacementPolicyExists)
	// ErrPolicyNotExists is the error for policy not exists.
	ErrPolicyNotExists = dbterror.ClassMeta.NewStd(errno.ErrPlacementPolicyNotExists)
	// ErrResourceGroupExists is the error for resource group exists.
	ErrResourceGroupExists = dbterror.ClassMeta.NewStd(errno.ErrResourceGroupExists)
	// ErrResourceGroupNotExists is the error for resource group not exists.
	ErrResourceGroupNotExists = dbterror.ClassMeta.NewStd(errno.ErrResourceGroupNotExists)
	// ErrTableExists is the error for table exists.
	ErrTableExists = dbterror.ClassMeta.NewStd(mysql.ErrTableExists)
	// ErrTableNotExists is the error for table not exists.
	ErrTableNotExists = dbterror.ClassMeta.NewStd(mysql.ErrNoSuchTable)
	// ErrDDLReorgElementNotExist is the error for reorg element not exists.
	ErrDDLReorgElementNotExist = dbterror.ClassMeta.NewStd(errno.ErrDDLReorgElementNotExist)
	// ErrInvalidString is the error for invalid string to parse
	ErrInvalidString = dbterror.ClassMeta.NewStd(errno.ErrInvalidCharacterString)
)

// DDLTableVersion is to display ddl related table versions
type DDLTableVersion int

const (
	// InitDDLTableVersion is the original version.
	InitDDLTableVersion DDLTableVersion = 0
	// BaseDDLTableVersion is for support concurrent DDL, it added tidb_ddl_job, tidb_ddl_reorg and tidb_ddl_history.
	BaseDDLTableVersion DDLTableVersion = 1
	// MDLTableVersion is for support MDL tables.
	MDLTableVersion DDLTableVersion = 2
	// BackfillTableVersion is for support distributed reorg stage, it added tidb_background_subtask, tidb_background_subtask_history.
	BackfillTableVersion DDLTableVersion = 3
	// DDLNotifierTableVersion is for support ddl notifier, it added tidb_ddl_notifier.
	DDLNotifierTableVersion DDLTableVersion = 4
)

// Bytes returns the byte slice.
func (ver DDLTableVersion) Bytes() []byte {
	return []byte(strconv.Itoa(int(ver)))
}

// Option is for Mutator option.
type Option func(m *Mutator)

// Mutator is for handling meta information in a transaction.
type Mutator struct {
	txn     *structure.TxStructure
	StartTS uint64 // StartTS is the txn's start TS.
}

var _ Reader = (*Mutator)(nil)

// NewMutator creates a meta Mutator in transaction txn.
// If the current Mutator needs to handle a job, jobListKey is the type of the job's list.
func NewMutator(txn kv.Transaction, options ...Option) *Mutator {
	txn.SetOption(kv.Priority, kv.PriorityHigh)
	txn.SetDiskFullOpt(kvrpcpb.DiskFullOpt_AllowedOnAlmostFull)
	t := structure.NewStructure(txn, txn, mMetaPrefix)
	m := &Mutator{txn: t,
		StartTS: txn.StartTS(),
	}
	for _, opt := range options {
		opt(m)
	}
	return m
}

// GenGlobalID generates next id globally.
func (m *Mutator) GenGlobalID() (int64, error) {
	globalIDMutex.Lock()
	defer globalIDMutex.Unlock()

	newID, err := m.txn.Inc(mNextGlobalIDKey, 1)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if newID > MaxGlobalID {
		return 0, errors.Errorf("global id:%d exceeds the limit:%d", newID, MaxGlobalID)
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
	if newID > MaxGlobalID {
		return 0, errors.Errorf("global id:%d exceeds the limit:%d", newID, MaxGlobalID)
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
	if newID > MaxGlobalID {
		return nil, errors.Errorf("global id:%d exceeds the limit:%d", newID, MaxGlobalID)
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
	return []byte(fmt.Sprintf("%s:%d", mPolicyPrefix, policyID))
}

func (*Mutator) resourceGroupKey(groupID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mResourceGroupPrefix, groupID))
}

func (*Mutator) dbKey(dbID int64) []byte {
	return DBkey(dbID)
}

// DBkey encodes the dbID into dbKey.
func DBkey(dbID int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mDBPrefix, dbID))
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
	return []byte(fmt.Sprintf("%s:%d", mTableIDPrefix, tableID))
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
	return []byte(fmt.Sprintf("%s:%d", mIncIDPrefix, tableID))
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
	return []byte(fmt.Sprintf("%s:%d", mRandomIDPrefix, tableID))
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
	return []byte(fmt.Sprintf("%s:%d", mTablePrefix, tableID))
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
	return []byte(fmt.Sprintf("%s:%d", mSequencePrefix, sequenceID))
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
	return []byte(fmt.Sprintf("%s:%d", mSeqCyclePrefix, sequenceID))
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

// SetDDLTables write a key into storage.
func (m *Mutator) SetDDLTables(ddlTableVersion DDLTableVersion) error {
	return errors.Trace(m.txn.Set(mDDLTableVersion, ddlTableVersion.Bytes()))
}

// CheckDDLTableVersion check if the tables related to concurrent DDL exists.
func (m *Mutator) CheckDDLTableVersion() (DDLTableVersion, error) {
	v, err := m.txn.Get(mDDLTableVersion)
	if err != nil {
		return -1, errors.Trace(err)
	}
	if string(v) == "" {
		return InitDDLTableVersion, nil
	}
	ver, err := strconv.Atoi(string(v))
	if err != nil {
		return -1, errors.Trace(err)
	}
	return DDLTableVersion(ver), nil
}

// CreateMySQLDatabaseIfNotExists creates mysql schema and return its DB ID.
func (m *Mutator) CreateMySQLDatabaseIfNotExists() (int64, error) {
	id, err := m.GetSystemDBID()
	if id != 0 || err != nil {
		return id, err
	}

	id, err = m.GenGlobalID()
	if err != nil {
		return 0, errors.Trace(err)
	}
	db := model.DBInfo{
		ID:      id,
		Name:    ast.NewCIStr(mysql.SystemDB),
		Charset: mysql.UTF8MB4Charset,
		Collate: mysql.UTF8MB4DefaultCollation,
		State:   model.StatePublic,
	}
	err = m.CreateDatabase(&db)
	return db.ID, err
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

// IterTables iterates all the table at once, in order to avoid oom.
func (m *Mutator) IterTables(dbID int64, fn func(info *model.TableInfo) error) error {
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return errors.Trace(err)
	}

	err := m.txn.HGetIter(dbKey, func(r structure.HashPair) error {
		// only handle table meta
		tableKey := string(r.Field)
		if !strings.HasPrefix(tableKey, mTablePrefix) {
			return nil
		}

		tbInfo := &model.TableInfo{}
		err := json.Unmarshal(r.Value, tbInfo)
		if err != nil {
			return errors.Trace(err)
		}
		tbInfo.DBID = dbID

		err = fn(tbInfo)
		return errors.Trace(err)
	})
	return errors.Trace(err)
}

// GetMetasByDBID return all meta information of a database.
// Note(dongmen): This method is used by TiCDC to reduce the time of changefeed initialization.
// Ref: https://github.com/pingcap/tiflow/issues/11109
func (m *Mutator) GetMetasByDBID(dbID int64) ([]structure.HashPair, error) {
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}
	res, err := m.txn.HGetAll(dbKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return res, nil
}

var checkAttributesInOrder = []string{
	`"fk_info":null`,
	`"partition":null`,
	`"Lock":null`,
	`"tiflash_replica":null`,
	`"temp_table_type":0`,
	`"policy_ref_info":null`,
	`"ttl_info":null`,
}

// isTableInfoMustLoad checks whether the table info needs to be loaded.
// If the byte representation contains all the given attributes,
// then it does not need to be loaded and this function will return false.
// Otherwise, it will return true, indicating that the table info should be loaded.
// Since attributes are checked in sequence, it's important to choose the order carefully.
func isTableInfoMustLoad(json []byte, filterAttrs ...string) bool {
	idx := 0
	for _, substr := range filterAttrs {
		idx = bytes.Index(json, hack.Slice(substr))
		if idx == -1 {
			return true
		}
		json = json[idx:]
	}
	return false
}

// IsTableInfoMustLoad checks whether the table info needs to be loaded.
// Exported for testing.
func IsTableInfoMustLoad(json []byte) bool {
	return isTableInfoMustLoad(json, checkAttributesInOrder...)
}

// NameExtractRegexp is exported for testing.
const NameExtractRegexp = `"O":"([^"\\]*(?:\\.[^"\\]*)*)",`

// Unescape is exported for testing.
func Unescape(s string) string {
	s = strings.ReplaceAll(s, `\"`, `"`)
	s = strings.ReplaceAll(s, `\\`, `\`)
	return s
}

// GetAllNameToIDAndTheMustLoadedTableInfo gets all the fields and values and table info for special attributes in a hash.
// It's used to get some infos for information schema cache in a faster way.
// If a table contains any of the attributes listed in checkSubstringsInOrder, it must be loaded during schema full load.
// hasSpecialAttributes() is a subset of it, the difference is that:
// If a table need to be resident in-memory, its table info MUST be loaded.
// If a table info is loaded, it's NOT NECESSARILY to be keep in-memory.
func (m *Mutator) GetAllNameToIDAndTheMustLoadedTableInfo(dbID int64) (map[string]int64, []*model.TableInfo, error) {
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, nil, errors.Trace(err)
	}

	res := make(map[string]int64)
	idRegex := regexp.MustCompile(`"id":(\d+)`)
	nameLRegex := regexp.MustCompile(NameExtractRegexp)

	tableInfos := make([]*model.TableInfo, 0)

	err := m.txn.IterateHash(dbKey, func(field []byte, value []byte) error {
		if !strings.HasPrefix(string(hack.String(field)), "Table") {
			return nil
		}

		idMatch := idRegex.FindStringSubmatch(string(hack.String(value)))
		nameLMatch := nameLRegex.FindStringSubmatch(string(hack.String(value)))
		id, err := strconv.Atoi(idMatch[1])
		if err != nil {
			return errors.Trace(err)
		}

		key := Unescape(nameLMatch[1])
		res[strings.Clone(key)] = int64(id)
		if isTableInfoMustLoad(value, checkAttributesInOrder...) {
			tbInfo := &model.TableInfo{}
			err = json.Unmarshal(value, tbInfo)
			if err != nil {
				return errors.Trace(err)
			}
			tbInfo.DBID = dbID
			tableInfos = append(tableInfos, tbInfo)
		}
		return nil
	})

	return res, tableInfos, errors.Trace(err)
}

// GetTableInfoWithAttributes retrieves all the table infos for a given db.
// The filterAttrs are used to filter out any table that is not needed.
func GetTableInfoWithAttributes(m *Mutator, dbID int64, filterAttrs ...string) ([]*model.TableInfo, error) {
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return nil, errors.Trace(err)
	}

	tableInfos := make([]*model.TableInfo, 0)
	err := m.txn.IterateHash(dbKey, func(field []byte, value []byte) error {
		if !strings.HasPrefix(string(hack.String(field)), "Table") {
			return nil
		}

		if isTableInfoMustLoad(value, filterAttrs...) {
			tbInfo := &model.TableInfo{}
			err := json.Unmarshal(value, tbInfo)
			if err != nil {
				return errors.Trace(err)
			}
			tbInfo.DBID = dbID
			tableInfos = append(tableInfos, tbInfo)
		}
		return nil
	})

	return tableInfos, errors.Trace(err)
}

// ListTables shows all tables in database.
func (m *Mutator) ListTables(ctx context.Context, dbID int64) ([]*model.TableInfo, error) {
	res, err := m.GetMetasByDBID(dbID)
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
		if ctx.Err() != nil {
			return nil, errors.Trace(ctx.Err())
		}

		tbInfo := &model.TableInfo{}
		err = json.Unmarshal(r.Value, tbInfo)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tbInfo.DBID = dbID

		tables = append(tables, tbInfo)
	}

	return tables, nil
}

// ListSimpleTables shows all simple tables in database.
func (m *Mutator) ListSimpleTables(dbID int64) ([]*model.TableNameInfo, error) {
	res, err := m.GetMetasByDBID(dbID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	tables := make([]*model.TableNameInfo, 0, len(res)/2)
	for _, r := range res {
		// only handle table meta
		tableKey := string(r.Field)
		if !strings.HasPrefix(tableKey, mTablePrefix) {
			continue
		}

		tbInfo, err2 := FastUnmarshalTableNameInfo(r.Value)
		if err2 != nil {
			return nil, errors.Trace(err2)
		}

		tables = append(tables, tbInfo)
	}

	return tables, nil
}

var tableNameInfoFields = []string{"id", "name"}

// FastUnmarshalTableNameInfo is exported for testing.
func FastUnmarshalTableNameInfo(data []byte) (*model.TableNameInfo, error) {
	m, err := partialjson.ExtractTopLevelMembers(data, tableNameInfoFields)
	if err != nil {
		return nil, errors.Trace(err)
	}

	idTokens, ok := m["id"]
	if !ok {
		return nil, errors.New("id field not found in JSON")
	}
	if len(idTokens) != 1 {
		return nil, errors.Errorf("unexpected id field in JSON, %v", idTokens)
	}
	num, ok := idTokens[0].(json.Number)
	if !ok {
		return nil, errors.Errorf(
			"id field is not a number, got %T %v", idTokens[0], idTokens[0],
		)
	}
	id, err := num.Int64()
	if err != nil {
		return nil, errors.Trace(err)
	}

	nameTokens, ok := m["name"]
	if !ok {
		return nil, errors.New("name field not found in JSON")
	}
	// 6 tokens; {, O, ..., L, ..., }, the data looks like this {123,"O","t","L","t",125}
	if len(nameTokens) != 6 {
		return nil, errors.Errorf("unexpected name field in JSON, %v", nameTokens)
	}
	name, ok := nameTokens[2].(string)
	if !ok {
		return nil, errors.Errorf("unexpected name field in JSON, %v", nameTokens)
	}

	return &model.TableNameInfo{
		ID:   id,
		Name: ast.NewCIStr(name),
	}, nil
}

// ListDatabases shows all databases.
func (m *Mutator) ListDatabases() ([]*model.DBInfo, error) {
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
func (m *Mutator) GetDatabase(dbID int64) (*model.DBInfo, error) {
	dbKey := m.dbKey(dbID)
	value, err := m.txn.HGet(mDBs, dbKey)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	dbInfo := &model.DBInfo{}
	err = json.Unmarshal(value, dbInfo)
	return dbInfo, errors.Trace(err)
}

// ListPolicies shows all policies.
func (m *Mutator) ListPolicies() ([]*model.PolicyInfo, error) {
	res, err := m.txn.HGetAll(mPolicies)
	if err != nil {
		return nil, errors.Trace(err)
	}

	policies := make([]*model.PolicyInfo, 0, len(res))
	for _, r := range res {
		value, err := detachMagicByte(r.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		policy := &model.PolicyInfo{}
		err = json.Unmarshal(value, policy)
		if err != nil {
			return nil, errors.Trace(err)
		}
		policies = append(policies, policy)
	}
	return policies, nil
}

// GetPolicy gets the database value with ID.
func (m *Mutator) GetPolicy(policyID int64) (*model.PolicyInfo, error) {
	policyKey := m.policyKey(policyID)
	value, err := m.txn.HGet(mPolicies, policyKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if value == nil {
		return nil, ErrPolicyNotExists.GenWithStack("policy id : %d doesn't exist", policyID)
	}

	value, err = detachMagicByte(value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	policy := &model.PolicyInfo{}
	err = json.Unmarshal(value, policy)
	return policy, errors.Trace(err)
}

// ListResourceGroups shows all resource groups.
func (m *Mutator) ListResourceGroups() ([]*model.ResourceGroupInfo, error) {
	res, err := m.txn.HGetAll(mResourceGroups)
	if err != nil {
		return nil, errors.Trace(err)
	}

	hasDefault := false
	groups := make([]*model.ResourceGroupInfo, 0, len(res))
	for _, r := range res {
		value, err := detachMagicByte(r.Value)
		if err != nil {
			return nil, errors.Trace(err)
		}
		group := &model.ResourceGroupInfo{}
		err = json.Unmarshal(value, group)
		if err != nil {
			return nil, errors.Trace(err)
		}
		groups = append(groups, group)
		hasDefault = hasDefault || (group.Name.L == resourcegroup.DefaultResourceGroupName)
	}
	if !hasDefault {
		groups = append(groups, defaultRGroupMeta)
	}
	return groups, nil
}

// DefaultGroupMeta4Test return the default group info for test usage.
func DefaultGroupMeta4Test() *model.ResourceGroupInfo {
	return defaultRGroupMeta
}

// GetResourceGroup gets the database value with ID.
func (m *Mutator) GetResourceGroup(groupID int64) (*model.ResourceGroupInfo, error) {
	groupKey := m.resourceGroupKey(groupID)
	value, err := m.txn.HGet(mResourceGroups, groupKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if value == nil {
		// the default group is not persistent to tikv by default.
		if groupID == defaultGroupID {
			return defaultRGroupMeta, nil
		}
		return nil, ErrResourceGroupNotExists.GenWithStack("resource group id : %d doesn't exist", groupID)
	}

	value, err = detachMagicByte(value)
	if err != nil {
		return nil, errors.Trace(err)
	}

	group := &model.ResourceGroupInfo{}
	err = json.Unmarshal(value, group)
	return group, errors.Trace(err)
}

func attachMagicByte(data []byte) []byte {
	data = append(data, 0)
	copy(data[1:], data)
	data[0] = mPolicyMagicByte
	return data
}

func detachMagicByte(value []byte) ([]byte, error) {
	magic, data := value[:1], value[1:]
	switch whichMagicType(magic[0]) {
	case typeJSON:
		if magic[0] != CurrentMagicByteVer {
			return nil, errors.New("incompatible magic type handling module")
		}
		return data, nil
	default:
		return nil, errors.New("unknown magic type handling module")
	}
}

func whichMagicType(b byte) int {
	if b <= 0x3F {
		return typeJSON
	}
	return typeUnknown
}

// GetTable gets the table value in database with tableID.
func (m *Mutator) GetTable(dbID int64, tableID int64) (*model.TableInfo, error) {
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
	tableInfo.DBID = dbID
	return tableInfo, errors.Trace(err)
}

// CheckTableExists checks if the table is existed with dbID and tableID.
func (m *Mutator) CheckTableExists(dbID int64, tableID int64) (bool, error) {
	// Check if db exists.
	dbKey := m.dbKey(dbID)
	if err := m.checkDBExists(dbKey); err != nil {
		return false, errors.Trace(err)
	}

	// Check if table exists.
	tableKey := m.tableKey(tableID)
	v, err := m.txn.HGet(dbKey, tableKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	if v != nil {
		return true, nil
	}

	return false, nil
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
)

// JobListKeyType is a key type of the DDL job queue.
type JobListKeyType []byte

func (m *Mutator) getDDLJob(key []byte, index int64) (*model.Job, error) {
	value, err := m.txn.LIndex(key, index)
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{
		// For compatibility, if the job is enqueued by old version TiDB and Priority field is omitted,
		// set the default priority to kv.PriorityLow.
		Priority: kv.PriorityLow,
	}
	err = job.Decode(value)
	// Check if the job.Priority is valid.
	if job.Priority < kv.PriorityNormal || job.Priority > kv.PriorityHigh {
		job.Priority = kv.PriorityLow
	}
	return job, errors.Trace(err)
}

func (*Mutator) jobIDKey(id int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(id))
	return b
}

func (m *Mutator) addHistoryDDLJob(key []byte, job *model.Job, updateRawArgs bool) error {
	b, err := job.Encode(updateRawArgs)
	if err == nil {
		err = m.txn.HSet(key, m.jobIDKey(job.ID), b)
	}
	return errors.Trace(err)
}

// AddHistoryDDLJob adds DDL job to history.
func (m *Mutator) AddHistoryDDLJob(job *model.Job, updateRawArgs bool) error {
	return m.addHistoryDDLJob(mDDLJobHistoryKey, job, updateRawArgs)
}

func (m *Mutator) getHistoryDDLJob(key []byte, id int64) (*model.Job, error) {
	value, err := m.txn.HGet(key, m.jobIDKey(id))
	if err != nil || value == nil {
		return nil, errors.Trace(err)
	}

	job := &model.Job{}
	err = job.Decode(value)
	return job, errors.Trace(err)
}

// GetHistoryDDLJob gets a history DDL job.
func (m *Mutator) GetHistoryDDLJob(id int64) (*model.Job, error) {
	startTime := time.Now()
	job, err := m.getHistoryDDLJob(mDDLJobHistoryKey, id)
	metrics.MetaHistogram.WithLabelValues(metrics.GetHistoryDDLJob, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	return job, errors.Trace(err)
}

// GetHistoryDDLCount the count of all history DDL jobs.
func (m *Mutator) GetHistoryDDLCount() (uint64, error) {
	return m.txn.HGetLen(mDDLJobHistoryKey)
}

// LastJobIterator is the iterator for gets latest history.
type LastJobIterator interface {
	GetLastJobs(num int, jobs []*model.Job) ([]*model.Job, error)
}

// GetLastHistoryDDLJobsIterator gets latest history ddl jobs iterator.
func (m *Mutator) GetLastHistoryDDLJobsIterator() (LastJobIterator, error) {
	iter, err := structure.NewHashReverseIter(m.txn, mDDLJobHistoryKey)
	if err != nil {
		return nil, err
	}
	return &HLastJobIterator{
		iter: iter,
	}, nil
}

// GetLastHistoryDDLJobsIteratorWithFilter returns a iterator for getting latest history ddl jobs.
// This iterator will also filter jobs using given schemaNames and tableNames
func (m *Mutator) GetLastHistoryDDLJobsIteratorWithFilter(
	schemaNames set.StringSet,
	tableNames set.StringSet,
) (LastJobIterator, error) {
	iter, err := structure.NewHashReverseIter(m.txn, mDDLJobHistoryKey)
	if err != nil {
		return nil, err
	}
	return &HLastJobIterator{
		iter:        iter,
		schemaNames: schemaNames,
		tableNames:  tableNames,
	}, nil
}

// GetHistoryDDLJobsIterator gets the jobs iterator begin with startJobID.
func (m *Mutator) GetHistoryDDLJobsIterator(startJobID int64) (LastJobIterator, error) {
	field := m.jobIDKey(startJobID)
	iter, err := structure.NewHashReverseIterBeginWithField(m.txn, mDDLJobHistoryKey, field)
	if err != nil {
		return nil, err
	}
	return &HLastJobIterator{
		iter: iter,
	}, nil
}

// HLastJobIterator is the iterator for gets the latest history.
type HLastJobIterator struct {
	iter        *structure.ReverseHashIterator
	schemaNames set.StringSet
	tableNames  set.StringSet
}

var jobExtractFields = []string{"schema_name", "table_name"}

// ExtractSchemaAndTableNameFromJob extract schema_name and table_name from encoded Job structure
// Note, here we strongly rely on the order of fields in marshalled string, just like checkSubstringsInOrder
// Exported for test
func ExtractSchemaAndTableNameFromJob(data []byte) (schemaName, tableName string, err error) {
	m, err := partialjson.ExtractTopLevelMembers(data, jobExtractFields)

	schemaNameToken, ok := m["schema_name"]
	if !ok || len(schemaNameToken) != 1 {
		return "", "", errors.New("name field not found in JSON")
	}
	schemaName, ok = schemaNameToken[0].(string)
	if !ok {
		return "", "", errors.Errorf("unexpected name field in JSON, %v", schemaNameToken)
	}

	tableNameToken, ok := m["table_name"]
	if !ok || len(tableNameToken) != 1 {
		return "", "", errors.New("name field not found in JSON")
	}
	tableName, ok = tableNameToken[0].(string)
	if !ok {
		return "", "", errors.Errorf("unexpected name field in JSON, %v", tableNameToken)
	}
	return
}

// IsJobMatch examines whether given job's table/schema name matches.
func IsJobMatch(job []byte, schemaNames, tableNames set.StringSet) (match bool, err error) {
	if schemaNames.Count() == 0 && tableNames.Count() == 0 {
		return true, nil
	}
	schemaName, tableName, err := ExtractSchemaAndTableNameFromJob(job)
	if err != nil {
		return
	}
	if (schemaNames.Count() == 0 || schemaNames.Exist(schemaName)) &&
		tableNames.Count() == 0 || tableNames.Exist(tableName) {
		match = true
	}
	return
}

// GetLastJobs gets last several jobs.
func (i *HLastJobIterator) GetLastJobs(num int, jobs []*model.Job) ([]*model.Job, error) {
	if len(jobs) < num {
		jobs = make([]*model.Job, 0, num)
	}
	jobs = jobs[:0]
	iter := i.iter
	for iter.Valid() && len(jobs) < num {
		match, err := IsJobMatch(iter.Value(), i.schemaNames, i.tableNames)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if !match {
			err := iter.Next()
			if err != nil {
				return nil, errors.Trace(err)
			}
			continue
		}

		job := &model.Job{}
		err = job.Decode(iter.Value())
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

// GetBootstrapVersion returns the version of the server which bootstrap the store.
// If the store is not bootstraped, the version will be zero.
func (m *Mutator) GetBootstrapVersion() (int64, error) {
	value, err := m.txn.GetInt64(mBootstrapKey)
	return value, errors.Trace(err)
}

// FinishBootstrap finishes bootstrap.
func (m *Mutator) FinishBootstrap(version int64) error {
	err := m.txn.Set(mBootstrapKey, []byte(strconv.FormatInt(version, 10)))
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

func (*Mutator) schemaDiffKey(schemaVersion int64) []byte {
	return []byte(fmt.Sprintf("%s:%d", mSchemaDiffPrefix, schemaVersion))
}

// GetSchemaDiff gets the modification information on a given schema version.
func (m *Mutator) GetSchemaDiff(schemaVersion int64) (*model.SchemaDiff, error) {
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
func (m *Mutator) SetSchemaDiff(diff *model.SchemaDiff) error {
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

// GroupRUStats keeps the ru consumption statistics data.
type GroupRUStats struct {
	ID            int64             `json:"id"`
	Name          string            `json:"name"`
	RUConsumption *rmpb.Consumption `json:"ru_consumption"`
}

// DailyRUStats keeps all the ru consumption statistics data.
type DailyRUStats struct {
	EndTime time.Time      `json:"date"`
	Stats   []GroupRUStats `json:"stats"`
}

// RUStats keeps the lastest and second lastest DailyRUStats data.
type RUStats struct {
	Latest   *DailyRUStats `json:"latest"`
	Previous *DailyRUStats `json:"previous"`
}

// GetRUStats load the persisted RUStats data.
func (m *Mutator) GetRUStats() (*RUStats, error) {
	data, err := m.txn.Get(mRequestUnitStats)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var ruStats *RUStats
	if data != nil {
		ruStats = &RUStats{}
		if err = json.Unmarshal(data, &ruStats); err != nil {
			return nil, errors.Trace(err)
		}
	}
	return ruStats, nil
}

// SetRUStats persist new ru stats data to meta storage.
func (m *Mutator) SetRUStats(stats *RUStats) error {
	data, err := json.Marshal(stats)
	if err != nil {
		return errors.Trace(err)
	}

	err = m.txn.Set(mRequestUnitStats, data)
	return errors.Trace(err)
}

// GetOldestSchemaVersion gets the oldest schema version at the GC safe point.
// It works by checking the MVCC information (internal txn API) of the schema version meta key.
// This function is only used by infoschema v2 currently.
func GetOldestSchemaVersion(h *helper.Helper) (int64, error) {
	ek := make([]byte, 0, len(mMetaPrefix)+len(mSchemaVersionKey)+24)
	ek = append(ek, mMetaPrefix...)
	ek = codec.EncodeBytes(ek, mSchemaVersionKey)
	key := codec.EncodeUint(ek, uint64(structure.StringData))
	mvccResp, err := h.GetMvccByEncodedKeyWithTS(key, math.MaxUint64)
	if err != nil {
		return 0, err
	}
	if mvccResp == nil || mvccResp.Info == nil || len(mvccResp.Info.Writes) == 0 {
		return 0, errors.Errorf("There is no Write MVCC info for the schema version key")
	}

	v := mvccResp.Info.Writes[len(mvccResp.Info.Writes)-1]
	var n int64
	n, err = strconv.ParseInt(string(v.ShortValue), 10, 64)
	return n, errors.Trace(err)
}
