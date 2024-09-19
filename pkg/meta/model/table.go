// Copyright 2024 PingCAP, Inc.
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

package model

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/duration"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
)

// ExtraHandleID is the column ID of column which we need to append to schema to occupy the handle's position
// for use of execution phase.
const ExtraHandleID = -1

// ExtraPhysTblID is the column ID of column that should be filled in with the physical table id.
// Primarily used for table partition dynamic prune mode, to return which partition (physical table id) the row came from.
// If used with a global index, the partition ID decoded from the key value will be filled in.
const ExtraPhysTblID = -3

// Deprecated: Use ExtraPhysTblID instead.
// const ExtraPidColID = -2

// ExtraRowChecksumID is the column ID of column which holds the row checksum info.
const ExtraRowChecksumID = -4

const (
	// TableInfoVersion0 means the table info version is 0.
	// Upgrade from v2.1.1 or v2.1.2 to v2.1.3 and later, and then execute a "change/modify column" statement
	// that does not specify a charset value for column. Then the following error may be reported:
	// ERROR 1105 (HY000): unsupported modify charset from utf8mb4 to utf8.
	// To eliminate this error, we will not modify the charset of this column
	// when executing a change/modify column statement that does not specify a charset value for column.
	// This behavior is not compatible with MySQL.
	TableInfoVersion0 = uint16(0)
	// TableInfoVersion1 means the table info version is 1.
	// When we execute a change/modify column statement that does not specify a charset value for column,
	// we set the charset of this column to the charset of table. This behavior is compatible with MySQL.
	TableInfoVersion1 = uint16(1)
	// TableInfoVersion2 means the table info version is 2.
	// This is for v2.1.7 to Compatible with older versions charset problem.
	// Old version such as v2.0.8 treat utf8 as utf8mb4, because there is no UTF8 check in v2.0.8.
	// After version V2.1.2 (PR#8738) , TiDB add UTF8 check, then the user upgrade from v2.0.8 insert some UTF8MB4 characters will got error.
	// This is not compatibility for user. Then we try to fix this in PR #9820, and increase the version number.
	TableInfoVersion2 = uint16(2)
	// TableInfoVersion3 means the table info version is 3.
	// This version aims to deal with upper-cased charset name in TableInfo stored by versions prior to TiDB v2.1.9:
	// TiDB always suppose all charsets / collations as lower-cased and try to convert them if they're not.
	// However, the convert is missed in some scenarios before v2.1.9, so for all those tables prior to TableInfoVersion3, their
	// charsets / collations will be converted to lower-case while loading from the storage.
	TableInfoVersion3 = uint16(3)
	// TableInfoVersion4 is not used.
	TableInfoVersion4 = uint16(4)
	// TableInfoVersion5 indicates that the auto_increment allocator in TiDB has been separated from
	// _tidb_rowid allocator when AUTO_ID_CACHE is 1. This version is introduced to preserve the compatibility of old tables:
	// the tables with version <= TableInfoVersion4 still use a single allocator for auto_increment and _tidb_rowid.
	// Also see https://github.com/pingcap/tidb/issues/982.
	TableInfoVersion5 = uint16(5)

	// CurrLatestTableInfoVersion means the latest table info in the current TiDB.
	CurrLatestTableInfoVersion = TableInfoVersion5
)

// ExtraHandleName is the name of ExtraHandle Column.
var ExtraHandleName = model.NewCIStr("_tidb_rowid")

// ExtraPhysTblIDName is the name of ExtraPhysTblID Column.
var ExtraPhysTblIDName = model.NewCIStr("_tidb_tid")

// Deprecated: Use ExtraPhysTblIDName instead.
// var ExtraPartitionIdName = NewCIStr("_tidb_pid") //nolint:revive

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	ID      int64       `json:"id"`
	Name    model.CIStr `json:"name"`
	Charset string      `json:"charset"`
	Collate string      `json:"collate"`
	// Columns are listed in the order in which they appear in the schema.
	Columns     []*ColumnInfo     `json:"cols"`
	Indices     []*IndexInfo      `json:"index_info"`
	Constraints []*ConstraintInfo `json:"constraint_info"`
	ForeignKeys []*FKInfo         `json:"fk_info"`
	State       SchemaState       `json:"state"`
	// PKIsHandle is true when primary key is a single integer column.
	PKIsHandle bool `json:"pk_is_handle"`
	// IsCommonHandle is true when clustered index feature is
	// enabled and the primary key is not a single integer column.
	IsCommonHandle bool `json:"is_common_handle"`
	// CommonHandleVersion is the version of the clustered index.
	// 0 for the clustered index created == 5.0.0 RC.
	// 1 for the clustered index created > 5.0.0 RC.
	CommonHandleVersion uint16 `json:"common_handle_version"`

	Comment   string `json:"comment"`
	AutoIncID int64  `json:"auto_inc_id"`

	// Only used by BR when:
	// 1. SepAutoInc() is true
	// 2. The table is nonclustered and has auto_increment column.
	// In that case, both auto_increment_id and tidb_rowid need to be backup & recover.
	// See also https://github.com/pingcap/tidb/issues/46093
	//
	// It should have been named TiDBRowID, but for historial reasons, we do not use separate meta key for _tidb_rowid and auto_increment_id,
	// and field `AutoIncID` is used to serve both _tidb_rowid and auto_increment_id.
	// If we introduce a TiDBRowID here, it could make furthur misunderstanding:
	//	in most cases, AutoIncID is _tidb_rowid and TiDBRowID is null
	//      but in some cases, AutoIncID is auto_increment_id and TiDBRowID is _tidb_rowid
	// So let's just use another name AutoIncIDExtra to avoid misconception.
	AutoIncIDExtra int64 `json:"auto_inc_id_extra,omitempty"`

	AutoIDCache     int64 `json:"auto_id_cache"`
	AutoRandID      int64 `json:"auto_rand_id"`
	MaxColumnID     int64 `json:"max_col_id"`
	MaxIndexID      int64 `json:"max_idx_id"`
	MaxForeignKeyID int64 `json:"max_fk_id"`
	MaxConstraintID int64 `json:"max_cst_id"`
	// UpdateTS is used to record the timestamp of updating the table's schema information.
	// These changing schema operations don't include 'truncate table' and 'rename table'.
	UpdateTS uint64 `json:"update_timestamp"`
	// OldSchemaID :
	// Because auto increment ID has schemaID as prefix,
	// We need to save original schemaID to keep autoID unchanged
	// while renaming a table from one database to another.
	// Only set if table has been renamed across schemas
	// Old name 'old_schema_id' is kept for backwards compatibility
	AutoIDSchemaID int64 `json:"old_schema_id,omitempty"`

	// ShardRowIDBits specify if the implicit row ID is sharded.
	ShardRowIDBits uint64
	// MaxShardRowIDBits uses to record the max ShardRowIDBits be used so far.
	MaxShardRowIDBits uint64 `json:"max_shard_row_id_bits"`
	// AutoRandomBits is used to set the bit number to shard automatically when PKIsHandle.
	AutoRandomBits uint64 `json:"auto_random_bits"`
	// AutoRandomRangeBits represents the bit number of the int primary key that will be used by TiDB.
	AutoRandomRangeBits uint64 `json:"auto_random_range_bits"`
	// PreSplitRegions specify the pre-split region when create table.
	// The pre-split region num is 2^(PreSplitRegions-1).
	// And the PreSplitRegions should less than or equal to ShardRowIDBits.
	PreSplitRegions uint64 `json:"pre_split_regions"`

	Partition *PartitionInfo `json:"partition"`

	Compression string `json:"compression"`

	View *ViewInfo `json:"view"`

	Sequence *SequenceInfo `json:"sequence"`

	// Lock represent the table lock info.
	Lock *TableLockInfo `json:"Lock"`

	// Version means the version of the table info.
	Version uint16 `json:"version"`

	// TiFlashReplica means the TiFlash replica info.
	TiFlashReplica *TiFlashReplicaInfo `json:"tiflash_replica"`

	// IsColumnar means the table is column-oriented.
	// It's true when the engine of the table is TiFlash only.
	IsColumnar bool `json:"is_columnar"`

	TempTableType        `json:"temp_table_type"`
	TableCacheStatusType `json:"cache_table_status"`
	PlacementPolicyRef   *PolicyRefInfo `json:"policy_ref_info"`

	// StatsOptions is used when do analyze/auto-analyze for each table
	StatsOptions *StatsOptions `json:"stats_options"`

	ExchangePartitionInfo *ExchangePartitionInfo `json:"exchange_partition_info"`

	TTLInfo *TTLInfo `json:"ttl_info"`

	// Revision is per table schema's version, it will be increased when the schema changed.
	Revision uint64 `json:"revision"`

	DBID int64 `json:"-"`
}

// SepAutoInc decides whether _rowid and auto_increment id use separate allocator.
func (t *TableInfo) SepAutoInc() bool {
	return t.Version >= TableInfoVersion5 && t.AutoIDCache == 1
}

// GetPartitionInfo returns the partition information.
func (t *TableInfo) GetPartitionInfo() *PartitionInfo {
	if t.Partition != nil && t.Partition.Enable {
		return t.Partition
	}
	return nil
}

// GetUpdateTime gets the table's updating time.
func (t *TableInfo) GetUpdateTime() time.Time {
	return TSConvert2Time(t.UpdateTS)
}

// Clone clones TableInfo.
func (t *TableInfo) Clone() *TableInfo {
	nt := *t
	nt.Columns = make([]*ColumnInfo, len(t.Columns))
	nt.Indices = make([]*IndexInfo, len(t.Indices))
	nt.ForeignKeys = make([]*FKInfo, len(t.ForeignKeys))

	for i := range t.Columns {
		nt.Columns[i] = t.Columns[i].Clone()
	}

	for i := range t.Indices {
		nt.Indices[i] = t.Indices[i].Clone()
	}

	for i := range t.ForeignKeys {
		nt.ForeignKeys[i] = t.ForeignKeys[i].Clone()
	}

	if t.Partition != nil {
		nt.Partition = t.Partition.Clone()
	}
	if t.TTLInfo != nil {
		nt.TTLInfo = t.TTLInfo.Clone()
	}

	return &nt
}

// GetPkName will return the pk name if pk exists.
func (t *TableInfo) GetPkName() model.CIStr {
	for _, colInfo := range t.Columns {
		if mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			return colInfo.Name
		}
	}
	return model.CIStr{}
}

// GetPkColInfo gets the ColumnInfo of pk if exists.
// Make sure PkIsHandle checked before call this method.
func (t *TableInfo) GetPkColInfo() *ColumnInfo {
	for _, colInfo := range t.Columns {
		if mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			return colInfo
		}
	}
	return nil
}

// GetAutoIncrementColInfo gets the ColumnInfo of auto_increment column if exists.
func (t *TableInfo) GetAutoIncrementColInfo() *ColumnInfo {
	for _, colInfo := range t.Columns {
		if mysql.HasAutoIncrementFlag(colInfo.GetFlag()) {
			return colInfo
		}
	}
	return nil
}

// IsAutoIncColUnsigned checks whether the auto increment column is unsigned.
func (t *TableInfo) IsAutoIncColUnsigned() bool {
	col := t.GetAutoIncrementColInfo()
	if col == nil {
		return false
	}
	return mysql.HasUnsignedFlag(col.GetFlag())
}

// ContainsAutoRandomBits indicates whether a table contains auto_random column.
func (t *TableInfo) ContainsAutoRandomBits() bool {
	return t.AutoRandomBits != 0
}

// IsAutoRandomBitColUnsigned indicates whether the auto_random column is unsigned. Make sure the table contains auto_random before calling this method.
func (t *TableInfo) IsAutoRandomBitColUnsigned() bool {
	if !t.PKIsHandle || t.AutoRandomBits == 0 {
		return false
	}
	return mysql.HasUnsignedFlag(t.GetPkColInfo().GetFlag())
}

// Cols returns the columns of the table in public state.
func (t *TableInfo) Cols() []*ColumnInfo {
	publicColumns := make([]*ColumnInfo, len(t.Columns))
	maxOffset := -1
	for _, col := range t.Columns {
		if col.State != StatePublic {
			continue
		}
		publicColumns[col.Offset] = col
		if maxOffset < col.Offset {
			maxOffset = col.Offset
		}
	}
	return publicColumns[0 : maxOffset+1]
}

// FindIndexByName finds index by name.
func (t *TableInfo) FindIndexByName(idxName string) *IndexInfo {
	for _, idx := range t.Indices {
		if idx.Name.L == idxName {
			return idx
		}
	}
	return nil
}

// FindPublicColumnByName finds the public column by name.
func (t *TableInfo) FindPublicColumnByName(colNameL string) *ColumnInfo {
	for _, col := range t.Cols() {
		if col.Name.L == colNameL {
			return col
		}
	}
	return nil
}

// IsLocked checks whether the table was locked.
func (t *TableInfo) IsLocked() bool {
	return t.Lock != nil && len(t.Lock.Sessions) > 0
}

// MoveColumnInfo moves a column to another offset. It maintains the offsets of all affects columns and index columns,
func (t *TableInfo) MoveColumnInfo(from, to int) {
	if from == to {
		return
	}
	updatedOffsets := make(map[int]int)
	src := t.Columns[from]
	if from < to {
		for i := from; i < to; i++ {
			t.Columns[i] = t.Columns[i+1]
			t.Columns[i].Offset = i
			updatedOffsets[i+1] = i
		}
	} else if from > to {
		for i := from; i > to; i-- {
			t.Columns[i] = t.Columns[i-1]
			t.Columns[i].Offset = i
			updatedOffsets[i-1] = i
		}
	}
	t.Columns[to] = src
	t.Columns[to].Offset = to
	updatedOffsets[from] = to
	for _, idx := range t.Indices {
		for _, idxCol := range idx.Columns {
			newOffset, ok := updatedOffsets[idxCol.Offset]
			if ok {
				idxCol.Offset = newOffset
			}
		}
	}
}

// ClearPlacement clears all table and partitions' placement settings
func (t *TableInfo) ClearPlacement() {
	t.PlacementPolicyRef = nil
	if t.Partition != nil {
		for i := range t.Partition.Definitions {
			def := &t.Partition.Definitions[i]
			def.PlacementPolicyRef = nil
		}
	}
}

// GetPrimaryKey extract the primary key in a table and return `IndexInfo`
// The returned primary key could be explicit or implicit.
// If there is no explicit primary key in table,
// the first UNIQUE INDEX on NOT NULL columns will be the implicit primary key.
// For more information about implicit primary key, see
// https://dev.mysql.com/doc/refman/8.0/en/invisible-indexes.html
func (t *TableInfo) GetPrimaryKey() *IndexInfo {
	var implicitPK *IndexInfo

	for _, key := range t.Indices {
		if key.Primary {
			// table has explicit primary key
			return key
		}
		// The case index without any columns should never happen, but still do a check here
		if len(key.Columns) == 0 {
			continue
		}
		// find the first unique key with NOT NULL columns
		if implicitPK == nil && key.Unique {
			// ensure all columns in unique key have NOT NULL flag
			allColNotNull := true
			skip := false
			for _, idxCol := range key.Columns {
				col := FindColumnInfo(t.Cols(), idxCol.Name.L)
				// This index has a column in DeleteOnly state,
				// or it is expression index (it defined on a hidden column),
				// it can not be implicit PK, go to next index iterator
				if col == nil || col.Hidden {
					skip = true
					break
				}
				if !mysql.HasNotNullFlag(col.GetFlag()) {
					allColNotNull = false
					break
				}
			}
			if skip {
				continue
			}
			if allColNotNull {
				implicitPK = key
			}
		}
	}
	return implicitPK
}

// ColumnIsInIndex checks whether c is included in any indices of t.
func (t *TableInfo) ColumnIsInIndex(c *ColumnInfo) bool {
	for _, index := range t.Indices {
		for _, column := range index.Columns {
			if column.Name.L == c.Name.L {
				return true
			}
		}
	}
	return false
}

// HasClusteredIndex checks whether the table has a clustered index.
func (t *TableInfo) HasClusteredIndex() bool {
	return t.PKIsHandle || t.IsCommonHandle
}

// IsView checks if TableInfo is a view.
func (t *TableInfo) IsView() bool {
	return t.View != nil
}

// IsSequence checks if TableInfo is a sequence.
func (t *TableInfo) IsSequence() bool {
	return t.Sequence != nil
}

// IsBaseTable checks to see the table is neither a view or a sequence.
func (t *TableInfo) IsBaseTable() bool {
	return t.Sequence == nil && t.View == nil
}

// FindConstraintInfoByName finds constraintInfo by name.
func (t *TableInfo) FindConstraintInfoByName(constrName string) *ConstraintInfo {
	lowConstrName := strings.ToLower(constrName)
	for _, chk := range t.Constraints {
		if chk.Name.L == lowConstrName {
			return chk
		}
	}
	return nil
}

// FindIndexNameByID finds index name by id.
func (t *TableInfo) FindIndexNameByID(id int64) string {
	indexInfo := FindIndexInfoByID(t.Indices, id)
	if indexInfo != nil {
		return indexInfo.Name.L
	}
	return ""
}

// FindColumnNameByID finds column name by id.
func (t *TableInfo) FindColumnNameByID(id int64) string {
	colInfo := FindColumnInfoByID(t.Columns, id)
	if colInfo != nil {
		return colInfo.Name.L
	}
	return ""
}

// GetColumnByID finds the column by ID.
func (t *TableInfo) GetColumnByID(id int64) *ColumnInfo {
	for _, col := range t.Columns {
		if col.State != StatePublic {
			continue
		}
		if col.ID == id {
			return col
		}
	}
	return nil
}

// FindFKInfoByName finds FKInfo in fks by lowercase name.
func FindFKInfoByName(fks []*FKInfo, name string) *FKInfo {
	for _, fk := range fks {
		if fk.Name.L == name {
			return fk
		}
	}
	return nil
}

// TableNameInfo provides meta data describing a table name info.
type TableNameInfo struct {
	ID   int64       `json:"id"`
	Name model.CIStr `json:"name"`
}

// TableCacheStatusType is the type of the table cache status
type TableCacheStatusType int

// TableCacheStatusType values.
const (
	TableCacheStatusDisable TableCacheStatusType = iota
	TableCacheStatusEnable
	TableCacheStatusSwitching
)

// String implements fmt.Stringer interface.
func (t TableCacheStatusType) String() string {
	switch t {
	case TableCacheStatusDisable:
		return "disable"
	case TableCacheStatusEnable:
		return "enable"
	case TableCacheStatusSwitching:
		return "switching"
	default:
		return ""
	}
}

// TempTableType is the type of the temp table
type TempTableType byte

// TempTableType values.
const (
	TempTableNone TempTableType = iota
	TempTableGlobal
	TempTableLocal
)

// String implements fmt.Stringer interface.
func (t TempTableType) String() string {
	switch t {
	case TempTableGlobal:
		return "global"
	case TempTableLocal:
		return "local"
	default:
		return ""
	}
}

// TableLockInfo provides meta data describing a table lock.
type TableLockInfo struct {
	Tp model.TableLockType
	// Use array because there may be multiple sessions holding the same read lock.
	Sessions []SessionInfo
	State    TableLockState
	// TS is used to record the timestamp this table lock been locked.
	TS uint64
}

// SessionInfo contain the session ID and the server ID.
type SessionInfo struct {
	ServerID  string
	SessionID uint64
}

// String implements fmt.Stringer interface.
func (s SessionInfo) String() string {
	return "server: " + s.ServerID + "_session: " + strconv.FormatUint(s.SessionID, 10)
}

// TableLockTpInfo is composed by schema ID, table ID and table lock type.
type TableLockTpInfo struct {
	SchemaID int64
	TableID  int64
	Tp       model.TableLockType
}

// TableLockState is the state for table lock.
type TableLockState byte

const (
	// TableLockStateNone means this table lock is absent.
	TableLockStateNone TableLockState = iota
	// TableLockStatePreLock means this table lock is pre-lock state. Other session doesn't hold this lock should't do corresponding operation according to the lock type.
	TableLockStatePreLock
	// TableLockStatePublic means this table lock is public state.
	TableLockStatePublic
)

// String implements fmt.Stringer interface.
func (t TableLockState) String() string {
	switch t {
	case TableLockStatePreLock:
		return "pre-lock"
	case TableLockStatePublic:
		return "public"
	default:
		return "none"
	}
}

// TiFlashReplicaInfo means the flash replica info.
type TiFlashReplicaInfo struct {
	Count                 uint64
	LocationLabels        []string
	Available             bool
	AvailablePartitionIDs []int64
}

// IsPartitionAvailable checks whether the partition table replica was available.
func (tr *TiFlashReplicaInfo) IsPartitionAvailable(pid int64) bool {
	for _, id := range tr.AvailablePartitionIDs {
		if id == pid {
			return true
		}
	}
	return false
}

// ViewInfo provides meta data describing a DB view.
type ViewInfo struct {
	Algorithm   model.ViewAlgorithm   `json:"view_algorithm"`
	Definer     *auth.UserIdentity    `json:"view_definer"`
	Security    model.ViewSecurity    `json:"view_security"`
	SelectStmt  string                `json:"view_select"`
	CheckOption model.ViewCheckOption `json:"view_checkoption"`
	Cols        []model.CIStr         `json:"view_cols"`
}

// Some constants for sequence.
const (
	DefaultSequenceCacheBool          = true
	DefaultSequenceCycleBool          = false
	DefaultSequenceOrderBool          = false
	DefaultSequenceCacheValue         = int64(1000)
	DefaultSequenceIncrementValue     = int64(1)
	DefaultPositiveSequenceStartValue = int64(1)
	DefaultNegativeSequenceStartValue = int64(-1)
	DefaultPositiveSequenceMinValue   = int64(1)
	DefaultPositiveSequenceMaxValue   = int64(9223372036854775806)
	DefaultNegativeSequenceMaxValue   = int64(-1)
	DefaultNegativeSequenceMinValue   = int64(-9223372036854775807)
)

// SequenceInfo provide meta data describing a DB sequence.
type SequenceInfo struct {
	Start      int64  `json:"sequence_start"`
	Cache      bool   `json:"sequence_cache"`
	Cycle      bool   `json:"sequence_cycle"`
	MinValue   int64  `json:"sequence_min_value"`
	MaxValue   int64  `json:"sequence_max_value"`
	Increment  int64  `json:"sequence_increment"`
	CacheValue int64  `json:"sequence_cache_value"`
	Comment    string `json:"sequence_comment"`
}

// ExchangePartitionInfo provides exchange partition info.
type ExchangePartitionInfo struct {
	// It is nt tableID when table which has the info is a partition table, else pt tableID.
	ExchangePartitionTableID int64 `json:"exchange_partition_id"`
	ExchangePartitionDefID   int64 `json:"exchange_partition_def_id"`
	// Deprecated, not used
	XXXExchangePartitionFlag bool `json:"exchange_partition_flag"`
}

// UpdateIndexInfo is to carry the entries in the list of indexes in UPDATE INDEXES
// during ALTER TABLE t PARTITION BY ... UPDATE INDEXES (idx_a GLOBAL, idx_b LOCAL...)
type UpdateIndexInfo struct {
	IndexName string `json:"index_name"`
	Global    bool   `json:"global"`
}

// PartitionInfo provides table partition info.
type PartitionInfo struct {
	Type    model.PartitionType `json:"type"`
	Expr    string              `json:"expr"`
	Columns []model.CIStr       `json:"columns"`

	// User may already create table with partition but table partition is not
	// yet supported back then. When Enable is true, write/read need use tid
	// rather than pid.
	Enable bool `json:"enable"`

	// IsEmptyColumns is for syntax like `partition by key()`.
	// When IsEmptyColums is true, it will not display column name in `show create table` stmt.
	IsEmptyColumns bool `json:"is_empty_columns"`

	Definitions []PartitionDefinition `json:"definitions"`
	// AddingDefinitions is filled when adding partitions that is in the mid state.
	AddingDefinitions []PartitionDefinition `json:"adding_definitions"`
	// DroppingDefinitions is filled when dropping/truncating partitions that is in the mid state.
	DroppingDefinitions []PartitionDefinition `json:"dropping_definitions"`
	// NewPartitionIDs is filled when truncating partitions that is in the mid state.
	NewPartitionIDs []int64

	States []PartitionState `json:"states"`
	Num    uint64           `json:"num"`
	// Only used during ReorganizePartition so far
	DDLState SchemaState `json:"ddl_state"`
	// Set during ALTER TABLE ... if the table id needs to change
	// like if there is a global index or going between non-partitioned
	// and partitioned table, to make the data dropping / range delete
	// optimized.
	NewTableID int64 `json:"new_table_id"`
	// Set during ALTER TABLE ... PARTITION BY ...
	// First as the new partition scheme, then in StateDeleteReorg as the old
	DDLType    model.PartitionType `json:"ddl_type"`
	DDLExpr    string              `json:"ddl_expr"`
	DDLColumns []model.CIStr       `json:"ddl_columns"`
	// For ActionAlterTablePartitioning, UPDATE INDEXES
	DDLUpdateIndexes []UpdateIndexInfo `json:"ddl_update_indexes"`
}

// Clone clones itself.
func (pi *PartitionInfo) Clone() *PartitionInfo {
	newPi := *pi
	newPi.Columns = make([]model.CIStr, len(pi.Columns))
	copy(newPi.Columns, pi.Columns)

	newPi.Definitions = make([]PartitionDefinition, len(pi.Definitions))
	for i := range pi.Definitions {
		newPi.Definitions[i] = pi.Definitions[i].Clone()
	}

	newPi.AddingDefinitions = make([]PartitionDefinition, len(pi.AddingDefinitions))
	for i := range pi.AddingDefinitions {
		newPi.AddingDefinitions[i] = pi.AddingDefinitions[i].Clone()
	}

	newPi.DroppingDefinitions = make([]PartitionDefinition, len(pi.DroppingDefinitions))
	for i := range pi.DroppingDefinitions {
		newPi.DroppingDefinitions[i] = pi.DroppingDefinitions[i].Clone()
	}

	return &newPi
}

// GetNameByID gets the partition name by ID.
// TODO: Remove the need for this function!
func (pi *PartitionInfo) GetNameByID(id int64) string {
	definitions := pi.Definitions
	// do not convert this loop to `for _, def := range definitions`.
	// see https://github.com/pingcap/parser/pull/1072 for the benchmark.
	for i := range definitions {
		if id == definitions[i].ID {
			return definitions[i].Name.O
		}
	}
	return ""
}

// GetStateByID gets the partition state by ID.
func (pi *PartitionInfo) GetStateByID(id int64) SchemaState {
	for _, pstate := range pi.States {
		if pstate.ID == id {
			return pstate.State
		}
	}
	return StatePublic
}

// SetStateByID sets the state of the partition by ID.
func (pi *PartitionInfo) SetStateByID(id int64, state SchemaState) {
	newState := PartitionState{ID: id, State: state}
	for i, pstate := range pi.States {
		if pstate.ID == id {
			pi.States[i] = newState
			return
		}
	}
	if pi.States == nil {
		pi.States = make([]PartitionState, 0, 1)
	}
	pi.States = append(pi.States, newState)
}

// GCPartitionStates cleans up the partition state.
func (pi *PartitionInfo) GCPartitionStates() {
	if len(pi.States) < 1 {
		return
	}
	newStates := make([]PartitionState, 0, len(pi.Definitions))
	for _, state := range pi.States {
		found := false
		for _, def := range pi.Definitions {
			if def.ID == state.ID {
				found = true
				break
			}
		}
		if found {
			newStates = append(newStates, state)
		}
	}
	pi.States = newStates
}

// HasTruncatingPartitionID checks whether the pid is truncating.
func (pi *PartitionInfo) HasTruncatingPartitionID(pid int64) bool {
	for i := range pi.NewPartitionIDs {
		if pi.NewPartitionIDs[i] == pid {
			return true
		}
	}
	return false
}

// ClearReorgIntermediateInfo remove intermediate information used during reorganize partition.
func (pi *PartitionInfo) ClearReorgIntermediateInfo() {
	pi.DDLType = model.PartitionTypeNone
	pi.DDLExpr = ""
	pi.DDLColumns = nil
	pi.NewTableID = 0
}

// FindPartitionDefinitionByName finds PartitionDefinition by name.
func (pi *PartitionInfo) FindPartitionDefinitionByName(partitionDefinitionName string) int {
	lowConstrName := strings.ToLower(partitionDefinitionName)
	definitions := pi.Definitions
	for i := range definitions {
		if definitions[i].Name.L == lowConstrName {
			return i
		}
	}
	return -1
}

// GetPartitionIDByName gets the partition ID by name.
func (pi *PartitionInfo) GetPartitionIDByName(partitionDefinitionName string) int64 {
	lowConstrName := strings.ToLower(partitionDefinitionName)
	for _, definition := range pi.Definitions {
		if definition.Name.L == lowConstrName {
			return definition.ID
		}
	}
	return -1
}

// PartitionState is the state of the partition.
type PartitionState struct {
	ID    int64       `json:"id"`
	State SchemaState `json:"state"`
}

// PartitionDefinition defines a single partition.
type PartitionDefinition struct {
	ID                 int64          `json:"id"`
	Name               model.CIStr    `json:"name"`
	LessThan           []string       `json:"less_than"`
	InValues           [][]string     `json:"in_values"`
	PlacementPolicyRef *PolicyRefInfo `json:"policy_ref_info"`
	Comment            string         `json:"comment,omitempty"`
}

// Clone clones ConstraintInfo.
func (ci *PartitionDefinition) Clone() PartitionDefinition {
	nci := *ci
	nci.LessThan = make([]string, len(ci.LessThan))
	copy(nci.LessThan, ci.LessThan)
	return nci
}

const emptyPartitionDefinitionSize = int64(unsafe.Sizeof(PartitionState{}))

// MemoryUsage return the memory usage of PartitionDefinition
func (ci *PartitionDefinition) MemoryUsage() (sum int64) {
	if ci == nil {
		return
	}

	sum = emptyPartitionDefinitionSize + ci.Name.MemoryUsage()
	if ci.PlacementPolicyRef != nil {
		sum += int64(unsafe.Sizeof(ci.PlacementPolicyRef.ID)) + ci.PlacementPolicyRef.Name.MemoryUsage()
	}

	for _, str := range ci.LessThan {
		sum += int64(len(str))
	}
	for _, strs := range ci.InValues {
		for _, str := range strs {
			sum += int64(len(str))
		}
	}
	return
}

// ConstraintInfo provides meta data describing check-expression constraint.
type ConstraintInfo struct {
	ID             int64         `json:"id"`
	Name           model.CIStr   `json:"constraint_name"`
	Table          model.CIStr   `json:"tbl_name"`        // Table name.
	ConstraintCols []model.CIStr `json:"constraint_cols"` // Depended column names.
	Enforced       bool          `json:"enforced"`
	InColumn       bool          `json:"in_column"` // Indicate whether the constraint is column type check.
	ExprString     string        `json:"expr_string"`
	State          SchemaState   `json:"state"`
}

// Clone clones ConstraintInfo.
func (ci *ConstraintInfo) Clone() *ConstraintInfo {
	nci := *ci

	nci.ConstraintCols = make([]model.CIStr, len(ci.ConstraintCols))
	copy(nci.ConstraintCols, ci.ConstraintCols)
	return &nci
}

// FKInfo provides meta data describing a foreign key constraint.
type FKInfo struct {
	ID        int64         `json:"id"`
	Name      model.CIStr   `json:"fk_name"`
	RefSchema model.CIStr   `json:"ref_schema"`
	RefTable  model.CIStr   `json:"ref_table"`
	RefCols   []model.CIStr `json:"ref_cols"`
	Cols      []model.CIStr `json:"cols"`
	OnDelete  int           `json:"on_delete"`
	OnUpdate  int           `json:"on_update"`
	State     SchemaState   `json:"state"`
	Version   int           `json:"version"`
}

// String returns the string representation of FKInfo.
func (fk *FKInfo) String(db, tb string) string {
	buf := bytes.Buffer{}
	buf.WriteString("`" + db + "`.`")
	buf.WriteString(tb + "`, CONSTRAINT `")
	buf.WriteString(fk.Name.O + "` FOREIGN KEY (")
	for i, col := range fk.Cols {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("`" + col.O + "`")
	}
	buf.WriteString(") REFERENCES `")
	if fk.RefSchema.L != db {
		buf.WriteString(fk.RefSchema.L)
		buf.WriteString("`.`")
	}
	buf.WriteString(fk.RefTable.L)
	buf.WriteString("` (")
	for i, col := range fk.RefCols {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString("`" + col.O + "`")
	}
	buf.WriteString(")")
	if onDelete := model.ReferOptionType(fk.OnDelete); onDelete != model.ReferOptionNoOption {
		buf.WriteString(" ON DELETE ")
		buf.WriteString(onDelete.String())
	}
	if onUpdate := model.ReferOptionType(fk.OnUpdate); onUpdate != model.ReferOptionNoOption {
		buf.WriteString(" ON UPDATE ")
		buf.WriteString(onUpdate.String())
	}
	return buf.String()
}

// Clone clones FKInfo.
func (fk *FKInfo) Clone() *FKInfo {
	nfk := *fk

	nfk.RefCols = make([]model.CIStr, len(fk.RefCols))
	nfk.Cols = make([]model.CIStr, len(fk.Cols))
	copy(nfk.RefCols, fk.RefCols)
	copy(nfk.Cols, fk.Cols)

	return &nfk
}

const (
	// FKVersion0 indicate the FKInfo version is 0.
	// In FKVersion0, TiDB only supported syntax of foreign key, but the foreign key constraint doesn't take effect.
	FKVersion0 = 0
	// FKVersion1 indicate the FKInfo version is 1.
	// In FKVersion1, TiDB supports the foreign key constraint.
	FKVersion1 = 1
)

// ReferredFKInfo provides the cited foreign key in the child table.
type ReferredFKInfo struct {
	Cols        []model.CIStr `json:"cols"`
	ChildSchema model.CIStr   `json:"child_schema"`
	ChildTable  model.CIStr   `json:"child_table"`
	ChildFKName model.CIStr   `json:"child_fk_name"`
}

// TableItemID is composed by table ID and column/index ID
type TableItemID struct {
	TableID          int64
	ID               int64
	IsIndex          bool
	IsSyncLoadFailed bool
}

// Key is used to generate unique key for TableItemID to use in the syncload
func (t TableItemID) Key() string {
	return fmt.Sprintf("%d#%d#%t", t.ID, t.TableID, t.IsIndex)
}

// StatsLoadItem represents the load unit for statistics's memory loading.
type StatsLoadItem struct {
	TableItemID
	FullLoad bool
}

// Key is used to generate unique key for TableItemID to use in the syncload
func (s StatsLoadItem) Key() string {
	return fmt.Sprintf("%s#%t", s.TableItemID.Key(), s.FullLoad)
}

// StatsOptions is the struct to store the stats options.
type StatsOptions struct {
	*StatsWindowSettings
	AutoRecalc   bool               `json:"auto_recalc"`
	ColumnChoice model.ColumnChoice `json:"column_choice"`
	ColumnList   []model.CIStr      `json:"column_list"`
	SampleNum    uint64             `json:"sample_num"`
	SampleRate   float64            `json:"sample_rate"`
	Buckets      uint64             `json:"buckets"`
	TopN         uint64             `json:"topn"`
	Concurrency  uint               `json:"concurrency"`
}

// NewStatsOptions creates a new StatsOptions.
func NewStatsOptions() *StatsOptions {
	return &StatsOptions{
		AutoRecalc:   true,
		ColumnChoice: model.DefaultChoice,
		ColumnList:   []model.CIStr{},
		SampleNum:    uint64(0),
		SampleRate:   0.0,
		Buckets:      uint64(0),
		TopN:         uint64(0),
		Concurrency:  uint(0),
	}
}

// StatsWindowSettings is the settings of the stats window.
type StatsWindowSettings struct {
	WindowStart    time.Time        `json:"window_start"`
	WindowEnd      time.Time        `json:"window_end"`
	RepeatType     WindowRepeatType `json:"repeat_type"`
	RepeatInterval uint             `json:"repeat_interval"`
}

// WindowRepeatType is the type of the window repeat.
type WindowRepeatType byte

// WindowRepeatType values.
const (
	Never WindowRepeatType = iota
	Day
	Week
	Month
)

// String implements fmt.Stringer interface.
func (s WindowRepeatType) String() string {
	switch s {
	case Never:
		return "Never"
	case Day:
		return "Day"
	case Week:
		return "Week"
	case Month:
		return "Month"
	default:
		return ""
	}
}

// DefaultJobInterval sets the default interval between TTL jobs
const DefaultJobInterval = time.Hour

// TTLInfo records the TTL config
type TTLInfo struct {
	ColumnName      model.CIStr `json:"column"`
	IntervalExprStr string      `json:"interval_expr"`
	// `IntervalTimeUnit` is actually ast.TimeUnitType. Use `int` to avoid cycle dependency
	IntervalTimeUnit int  `json:"interval_time_unit"`
	Enable           bool `json:"enable"`
	// JobInterval is the interval between two TTL scan jobs.
	// It's suggested to get a duration with `(*TTLInfo).GetJobInterval`
	JobInterval string `json:"job_interval"`
}

// Clone clones TTLInfo
func (t *TTLInfo) Clone() *TTLInfo {
	cloned := *t
	return &cloned
}

// GetJobInterval parses the job interval and return
// if the job interval is an empty string, the "1h" will be returned, to keep compatible with 6.5 (in which
// TTL_JOB_INTERVAL attribute doesn't exist)
// Didn't set TTL_JOB_INTERVAL during upgrade and bootstrap because setting default value here is much simpler
// and could avoid bugs blocking users from upgrading or bootstrapping the cluster.
func (t *TTLInfo) GetJobInterval() (time.Duration, error) {
	if len(t.JobInterval) == 0 {
		return DefaultJobInterval, nil
	}

	return duration.ParseDuration(t.JobInterval)
}
