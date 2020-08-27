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

package model

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/auth"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/types"
)

// SchemaState is the state for schema elements.
type SchemaState byte

const (
	// StateNone means this schema element is absent and can't be used.
	StateNone SchemaState = iota
	// StateDeleteOnly means we can only delete items for this schema element.
	StateDeleteOnly
	// StateWriteOnly means we can use any write operation on this schema element,
	// but outer can't read the changed data.
	StateWriteOnly
	// StateWriteReorganization means we are re-organizing whole data after write only state.
	StateWriteReorganization
	// StateDeleteReorganization means we are re-organizing whole data after delete only state.
	StateDeleteReorganization
	// StatePublic means this schema element is ok for all write and read operations.
	StatePublic
	// StateReplica means we're waiting tiflash replica to be finished.
	StateReplicaOnly
	/*
	 *  Please add the new state at the end to keep the values consistent across versions.
	 */
)

// String implements fmt.Stringer interface.
func (s SchemaState) String() string {
	switch s {
	case StateDeleteOnly:
		return "delete only"
	case StateWriteOnly:
		return "write only"
	case StateWriteReorganization:
		return "write reorganization"
	case StateDeleteReorganization:
		return "delete reorganization"
	case StatePublic:
		return "public"
	case StateReplicaOnly:
		return "replica only"
	default:
		return "none"
	}
}

const (
	// ColumnInfoVersion0 means the column info version is 0.
	ColumnInfoVersion0 = uint64(0)
	// ColumnInfoVersion1 means the column info version is 1.
	ColumnInfoVersion1 = uint64(1)
	// ColumnInfoVersion2 means the column info version is 2.
	// This is for v2.1.7 to Compatible with older versions charset problem.
	// Old version such as v2.0.8 treat utf8 as utf8mb4, because there is no UTF8 check in v2.0.8.
	// After version V2.1.2 (PR#8738) , TiDB add UTF8 check, then the user upgrade from v2.0.8 insert some UTF8MB4 characters will got error.
	// This is not compatibility for user. Then we try to fix this in PR #9820, and increase the version number.
	ColumnInfoVersion2 = uint64(2)

	// CurrLatestColumnInfoVersion means the latest column info in the current TiDB.
	CurrLatestColumnInfoVersion = ColumnInfoVersion2
)

// ChangeStateInfo is used for recording the information of schema changing.
type ChangeStateInfo struct {
	// DependencyColumnOffset is the changing column offset that the current column depends on when executing modify/change column.
	DependencyColumnOffset int `json:"relative_col_offset"`
}

// ColumnInfo provides meta data describing of a table column.
type ColumnInfo struct {
	ID                    int64       `json:"id"`
	Name                  CIStr       `json:"name"`
	Offset                int         `json:"offset"`
	OriginDefaultValue    interface{} `json:"origin_default"`
	OriginDefaultValueBit []byte      `json:"origin_default_bit"`
	DefaultValue          interface{} `json:"default"`
	DefaultValueBit       []byte      `json:"default_bit"`
	// DefaultIsExpr is indicates the default value string is expr.
	DefaultIsExpr       bool                `json:"default_is_expr"`
	GeneratedExprString string              `json:"generated_expr_string"`
	GeneratedStored     bool                `json:"generated_stored"`
	Dependences         map[string]struct{} `json:"dependences"`
	types.FieldType     `json:"type"`
	State               SchemaState `json:"state"`
	Comment             string      `json:"comment"`
	// A hidden column is used internally(expression index) and are not accessible by users.
	Hidden           bool `json:"hidden"`
	*ChangeStateInfo `json:"change_state_info"`
	// Version means the version of the column info.
	// Version = 0: For OriginDefaultValue and DefaultValue of timestamp column will stores the default time in system time zone.
	//              That is a bug if multiple TiDB servers in different system time zone.
	// Version = 1: For OriginDefaultValue and DefaultValue of timestamp column will stores the default time in UTC time zone.
	//              This will fix bug in version 0. For compatibility with version 0, we add version field in column info struct.
	Version uint64 `json:"version"`
}

// Clone clones ColumnInfo.
func (c *ColumnInfo) Clone() *ColumnInfo {
	nc := *c
	return &nc
}

// IsGenerated returns true if the column is generated column.
func (c *ColumnInfo) IsGenerated() bool {
	return len(c.GeneratedExprString) != 0
}

// SetOriginalDefaultValue sets the origin default value.
// For mysql.TypeBit type, the default value storage format must be a string.
// Other value such as int must convert to string format first.
// The mysql.TypeBit type supports the null default value.
func (c *ColumnInfo) SetOriginDefaultValue(value interface{}) error {
	c.OriginDefaultValue = value
	if c.Tp == mysql.TypeBit {
		if value == nil {
			return nil
		}
		if v, ok := value.(string); ok {
			c.OriginDefaultValueBit = []byte(v)
			return nil
		}
		return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
	}
	return nil
}

// GetOriginalDefaultValue gets the origin default value.
func (c *ColumnInfo) GetOriginDefaultValue() interface{} {
	if c.Tp == mysql.TypeBit && c.OriginDefaultValueBit != nil {
		// If the column type is BIT, both `OriginDefaultValue` and `DefaultValue` of ColumnInfo are corrupted,
		// because the content before json.Marshal is INCONSISTENT with the content after json.Unmarshal.
		return string(c.OriginDefaultValueBit)
	}
	return c.OriginDefaultValue
}

// SetDefaultValue sets the default value.
func (c *ColumnInfo) SetDefaultValue(value interface{}) error {
	c.DefaultValue = value
	if c.Tp == mysql.TypeBit {
		// For mysql.TypeBit type, the default value storage format must be a string.
		// Other value such as int must convert to string format first.
		// The mysql.TypeBit type supports the null default value.
		if value == nil {
			return nil
		}
		if v, ok := value.(string); ok {
			c.DefaultValueBit = []byte(v)
			return nil
		}
		return types.ErrInvalidDefault.GenWithStackByArgs(c.Name)
	}
	return nil
}

// GetDefaultValue gets the default value of the column.
// Default value use to stored in DefaultValue field, but now,
// bit type default value will store in DefaultValueBit for fix bit default value decode/encode bug.
func (c *ColumnInfo) GetDefaultValue() interface{} {
	if c.Tp == mysql.TypeBit && c.DefaultValueBit != nil {
		return string(c.DefaultValueBit)
	}
	return c.DefaultValue
}

// GetTypeDesc gets the description for column type.
func (c *ColumnInfo) GetTypeDesc() string {
	desc := c.FieldType.CompactStr()
	if mysql.HasUnsignedFlag(c.Flag) && c.Tp != mysql.TypeBit && c.Tp != mysql.TypeYear {
		desc += " unsigned"
	}
	if mysql.HasZerofillFlag(c.Flag) && c.Tp != mysql.TypeYear {
		desc += " zerofill"
	}
	return desc
}

// FindColumnInfo finds ColumnInfo in cols by name.
func FindColumnInfo(cols []*ColumnInfo, name string) *ColumnInfo {
	name = strings.ToLower(name)
	for _, col := range cols {
		if col.Name.L == name {
			return col
		}
	}

	return nil
}

// ExtraHandleID is the column ID of column which we need to append to schema to occupy the handle's position
// for use of execution phase.
const ExtraHandleID = -1

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

	// CurrLatestTableInfoVersion means the latest table info in the current TiDB.
	CurrLatestTableInfoVersion = TableInfoVersion3
)

// ExtraHandleName is the name of ExtraHandle Column.
var ExtraHandleName = NewCIStr("_tidb_rowid")

// TableInfo provides meta data describing a DB table.
type TableInfo struct {
	ID      int64  `json:"id"`
	Name    CIStr  `json:"name"`
	Charset string `json:"charset"`
	Collate string `json:"collate"`
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

	Comment         string `json:"comment"`
	AutoIncID       int64  `json:"auto_inc_id"`
	AutoIdCache     int64  `json:"auto_id_cache"`
	AutoRandID      int64  `json:"auto_rand_id"`
	MaxColumnID     int64  `json:"max_col_id"`
	MaxIndexID      int64  `json:"max_idx_id"`
	MaxConstraintID int64  `json:"max_cst_id"`
	// UpdateTS is used to record the timestamp of updating the table's schema information.
	// These changing schema operations don't include 'truncate table' and 'rename table'.
	UpdateTS uint64 `json:"update_timestamp"`
	// OldSchemaID :
	// Because auto increment ID has schemaID as prefix,
	// We need to save original schemaID to keep autoID unchanged
	// while renaming a table from one database to another.
	// TODO: Remove it.
	// Now it only uses for compatibility with the old version that already uses this field.
	OldSchemaID int64 `json:"old_schema_id,omitempty"`

	// ShardRowIDBits specify if the implicit row ID is sharded.
	ShardRowIDBits uint64
	// MaxShardRowIDBits uses to record the max ShardRowIDBits be used so far.
	MaxShardRowIDBits uint64 `json:"max_shard_row_id_bits"`
	// AutoRandomBits is used to set the bit number to shard automatically when PKIsHandle.
	AutoRandomBits uint64 `json:"auto_random_bits"`
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
}

// TableLockInfo provides meta data describing a table lock.
type TableLockInfo struct {
	Tp TableLockType
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

func (s SessionInfo) String() string {
	return "server: " + s.ServerID + "_session: " + strconv.FormatUint(s.SessionID, 10)
}

// TableLockTpInfo is composed by schema ID, table ID and table lock type.
type TableLockTpInfo struct {
	SchemaID int64
	TableID  int64
	Tp       TableLockType
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

// TableLockType is the type of the table lock.
type TableLockType byte

const (
	TableLockNone TableLockType = iota
	// TableLockRead means the session with this lock can read the table (but not write it).
	// Multiple sessions can acquire a READ lock for the table at the same time.
	// Other sessions can read the table without explicitly acquiring a READ lock.
	TableLockRead
	// TableLockReadLocal is not supported.
	TableLockReadLocal
	// TableLockWrite means only the session with this lock has write/read permission.
	// Only the session that holds the lock can access the table. No other session can access it until the lock is released.
	TableLockWrite
	// TableLockWriteLocal means the session with this lock has write/read permission, and the other session still has read permission.
	TableLockWriteLocal
)

func (t TableLockType) String() string {
	switch t {
	case TableLockNone:
		return "NONE"
	case TableLockRead:
		return "READ"
	case TableLockReadLocal:
		return "READ LOCAL"
	case TableLockWriteLocal:
		return "WRITE LOCAL"
	case TableLockWrite:
		return "WRITE"
	}
	return ""
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

// GetDBID returns the schema ID that is used to create an allocator.
// TODO: Remove it after removing OldSchemaID.
func (t *TableInfo) GetDBID(dbID int64) int64 {
	if t.OldSchemaID != 0 {
		return t.OldSchemaID
	}
	return dbID
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

	return &nt
}

// GetPkName will return the pk name if pk exists.
func (t *TableInfo) GetPkName() CIStr {
	for _, colInfo := range t.Columns {
		if mysql.HasPriKeyFlag(colInfo.Flag) {
			return colInfo.Name
		}
	}
	return CIStr{}
}

// GetPkColInfo gets the ColumnInfo of pk if exists.
// Make sure PkIsHandle checked before call this method.
func (t *TableInfo) GetPkColInfo() *ColumnInfo {
	for _, colInfo := range t.Columns {
		if mysql.HasPriKeyFlag(colInfo.Flag) {
			return colInfo
		}
	}
	return nil
}

func (t *TableInfo) GetAutoIncrementColInfo() *ColumnInfo {
	for _, colInfo := range t.Columns {
		if mysql.HasAutoIncrementFlag(colInfo.Flag) {
			return colInfo
		}
	}
	return nil
}

func (t *TableInfo) IsAutoIncColUnsigned() bool {
	col := t.GetAutoIncrementColInfo()
	if col == nil {
		return false
	}
	return mysql.HasUnsignedFlag(col.Flag)
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
	return mysql.HasUnsignedFlag(t.GetPkColInfo().Flag)
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

// IsLocked checks whether the table was locked.
func (t *TableInfo) IsLocked() bool {
	return t.Lock != nil && len(t.Lock.Sessions) > 0
}

// NewExtraHandleColInfo mocks a column info for extra handle column.
func NewExtraHandleColInfo() *ColumnInfo {
	colInfo := &ColumnInfo{
		ID:   ExtraHandleID,
		Name: ExtraHandleName,
	}
	colInfo.Flag = mysql.PriKeyFlag
	colInfo.Tp = mysql.TypeLonglong
	colInfo.Flen, colInfo.Decimal = mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	return colInfo
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

// IsView checks if TableInfo is a view.
func (t *TableInfo) IsView() bool {
	return t.View != nil
}

// IsSequence checks if TableInfo is a sequence.
func (t *TableInfo) IsSequence() bool {
	return t.Sequence != nil
}

// ViewAlgorithm is VIEW's SQL AlGORITHM characteristic.
// See https://dev.mysql.com/doc/refman/5.7/en/view-algorithms.html
type ViewAlgorithm int

const (
	AlgorithmUndefined ViewAlgorithm = iota
	AlgorithmMerge
	AlgorithmTemptable
)

func (v *ViewAlgorithm) String() string {
	switch *v {
	case AlgorithmMerge:
		return "MERGE"
	case AlgorithmTemptable:
		return "TEMPTABLE"
	case AlgorithmUndefined:
		return "UNDEFINED"
	default:
		return "UNDEFINED"
	}
}

// ViewSecurity is VIEW's SQL SECURITY characteristic.
// See https://dev.mysql.com/doc/refman/5.7/en/create-view.html
type ViewSecurity int

const (
	SecurityDefiner ViewSecurity = iota
	SecurityInvoker
)

func (v *ViewSecurity) String() string {
	switch *v {
	case SecurityInvoker:
		return "INVOKER"
	case SecurityDefiner:
		return "DEFINER"
	default:
		return "DEFINER"
	}
}

// ViewCheckOption is VIEW's WITH CHECK OPTION clause part.
// See https://dev.mysql.com/doc/refman/5.7/en/view-check-option.html
type ViewCheckOption int

const (
	CheckOptionLocal ViewCheckOption = iota
	CheckOptionCascaded
)

func (v *ViewCheckOption) String() string {
	switch *v {
	case CheckOptionLocal:
		return "LOCAL"
	case CheckOptionCascaded:
		return "CASCADED"
	default:
		return "CASCADED"
	}
}

// ViewInfo provides meta data describing a DB view.
type ViewInfo struct {
	Algorithm   ViewAlgorithm      `json:"view_algorithm"`
	Definer     *auth.UserIdentity `json:"view_definer"`
	Security    ViewSecurity       `json:"view_security"`
	SelectStmt  string             `json:"view_select"`
	CheckOption ViewCheckOption    `json:"view_checkoption"`
	Cols        []CIStr            `json:"view_cols"`
}

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

// PartitionType is the type for PartitionInfo
type PartitionType int

// Partition types.
const (
	PartitionTypeRange      PartitionType = 1
	PartitionTypeHash                     = 2
	PartitionTypeList                     = 3
	PartitionTypeKey                      = 4
	PartitionTypeSystemTime               = 5
)

func (p PartitionType) String() string {
	switch p {
	case PartitionTypeRange:
		return "RANGE"
	case PartitionTypeHash:
		return "HASH"
	case PartitionTypeList:
		return "LIST"
	case PartitionTypeKey:
		return "KEY"
	case PartitionTypeSystemTime:
		return "SYSTEM_TIME"
	default:
		return ""
	}

}

// PartitionInfo provides table partition info.
type PartitionInfo struct {
	Type    PartitionType `json:"type"`
	Expr    string        `json:"expr"`
	Columns []CIStr       `json:"columns"`

	// User may already creates table with partition but table partition is not
	// yet supported back then. When Enable is true, write/read need use tid
	// rather than pid.
	Enable bool `json:"enable"`

	Definitions []PartitionDefinition `json:"definitions"`
	// AddingDefinitions is filled when adding a partition that is in the mid state.
	AddingDefinitions []PartitionDefinition `json:"adding_definitions"`
	Num               uint64                `json:"num"`
}

// GetNameByID gets the partition name by ID.
func (pi *PartitionInfo) GetNameByID(id int64) string {
	for _, def := range pi.Definitions {
		if id == def.ID {
			return def.Name.L
		}
	}
	return ""
}

// PartitionDefinition defines a single partition.
type PartitionDefinition struct {
	ID       int64    `json:"id"`
	Name     CIStr    `json:"name"`
	LessThan []string `json:"less_than"`
	Comment  string   `json:"comment,omitempty"`
}

// Clone clones ConstraintInfo.
func (ci *PartitionDefinition) Clone() PartitionDefinition {
	nci := *ci
	nci.LessThan = make([]string, len(ci.LessThan))
	copy(nci.LessThan, ci.LessThan)
	return nci
}

// FindPartitionDefinitionByName finds PartitionDefinition by name.
func (t *TableInfo) FindPartitionDefinitionByName(partitionDefinitionName string) *PartitionDefinition {
	lowConstrName := strings.ToLower(partitionDefinitionName)
	for i, pd := range t.Partition.Definitions {
		if pd.Name.L == lowConstrName {
			return &t.Partition.Definitions[i]
		}
	}
	return nil
}

// IndexColumn provides index column info.
type IndexColumn struct {
	Name   CIStr `json:"name"`   // Index name
	Offset int   `json:"offset"` // Index offset
	// Length of prefix when using column prefix
	// for indexing;
	// UnspecifedLength if not using prefix indexing
	Length int `json:"length"`
}

// Clone clones IndexColumn.
func (i *IndexColumn) Clone() *IndexColumn {
	ni := *i
	return &ni
}

// IndexType is the type of index
type IndexType int

// String implements Stringer interface.
func (t IndexType) String() string {
	switch t {
	case IndexTypeBtree:
		return "BTREE"
	case IndexTypeHash:
		return "HASH"
	case IndexTypeRtree:
		return "RTREE"
	default:
		return ""
	}
}

// IndexTypes
const (
	IndexTypeInvalid IndexType = iota
	IndexTypeBtree
	IndexTypeHash
	IndexTypeRtree
)

// IndexInfo provides meta data describing a DB index.
// It corresponds to the statement `CREATE INDEX Name ON Table (Column);`
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type IndexInfo struct {
	ID        int64          `json:"id"`
	Name      CIStr          `json:"idx_name"` // Index name.
	Table     CIStr          `json:"tbl_name"` // Table name.
	Columns   []*IndexColumn `json:"idx_cols"` // Index columns.
	State     SchemaState    `json:"state"`
	Comment   string         `json:"comment"`      // Comment
	Tp        IndexType      `json:"index_type"`   // Index type: Btree, Hash or Rtree
	Unique    bool           `json:"is_unique"`    // Whether the index is unique.
	Primary   bool           `json:"is_primary"`   // Whether the index is primary key.
	Invisible bool           `json:"is_invisible"` // Whether the index is invisible.
	Global    bool           `json:"is_global"`    // Whether the index is global.
}

// Clone clones IndexInfo.
func (index *IndexInfo) Clone() *IndexInfo {
	ni := *index
	ni.Columns = make([]*IndexColumn, len(index.Columns))
	for i := range index.Columns {
		ni.Columns[i] = index.Columns[i].Clone()
	}
	return &ni
}

// HasPrefixIndex returns whether any columns of this index uses prefix length.
func (index *IndexInfo) HasPrefixIndex() bool {
	for _, ic := range index.Columns {
		if ic.Length != types.UnspecifiedLength {
			return true
		}
	}
	return false
}

// ConstraintInfo provides meta data describing check-expression constraint.
type ConstraintInfo struct {
	ID             int64       `json:"id"`
	Name           CIStr       `json:"constraint_name"`
	Table          CIStr       `json:"tbl_name"`        // Table name.
	ConstraintCols []CIStr     `json:"constraint_cols"` // Depended column names.
	Enforced       bool        `json:"enforced"`
	InColumn       bool        `json:"in_column"` // Indicate whether the constraint is column type check.
	ExprString     string      `json:"expr_string"`
	State          SchemaState `json:"state"`
}

// Clone clones ConstraintInfo.
func (ci *ConstraintInfo) Clone() *ConstraintInfo {
	nci := *ci

	nci.ConstraintCols = make([]CIStr, len(ci.ConstraintCols))
	copy(nci.ConstraintCols, ci.ConstraintCols)
	return &nci
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

// FKInfo provides meta data describing a foreign key constraint.
type FKInfo struct {
	ID       int64       `json:"id"`
	Name     CIStr       `json:"fk_name"`
	RefTable CIStr       `json:"ref_table"`
	RefCols  []CIStr     `json:"ref_cols"`
	Cols     []CIStr     `json:"cols"`
	OnDelete int         `json:"on_delete"`
	OnUpdate int         `json:"on_update"`
	State    SchemaState `json:"state"`
}

// Clone clones FKInfo.
func (fk *FKInfo) Clone() *FKInfo {
	nfk := *fk

	nfk.RefCols = make([]CIStr, len(fk.RefCols))
	nfk.Cols = make([]CIStr, len(fk.Cols))
	copy(nfk.RefCols, fk.RefCols)
	copy(nfk.Cols, fk.Cols)

	return &nfk
}

// DBInfo provides meta data describing a DB.
type DBInfo struct {
	ID      int64        `json:"id"`      // Database ID
	Name    CIStr        `json:"db_name"` // DB name.
	Charset string       `json:"charset"`
	Collate string       `json:"collate"`
	Tables  []*TableInfo `json:"-"` // Tables in the DB.
	State   SchemaState  `json:"state"`
}

// Clone clones DBInfo.
func (db *DBInfo) Clone() *DBInfo {
	newInfo := *db
	newInfo.Tables = make([]*TableInfo, len(db.Tables))
	for i := range db.Tables {
		newInfo.Tables[i] = db.Tables[i].Clone()
	}
	return &newInfo
}

// Copy shallow copies DBInfo.
func (db *DBInfo) Copy() *DBInfo {
	newInfo := *db
	newInfo.Tables = make([]*TableInfo, len(db.Tables))
	copy(newInfo.Tables, db.Tables)
	return &newInfo
}

// CIStr is case insensitive string.
type CIStr struct {
	O string `json:"O"` // Original string.
	L string `json:"L"` // Lower case string.
}

// String implements fmt.Stringer interface.
func (cis CIStr) String() string {
	return cis.O
}

// NewCIStr creates a new CIStr.
func NewCIStr(s string) (cs CIStr) {
	cs.O = s
	cs.L = strings.ToLower(s)
	return
}

// UnmarshalJSON implements the user defined unmarshal method.
// CIStr can be unmarshaled from a single string, so PartitionDefinition.Name
// in this change https://github.com/pingcap/tidb/pull/6460/files would be
// compatible during TiDB upgrading.
func (cis *CIStr) UnmarshalJSON(b []byte) error {
	type T CIStr
	if err := json.Unmarshal(b, (*T)(cis)); err == nil {
		return nil
	}

	// Unmarshal CIStr from a single string.
	err := json.Unmarshal(b, &cis.O)
	if err != nil {
		return errors.Trace(err)
	}
	cis.L = strings.ToLower(cis.O)
	return nil
}

// TableColumnID is composed by table ID and column ID.
type TableColumnID struct {
	TableID  int64
	ColumnID int64
}
