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
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/duration"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
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
	// StateReplicaOnly means we're waiting tiflash replica to be finished.
	StateReplicaOnly
	// StateGlobalTxnOnly means we can only use global txn for operator on this schema element
	StateGlobalTxnOnly
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
	case StateGlobalTxnOnly:
		return "global txn only"
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
	FieldType           types.FieldType     `json:"type"`
	State               SchemaState         `json:"state"`
	Comment             string              `json:"comment"`
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

// IsVirtualGenerated checks the column if it is virtual.
func (c *ColumnInfo) IsVirtualGenerated() bool {
	return c.IsGenerated() && !c.GeneratedStored
}

// Clone clones ColumnInfo.
func (c *ColumnInfo) Clone() *ColumnInfo {
	if c == nil {
		return nil
	}
	nc := *c
	return &nc
}

// GetType returns the type of ColumnInfo.
func (c *ColumnInfo) GetType() byte {
	return c.FieldType.GetType()
}

// GetFlag returns the flag of ColumnInfo.
func (c *ColumnInfo) GetFlag() uint {
	return c.FieldType.GetFlag()
}

// GetFlen returns the flen of ColumnInfo.
func (c *ColumnInfo) GetFlen() int {
	return c.FieldType.GetFlen()
}

// GetDecimal returns the decimal of ColumnInfo.
func (c *ColumnInfo) GetDecimal() int {
	return c.FieldType.GetDecimal()
}

// GetCharset returns the charset of ColumnInfo.
func (c *ColumnInfo) GetCharset() string {
	return c.FieldType.GetCharset()
}

// GetCollate returns the collation of ColumnInfo.
func (c *ColumnInfo) GetCollate() string {
	return c.FieldType.GetCollate()
}

// GetElems returns the elems of ColumnInfo.
func (c *ColumnInfo) GetElems() []string {
	return c.FieldType.GetElems()
}

// SetType set the type of ColumnInfo.
func (c *ColumnInfo) SetType(tp byte) {
	c.FieldType.SetType(tp)
}

// SetFlag set the flag of ColumnInfo.
func (c *ColumnInfo) SetFlag(flag uint) {
	c.FieldType.SetFlag(flag)
}

// AddFlag adds the flag of ColumnInfo.
func (c *ColumnInfo) AddFlag(flag uint) {
	c.FieldType.AddFlag(flag)
}

// AndFlag adds a flag to the column.
func (c *ColumnInfo) AndFlag(flag uint) {
	c.FieldType.AndFlag(flag)
}

// ToggleFlag flips the flag according to the value.
func (c *ColumnInfo) ToggleFlag(flag uint) {
	c.FieldType.ToggleFlag(flag)
}

// DelFlag removes the flag from the column's flag.
func (c *ColumnInfo) DelFlag(flag uint) {
	c.FieldType.DelFlag(flag)
}

// SetFlen sets the flen of ColumnInfo.
func (c *ColumnInfo) SetFlen(flen int) {
	c.FieldType.SetFlen(flen)
}

// SetDecimal sets the decimal of ColumnInfo.
func (c *ColumnInfo) SetDecimal(decimal int) {
	c.FieldType.SetDecimal(decimal)
}

// SetCharset sets charset of the ColumnInfo
func (c *ColumnInfo) SetCharset(charset string) {
	c.FieldType.SetCharset(charset)
}

// SetCollate sets the collation of the column.
func (c *ColumnInfo) SetCollate(collate string) {
	c.FieldType.SetCollate(collate)
}

// SetElems set the elements of enum column.
func (c *ColumnInfo) SetElems(elems []string) {
	c.FieldType.SetElems(elems)
}

// IsGenerated returns true if the column is generated column.
func (c *ColumnInfo) IsGenerated() bool {
	return len(c.GeneratedExprString) != 0
}

// SetOriginDefaultValue sets the origin default value.
// For mysql.TypeBit type, the default value storage format must be a string.
// Other value such as int must convert to string format first.
// The mysql.TypeBit type supports the null default value.
func (c *ColumnInfo) SetOriginDefaultValue(value interface{}) error {
	c.OriginDefaultValue = value
	if c.GetType() == mysql.TypeBit {
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

// GetOriginDefaultValue gets the origin default value.
func (c *ColumnInfo) GetOriginDefaultValue() interface{} {
	if c.GetType() == mysql.TypeBit && c.OriginDefaultValueBit != nil {
		// If the column type is BIT, both `OriginDefaultValue` and `DefaultValue` of ColumnInfo are corrupted,
		// because the content before json.Marshal is INCONSISTENT with the content after json.Unmarshal.
		return string(c.OriginDefaultValueBit)
	}
	return c.OriginDefaultValue
}

// SetDefaultValue sets the default value.
func (c *ColumnInfo) SetDefaultValue(value interface{}) error {
	c.DefaultValue = value
	if c.GetType() == mysql.TypeBit {
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
	if c.GetType() == mysql.TypeBit && c.DefaultValueBit != nil {
		return string(c.DefaultValueBit)
	}
	return c.DefaultValue
}

// GetTypeDesc gets the description for column type.
func (c *ColumnInfo) GetTypeDesc() string {
	desc := c.FieldType.CompactStr()
	if mysql.HasUnsignedFlag(c.GetFlag()) && c.GetType() != mysql.TypeBit && c.GetType() != mysql.TypeYear {
		desc += " unsigned"
	}
	if mysql.HasZerofillFlag(c.GetFlag()) && c.GetType() != mysql.TypeYear {
		desc += " zerofill"
	}
	return desc
}

// EmptyColumnInfoSize is the memory usage of ColumnInfoSize
const EmptyColumnInfoSize = int64(unsafe.Sizeof(ColumnInfo{}))

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

// FindColumnInfoByID finds ColumnInfo in cols by id.
func FindColumnInfoByID(cols []*ColumnInfo, id int64) *ColumnInfo {
	for _, col := range cols {
		if col.ID == id {
			return col
		}
	}
	return nil
}

// FindIndexInfoByID finds IndexInfo in indices by id.
func FindIndexInfoByID(indices []*IndexInfo, id int64) *IndexInfo {
	for _, idx := range indices {
		if idx.ID == id {
			return idx
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

// FindIndexByColumns find IndexInfo in indices which is cover the specified columns.
func FindIndexByColumns(tbInfo *TableInfo, indices []*IndexInfo, cols ...CIStr) *IndexInfo {
	for _, index := range indices {
		if IsIndexPrefixCovered(tbInfo, index, cols...) {
			return index
		}
	}
	return nil
}

// IsIndexPrefixCovered checks the index's columns beginning with the cols.
func IsIndexPrefixCovered(tbInfo *TableInfo, index *IndexInfo, cols ...CIStr) bool {
	if len(index.Columns) < len(cols) {
		return false
	}
	for i := range cols {
		if cols[i].L != index.Columns[i].Name.L ||
			index.Columns[i].Offset >= len(tbInfo.Columns) {
			return false
		}
		colInfo := tbInfo.Columns[index.Columns[i].Offset]
		if index.Columns[i].Length != types.UnspecifiedLength && index.Columns[i].Length < colInfo.GetFlen() {
			return false
		}
	}
	return true
}

// ExtraHandleID is the column ID of column which we need to append to schema to occupy the handle's position
// for use of execution phase.
const ExtraHandleID = -1

// ExtraPidColID is the column ID of column which store the partitionID decoded in global index values.
const ExtraPidColID = -2

// ExtraPhysTblID is the column ID of column that should be filled in with the physical table id.
// Primarily used for table partition dynamic prune mode, to return which partition (physical table id) the row came from.
// Using a dedicated id for this, since in the future ExtraPidColID and ExtraPhysTblID may be used for the same request.
// Must be after ExtraPidColID!
const ExtraPhysTblID = -3

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
var ExtraHandleName = NewCIStr("_tidb_rowid")

// ExtraPartitionIdName is the name of ExtraPartitionId Column.
var ExtraPartitionIdName = NewCIStr("_tidb_pid") //nolint:revive

// ExtraPhysTblIdName is the name of ExtraPhysTblID Column.
var ExtraPhysTblIdName = NewCIStr("_tidb_tid") //nolint:revive

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

	AutoIdCache     int64 `json:"auto_id_cache"` //nolint:revive
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

// TableNameInfo provides meta data describing a table name info.
type TableNameInfo struct {
	ID   int64 `json:"id"`
	Name CIStr `json:"name"`
}

// SepAutoInc decides whether _rowid and auto_increment id use separate allocator.
func (t *TableInfo) SepAutoInc() bool {
	return t.Version >= TableInfoVersion5 && t.AutoIdCache == 1
}

// TableCacheStatusType is the type of the table cache status
type TableCacheStatusType int

//revive:disable:exported
const (
	TableCacheStatusDisable TableCacheStatusType = iota
	TableCacheStatusEnable
	TableCacheStatusSwitching
)

//revive:enable:exported

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

//revive:disable:exported
const (
	TempTableNone TempTableType = iota
	TempTableGlobal
	TempTableLocal
)

//revive:enable:exported

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
	// TableLockNone means this table lock is absent.
	TableLockNone TableLockType = iota
	// TableLockRead means the session with this lock can read the table (but not write it).
	// Multiple sessions can acquire a READ lock for the table at the same time.
	// Other sessions can read the table without explicitly acquiring a READ lock.
	TableLockRead
	// TableLockReadLocal is not supported.
	TableLockReadLocal
	// TableLockReadOnly is used to set a table into read-only status,
	// when the session exits, it will not release its lock automatically.
	TableLockReadOnly
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
	case TableLockReadOnly:
		return "READ ONLY"
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

// GetAutoIDSchemaID returns the schema ID that was used to create an allocator.
func (t *TableInfo) GetAutoIDSchemaID(dbID int64) int64 {
	if t.AutoIDSchemaID != 0 {
		return t.AutoIDSchemaID
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

	if t.Partition != nil {
		nt.Partition = t.Partition.Clone()
	}
	if t.TTLInfo != nil {
		nt.TTLInfo = t.TTLInfo.Clone()
	}

	return &nt
}

// GetPkName will return the pk name if pk exists.
func (t *TableInfo) GetPkName() CIStr {
	for _, colInfo := range t.Columns {
		if mysql.HasPriKeyFlag(colInfo.GetFlag()) {
			return colInfo.Name
		}
	}
	return CIStr{}
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

// NewExtraHandleColInfo mocks a column info for extra handle column.
func NewExtraHandleColInfo() *ColumnInfo {
	colInfo := &ColumnInfo{
		ID:   ExtraHandleID,
		Name: ExtraHandleName,
	}

	colInfo.SetFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	colInfo.SetType(mysql.TypeLonglong)

	flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	colInfo.SetFlen(flen)
	colInfo.SetDecimal(decimal)

	colInfo.SetCharset(charset.CharsetBin)
	colInfo.SetCollate(charset.CollationBin)
	return colInfo
}

// NewExtraPartitionIDColInfo mocks a column info for extra partition id column.
func NewExtraPartitionIDColInfo() *ColumnInfo {
	colInfo := &ColumnInfo{
		ID:   ExtraPidColID,
		Name: ExtraPartitionIdName,
	}
	colInfo.SetType(mysql.TypeLonglong)
	flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	colInfo.SetFlen(flen)
	colInfo.SetDecimal(decimal)
	colInfo.SetCharset(charset.CharsetBin)
	colInfo.SetCollate(charset.CollationBin)
	return colInfo
}

// NewExtraPhysTblIDColInfo mocks a column info for extra partition id column.
func NewExtraPhysTblIDColInfo() *ColumnInfo {
	colInfo := &ColumnInfo{
		ID:   ExtraPhysTblID,
		Name: ExtraPhysTblIdName,
	}
	colInfo.SetType(mysql.TypeLonglong)
	flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	colInfo.SetFlen(flen)
	colInfo.SetDecimal(decimal)
	colInfo.SetCharset(charset.CharsetBin)
	colInfo.SetCollate(charset.CollationBin)
	return colInfo
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

// ViewAlgorithm is VIEW's SQL ALGORITHM characteristic.
// See https://dev.mysql.com/doc/refman/5.7/en/view-algorithms.html
type ViewAlgorithm int

//revive:disable:exported
const (
	AlgorithmUndefined ViewAlgorithm = iota
	AlgorithmMerge
	AlgorithmTemptable
)

//revive:enable:exported

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

//revive:disable:exported
const (
	SecurityDefiner ViewSecurity = iota
	SecurityInvoker
)

//revive:enable:exported

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

//revive:disable:exported
const (
	CheckOptionLocal ViewCheckOption = iota
	CheckOptionCascaded
)

//revive:enable:exported

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
//
//revive:disable:exported
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

//revive:enable:exported

// PartitionType is the type for PartitionInfo
type PartitionType int

// Partition types.
const (
	// Actually non-partitioned, but during DDL keeping the table as
	// a single partition
	PartitionTypeNone PartitionType = 0

	PartitionTypeRange      PartitionType = 1
	PartitionTypeHash       PartitionType = 2
	PartitionTypeList       PartitionType = 3
	PartitionTypeKey        PartitionType = 4
	PartitionTypeSystemTime PartitionType = 5
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
	case PartitionTypeNone:
		return "NONE"
	default:
		return ""
	}
}

// ExchangePartitionInfo provides exchange partition info.
type ExchangePartitionInfo struct {
	// It is nt tableID when table which has the info is a partition table, else pt tableID.
	ExchangePartitionTableID int64 `json:"exchange_partition_id"`
	ExchangePartitionDefID   int64 `json:"exchange_partition_def_id"`
	// Deprecated, not used
	XXXExchangePartitionFlag bool `json:"exchange_partition_flag"`
}

// PartitionInfo provides table partition info.
type PartitionInfo struct {
	Type    PartitionType `json:"type"`
	Expr    string        `json:"expr"`
	Columns []CIStr       `json:"columns"`

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
	DDLType    PartitionType `json:"ddl_type"`
	DDLExpr    string        `json:"ddl_expr"`
	DDLColumns []CIStr       `json:"ddl_columns"`
}

// Clone clones itself.
func (pi *PartitionInfo) Clone() *PartitionInfo {
	newPi := *pi
	newPi.Columns = make([]CIStr, len(pi.Columns))
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
	pi.DDLType = PartitionTypeNone
	pi.DDLExpr = ""
	pi.DDLColumns = nil
	pi.NewTableID = 0
}

// PartitionState is the state of the partition.
type PartitionState struct {
	ID    int64       `json:"id"`
	State SchemaState `json:"state"`
}

// PartitionDefinition defines a single partition.
type PartitionDefinition struct {
	ID                 int64          `json:"id"`
	Name               CIStr          `json:"name"`
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

// PrimaryKeyType is the type of primary key.
// Available values are "clustered", "nonclustered", and ""(default).
type PrimaryKeyType int8

func (p PrimaryKeyType) String() string {
	switch p {
	case PrimaryKeyTypeClustered:
		return "CLUSTERED"
	case PrimaryKeyTypeNonClustered:
		return "NONCLUSTERED"
	default:
		return ""
	}
}

//revive:disable:exported
const (
	PrimaryKeyTypeDefault PrimaryKeyType = iota
	PrimaryKeyTypeClustered
	PrimaryKeyTypeNonClustered
)

//revive:enable:exported

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
	case IndexTypeHypo:
		return "HYPO"
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
	IndexTypeHypo
)

// IndexInfo provides meta data describing a DB index.
// It corresponds to the statement `CREATE INDEX Name ON Table (Column);`
// See https://dev.mysql.com/doc/refman/5.7/en/create-index.html
type IndexInfo struct {
	ID            int64          `json:"id"`
	Name          CIStr          `json:"idx_name"` // Index name.
	Table         CIStr          `json:"tbl_name"` // Table name.
	Columns       []*IndexColumn `json:"idx_cols"` // Index columns.
	State         SchemaState    `json:"state"`
	BackfillState BackfillState  `json:"backfill_state"`
	Comment       string         `json:"comment"`      // Comment
	Tp            IndexType      `json:"index_type"`   // Index type: Btree, Hash or Rtree
	Unique        bool           `json:"is_unique"`    // Whether the index is unique.
	Primary       bool           `json:"is_primary"`   // Whether the index is primary key.
	Invisible     bool           `json:"is_invisible"` // Whether the index is invisible.
	Global        bool           `json:"is_global"`    // Whether the index is global.
	MVIndex       bool           `json:"mv_index"`     // Whether the index is multivalued index.
}

// Clone clones IndexInfo.
func (index *IndexInfo) Clone() *IndexInfo {
	if index == nil {
		return nil
	}
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

// HasColumnInIndexColumns checks whether the index contains the column with the specified ID.
func (index *IndexInfo) HasColumnInIndexColumns(tblInfo *TableInfo, colID int64) bool {
	for _, ic := range index.Columns {
		if tblInfo.Columns[ic.Offset].ID == colID {
			return true
		}
	}
	return false
}

// FindColumnByName finds the index column with the specified name.
func (index *IndexInfo) FindColumnByName(nameL string) *IndexColumn {
	_, ret := FindIndexColumnByName(index.Columns, nameL)
	return ret
}

// IsPublic checks if the index state is public
func (index *IndexInfo) IsPublic() bool {
	return index.State == StatePublic
}

// FindIndexColumnByName finds IndexColumn by name. When IndexColumn is not found, returns (-1, nil).
func FindIndexColumnByName(indexCols []*IndexColumn, nameL string) (int, *IndexColumn) {
	for i, ic := range indexCols {
		if ic.Name.L == nameL {
			return i, ic
		}
	}
	return -1, nil
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

// FKInfo provides meta data describing a foreign key constraint.
type FKInfo struct {
	ID        int64       `json:"id"`
	Name      CIStr       `json:"fk_name"`
	RefSchema CIStr       `json:"ref_schema"`
	RefTable  CIStr       `json:"ref_table"`
	RefCols   []CIStr     `json:"ref_cols"`
	Cols      []CIStr     `json:"cols"`
	OnDelete  int         `json:"on_delete"`
	OnUpdate  int         `json:"on_update"`
	State     SchemaState `json:"state"`
	Version   int         `json:"version"`
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
	Cols        []CIStr `json:"cols"`
	ChildSchema CIStr   `json:"child_schema"`
	ChildTable  CIStr   `json:"child_table"`
	ChildFKName CIStr   `json:"child_fk_name"`
}

// ReferOptionType is the type for refer options.
type ReferOptionType int

// Refer option types.
const (
	ReferOptionNoOption ReferOptionType = iota
	ReferOptionRestrict
	ReferOptionCascade
	ReferOptionSetNull
	ReferOptionNoAction
	ReferOptionSetDefault
)

// String implements fmt.Stringer interface.
func (r ReferOptionType) String() string {
	switch r {
	case ReferOptionRestrict:
		return "RESTRICT"
	case ReferOptionCascade:
		return "CASCADE"
	case ReferOptionSetNull:
		return "SET NULL"
	case ReferOptionNoAction:
		return "NO ACTION"
	case ReferOptionSetDefault:
		return "SET DEFAULT"
	}
	return ""
}

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
	if onDelete := ReferOptionType(fk.OnDelete); onDelete != ReferOptionNoOption {
		buf.WriteString(" ON DELETE ")
		buf.WriteString(onDelete.String())
	}
	if onUpdate := ReferOptionType(fk.OnUpdate); onUpdate != ReferOptionNoOption {
		buf.WriteString(" ON UPDATE ")
		buf.WriteString(onUpdate.String())
	}
	return buf.String()
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
	ID                 int64          `json:"id"`      // Database ID
	Name               CIStr          `json:"db_name"` // DB name.
	Charset            string         `json:"charset"`
	Collate            string         `json:"collate"`
	Tables             []*TableInfo   `json:"-"` // Tables in the DB.
	State              SchemaState    `json:"state"`
	PlacementPolicyRef *PolicyRefInfo `json:"policy_ref_info"`
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

// LessDBInfo is used for sorting DBInfo by DBInfo.Name.
func LessDBInfo(a *DBInfo, b *DBInfo) int {
	return strings.Compare(a.Name.L, b.Name.L)
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

// MemoryUsage return the memory usage of CIStr
func (cis *CIStr) MemoryUsage() (sum int64) {
	if cis == nil {
		return
	}

	return int64(unsafe.Sizeof(cis.O))*2 + int64(len(cis.O)+len(cis.L))
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

// PolicyRefInfo is the struct to refer the placement policy.
type PolicyRefInfo struct {
	ID   int64 `json:"id"`
	Name CIStr `json:"name"`
}

// PlacementSettings is the settings of the placement
type PlacementSettings struct {
	PrimaryRegion       string `json:"primary_region"`
	Regions             string `json:"regions"`
	Learners            uint64 `json:"learners"`
	Followers           uint64 `json:"followers"`
	Voters              uint64 `json:"voters"`
	Schedule            string `json:"schedule"`
	Constraints         string `json:"constraints"`
	LeaderConstraints   string `json:"leader_constraints"`
	LearnerConstraints  string `json:"learner_constraints"`
	FollowerConstraints string `json:"follower_constraints"`
	VoterConstraints    string `json:"voter_constraints"`
	SurvivalPreferences string `json:"survival_preferences"`
}

// PolicyInfo is the struct to store the placement policy.
type PolicyInfo struct {
	*PlacementSettings
	ID    int64       `json:"id"`
	Name  CIStr       `json:"name"`
	State SchemaState `json:"state"`
}

// Clone clones PolicyInfo.
func (p *PolicyInfo) Clone() *PolicyInfo {
	cloned := *p
	cloned.PlacementSettings = p.PlacementSettings.Clone()
	return &cloned
}

// DefaultJobInterval sets the default interval between TTL jobs
const DefaultJobInterval = time.Hour

// TTLInfo records the TTL config
type TTLInfo struct {
	ColumnName      CIStr  `json:"column"`
	IntervalExprStr string `json:"interval_expr"`
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

func writeSettingItemToBuilder(sb *strings.Builder, item string, separatorFns ...func()) {
	if sb.Len() != 0 {
		for _, fn := range separatorFns {
			fn()
		}
		if len(separatorFns) == 0 {
			sb.WriteString(" ")
		}
	}
	sb.WriteString(item)
}
func writeSettingStringToBuilder(sb *strings.Builder, item string, value string, separatorFns ...func()) {
	writeSettingItemToBuilder(sb, fmt.Sprintf("%s=\"%s\"", item, strings.ReplaceAll(value, "\"", "\\\"")), separatorFns...)
}
func writeSettingIntegerToBuilder(sb *strings.Builder, item string, value uint64, separatorFns ...func()) {
	writeSettingItemToBuilder(sb, fmt.Sprintf("%s=%d", item, value), separatorFns...)
}

func writeSettingDurationToBuilder(sb *strings.Builder, item string, dur time.Duration, separatorFns ...func()) {
	writeSettingStringToBuilder(sb, item, dur.String(), separatorFns...)
}

func (p *PlacementSettings) String() string {
	sb := new(strings.Builder)
	if len(p.PrimaryRegion) > 0 {
		writeSettingStringToBuilder(sb, "PRIMARY_REGION", p.PrimaryRegion)
	}

	if len(p.Regions) > 0 {
		writeSettingStringToBuilder(sb, "REGIONS", p.Regions)
	}

	if len(p.Schedule) > 0 {
		writeSettingStringToBuilder(sb, "SCHEDULE", p.Schedule)
	}

	if len(p.Constraints) > 0 {
		writeSettingStringToBuilder(sb, "CONSTRAINTS", p.Constraints)
	}

	if len(p.LeaderConstraints) > 0 {
		writeSettingStringToBuilder(sb, "LEADER_CONSTRAINTS", p.LeaderConstraints)
	}

	if p.Voters > 0 {
		writeSettingIntegerToBuilder(sb, "VOTERS", p.Voters)
	}

	if len(p.VoterConstraints) > 0 {
		writeSettingStringToBuilder(sb, "VOTER_CONSTRAINTS", p.VoterConstraints)
	}

	if p.Followers > 0 {
		writeSettingIntegerToBuilder(sb, "FOLLOWERS", p.Followers)
	}

	if len(p.FollowerConstraints) > 0 {
		writeSettingStringToBuilder(sb, "FOLLOWER_CONSTRAINTS", p.FollowerConstraints)
	}

	if p.Learners > 0 {
		writeSettingIntegerToBuilder(sb, "LEARNERS", p.Learners)
	}

	if len(p.LearnerConstraints) > 0 {
		writeSettingStringToBuilder(sb, "LEARNER_CONSTRAINTS", p.LearnerConstraints)
	}

	if len(p.SurvivalPreferences) > 0 {
		writeSettingStringToBuilder(sb, "SURVIVAL_PREFERENCES", p.SurvivalPreferences)
	}

	return sb.String()
}

// Clone clones the placement settings.
func (p *PlacementSettings) Clone() *PlacementSettings {
	cloned := *p
	return &cloned
}

// RunawayActionType is the type of runaway action.
type RunawayActionType int32

//revive:disable:exported
const (
	RunawayActionNone RunawayActionType = iota
	RunawayActionDryRun
	RunawayActionCooldown
	RunawayActionKill
)

// RunawayWatchType is the type of runaway watch.
type RunawayWatchType int32

//revive:disable:exported
const (
	WatchNone RunawayWatchType = iota
	WatchExact
	WatchSimilar
	WatchPlan
)

func (t RunawayWatchType) String() string {
	switch t {
	case WatchExact:
		return "EXACT"
	case WatchSimilar:
		return "SIMILAR"
	case WatchPlan:
		return "PLAN"
	default:
		return "NONE"
	}
}

// RunawayOptionType is the runaway's option type.
type RunawayOptionType int

//revive:disable:exported
const (
	RunawayRule RunawayOptionType = iota
	RunawayAction
	RunawayWatch
)

func (t RunawayActionType) String() string {
	switch t {
	case RunawayActionDryRun:
		return "DRYRUN"
	case RunawayActionCooldown:
		return "COOLDOWN"
	case RunawayActionKill:
		return "KILL"
	default:
		return "DRYRUN"
	}
}

// ResourceGroupRefInfo is the struct to refer the resource group.
type ResourceGroupRefInfo struct {
	ID   int64 `json:"id"`
	Name CIStr `json:"name"`
}

// ResourceGroupRunawaySettings is the runaway settings of the resource group
type ResourceGroupRunawaySettings struct {
	ExecElapsedTimeMs uint64            `json:"exec_elapsed_time_ms"`
	Action            RunawayActionType `json:"action"`
	WatchType         RunawayWatchType  `json:"watch_type"`
	WatchDurationMs   int64             `json:"watch_duration_ms"`
}

type ResourceGroupBackgroundSettings struct {
	JobTypes []string `json:"job_types"`
}

// ResourceGroupSettings is the settings of the resource group
type ResourceGroupSettings struct {
	RURate           uint64                           `json:"ru_per_sec"`
	Priority         uint64                           `json:"priority"`
	CPULimiter       string                           `json:"cpu_limit"`
	IOReadBandwidth  string                           `json:"io_read_bandwidth"`
	IOWriteBandwidth string                           `json:"io_write_bandwidth"`
	BurstLimit       int64                            `json:"burst_limit"`
	Runaway          *ResourceGroupRunawaySettings    `json:"runaway"`
	Background       *ResourceGroupBackgroundSettings `json:"background"`
}

// NewResourceGroupSettings creates a new ResourceGroupSettings.
func NewResourceGroupSettings() *ResourceGroupSettings {
	return &ResourceGroupSettings{
		RURate:           0,
		Priority:         MediumPriorityValue,
		CPULimiter:       "",
		IOReadBandwidth:  "",
		IOWriteBandwidth: "",
		BurstLimit:       0,
	}
}

// PriorityValueToName converts the priority value to corresponding name
func PriorityValueToName(value uint64) string {
	switch value {
	case LowPriorityValue:
		return "LOW"
	case MediumPriorityValue:
		return "MEDIUM"
	case HighPriorityValue:
		return "HIGH"
	default:
		return "MEDIUM"
	}
}

//revive:disable:exported
const (
	LowPriorityValue    = 1
	MediumPriorityValue = 8
	HighPriorityValue   = 16
)

func (p *ResourceGroupSettings) String() string {
	sb := new(strings.Builder)
	separatorFn := func() {
		sb.WriteString(", ")
	}
	if p.RURate != 0 {
		writeSettingIntegerToBuilder(sb, "RU_PER_SEC", p.RURate, separatorFn)
	}
	writeSettingItemToBuilder(sb, "PRIORITY="+PriorityValueToName(p.Priority), separatorFn)
	if len(p.CPULimiter) > 0 {
		writeSettingStringToBuilder(sb, "CPU", p.CPULimiter, separatorFn)
	}
	if len(p.IOReadBandwidth) > 0 {
		writeSettingStringToBuilder(sb, "IO_READ_BANDWIDTH", p.IOReadBandwidth, separatorFn)
	}
	if len(p.IOWriteBandwidth) > 0 {
		writeSettingStringToBuilder(sb, "IO_WRITE_BANDWIDTH", p.IOWriteBandwidth, separatorFn)
	}
	// Once burst limit is negative, meaning allow burst with unlimit.
	if p.BurstLimit < 0 {
		writeSettingItemToBuilder(sb, "BURSTABLE", separatorFn)
	}
	if p.Runaway != nil {
		writeSettingDurationToBuilder(sb, "QUERY_LIMIT=(EXEC_ELAPSED", time.Duration(p.Runaway.ExecElapsedTimeMs)*time.Millisecond, separatorFn)
		writeSettingItemToBuilder(sb, "ACTION="+p.Runaway.Action.String())
		if p.Runaway.WatchType != WatchNone {
			writeSettingItemToBuilder(sb, "WATCH="+p.Runaway.WatchType.String())
			if p.Runaway.WatchDurationMs > 0 {
				writeSettingDurationToBuilder(sb, "DURATION", time.Duration(p.Runaway.WatchDurationMs)*time.Millisecond)
			} else {
				writeSettingItemToBuilder(sb, "DURATION=UNLIMITED")
			}
		}
		sb.WriteString(")")
	}
	if p.Background != nil {
		fmt.Fprintf(sb, ", BACKGROUND=(TASK_TYPES='%s')", strings.Join(p.Background.JobTypes, ","))
	}

	return sb.String()
}

// Adjust adjusts the resource group settings.
func (p *ResourceGroupSettings) Adjust() {
	// Curretly we only support ru_per_sec sytanx, so BurstLimit(capicity) is always same as ru_per_sec except burstable.
	if p.BurstLimit >= 0 {
		p.BurstLimit = int64(p.RURate)
	}
}

// Clone clones the resource group settings.
func (p *ResourceGroupSettings) Clone() *ResourceGroupSettings {
	cloned := *p
	return &cloned
}

// ResourceGroupInfo is the struct to store the resource group.
type ResourceGroupInfo struct {
	*ResourceGroupSettings
	ID    int64       `json:"id"`
	Name  CIStr       `json:"name"`
	State SchemaState `json:"state"`
}

// Clone clones the ResourceGroupInfo.
func (p *ResourceGroupInfo) Clone() *ResourceGroupInfo {
	cloned := *p
	cloned.ResourceGroupSettings = p.ResourceGroupSettings.Clone()
	return &cloned
}

// StatsOptions is the struct to store the stats options.
type StatsOptions struct {
	*StatsWindowSettings
	AutoRecalc   bool         `json:"auto_recalc"`
	ColumnChoice ColumnChoice `json:"column_choice"`
	ColumnList   []CIStr      `json:"column_list"`
	SampleNum    uint64       `json:"sample_num"`
	SampleRate   float64      `json:"sample_rate"`
	Buckets      uint64       `json:"buckets"`
	TopN         uint64       `json:"topn"`
	Concurrency  uint         `json:"concurrency"`
}

// NewStatsOptions creates a new StatsOptions.
func NewStatsOptions() *StatsOptions {
	return &StatsOptions{
		AutoRecalc:   true,
		ColumnChoice: DefaultChoice,
		ColumnList:   []CIStr{},
		SampleNum:    uint64(0),
		SampleRate:   0.0,
		Buckets:      uint64(0),
		TopN:         uint64(0),
		Concurrency:  uint(0),
	}
}

// ColumnChoice is the type of the column choice.
type ColumnChoice byte

//revive:disable:exported
const (
	DefaultChoice ColumnChoice = iota
	AllColumns
	PredicateColumns
	ColumnList
)

//revive:enable:exported

func (s ColumnChoice) String() string {
	switch s {
	case AllColumns:
		return "ALL"
	case PredicateColumns:
		return "PREDICATE"
	case ColumnList:
		return "LIST"
	default:
		return "DEFAULT"
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

//revive:disable:exported
const (
	Never WindowRepeatType = iota
	Day
	Week
	Month
)

//revive:enable:exported

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

// TraceInfo is the information for trace.
type TraceInfo struct {
	// ConnectionID is the id of the connection
	ConnectionID uint64 `json:"connection_id"`
	// SessionAlias is the alias of session
	SessionAlias string `json:"session_alias"`
}
