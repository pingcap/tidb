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
	"strings"
	"unsafe"

	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
)

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
	Name                  model.CIStr `json:"name"`
	Offset                int         `json:"offset"`
	OriginDefaultValue    any         `json:"origin_default"`
	OriginDefaultValueBit []byte      `json:"origin_default_bit"`
	DefaultValue          any         `json:"default"`
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
func (c *ColumnInfo) SetOriginDefaultValue(value any) error {
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
func (c *ColumnInfo) GetOriginDefaultValue() any {
	if c.GetType() == mysql.TypeBit && c.OriginDefaultValueBit != nil {
		// If the column type is BIT, both `OriginDefaultValue` and `DefaultValue` of ColumnInfo are corrupted,
		// because the content before json.Marshal is INCONSISTENT with the content after json.Unmarshal.
		return string(c.OriginDefaultValueBit)
	}
	return c.OriginDefaultValue
}

// SetDefaultValue sets the default value.
func (c *ColumnInfo) SetDefaultValue(value any) error {
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
func (c *ColumnInfo) GetDefaultValue() any {
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

// NewExtraPhysTblIDColInfo mocks a column info for extra partition id column.
func NewExtraPhysTblIDColInfo() *ColumnInfo {
	colInfo := &ColumnInfo{
		ID:   ExtraPhysTblID,
		Name: ExtraPhysTblIDName,
	}
	colInfo.SetType(mysql.TypeLonglong)
	colInfo.SetFlag(mysql.NotNullFlag)
	flen, decimal := mysql.GetDefaultFieldLengthAndDecimal(mysql.TypeLonglong)
	colInfo.SetFlen(flen)
	colInfo.SetDecimal(decimal)
	colInfo.SetCharset(charset.CharsetBin)
	colInfo.SetCollate(charset.CollationBin)
	return colInfo
}
