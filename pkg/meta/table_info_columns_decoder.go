// Copyright 2026 PingCAP, Inc.
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
	"strings"
	"unsafe"

	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tidb/pkg/util/hack"
	"github.com/tidwall/gjson"
)

const maxColumnsDecodeInternedStrings = 256

// tryDecodeSimpleColumnsTableInfo handles the common base-table metadata shape
// without reflection. Strings returned by gjson borrow data, so only field
// names used during parsing are borrowed; every string retained by TableInfo is
// either cloned or interned for the lifetime of this iterator.
func (i *TableInfoIterator) tryDecodeSimpleColumnsTableInfo(data []byte, tableInfo *model.TableInfo) bool {
	if !gjson.ValidBytes(data) {
		return false
	}
	root := gjson.Parse(string(hack.String(data)))
	if !root.IsObject() {
		return false
	}

	columns := resetColumnsForJSONDecode(tableInfo.Columns)
	*tableInfo = model.TableInfo{Columns: columns}
	seenID, seenName, seenColumns, seenState := false, false, false, false
	simple := true
	root.ForEach(func(key, value gjson.Result) bool {
		switch key.Str {
		case "id":
			if value.Type != gjson.Number {
				simple = false
				return false
			}
			tableInfo.ID = value.Int()
			seenID = true
		case "name":
			name, ok := i.cloneColumnsDecodeCIStr(value)
			if !ok {
				simple = false
				return false
			}
			tableInfo.Name = name
			seenName = true
		case "charset":
			if value.Type != gjson.String {
				simple = false
				return false
			}
			tableInfo.Charset = i.internColumnsDecodeString(value.Str)
		case "collate":
			if value.Type != gjson.String {
				simple = false
				return false
			}
			tableInfo.Collate = i.internColumnsDecodeString(value.Str)
		case "cols":
			if !value.IsArray() || !i.decodeSimpleColumns(value, tableInfo) {
				simple = false
				return false
			}
			seenColumns = true
		case "state":
			if value.Type != gjson.Number {
				simple = false
				return false
			}
			tableInfo.State = model.SchemaState(value.Int())
			seenState = true
		case "view":
			if value.Raw != "null" {
				simple = false
				return false
			}
		}
		return true
	})
	return simple && seenID && seenName && seenColumns && seenState
}

func (i *TableInfoIterator) decodeSimpleColumns(value gjson.Result, tableInfo *model.TableInfo) bool {
	columnIndex := 0
	simple := true
	value.ForEach(func(_, columnValue gjson.Result) bool {
		if !columnValue.IsObject() {
			simple = false
			return false
		}
		var column *model.ColumnInfo
		if columnIndex < cap(tableInfo.Columns) {
			tableInfo.Columns = tableInfo.Columns[:columnIndex+1]
			column = tableInfo.Columns[columnIndex]
			if column == nil {
				column = &model.ColumnInfo{}
				tableInfo.Columns[columnIndex] = column
			}
		} else {
			column = &model.ColumnInfo{}
			tableInfo.Columns = append(tableInfo.Columns, column)
		}
		if !i.decodeSimpleColumnInfo(columnValue, column) {
			simple = false
			return false
		}
		columnIndex++
		return true
	})
	if simple {
		tableInfo.Columns = tableInfo.Columns[:columnIndex]
	}
	return simple
}

func (i *TableInfoIterator) decodeSimpleColumnInfo(value gjson.Result, column *model.ColumnInfo) bool {
	*column = model.ColumnInfo{}
	seenID, seenName, seenOffset, seenType, seenState := false, false, false, false, false
	simple := true
	value.ForEach(func(key, field gjson.Result) bool {
		switch key.Str {
		case "id":
			if field.Type != gjson.Number {
				simple = false
				return false
			}
			column.ID = field.Int()
			seenID = true
		case "name":
			name, ok := i.cloneColumnsDecodeCIStr(field)
			if !ok {
				simple = false
				return false
			}
			column.Name = name
			seenName = true
		case "offset":
			if field.Type != gjson.Number {
				simple = false
				return false
			}
			column.Offset = int(field.Int())
			seenOffset = true
		case "default":
			if field.Raw != "null" {
				simple = false
				return false
			}
		case "default_bit":
			if field.Raw != "null" {
				simple = false
				return false
			}
		case "default_is_expr":
			column.DefaultIsExpr = field.Bool()
		case "generated_expr_string":
			if field.Type != gjson.String {
				simple = false
				return false
			}
			column.GeneratedExprString = i.cloneColumnsDecodeString(field.Str)
		case "generated_stored":
			column.GeneratedStored = field.Bool()
		case "type":
			if !i.decodeSimpleFieldType(field, &column.FieldType) {
				simple = false
				return false
			}
			seenType = true
		case "state":
			if field.Type != gjson.Number {
				simple = false
				return false
			}
			column.State = model.SchemaState(field.Int())
			seenState = true
		case "comment":
			if field.Type != gjson.String {
				simple = false
				return false
			}
			column.Comment = i.cloneColumnsDecodeString(field.Str)
		case "hidden":
			column.Hidden = field.Bool()
		case "version":
			if field.Type != gjson.Number {
				simple = false
				return false
			}
			column.Version = field.Uint()
		}
		return true
	})
	return simple && seenID && seenName && seenOffset && seenType && seenState
}

func (i *TableInfoIterator) decodeSimpleFieldType(value gjson.Result, fieldType *types.FieldType) bool {
	if !value.IsObject() {
		return false
	}
	var tp byte
	var flag uint
	var flen, decimal int
	var charset, collate string
	simple := true
	value.ForEach(func(key, field gjson.Result) bool {
		switch key.Str {
		case "Tp":
			if field.Type != gjson.Number {
				simple = false
				return false
			}
			tp = byte(field.Uint())
		case "Flag":
			if field.Type != gjson.Number {
				simple = false
				return false
			}
			flag = uint(field.Uint())
		case "Flen":
			if field.Type != gjson.Number {
				simple = false
				return false
			}
			flen = int(field.Int())
		case "Decimal":
			if field.Type != gjson.Number {
				simple = false
				return false
			}
			decimal = int(field.Int())
		case "Charset":
			if field.Type != gjson.String {
				simple = false
				return false
			}
			charset = i.internColumnsDecodeString(field.Str)
		case "Collate":
			if field.Type != gjson.String {
				simple = false
				return false
			}
			collate = i.internColumnsDecodeString(field.Str)
		case "Elems", "ElemsIsBinaryLit":
			if field.Raw != "null" && field.Raw != "[]" {
				simple = false
				return false
			}
		case "Array":
			if field.Bool() {
				simple = false
				return false
			}
		}
		return true
	})
	if !simple {
		return false
	}
	*fieldType = types.FieldType{}
	fieldType.SetType(tp)
	fieldType.SetFlag(flag)
	fieldType.SetFlen(flen)
	fieldType.SetDecimal(decimal)
	fieldType.SetCharset(charset)
	fieldType.SetCollate(collate)
	return true
}

func (i *TableInfoIterator) cloneColumnsDecodeCIStr(value gjson.Result) (ast.CIStr, bool) {
	if !value.IsObject() {
		return ast.CIStr{}, false
	}
	var original, lower string
	seenOriginal, seenLower, valid := false, false, true
	value.ForEach(func(key, field gjson.Result) bool {
		if field.Type != gjson.String {
			valid = false
			return false
		}
		switch key.Str {
		case "O":
			original = field.Str
			seenOriginal = true
		case "L":
			lower = field.Str
			seenLower = true
		}
		return true
	})
	if !valid || !seenOriginal || !seenLower {
		return ast.CIStr{}, false
	}
	if original == lower {
		owned := i.cloneColumnsDecodeString(original)
		return ast.CIStr{O: owned, L: owned}, true
	}
	return ast.CIStr{
		O: i.cloneColumnsDecodeString(original),
		L: i.cloneColumnsDecodeString(lower),
	}, true
}

func (i *TableInfoIterator) cloneColumnsDecodeString(value string) string {
	if value == "" {
		return ""
	}
	if i.stats != nil {
		i.stats.ColumnStringCloneAllocatedBytes += uint64(len(value))
	}
	return strings.Clone(value)
}

func (i *TableInfoIterator) internColumnsDecodeString(value string) string {
	if value == "" {
		return ""
	}
	for _, existing := range i.internedColumnStrings {
		if existing == value {
			return existing
		}
	}
	if i.stats != nil {
		i.stats.ColumnStringCloneAllocatedBytes += uint64(len(value))
	}
	owned := strings.Clone(value)
	if len(i.internedColumnStrings) < maxColumnsDecodeInternedStrings {
		i.internedColumnStrings = append(i.internedColumnStrings, owned)
	}
	return owned
}

func (i *TableInfoIterator) columnsDecodeRetainedMemory() int64 {
	usage := int64(cap(i.internedColumnStrings)) * int64(unsafe.Sizeof(""))
	for _, value := range i.internedColumnStrings {
		usage += int64(len(value))
	}
	return usage
}
