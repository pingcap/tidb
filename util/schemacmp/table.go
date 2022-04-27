// Copyright 2022 PingCAP, Inc.
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

package schemacmp

import (
	"sort"
	"strings"

	"github.com/pingcap/tidb/parser/format"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/types"
)

const (
	columnInfoTupleIndexDefaultValue = iota
	columnInfoTupleIndexGeneratedExprString
	columnInfoTupleIndexGeneratedStored
	columnInfoTupleIndexFieldTypes
)

// encodeColumnInfoToLattice collects the necessary information for comparing a column.
func encodeColumnInfoToLattice(ci *model.ColumnInfo) Tuple {
	return Tuple{
		MaybeSingletonInterface(ci.DefaultValue),
		Singleton(ci.GeneratedExprString),
		Singleton(ci.GeneratedStored),
		Type(&ci.FieldType),
	}
}

// restoreColumnInfoFromUnwrapped restores the text representation of a column.
func restoreColumnInfoFromUnwrapped(ctx *format.RestoreCtx, col []interface{}, colName string) {
	typ := col[columnInfoTupleIndexFieldTypes].(*types.FieldType)

	ctx.WriteName(colName)
	ctx.WritePlain(" ")
	_ = typ.Restore(ctx)
	if genExpr := col[columnInfoTupleIndexGeneratedExprString].(string); len(genExpr) != 0 {
		ctx.WriteKeyWord(" GENERATED ALWAYS AS ")
		ctx.WritePlainf("(%s)", genExpr)
	}
	if col[columnInfoTupleIndexGeneratedStored].(bool) {
		ctx.WriteKeyWord(" STORED")
	}
	if mysql.HasNotNullFlag(typ.GetFlag()) {
		ctx.WriteKeyWord(" NOT NULL")
	}
	if defVal := col[columnInfoTupleIndexDefaultValue]; defVal != nil {
		ctx.WriteKeyWord(" DEFAULT ")
		ctx.WritePlainf("%v", defVal)
	}
	if mysql.HasAutoIncrementFlag(typ.GetFlag()) {
		ctx.WriteKeyWord(" AUTO_INCREMENT")
	}
}

const (
	indexInfoTupleIndexColumns = iota
	indexInfoTupleIndexNotUnique
	indexInfoTupleIndexNotPrimary
	indexInfoTupleIndexType
)

type indexColumn struct {
	colName string
	length  int
}

type indexColumnSlice []indexColumn

// Equals implements Equality
func (a indexColumnSlice) Equals(other Equality) bool {
	b, ok := other.(indexColumnSlice)
	if !ok || len(a) != len(b) {
		return false
	}
	for i, av := range a {
		if av.colName != b[i].colName || av.length != b[i].length {
			return false
		}
	}
	return true
}

func encodeIndexInfoToLattice(ii *model.IndexInfo) Tuple {
	indexColumns := make(indexColumnSlice, 0, len(ii.Columns))
	for _, column := range ii.Columns {
		indexColumns = append(indexColumns, indexColumn{colName: column.Name.L, length: column.Length})
	}

	return Tuple{
		EqualitySingleton(indexColumns),
		Bool(!ii.Unique),
		Bool(!ii.Primary),
		Singleton(ii.Tp),
	}
}
func encodeImplicitPrimaryKeyToLattice(ci *model.ColumnInfo) Tuple {
	return Tuple{
		EqualitySingleton(indexColumnSlice{indexColumn{colName: ci.Name.L, length: types.UnspecifiedLength}}),
		Bool(false),
		Bool(false),
		Singleton(model.IndexTypeBtree),
	}
}

func restoreIndexInfoFromUnwrapped(ctx *format.RestoreCtx, index []interface{}, keyName string) {
	isPrimary := !index[indexInfoTupleIndexNotPrimary].(bool)

	switch {
	case isPrimary:
		ctx.WriteKeyWord("PRIMARY KEY")
	case !index[indexInfoTupleIndexNotUnique].(bool):
		ctx.WriteKeyWord("UNIQUE KEY ")
		ctx.WriteName(keyName)
	default:
		ctx.WriteKeyWord("KEY ")
		ctx.WriteName(keyName)
	}

	if tp := index[indexInfoTupleIndexType].(model.IndexType); tp != model.IndexTypeBtree {
		ctx.WriteKeyWord(" USING ")
		ctx.WriteKeyWord(tp.String())
	}

	ctx.WritePlain(" (")
	for i, column := range index[indexInfoTupleIndexColumns].(indexColumnSlice) {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		ctx.WriteName(column.colName)
		if column.length != types.UnspecifiedLength {
			ctx.WritePlainf("(%d)", column.length)
		}
	}
	ctx.WritePlain(")")
}

type columnMap map[string]Tuple

func (columnMap) New() LatticeMap {
	return make(columnMap)
}

func (a columnMap) Get(key string) Lattice {
	val, ok := a[key]
	if !ok {
		return nil
	}
	return val
}

func (a columnMap) Insert(key string, value Lattice) {
	a[key] = value.(Tuple)
}

func (a columnMap) ForEach(f func(key string, value Lattice) error) error {
	for key, value := range a {
		if err := f(key, value); err != nil {
			return err
		}
	}
	return nil
}

func (columnMap) CompareWithNil(value Lattice) (int, error) {
	if value.(Tuple)[columnInfoTupleIndexFieldTypes].(typ).hasDefault() {
		return 1, nil
	}
	return 0, &IncompatibleError{Msg: "column with no default value cannot be missing"}
}

func (columnMap) ShouldDeleteIncompatibleJoin() bool {
	return false
}

func (columnMap) JoinWithNil(value Lattice) (Lattice, error) {
	col := append(make(Tuple, 0), value.(Tuple)...)
	ty := col[columnInfoTupleIndexFieldTypes].(typ).clone()
	if ty.setFlagForMissingColumn() && ty.isNotNull() {
		col[columnInfoTupleIndexDefaultValue] = Maybe(Singleton(ty.getStandardDefaultValue()))
	}
	col[columnInfoTupleIndexFieldTypes] = ty
	return col, nil
}

type indexMap map[string]Tuple

func (indexMap) New() LatticeMap {
	return make(indexMap)
}

func (a indexMap) Get(key string) Lattice {
	val, ok := a[key]
	if !ok {
		return nil
	}
	return val
}

func (a indexMap) Insert(key string, value Lattice) {
	a[key] = value.(Tuple)
}

func (a indexMap) ForEach(f func(key string, value Lattice) error) error {
	for key, value := range a {
		if err := f(key, value); err != nil {
			return err
		}
	}
	return nil
}

func (indexMap) CompareWithNil(value Lattice) (int, error) {
	return -1, nil
}

func (indexMap) ShouldDeleteIncompatibleJoin() bool {
	return true
}

func (indexMap) JoinWithNil(value Lattice) (Lattice, error) {
	return nil, nil
}

const (
	tableInfoTupleIndexCharset = iota
	tableInfoTupleIndexCollate
	tableInfoTupleIndexColumns
	tableInfoTupleIndexIndices
	// nolint:unused, varcheck, deadcode
	tableInfoTupleIndexAutoIncID
	tableInfoTupleIndexShardRowIDBits
	tableInfoTupleIndexAutoRandomBits
	// nolint: unused, varcheck, deadcode
	tableInfoTupleIndexPreSplitRegions
	tableInfoTupleIndexCompression
)

func encodeTableInfoToLattice(ti *model.TableInfo) Tuple {
	// TODO: Handle VIEW and PARTITION and SEQUENCE
	hasExplicitPrimaryKey := false
	indices := make(indexMap)
	for _, ii := range ti.Indices {
		if ii.Primary {
			hasExplicitPrimaryKey = true
		}
		indices[ii.Name.L] = encodeIndexInfoToLattice(ii)
	}
	columns := make(columnMap)
	for _, ci := range ti.Columns {
		columns[ci.Name.L] = encodeColumnInfoToLattice(ci)
		if !hasExplicitPrimaryKey && (ci.GetFlag()&mysql.PriKeyFlag) != 0 {
			indices["primary"] = encodeImplicitPrimaryKeyToLattice(ci)
		}
	}

	return Tuple{
		Singleton(ti.Charset),
		Singleton(ti.Collate),
		Map(columns),
		Map(indices),
		// TODO ForeignKeys?
		Int64(ti.AutoIncID),
		// TODO Relax these?
		Singleton(ti.ShardRowIDBits),
		Singleton(ti.AutoRandomBits),
		Singleton(ti.PreSplitRegions),
		MaybeSingletonString(ti.Compression),
	}
}

type sortedMapSlice []struct {
	key   string
	value interface{}
}

func (sl sortedMapSlice) Len() int {
	return len(sl)
}

func (sl sortedMapSlice) Less(i, j int) bool {
	return sl[i].key < sl[j].key
}

func (sl sortedMapSlice) Swap(i, j int) {
	sl[i], sl[j] = sl[j], sl[i]
}

func sortedMap(input map[string]interface{}) sortedMapSlice {
	res := make(sortedMapSlice, 0, len(input))
	for key, value := range input {
		res = append(res, struct {
			key   string
			value interface{}
		}{key: key, value: value})
	}
	sort.Sort(res)
	return res
}

func restoreTableInfoFromUnwrapped(ctx *format.RestoreCtx, table []interface{}, tableName string) {
	ctx.WriteKeyWord("CREATE TABLE ")
	ctx.WriteName(tableName)
	ctx.WritePlain("(")

	for i, pair := range sortedMap(table[tableInfoTupleIndexColumns].(map[string]interface{})) {
		if i != 0 {
			ctx.WritePlain(", ")
		}
		colName := pair.key
		column := pair.value.([]interface{})
		restoreColumnInfoFromUnwrapped(ctx, column, colName)
	}

	for _, pair := range sortedMap(table[tableInfoTupleIndexIndices].(map[string]interface{})) {
		ctx.WritePlain(", ")
		indexName := pair.key
		index := pair.value.([]interface{})
		restoreIndexInfoFromUnwrapped(ctx, index, indexName)
	}

	ctx.WritePlain(")")
	if charset := table[tableInfoTupleIndexCharset].(string); charset != "" {
		ctx.WriteKeyWord(" CHARSET ")
		ctx.WriteKeyWord(charset)
	}
	if collate := table[tableInfoTupleIndexCollate].(string); collate != "" {
		ctx.WriteKeyWord(" COLLATE ")
		ctx.WriteKeyWord(collate)
	}
	if bits := table[tableInfoTupleIndexShardRowIDBits].(uint64); bits > 0 {
		ctx.WriteKeyWord(" SHARD_ROW_ID_BITS ")
		ctx.WritePlainf("%d", bits)
	}
	if bits := table[tableInfoTupleIndexAutoRandomBits].(uint64); bits > 0 {
		ctx.WritePlain("/*")
		ctx.WriteKeyWord(" AUTO_RANDOM_BITS ")
		ctx.WritePlainf("%d */", bits)
	}
	if compression, ok := table[tableInfoTupleIndexCompression].(string); ok && len(compression) != 0 {
		ctx.WriteKeyWord(" COMPRESSION ")
		ctx.WriteString(compression)
	}
}

// Table is a table in the database.
type Table struct{ value Lattice }

// Encode is used to encode a Table.
func Encode(ti *model.TableInfo) Table {
	return Table{value: encodeTableInfoToLattice(ti)}
}

// DecodeColumnFieldTypes is used to decode column field types from Lattice.
func DecodeColumnFieldTypes(t Table) map[string]*types.FieldType {
	table := t.value.Unwrap().([]interface{})
	columnMaps := table[tableInfoTupleIndexColumns].(map[string]interface{})
	cols := make(map[string]*types.FieldType, len(columnMaps))
	for key, value := range columnMaps {
		cols[key] = value.([]interface{})[columnInfoTupleIndexFieldTypes].(*types.FieldType)
	}
	return cols
}

// Restore is for debug use only.
func (t Table) Restore(ctx *format.RestoreCtx, tableName string) {
	restoreTableInfoFromUnwrapped(ctx, t.value.Unwrap().([]interface{}), tableName)
}

// Compare is the implementation of Lattice interface.
func (t Table) Compare(other Table) (int, error) {
	return t.value.Compare(other.value)
}

// Join is a helper function to join two tables.
func (t Table) Join(other Table) (Table, error) {
	res, err := t.value.Join(other.value)
	if err != nil {
		return Table{value: nil}, err
	}

	// fix up the type's key flags.
	// unfortunately we cannot count on the type's own flag joining
	// because an index's joining rule is more complex than 3 bits.
	columnKeyFlags := make(map[string]uint)
	table := res.(Tuple)
	for _, index := range table[tableInfoTupleIndexIndices].(latticeMap).LatticeMap.(indexMap) {
		cols := index[indexInfoTupleIndexColumns].Unwrap().(indexColumnSlice)
		if len(cols) == 0 {
			continue
		}
		switch {
		case !index[indexInfoTupleIndexNotPrimary].Unwrap().(bool):
			for _, col := range cols {
				columnKeyFlags[col.colName] |= mysql.PriKeyFlag
			}
		case !index[indexInfoTupleIndexNotUnique].Unwrap().(bool) && len(cols) == 1:
			columnKeyFlags[cols[0].colName] |= mysql.UniqueKeyFlag
		default:
			// Only the first column can be set if index or unique index has multiple columns.
			// See https://dev.mysql.com/doc/refman/5.7/en/show-columns.html.
			columnKeyFlags[cols[0].colName] |= mysql.MultipleKeyFlag
		}
	}
	columns := table[tableInfoTupleIndexColumns].(latticeMap).LatticeMap.(columnMap)
	for name, column := range columns {
		ty := column[columnInfoTupleIndexFieldTypes].(typ)
		flag, ok := columnKeyFlags[name]
		if !ok && ty.inAutoIncrement() {
			return Table{value: nil}, &IncompatibleError{
				Msg:  ErrMsgAtMapKey,
				Args: []interface{}{name, &IncompatibleError{Msg: ErrMsgAutoTypeWithoutKey}},
			}
		}
		ty.setAntiKeyFlags(flag)
	}

	return Table{value: table}, nil
}

func (t Table) String() string {
	var sb strings.Builder
	ctx := format.NewRestoreCtx(format.DefaultRestoreFlags, &sb)
	t.Restore(ctx, "tbl")
	return sb.String()
}
