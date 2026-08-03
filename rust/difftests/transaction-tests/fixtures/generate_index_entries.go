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

//go:build ignore

// Go-authoritative index KEY and VALUE bytes for the entries a live write
// stores. A round-trip test cannot catch what these catch: an index key is
// built from new-collation SORT KEYS, which are lossy, and the entry VALUE is
// where Go puts the restored data that makes the original bytes recoverable.
// A Rust writer that stores its own (self-consistent) value passes every
// self-round-trip and still hands a Go reader -- an index-only scan, ADMIN
// CHECK INDEX, a DDL backfill -- case-folded or space-stripped data.
//
// Emits, for each case, the exact `tablecodec.GenIndexKey` /
// `tablecodec.GenIndexValuePortal` output, with `needRestoredData` and the
// handle's restored data computed the way `pkg/table/tables/index.go` and
// `tables.TryGetHandleRestoredDataWrapper` compute them.
package main

import (
	"fmt"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/table/tables"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/codec"
)

const tableID = 77

func main() {
	uniqueGeneralCi()
	nonUniqueGeneralCi()
	uniqueUtf8mb4BinTrailingSpace()
	shortCommonHandleNonUnique()
	commonHandleVersion1RestoredHandle()
}

func varchar(collate string) *types.FieldType {
	ft := types.NewFieldType(mysql.TypeVarchar)
	ft.SetFlen(32)
	ft.SetCharset("utf8mb4")
	ft.SetCollate(collate)
	return ft
}

func column(id int64, offset int, name string, ft *types.FieldType) *model.ColumnInfo {
	return &model.ColumnInfo{
		ID:        id,
		Name:      ast.NewCIStr(name),
		Offset:    offset,
		FieldType: *ft,
		State:     model.StatePublic,
	}
}

func index(id int64, name string, unique bool, cols ...*model.IndexColumn) *model.IndexInfo {
	return &model.IndexInfo{
		ID:      id,
		Name:    ast.NewCIStr(name),
		Unique:  unique,
		Columns: cols,
		State:   model.StatePublic,
	}
}

func indexColumn(name string, offset int) *model.IndexColumn {
	return &model.IndexColumn{
		Name:   ast.NewCIStr(name),
		Offset: offset,
		Length: types.UnspecifiedLength,
	}
}

// emit prints the key and value one write produces, with the same
// needRestoredData / handleRestoredData decisions the live Go writer makes.
func emit(name string, tbl *model.TableInfo, idx *model.IndexInfo, row []types.Datum, h kv.Handle) {
	indexed := make([]types.Datum, 0, len(idx.Columns))
	for _, idxCol := range idx.Columns {
		indexed = append(indexed, row[idxCol.Offset])
	}
	key, distinct, err := tablecodec.GenIndexKey(codec.NewEncoder(true), time.UTC, tbl, idx, tableID, indexed, h, nil)
	if err != nil {
		panic(err)
	}
	needRestored := tables.NeedRestoredData(true, idx.Columns, tbl.Columns)
	var handleRestored []types.Datum
	if tbl.IsCommonHandle && tbl.CommonHandleVersion != 0 {
		pkIdx := tables.FindPrimaryIndex(tbl)
		for _, pkIdxCol := range pkIdx.Columns {
			pkCol := tbl.Columns[pkIdxCol.Offset]
			if !types.NeedRestoredDataWithCollate(&pkCol.FieldType, true) {
				continue
			}
			datum := row[pkCol.Offset]
			tables.TryTruncateRestoredData(&datum, pkCol, pkIdxCol, idx)
			tables.ConvertDatumToTailSpaceCount(&datum, pkCol)
			handleRestored = append(handleRestored, datum)
		}
	}
	value, err := tablecodec.GenIndexValuePortal(true, time.UTC, tbl, idx, needRestored,
		distinct, false, indexed, h, 0, handleRestored, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s_distinct=%v\n", name, distinct)
	fmt.Printf("%s_key=%x\n", name, key)
	fmt.Printf("%s_value=%x\n", name, value)
}

// A row-ID table with one indexed VARCHAR column.
func rowIDTable(collate string, unique bool) (*model.TableInfo, *model.IndexInfo) {
	col := column(2, 0, "s", varchar(collate))
	idx := index(1, "idx", unique, indexColumn("s", 0))
	tbl := &model.TableInfo{
		ID:      tableID,
		Name:    ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{col},
		Indices: []*model.IndexInfo{idx},
		State:   model.StatePublic,
	}
	return tbl, idx
}

// A case-insensitive collation's sort key case-folds, so the index KEY holds
// `a` where the row holds `A`: the value must restore the original.
func uniqueGeneralCi() {
	tbl, idx := rowIDTable("utf8mb4_general_ci", true)
	emit("unique_general_ci_A", tbl, idx, []types.Datum{types.NewCollationStringDatum("A", "utf8mb4_general_ci")}, kv.IntHandle(1))
}

// The same column with a NON-unique index: the entry is non-distinct, so the
// handle moves into the key and the value is the v0-extensible restored-data
// form rather than the single `0` byte a restore-free index stores.
func nonUniqueGeneralCi() {
	tbl, idx := rowIDTable("utf8mb4_general_ci", false)
	emit("non_unique_general_ci_A", tbl, idx, []types.Datum{types.NewCollationStringDatum("A", "utf8mb4_general_ci")}, kv.IntHandle(1))
}

// `utf8mb4_bin` is a bin collation: its sort key TRIMS trailing spaces, and
// the restored data is their COUNT rather than the string.
func uniqueUtf8mb4BinTrailingSpace() {
	tbl, idx := rowIDTable("utf8mb4_bin", true)
	emit("unique_utf8mb4_bin_a_space", tbl, idx, []types.Datum{types.NewCollationStringDatum("a ", "utf8mb4_bin")}, kv.IntHandle(1))
}

// A clustered DECIMAL primary key encodes to four bytes; `kv.NewCommonHandle`
// pads it to nine, and the non-distinct entry's key carries the PADDED form.
func shortCommonHandleNonUnique() {
	decimalType := types.NewFieldType(mysql.TypeNewDecimal)
	decimalType.SetFlen(4)
	decimalType.SetDecimal(0)
	decimalType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	pk := column(1, 0, "pk", decimalType)
	v := types.NewFieldType(mysql.TypeLonglong)
	value := column(2, 1, "v", v)
	pkIdx := index(1, "primary", true, indexColumn("pk", 0))
	pkIdx.Primary = true
	idx := index(2, "idx", false, indexColumn("v", 1))
	tbl := &model.TableInfo{
		ID:                  tableID,
		Name:                ast.NewCIStr("t"),
		Columns:             []*model.ColumnInfo{pk, value},
		Indices:             []*model.IndexInfo{pkIdx, idx},
		IsCommonHandle:      true,
		CommonHandleVersion: 1,
		State:               model.StatePublic,
	}
	dec := new(types.MyDecimal)
	if err := dec.FromString([]byte("5")); err != nil {
		panic(err)
	}
	encoded, err := codec.EncodeKey(time.UTC, nil, types.NewDecimalDatum(dec))
	if err != nil {
		panic(err)
	}
	h, err := kv.NewCommonHandle(encoded)
	if err != nil {
		panic(err)
	}
	fmt.Printf("short_common_handle_encoded=%x\n", h.Encoded())
	emit("short_common_handle_non_unique", tbl, idx, []types.Datum{types.NewDecimalDatum(dec), types.NewIntDatum(42)}, h)
}

// A clustered VARCHAR primary key under CommonHandleVersion 1: the secondary
// index value carries the PRIMARY KEY's restored data as well as its own.
func commonHandleVersion1RestoredHandle() {
	pkType := varchar("utf8mb4_general_ci")
	pkType.AddFlag(mysql.PriKeyFlag | mysql.NotNullFlag)
	pk := column(1, 0, "pk", pkType)
	value := column(2, 1, "s", varchar("utf8mb4_general_ci"))
	pkIdx := index(1, "primary", true, indexColumn("pk", 0))
	pkIdx.Primary = true
	idx := index(2, "idx", false, indexColumn("s", 1))
	tbl := &model.TableInfo{
		ID:                  tableID,
		Name:                ast.NewCIStr("t"),
		Columns:             []*model.ColumnInfo{pk, value},
		Indices:             []*model.IndexInfo{pkIdx, idx},
		IsCommonHandle:      true,
		CommonHandleVersion: 1,
		State:               model.StatePublic,
	}
	pkDatum := types.NewCollationStringDatum("Key", "utf8mb4_general_ci")
	encoded, err := codec.EncodeKey(time.UTC, nil, pkDatum)
	if err != nil {
		panic(err)
	}
	h, err := kv.NewCommonHandle(encoded)
	if err != nil {
		panic(err)
	}
	emit("common_handle_v1_restored", tbl, idx,
		[]types.Datum{pkDatum, types.NewCollationStringDatum("Val", "utf8mb4_general_ci")}, h)
}
