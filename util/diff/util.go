// Copyright 2018 PingCAP, Inc.
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

package diff

import (
	"fmt"
	"strings"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

func equalStrings(str1, str2 []string) bool {
	if len(str1) != len(str2) {
		return false
	}
	for i := 0; i < len(str1); i++ {
		if str1[i] != str2[i] {
			return false
		}
	}
	return true
}

func ignoreColumns(tableInfo *model.TableInfo, columns []string) *model.TableInfo {
	if len(columns) == 0 {
		return tableInfo
	}

	removeColMap := utils.SliceToMap(columns)
	for i := 0; i < len(tableInfo.Indices); i++ {
		index := tableInfo.Indices[i]
		for j := 0; j < len(index.Columns); j++ {
			col := index.Columns[j]
			if _, ok := removeColMap[col.Name.O]; ok {
				tableInfo.Indices = append(tableInfo.Indices[:i], tableInfo.Indices[i+1:]...)
				i--
				break
			}
		}
	}

	for j := 0; j < len(tableInfo.Columns); j++ {
		col := tableInfo.Columns[j]
		if _, ok := removeColMap[col.Name.O]; ok {
			tableInfo.Columns = append(tableInfo.Columns[:j], tableInfo.Columns[j+1:]...)
			j--
		}
	}

	// calculate column offset
	colMap := make(map[string]int, len(tableInfo.Columns))
	for i, col := range tableInfo.Columns {
		col.Offset = i
		colMap[col.Name.O] = i
	}

	for _, index := range tableInfo.Indices {
		for _, col := range index.Columns {
			offset, ok := colMap[col.Name.O]
			if !ok {
				// this should never happened
				log.Fatal("column not exists", zap.String("column", col.Name.O))
			}
			col.Offset = offset
		}
	}

	return tableInfo
}

func getColumnsFromIndex(index *model.IndexInfo, tableInfo *model.TableInfo) []*model.ColumnInfo {
	indexColumns := make([]*model.ColumnInfo, 0, len(index.Columns))
	for _, indexColumn := range index.Columns {
		for _, column := range tableInfo.Columns {
			if column.Name.O == indexColumn.Name.O {
				indexColumns = append(indexColumns, column)
			}
		}
	}

	return indexColumns
}

func needQuotes(ft types.FieldType) bool {
	return !(dbutil.IsNumberType(ft.Tp) || dbutil.IsFloatType(ft.Tp))
}

func rowContainsCols(row map[string]*dbutil.ColumnData, cols []*model.ColumnInfo) bool {
	for _, col := range cols {
		if _, ok := row[col.Name.O]; !ok {
			return false
		}
	}

	return true
}

func rowToString(row map[string]*dbutil.ColumnData) string {
	var s strings.Builder
	s.WriteString("{ ")
	for key, val := range row {
		if val.IsNull {
			s.WriteString(fmt.Sprintf("%s: IsNull, ", key))
		} else {
			s.WriteString(fmt.Sprintf("%s: %s, ", key, val.Data))
		}
	}
	s.WriteString(" }")

	return s.String()
}

func stringSliceToInterfaceSlice(sli []string) []interface{} {
	iSli := make([]interface{}, len(sli))
	for i, item := range sli {
		iSli[i] = item
	}

	return iSli
}

func minLenInSlices(slices [][]string) int {
	min := 0
	for i, slice := range slices {
		if i == 0 || len(slice) < min {
			min = len(slice)
		}
	}

	return min
}
