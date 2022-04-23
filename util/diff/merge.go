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
	"strconv"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/parser/model"
	"go.uber.org/zap"
)

// RowData is the struct of rows selected from mysql/tidb
type RowData struct {
	Data   map[string]*dbutil.ColumnData
	Source int
}

// RowDatas is a heap of MergeItems.
type RowDatas struct {
	Rows         []RowData
	OrderKeyCols []*model.ColumnInfo
}

func (r RowDatas) Len() int { return len(r.Rows) }
func (r RowDatas) Less(i, j int) bool {
	for _, col := range r.OrderKeyCols {
		col1, ok := r.Rows[i].Data[col.Name.O]
		if !ok {
			log.Fatal("data don't have column", zap.String("column", col.Name.O), zap.Reflect("data", r.Rows[i].Data))
		}
		col2, ok := r.Rows[j].Data[col.Name.O]
		if !ok {
			log.Fatal("data don't have column", zap.String("column", col.Name.O), zap.Reflect("data", r.Rows[j].Data))
		}

		if col1.IsNull {
			if col2.IsNull {
				continue
			}

			return true
		}
		if col2.IsNull {
			return false
		}

		strData1 := string(col1.Data)
		strData2 := string(col2.Data)

		if needQuotes(col.FieldType) {
			if strData1 == strData2 {
				continue
			}
			if strData1 > strData2 {
				return false
			}
			return true
		}

		num1, err1 := strconv.ParseFloat(strData1, 64)
		if err1 != nil {
			log.Fatal("convert string to float failed", zap.String("column", col.Name.O), zap.String("data", strData1), zap.Error(err1))
		}
		num2, err2 := strconv.ParseFloat(strData2, 64)
		if err2 != nil {
			log.Fatal("convert string to float failed", zap.String("column", col.Name.O), zap.String("data", strData2), zap.Error(err2))
		}

		if num1 == num2 {
			continue
		}
		if num1 > num2 {
			return false
		}
		return true

	}

	return false
}
func (r RowDatas) Swap(i, j int) { r.Rows[i], r.Rows[j] = r.Rows[j], r.Rows[i] }

// Push implements heap.Interface's Push function
func (r *RowDatas) Push(x interface{}) {
	r.Rows = append(r.Rows, x.(RowData))
}

// Pop implements heap.Interface's Pop function
func (r *RowDatas) Pop() interface{} {
	if len(r.Rows) == 0 {
		return nil
	}
	old := r.Rows
	n := len(old)
	x := old[n-1]
	r.Rows = old[0 : n-1]
	return x
}
