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
	"fmt"

	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// SPFreshDistanceMetricAPIString returns the SPFresh worker API spelling for
// the persisted vector distance metric.
func SPFreshDistanceMetricAPIString(metric DistanceMetric) (string, bool) {
	switch metric {
	case DistanceMetricL2:
		return "l2", true
	case DistanceMetricCosine:
		return "cosine", true
	case DistanceMetricInnerProduct:
		return "inner_product", true
	default:
		return "", false
	}
}

// SPFreshHandleColumnInfo returns the integer handle column visible to SPFresh.
func SPFreshHandleColumnInfo(tblInfo *TableInfo) (int64, types.FieldType, bool) {
	if tblInfo == nil || tblInfo.IsCommonHandle {
		return 0, types.FieldType{}, false
	}
	if tblInfo.PKIsHandle {
		pkCol := tblInfo.GetPkColInfo()
		if pkCol == nil {
			return 0, types.FieldType{}, false
		}
		return pkCol.ID, pkCol.FieldType, true
	}
	return ExtraHandleID, NewExtraHandleColInfo().FieldType, true
}

// ResolveSPFreshStoredColumns returns the stored columns in their persisted order.
func ResolveSPFreshStoredColumns(tblInfo *TableInfo, idxInfo *IndexInfo) ([]*ColumnInfo, error) {
	if tblInfo == nil {
		return nil, fmt.Errorf("table info is not set")
	}
	if idxInfo == nil || !idxInfo.IsSPFreshIndex() {
		return nil, fmt.Errorf("index info is not a SPFresh vector index")
	}
	storedColumnIDs := idxInfo.VectorInfo.StoredColumnIDs
	columns := make([]*ColumnInfo, len(storedColumnIDs))
	columnsByID := make(map[int64]*ColumnInfo, len(storedColumnIDs))
	for _, columnID := range storedColumnIDs {
		columnsByID[columnID] = nil
	}
	for _, column := range tblInfo.Columns {
		if column != nil {
			if _, ok := columnsByID[column.ID]; ok {
				columnsByID[column.ID] = column
			}
		}
	}
	for i, columnID := range storedColumnIDs {
		column := columnsByID[columnID]
		if column == nil {
			return nil, fmt.Errorf("SPFresh index %s stored column %d not found", idxInfo.Name.O, columnID)
		}
		columns[i] = column
	}
	return columns, nil
}

// IsSPFreshIndex reports whether the index is a remote SPFresh vector index.
func (index *IndexInfo) IsSPFreshIndex() bool {
	return index.Tp == model.IndexTypeSPFresh &&
		index.VectorInfo != nil &&
		index.VectorInfo.Kind == VectorIndexKindSPFresh
}
