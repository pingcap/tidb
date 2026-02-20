// Copyright 2017 PingCAP, Inc.
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

package statistics

import (
	"slices"

	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
)

// ID2UniqueID generates a new HistColl whose `Columns` is built from UniqueID of given columns.
func (coll *HistColl) ID2UniqueID(columns []*expression.Column) *HistColl {
	cols := make(map[int64]*Column)
	for _, col := range columns {
		colHist, ok := coll.columns[col.ID]
		if ok {
			cols[col.UniqueID] = colHist
		}
	}
	newColl := &HistColl{
		PhysicalID:    coll.PhysicalID,
		Pseudo:        coll.Pseudo,
		RealtimeCount: coll.RealtimeCount,
		ModifyCount:   coll.ModifyCount,
		columns:       cols,
	}
	return newColl
}

// GenerateHistCollFromColumnInfo generates a new HistColl whose ColUniqueID2IdxIDs and Idx2ColUniqueIDs is built from the given parameter.
func (coll *HistColl) GenerateHistCollFromColumnInfo(tblInfo *model.TableInfo, columns []*expression.Column) *HistColl {
	newColHistMap := make(map[int64]*Column)
	colInfoID2Col := make(map[int64]*expression.Column, len(columns))
	colInfoID2UniqueID := make(map[int64]int64, len(columns))
	uniqueID2colInfoID := make(map[int64]int64, len(columns))
	idxID2idxInfo := make(map[int64]*model.IndexInfo)
	for _, col := range columns {
		colInfoID2Col[col.ID] = col
		colInfoID2UniqueID[col.ID] = col.UniqueID
		uniqueID2colInfoID[col.UniqueID] = col.ID
	}
	for id, colHist := range coll.columns {
		uniqueID, ok := colInfoID2UniqueID[id]
		// Collect the statistics by the given columns.
		if ok {
			newColHistMap[uniqueID] = colHist
		}
	}
	for _, idxInfo := range tblInfo.Indices {
		idxID2idxInfo[idxInfo.ID] = idxInfo
	}
	newIdxHistMap := make(map[int64]*Index)
	idx2Columns := make(map[int64][]int64)
	colID2IdxIDs := make(map[int64][]int64)
	mvIdx2Columns := make(map[int64][]*expression.Column)
	for id, idxHist := range coll.indices {
		idxInfo := idxID2idxInfo[id]
		if idxInfo == nil {
			continue
		}
		ids := make([]int64, 0, len(idxInfo.Columns))
		for _, idxCol := range idxInfo.Columns {
			uniqueID, ok := colInfoID2UniqueID[tblInfo.Columns[idxCol.Offset].ID]
			if !ok {
				break
			}
			ids = append(ids, uniqueID)
		}
		// If the length of the id list is 0, this index won't be used in this query.
		if len(ids) == 0 {
			continue
		}
		colID2IdxIDs[ids[0]] = append(colID2IdxIDs[ids[0]], idxHist.ID)
		newIdxHistMap[idxHist.ID] = idxHist
		idx2Columns[idxHist.ID] = ids
		if idxInfo.MVIndex {
			cols, ok := PrepareCols4MVIndex(tblInfo, idxInfo, colInfoID2Col, true)
			if ok {
				mvIdx2Columns[id] = cols
			}
		}
	}
	for _, idxIDs := range colID2IdxIDs {
		slices.Sort(idxIDs)
	}
	newColl := &HistColl{
		PhysicalID:         coll.PhysicalID,
		Pseudo:             coll.Pseudo,
		RealtimeCount:      coll.RealtimeCount,
		ModifyCount:        coll.ModifyCount,
		columns:            newColHistMap,
		indices:            newIdxHistMap,
		ColUniqueID2IdxIDs: colID2IdxIDs,
		Idx2ColUniqueIDs:   idx2Columns,
		UniqueID2colInfoID: uniqueID2colInfoID,
		MVIdx2Columns:      mvIdx2Columns,
	}
	return newColl
}

// PseudoHistColl creates a lightweight pseudo HistColl for cost calculation.
// This is optimized for cases where only HistColl is needed, avoiding the overhead
// of creating a full pseudo table with ColAndIdxExistenceMap and other structures.
func PseudoHistColl(physicalID int64, allowTriggerLoading bool) HistColl {
	return HistColl{
		RealtimeCount:     PseudoRowCount,
		PhysicalID:        physicalID,
		columns:           nil,
		indices:           nil,
		Pseudo:            true,
		CanNotTriggerLoad: !allowTriggerLoading,
		ModifyCount:       0,
		StatsVer:          0,
	}
}

// PseudoTable creates a pseudo table statistics.
// Usually, we don't want to trigger stats loading for pseudo table.
// But there are exceptional cases. In such cases, we should pass allowTriggerLoading as true.
// Such case could possibly happen in getStatsTable().
func PseudoTable(tblInfo *model.TableInfo, allowTriggerLoading bool, allowFillHistMeta bool) *Table {
	t := &Table{
		HistColl:              PseudoHistColl(tblInfo.ID, allowTriggerLoading),
		Version:               PseudoVersion,
		ColAndIdxExistenceMap: NewColAndIndexExistenceMap(len(tblInfo.Columns), len(tblInfo.Indices)),
	}

	// Initialize columns and indices maps only when allowFillHistMeta is true
	if allowFillHistMeta {
		t.columns = make(map[int64]*Column, len(tblInfo.Columns))
		t.indices = make(map[int64]*Index, len(tblInfo.Indices))
	}

	for _, col := range tblInfo.Columns {
		// The column is public to use. Also we should check the column is not hidden since hidden means that it's used by expression index.
		// We would not collect stats for the hidden column and we won't use the hidden column to estimate.
		// Thus we don't create pseudo stats for it.
		if col.State == model.StatePublic && !col.Hidden {
			t.ColAndIdxExistenceMap.InsertCol(col.ID, false)
			if allowFillHistMeta {
				t.columns[col.ID] = &Column{
					PhysicalID: tblInfo.ID,
					Info:       col,
					IsHandle:   tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.GetFlag()),
					Histogram:  *NewPseudoHistogram(col.ID, &col.FieldType),
				}
			}
		}
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			t.ColAndIdxExistenceMap.InsertIndex(idx.ID, false)
			if allowFillHistMeta {
				t.indices[idx.ID] = &Index{
					PhysicalID: tblInfo.ID,
					Info:       idx,
					Histogram:  *NewPseudoHistogram(idx.ID, types.NewFieldType(mysql.TypeBlob)),
				}
			}
		}
	}
	return t
}

// CheckAnalyzeVerOnTable checks whether the given version is the one from the tbl.
// If not, it will return false and set the version to the tbl's.
// We use this check to make sure all the statistics of the table are in the same version.
func CheckAnalyzeVerOnTable(tbl *Table, version *int) bool {
	if IsAnalyzed(int64(tbl.StatsVer)) && tbl.StatsVer != *version {
		*version = tbl.StatsVer
		return false
	}
	return true
}

// PrepareCols4MVIndex helps to identify the columns of an MV index. We need this information for estimation.
// This logic is shared between the estimation logic and the access path generation logic. We'd like to put the mv index
// related functions together in the planner/core package. So we use this trick here to avoid the import cycle.
var PrepareCols4MVIndex func(
	tableInfo *model.TableInfo,
	mvIndex *model.IndexInfo,
	tblColsByID map[int64]*expression.Column,
	checkOnly1ArrayTypeCol bool,
) (idxCols []*expression.Column, ok bool)
