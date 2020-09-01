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
// See the License for the specific language governing permissions and
// limitations under the License.

package statistics

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/expression"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/ranger"
	"go.uber.org/atomic"
)

const (
	// When we haven't analyzed a table, we use pseudo statistics to estimate costs.
	// It has row count 10000, equal condition selects 1/1000 of total rows, less condition selects 1/3 of total rows,
	// between condition selects 1/40 of total rows.
	pseudoRowCount    = 10000
	pseudoEqualRate   = 1000
	pseudoLessRate    = 3
	pseudoBetweenRate = 40

	outOfRangeBetweenRate = 100
)

// PseudoVersion means the pseudo statistics version is 0.
const PseudoVersion uint64 = 0

// Table represents statistics for a table.
type Table struct {
	HistColl
	Version uint64
	Name    string
}

// HistColl is a collection of histogram. It collects enough information for plan to calculate the selectivity.
type HistColl struct {
	PhysicalID int64
	// HavePhysicalID is true means this HistColl is from single table and have its ID's information.
	// The physical id is used when try to load column stats from storage.
	HavePhysicalID bool
	Columns        map[int64]*Column
	Indices        map[int64]*Index
	// Idx2ColumnIDs maps the index id to its column ids. It's used to calculate the selectivity in planner.
	Idx2ColumnIDs map[int64][]int64
	// ColID2IdxID maps the column id to index id whose first column is it. It's used to calculate the selectivity in planner.
	ColID2IdxID map[int64]int64
	Pseudo      bool
	Count       int64
	ModifyCount int64 // Total modify count in a table.
}

// Copy copies the current table.
func (t *Table) Copy() *Table {
	newHistColl := HistColl{
		PhysicalID:     t.PhysicalID,
		HavePhysicalID: t.HavePhysicalID,
		Count:          t.Count,
		Columns:        make(map[int64]*Column),
		Indices:        make(map[int64]*Index),
		Pseudo:         t.Pseudo,
		ModifyCount:    t.ModifyCount,
	}
	for id, col := range t.Columns {
		newHistColl.Columns[id] = col
	}
	for id, idx := range t.Indices {
		newHistColl.Indices[id] = idx
	}
	nt := &Table{
		HistColl: newHistColl,
		Version:  t.Version,
		Name:     t.Name,
	}
	return nt
}

// String implements Stringer interface.
func (t *Table) String() string {
	strs := make([]string, 0, len(t.Columns)+1)
	strs = append(strs, fmt.Sprintf("Table:%d Count:%d", t.PhysicalID, t.Count))
	cols := make([]*Column, 0, len(t.Columns))
	for _, col := range t.Columns {
		cols = append(cols, col)
	}
	sort.Slice(cols, func(i, j int) bool { return cols[i].ID < cols[j].ID })
	for _, col := range cols {
		strs = append(strs, col.String())
	}
	idxs := make([]*Index, 0, len(t.Indices))
	for _, idx := range t.Indices {
		idxs = append(idxs, idx)
	}
	sort.Slice(idxs, func(i, j int) bool { return idxs[i].ID < idxs[j].ID })
	for _, idx := range idxs {
		strs = append(strs, idx.String())
	}
	return strings.Join(strs, "\n")
}

// IndexStartWithColumn finds the first index whose first column is the given column.
func (t *Table) IndexStartWithColumn(colName string) *Index {
	for _, index := range t.Indices {
		if index.Info.Columns[0].Name.L == colName {
			return index
		}
	}
	return nil
}

// ColumnByName finds the statistics.Column for the given column.
func (t *Table) ColumnByName(colName string) *Column {
	for _, c := range t.Columns {
		if c.Info.Name.L == colName {
			return c
		}
	}
	return nil
}

type tableColumnID struct {
	TableID  int64
	ColumnID int64
}

type neededColumnMap struct {
	m    sync.Mutex
	cols map[tableColumnID]struct{}
}

func (n *neededColumnMap) AllCols() []tableColumnID {
	n.m.Lock()
	keys := make([]tableColumnID, 0, len(n.cols))
	for key := range n.cols {
		keys = append(keys, key)
	}
	n.m.Unlock()
	return keys
}

func (n *neededColumnMap) insert(col tableColumnID) {
	n.m.Lock()
	n.cols[col] = struct{}{}
	n.m.Unlock()
}

func (n *neededColumnMap) Delete(col tableColumnID) {
	n.m.Lock()
	delete(n.cols, col)
	n.m.Unlock()
}

// RatioOfPseudoEstimate means if modifyCount / statsTblCount is greater than this ratio, we think the stats is invalid
// and use pseudo estimation.
var RatioOfPseudoEstimate = atomic.NewFloat64(0.7)

// IsOutdated returns true if the table stats is outdated.
func (t *Table) IsOutdated() bool {
	if t.Count > 0 && float64(t.ModifyCount)/float64(t.Count) > RatioOfPseudoEstimate.Load() {
		return true
	}
	return false
}

// ColumnGreaterRowCount estimates the row count where the column greater than value.
func (t *Table) ColumnGreaterRowCount(sc *stmtctx.StatementContext, value types.Datum, colID int64) float64 {
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		return float64(t.Count) / pseudoLessRate
	}
	return c.greaterRowCount(value) * c.GetIncreaseFactor(t.Count)
}

// ColumnLessRowCount estimates the row count where the column less than value. Note that null values are not counted.
func (t *Table) ColumnLessRowCount(sc *stmtctx.StatementContext, value types.Datum, colID int64) float64 {
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		return float64(t.Count) / pseudoLessRate
	}
	return c.lessRowCount(value) * c.GetIncreaseFactor(t.Count)
}

// ColumnBetweenRowCount estimates the row count where column greater or equal to a and less than b.
func (t *Table) ColumnBetweenRowCount(sc *stmtctx.StatementContext, a, b types.Datum, colID int64) float64 {
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		return float64(t.Count) / pseudoBetweenRate
	}
	count := c.BetweenRowCount(a, b)
	if a.IsNull() {
		count += float64(c.NullCount)
	}
	return count * c.GetIncreaseFactor(t.Count)
}

// ColumnEqualRowCount estimates the row count where the column equals to value.
func (t *Table) ColumnEqualRowCount(sc *stmtctx.StatementContext, value types.Datum, colID int64) (float64, error) {
	c, ok := t.Columns[colID]
	if !ok || c.IsInvalid(sc, t.Pseudo) {
		return float64(t.Count) / pseudoEqualRate, nil
	}
	result, err := c.equalRowCount(sc, value, t.ModifyCount)
	result *= c.GetIncreaseFactor(t.Count)
	return result, errors.Trace(err)
}

// GetRowCountByIntColumnRanges estimates the row count by a slice of IntColumnRange.
func (coll *HistColl) GetRowCountByIntColumnRanges(sc *stmtctx.StatementContext, colID int64, intRanges []*ranger.Range) (float64, error) {
	c, ok := coll.Columns[colID]
	if !ok || c.IsInvalid(sc, coll.Pseudo) {
		if len(intRanges) == 0 {
			return 0, nil
		}
		if intRanges[0].LowVal[0].Kind() == types.KindInt64 {
			return getPseudoRowCountBySignedIntRanges(intRanges, float64(coll.Count)), nil
		}
		return getPseudoRowCountByUnsignedIntRanges(intRanges, float64(coll.Count)), nil
	}
	result, err := c.GetColumnRowCount(sc, intRanges, coll.ModifyCount, true)
	result *= c.GetIncreaseFactor(coll.Count)
	return result, errors.Trace(err)
}

// GetRowCountByColumnRanges estimates the row count by a slice of Range.
func (coll *HistColl) GetRowCountByColumnRanges(sc *stmtctx.StatementContext, colID int64, colRanges []*ranger.Range) (float64, error) {
	c, ok := coll.Columns[colID]
	if !ok || c.IsInvalid(sc, coll.Pseudo) {
		return GetPseudoRowCountByColumnRanges(sc, float64(coll.Count), colRanges, 0)
	}
	result, err := c.GetColumnRowCount(sc, colRanges, coll.ModifyCount, false)
	result *= c.GetIncreaseFactor(coll.Count)
	return result, errors.Trace(err)
}

// GetRowCountByIndexRanges estimates the row count by a slice of Range.
func (coll *HistColl) GetRowCountByIndexRanges(sc *stmtctx.StatementContext, idxID int64, indexRanges []*ranger.Range) (float64, error) {
	idx := coll.Indices[idxID]
	if idx == nil || idx.IsInvalid(coll.Pseudo) {
		colsLen := -1
		if idx != nil && idx.Info.Unique {
			colsLen = len(idx.Info.Columns)
		}
		return getPseudoRowCountByIndexRanges(sc, indexRanges, float64(coll.Count), colsLen)
	}
	var result float64
	var err error
	if idx.CMSketch != nil && idx.StatsVer == Version1 {
		result, err = coll.getIndexRowCount(sc, idxID, indexRanges)
	} else {
		result, err = idx.GetRowCount(sc, indexRanges, coll.ModifyCount)
	}
	result *= idx.GetIncreaseFactor(coll.Count)
	return result, errors.Trace(err)
}

// PseudoAvgCountPerValue gets a pseudo average count if histogram not exists.
func (t *Table) PseudoAvgCountPerValue() float64 {
	return float64(t.Count) / pseudoEqualRate
}

// GetOrdinalOfRangeCond gets the ordinal of the position range condition,
// if not exist, it returns the end position.
func GetOrdinalOfRangeCond(sc *stmtctx.StatementContext, ran *ranger.Range) int {
	for i := range ran.LowVal {
		a, b := ran.LowVal[i], ran.HighVal[i]
		cmp, err := a.CompareDatum(sc, &b)
		if err != nil {
			return 0
		}
		if cmp != 0 {
			return i
		}
	}
	return len(ran.LowVal)
}

// GenerateHistCollFromColumnInfo generates a new HistColl whose ColID2IdxID and IdxID2ColIDs is built from the given parameter.
func (coll *HistColl) GenerateHistCollFromColumnInfo(infos []*model.ColumnInfo, columns []*expression.Column) *HistColl {
	newColHistMap := make(map[int64]*Column)
	colInfoID2UniqueID := make(map[int64]int64)
	colNames2UniqueID := make(map[string]int64)
	for _, col := range columns {
		colInfoID2UniqueID[col.ID] = col.UniqueID
	}
	for _, colInfo := range infos {
		uniqueID, ok := colInfoID2UniqueID[colInfo.ID]
		if ok {
			colNames2UniqueID[colInfo.Name.L] = uniqueID
		}
	}
	for id, colHist := range coll.Columns {
		uniqueID, ok := colInfoID2UniqueID[id]
		// Collect the statistics by the given columns.
		if ok {
			newColHistMap[uniqueID] = colHist
		}
	}
	newIdxHistMap := make(map[int64]*Index)
	idx2Columns := make(map[int64][]int64)
	colID2IdxID := make(map[int64]int64)
	for _, idxHist := range coll.Indices {
		ids := make([]int64, 0, len(idxHist.Info.Columns))
		for _, idxCol := range idxHist.Info.Columns {
			uniqueID, ok := colNames2UniqueID[idxCol.Name.L]
			if !ok {
				break
			}
			ids = append(ids, uniqueID)
		}
		// If the length of the id list is 0, this index won't be used in this query.
		if len(ids) == 0 {
			continue
		}
		colID2IdxID[ids[0]] = idxHist.ID
		newIdxHistMap[idxHist.ID] = idxHist
		idx2Columns[idxHist.ID] = ids
	}
	newColl := &HistColl{
		PhysicalID:     coll.PhysicalID,
		HavePhysicalID: coll.HavePhysicalID,
		Pseudo:         coll.Pseudo,
		Count:          coll.Count,
		ModifyCount:    coll.ModifyCount,
		Columns:        newColHistMap,
		Indices:        newIdxHistMap,
		ColID2IdxID:    colID2IdxID,
		Idx2ColumnIDs:  idx2Columns,
	}
	return newColl
}

// isSingleColIdxNullRange checks if a range is [NULL, NULL] on a single-column index.
func isSingleColIdxNullRange(idx *Index, ran *ranger.Range) bool {
	if len(idx.Info.Columns) > 1 {
		return false
	}
	l, h := ran.LowVal[0], ran.HighVal[0]
	if l.IsNull() && h.IsNull() {
		return true
	}
	return false
}

// getEqualCondSelectivity gets the selectivity of the equal conditions.
func (coll *HistColl) getEqualCondSelectivity(idx *Index, bytes []byte, usedColsLen int) float64 {
	coverAll := len(idx.Info.Columns) == usedColsLen
	// In this case, the row count is at most 1.
	if idx.Info.Unique && coverAll {
		return 1.0 / float64(idx.TotalRowCount())
	}
	val := types.NewBytesDatum(bytes)
	if idx.outOfRange(val) {
		// When the value is out of range, we could not found this value in the CM Sketch,
		// so we use heuristic methods to estimate the selectivity.
		if idx.NDV > 0 && coverAll {
			// for equality queries
			return outOfRangeEQSelectivity(idx.NDV, coll.ModifyCount, int64(idx.TotalRowCount()))
		}
		// The equal condition only uses prefix columns of the index.
		colIDs := coll.Idx2ColumnIDs[idx.ID]
		var ndv int64
		for i, colID := range colIDs {
			if i >= usedColsLen {
				break
			}
			ndv = mathutil.MaxInt64(ndv, coll.Columns[colID].NDV)
		}
		return outOfRangeEQSelectivity(ndv, coll.ModifyCount, int64(idx.TotalRowCount()))
	}
	return float64(idx.CMSketch.QueryBytes(bytes)) / float64(idx.TotalRowCount())
}

func (coll *HistColl) getIndexRowCount(sc *stmtctx.StatementContext, idxID int64, indexRanges []*ranger.Range) (float64, error) {
	idx := coll.Indices[idxID]
	totalCount := float64(0)
	for _, ran := range indexRanges {
		rangePosition := GetOrdinalOfRangeCond(sc, ran)
		coverAll := len(ran.LowVal) == len(idx.Info.Columns) && rangePosition == len(ran.LowVal)
		// // In this case, the row count is at most 1.
		if coverAll && idx.Info.Unique {
			totalCount += 1.0
			continue
		}
		// If first one is range, just use the previous way to estimate; if it is [NULL, NULL] range
		// on single-column index, use previous way as well, because CMSketch does not contain null
		// values in this case.
		if rangePosition == 0 || isSingleColIdxNullRange(idx, ran) {
			count, err := idx.GetRowCount(sc, []*ranger.Range{ran}, coll.ModifyCount)
			if err != nil {
				return 0, errors.Trace(err)
			}
			totalCount += count
			continue
		}
		var selectivity float64
		// use CM Sketch to estimate the equal conditions
		bytes, err := codec.EncodeKey(sc, nil, ran.LowVal[:rangePosition]...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		selectivity = coll.getEqualCondSelectivity(idx, bytes, rangePosition)
		// use histogram to estimate the range condition
		if rangePosition != len(ran.LowVal) {
			rang := ranger.Range{
				LowVal:      []types.Datum{ran.LowVal[rangePosition]},
				LowExclude:  ran.LowExclude,
				HighVal:     []types.Datum{ran.HighVal[rangePosition]},
				HighExclude: ran.HighExclude,
			}
			var count float64
			var err error
			colIDs := coll.Idx2ColumnIDs[idxID]
			var colID int64
			if rangePosition >= len(colIDs) {
				colID = -1
			} else {
				colID = colIDs[rangePosition]
			}
			// prefer index stats over column stats
			if idx, ok := coll.ColID2IdxID[colID]; ok {
				count, err = coll.GetRowCountByIndexRanges(sc, idx, []*ranger.Range{&rang})
			} else {
				count, err = coll.GetRowCountByColumnRanges(sc, colID, []*ranger.Range{&rang})
			}
			if err != nil {
				return 0, errors.Trace(err)
			}
			selectivity = selectivity * count / float64(idx.TotalRowCount())
		}
		totalCount += selectivity * float64(idx.TotalRowCount())
	}
	if totalCount > idx.TotalRowCount() {
		totalCount = idx.TotalRowCount()
	}
	return totalCount, nil
}

const fakePhysicalID int64 = -1

// PseudoTable creates a pseudo table statistics.
func PseudoTable(tblInfo *model.TableInfo) *Table {
	pseudoHistColl := HistColl{
		Count:          pseudoRowCount,
		PhysicalID:     tblInfo.ID,
		HavePhysicalID: true,
		Columns:        make(map[int64]*Column, len(tblInfo.Columns)),
		Indices:        make(map[int64]*Index, len(tblInfo.Indices)),
		Pseudo:         true,
	}
	t := &Table{
		HistColl: pseudoHistColl,
	}
	for _, col := range tblInfo.Columns {
		if col.State == model.StatePublic {
			t.Columns[col.ID] = &Column{
				PhysicalID: fakePhysicalID,
				Info:       col,
				IsHandle:   tblInfo.PKIsHandle && mysql.HasPriKeyFlag(col.Flag),
				Histogram:  *NewHistogram(col.ID, 0, 0, 0, &col.FieldType, 0, 0),
			}
		}
	}
	for _, idx := range tblInfo.Indices {
		if idx.State == model.StatePublic {
			t.Indices[idx.ID] = &Index{
				Info:      idx,
				Histogram: *NewHistogram(idx.ID, 0, 0, 0, types.NewFieldType(mysql.TypeBlob), 0, 0)}
		}
	}
	return t
}

func getPseudoRowCountByIndexRanges(sc *stmtctx.StatementContext, indexRanges []*ranger.Range,
	tableRowCount float64, colsLen int) (float64, error) {
	if tableRowCount == 0 {
		return 0, nil
	}
	var totalCount float64
	for _, indexRange := range indexRanges {
		count := tableRowCount
		i, err := indexRange.PrefixEqualLen(sc)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if i == colsLen && !indexRange.LowExclude && !indexRange.HighExclude {
			totalCount += 1.0
			continue
		}
		if i >= len(indexRange.LowVal) {
			i = len(indexRange.LowVal) - 1
		}
		rowCount, err := GetPseudoRowCountByColumnRanges(sc, tableRowCount, []*ranger.Range{indexRange}, i)
		if err != nil {
			return 0, errors.Trace(err)
		}
		count = count / tableRowCount * rowCount
		// If the condition is a = 1, b = 1, c = 1, d = 1, we think every a=1, b=1, c=1 only filtrate 1/100 data,
		// so as to avoid collapsing too fast.
		for j := 0; j < i; j++ {
			count = count / float64(100)
		}
		totalCount += count
	}
	if totalCount > tableRowCount {
		totalCount = tableRowCount / 3.0
	}
	return totalCount, nil
}

// GetPseudoRowCountByColumnRanges calculate the row count by the ranges if there's no statistics information for this column.
func GetPseudoRowCountByColumnRanges(sc *stmtctx.StatementContext, tableRowCount float64, columnRanges []*ranger.Range, colIdx int) (float64, error) {
	var rowCount float64
	var err error
	for _, ran := range columnRanges {
		if ran.LowVal[colIdx].Kind() == types.KindNull && ran.HighVal[colIdx].Kind() == types.KindMaxValue {
			rowCount += tableRowCount
		} else if ran.LowVal[colIdx].Kind() == types.KindMinNotNull {
			nullCount := tableRowCount / pseudoEqualRate
			if ran.HighVal[colIdx].Kind() == types.KindMaxValue {
				rowCount += tableRowCount - nullCount
			} else if err == nil {
				lessCount := tableRowCount / pseudoLessRate
				rowCount += lessCount - nullCount
			}
		} else if ran.HighVal[colIdx].Kind() == types.KindMaxValue {
			rowCount += tableRowCount / pseudoLessRate
		} else {
			compare, err1 := ran.LowVal[colIdx].CompareDatum(sc, &ran.HighVal[colIdx])
			if err1 != nil {
				return 0, errors.Trace(err1)
			}
			if compare == 0 {
				rowCount += tableRowCount / pseudoEqualRate
			} else {
				rowCount += tableRowCount / pseudoBetweenRate
			}
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	if rowCount > tableRowCount {
		rowCount = tableRowCount
	}
	return rowCount, nil
}

func getPseudoRowCountBySignedIntRanges(intRanges []*ranger.Range, tableRowCount float64) float64 {
	var rowCount float64
	for _, rg := range intRanges {
		var cnt float64
		low := rg.LowVal[0].GetInt64()
		if rg.LowVal[0].Kind() == types.KindNull || rg.LowVal[0].Kind() == types.KindMinNotNull {
			low = math.MinInt64
		}
		high := rg.HighVal[0].GetInt64()
		if rg.HighVal[0].Kind() == types.KindMaxValue {
			high = math.MaxInt64
		}
		if low == math.MinInt64 && high == math.MaxInt64 {
			cnt = tableRowCount
		} else if low == math.MinInt64 {
			cnt = tableRowCount / pseudoLessRate
		} else if high == math.MaxInt64 {
			cnt = tableRowCount / pseudoLessRate
		} else {
			if low == high {
				cnt = 1 // When primary key is handle, the equal row count is at most one.
			} else {
				cnt = tableRowCount / pseudoBetweenRate
			}
		}
		if high-low > 0 && cnt > float64(high-low) {
			cnt = float64(high - low)
		}
		rowCount += cnt
	}
	if rowCount > tableRowCount {
		rowCount = tableRowCount
	}
	return rowCount
}

func getPseudoRowCountByUnsignedIntRanges(intRanges []*ranger.Range, tableRowCount float64) float64 {
	var rowCount float64
	for _, rg := range intRanges {
		var cnt float64
		low := rg.LowVal[0].GetUint64()
		if rg.LowVal[0].Kind() == types.KindNull || rg.LowVal[0].Kind() == types.KindMinNotNull {
			low = 0
		}
		high := rg.HighVal[0].GetUint64()
		if rg.HighVal[0].Kind() == types.KindMaxValue {
			high = math.MaxUint64
		}
		if low == 0 && high == math.MaxUint64 {
			cnt = tableRowCount
		} else if low == 0 {
			cnt = tableRowCount / pseudoLessRate
		} else if high == math.MaxUint64 {
			cnt = tableRowCount / pseudoLessRate
		} else {
			if low == high {
				cnt = 1 // When primary key is handle, the equal row count is at most one.
			} else {
				cnt = tableRowCount / pseudoBetweenRate
			}
		}
		if high > low && cnt > float64(high-low) {
			cnt = float64(high - low)
		}
		rowCount += cnt
	}
	if rowCount > tableRowCount {
		rowCount = tableRowCount
	}
	return rowCount
}

// outOfRangeEQSelectivity estimates selectivities for out-of-range values.
// It assumes all modifications are insertions and all new-inserted rows are uniformly distributed
// and has the same distribution with analyzed rows, which means each unique value should have the
// same number of rows(Tot/NDV) of it.
func outOfRangeEQSelectivity(ndv, modifyRows, totalRows int64) float64 {
	if modifyRows == 0 {
		return 0 // it must be 0 since the histogram contains the whole data
	}
	if ndv < outOfRangeBetweenRate {
		ndv = outOfRangeBetweenRate // avoid inaccurate selectivity caused by small NDV
	}
	selectivity := 1 / float64(ndv) // TODO: After extracting TopN from histograms, we can minus the TopN fraction here.
	if selectivity*float64(totalRows) > float64(modifyRows) {
		selectivity = float64(modifyRows) / float64(totalRows)
	}
	return selectivity
}
