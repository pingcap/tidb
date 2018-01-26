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

package statistics

import (
	"github.com/cznic/mathutil"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

// TableDelta stands for the changed stats for one table.
// Note that we only update stats for primary key and indices.
type TableDelta struct {
	Delta     int64 // Delta is the delta count for a table.
	Count     int64 // Count is the modify count for a table.
	PKID      int64
	PKDelta   *HistDelta
	IdxDeltas map[int64]*HistDelta
}

// HistDelta stands for the changed stats for one histogram.
type HistDelta struct {
	HistVersion uint64 // HistVersion is the version of the histogram when the delta info is collected.
	BktDeltas   map[int]*BktDelta
}

// BktDelta stands for the delta info for a bucket.
type BktDelta struct {
	Count int64
	Bound *types.Datum // Bound is the new lower bound of the bucket. For the last bucket, it is also used to store the new upper bound of the bucket.
}

// UpdateTableDelta updates the table delta according to the changed row.
func (delta *TableDelta) UpdateTableDelta(sc *stmtctx.StatementContext, h *Handle, tableInfo *model.TableInfo, row []types.Datum, deltaCount, count int64) {
	delta.Delta += deltaCount
	delta.Count += count
	table := h.GetTableStats(tableInfo.ID)
	if table.Pseudo {
		return
	}
	// Update the primary key's histogram.
	if tableInfo.PKIsHandle {
		pk := tableInfo.GetPkColInfo()
		c, ok := table.Columns[pk.ID]
		if ok && pk.State == model.StatePublic && pk.Offset >= 0 && pk.Offset < len(row) {
			if delta.PKDelta == nil {
				delta.PKID = pk.ID
				delta.PKDelta = &HistDelta{}
			}
			delta.PKDelta.updateHistDelta(sc, &c.Histogram, &row[pk.Offset], deltaCount)
		}
	}
	// Update the indices' histogram.
	for _, idxInfo := range tableInfo.Indices {
		if idxInfo.State != model.StatePublic {
			continue
		}
		idx, ok := table.Indices[idxInfo.ID]
		if !ok {
			continue
		}
		value := encodeIndexValue(h.ctx.GetSessionVars().StmtCtx, idxInfo, row)
		if value == nil {
			continue
		}
		if delta.IdxDeltas == nil {
			delta.IdxDeltas = make(map[int64]*HistDelta)
		}
		if delta.IdxDeltas[idxInfo.ID] == nil {
			delta.IdxDeltas[idxInfo.ID] = &HistDelta{}
		}
		idxDelta := delta.IdxDeltas[idxInfo.ID]
		idxDelta.updateHistDelta(sc, &idx.Histogram, value, deltaCount)
	}
}

func encodeIndexValue(sc *stmtctx.StatementContext, idxInfo *model.IndexInfo, row []types.Datum) *types.Datum {
	values := make([]types.Datum, 0, len(idxInfo.Columns))
	for _, col := range idxInfo.Columns {
		if col.Offset < 0 || col.Offset >= len(row) {
			return nil
		}
		values = append(values, row[col.Offset])
	}
	b, err := codec.EncodeKey(sc, nil, values...)
	if err != nil {
		terror.Log(err)
		return nil
	}
	d := types.NewBytesDatum(b)
	return &d
}

func (delta *BktDelta) updateBktBound(sc *stmtctx.StatementContext, value *types.Datum, isUpper bool) {
	if delta.Bound == nil {
		delta.Bound = value
		return
	}
	cmp, err := delta.Bound.CompareDatum(sc, value)
	if err != nil {
		terror.Log(err)
		return
	}
	if (isUpper && cmp == -1) || (!isUpper && cmp == 1) {
		delta.Bound = value
	}
}

func (delta *HistDelta) updateHistDelta(sc *stmtctx.StatementContext, hg *Histogram, value *types.Datum, deltaCount int64) {
	if hg.Len() == 0 {
		return
	}
	// The histogram version has changed, so we need to migrate the old delta info to the new histogram.
	if hg.LastUpdateVersion != delta.HistVersion {
		delta.HistVersion = hg.LastUpdateVersion
		bktDelta := delta.BktDeltas
		delta.BktDeltas = nil
		for _, bkt := range bktDelta {
			delta.updateHistDelta(sc, hg, bkt.Bound, 0)
		}
	}
	if value == nil {
		return
	}
	idx, match := hg.Bounds.LowerBound(0, value)
	if delta.BktDeltas == nil {
		delta.BktDeltas = make(map[int]*BktDelta)
	}
	if delta.BktDeltas[idx/2] == nil {
		delta.BktDeltas[idx/2] = &BktDelta{}
	}
	bkt := delta.BktDeltas[idx/2]
	bkt.Count += deltaCount
	// This value falls outside the whole histogram, so we need to update the last bucket's upper bound.
	if idx == hg.Bounds.NumRows() {
		bkt.updateBktBound(sc, value, true)
		return
	}
	if idx%2 == 0 && !match {
		bkt.updateBktBound(sc, value, false)
	}
}

func (delta *HistDelta) mergeHistDelta(sc *stmtctx.StatementContext, rDelta *HistDelta, hg *Histogram) {
	// Make sure that the two delta's histogram version are up to date.
	delta.updateHistDelta(sc, hg, nil, 0)
	rDelta.updateHistDelta(sc, hg, nil, 0)
	for id, rBkt := range rDelta.BktDeltas {
		bkt, ok := delta.BktDeltas[id]
		if !ok {
			delta.BktDeltas[id] = rBkt
			continue
		}
		bkt.Count += rBkt.Count
		if rBkt.Bound == nil {
			continue
		}
		bkt.updateBktBound(sc, rBkt.Bound, id == hg.Len())
	}
}

func (delta *TableDelta) mergeTableDelta(sc *stmtctx.StatementContext, rDelta *TableDelta, table *Table) {
	delta.Count += rDelta.Count
	delta.Delta += rDelta.Delta
	if delta.IdxDeltas == nil {
		delta.IdxDeltas = rDelta.IdxDeltas
	} else {
		for id, rd := range rDelta.IdxDeltas {
			idx, ok := table.Indices[id]
			if !ok {
				delete(delta.IdxDeltas, id)
				continue
			}
			d, ok := delta.IdxDeltas[id]
			if !ok {
				delta.IdxDeltas[id] = rd
				continue
			}
			d.mergeHistDelta(sc, rd, &idx.Histogram)
		}
	}
	if rDelta.PKDelta != nil {
		col, ok := table.Columns[rDelta.PKID]
		if !ok {
			delta.PKDelta = nil
			return
		}
		if delta.PKDelta == nil {
			delta.PKID, delta.PKDelta = rDelta.PKID, rDelta.PKDelta
			return
		}
		delta.PKDelta.mergeHistDelta(sc, rDelta.PKDelta, &col.Histogram)
	}
}

func (delta *HistDelta) updateHistogram(sc *stmtctx.StatementContext, oldHg *Histogram) *Histogram {
	if oldHg.Len() == 0 {
		return oldHg
	}
	delta.updateHistDelta(sc, oldHg, nil, 0)
	hg := NewHistogram(oldHg.ID, oldHg.NDV, oldHg.NullCount, oldHg.LastUpdateVersion, oldHg.tp, oldHg.Len())
	for bktID := 0; bktID < oldHg.Len(); bktID++ {
		hg.Buckets = append(hg.Buckets, Bucket{})
		// First calculate the bucket count before update, because we may have update the prior bucket's count.
		if bktID == 0 {
			hg.Buckets[0] = oldHg.Buckets[0]
		} else {
			hg.Buckets[bktID] = hg.Buckets[bktID-1]
			hg.Buckets[bktID].Count += oldHg.Buckets[bktID].Count - oldHg.Buckets[bktID-1].Count
		}
		// Then update the bucket's count.
		bkt, ok := delta.BktDeltas[bktID]
		if ok {
			hg.Buckets[bktID].Count = mathutil.MaxInt64(0, hg.Buckets[bktID].Count+bkt.Count)
		}
		// Finally update the bucket's lower bound.
		if !ok || bkt.Bound == nil {
			hg.Bounds.AppendDatum(0, oldHg.GetLower(bktID))
		} else {
			hg.Bounds.AppendDatum(0, bkt.Bound)
		}
		hg.Bounds.AppendDatum(0, oldHg.GetUpper(bktID))
	}
	// Update the last bucket's upper bound.
	if bkt, ok := delta.BktDeltas[oldHg.Len()]; ok {
		bktID := oldHg.Len() - 1
		hg.Buckets[bktID].Count = mathutil.MaxInt64(0, bkt.Count+hg.Buckets[bktID].Count)
		hg.Bounds.TruncateTo(oldHg.Bounds.NumRows() - 1)
		hg.Bounds.AppendDatum(0, bkt.Bound)
	}
	return hg
}

// UpdateTable update the stats according to the delta info.
func (delta *TableDelta) UpdateTable(sc *stmtctx.StatementContext, oldTable *Table) *Table {
	t := oldTable.copy()
	if delta.PKDelta != nil {
		col, ok := t.Columns[delta.PKID]
		if ok {
			hg := *delta.PKDelta.updateHistogram(sc, &col.Histogram)
			t.Columns[delta.PKID] = &Column{Histogram: hg}
		}
	}
	for id, d := range delta.IdxDeltas {
		idx, ok := t.Indices[id]
		if ok {
			t.Indices[id] = &Index{Histogram: *d.updateHistogram(sc, &idx.Histogram),
				Info: idx.Info}
		}
	}
	return t
}
