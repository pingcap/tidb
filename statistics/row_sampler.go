// Copyright 2021 PingCAP, Inc.
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
	"container/heap"
	"context"
	"math/rand"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
)

type baseCollector struct {
	Samples    WeightedRowSampleHeap
	NullCount  []int64
	FMSketches []*FMSketch
	TotalSizes []int64
	Count      int64
}

// ReservoirRowSampleCollector collects the samples from the source and organize the samples by row.
// It will maintain the following things:
//   Row samples.
//   FM sketches(To calculate the NDV).
//   Null counts.
//   The data sizes.
//   The number of rows.
// It uses weighted reservoir sampling(A-Res) to do the sampling.
type ReservoirRowSampleCollector struct {
	*baseCollector
	MaxSampleSize int
}

// ReservoirRowSampleItem is the item for the ReservoirRowSampleCollector. The weight is needed for the sampling algorithm.
type ReservoirRowSampleItem struct {
	Columns []types.Datum
	Weight  int64
	Handle  kv.Handle
}

// WeightedRowSampleHeap implements the Heap interface.
type WeightedRowSampleHeap []*ReservoirRowSampleItem

// Len implements the Heap interface.
func (h WeightedRowSampleHeap) Len() int {
	return len(h)
}

// Swap implements the Heap interface.
func (h WeightedRowSampleHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

// Less implements the Heap interface.
func (h WeightedRowSampleHeap) Less(i, j int) bool {
	return h[i].Weight < h[j].Weight
}

// Push implements the Heap interface.
func (h *WeightedRowSampleHeap) Push(i interface{}) {
	*h = append(*h, i.(*ReservoirRowSampleItem))
}

// Pop implements the Heap interface.
func (h *WeightedRowSampleHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// ReservoirRowSampleBuilder is used to construct the ReservoirRowSampleCollector to get the samples.
type ReservoirRowSampleBuilder struct {
	Sc              *stmtctx.StatementContext
	RecordSet       sqlexec.RecordSet
	ColsFieldType   []*types.FieldType
	Collators       []collate.Collator
	ColGroups       [][]int64
	MaxSampleSize   int
	MaxFMSketchSize int
	Rng             *rand.Rand
}

// NewReservoirRowSampleCollector creates the new collector by the given inputs.
func NewReservoirRowSampleCollector(maxSampleSize int, totalLen int) *ReservoirRowSampleCollector {
	base := &baseCollector{
		Samples:    make(WeightedRowSampleHeap, 0, maxSampleSize),
		NullCount:  make([]int64, totalLen),
		FMSketches: make([]*FMSketch, 0, totalLen),
		TotalSizes: make([]int64, totalLen),
	}
	return &ReservoirRowSampleCollector{
		baseCollector: base,
		MaxSampleSize: maxSampleSize,
	}
}

// Collect first builds the collector. Then maintain the null count, FM sketch and the data size for each column and
// column group.
// Then use the weighted reservoir sampling to collect the samples.
func (s *ReservoirRowSampleBuilder) Collect() (*ReservoirRowSampleCollector, error) {
	base := &baseCollector{
		Samples:    make(WeightedRowSampleHeap, 0, s.MaxSampleSize),
		NullCount:  make([]int64, len(s.ColsFieldType)+len(s.ColGroups)),
		FMSketches: make([]*FMSketch, 0, len(s.ColsFieldType)+len(s.ColGroups)),
		TotalSizes: make([]int64, len(s.ColsFieldType)+len(s.ColGroups)),
	}
	collector := &ReservoirRowSampleCollector{
		baseCollector: base,
		MaxSampleSize: s.MaxSampleSize,
	}
	for i := 0; i < len(s.ColsFieldType)+len(s.ColGroups); i++ {
		collector.FMSketches = append(collector.FMSketches, NewFMSketch(s.MaxFMSketchSize))
	}
	ctx := context.TODO()
	chk := s.RecordSet.NewChunk()
	it := chunk.NewIterator4Chunk(chk)
	for {
		err := s.RecordSet.Next(ctx, chk)
		if err != nil {
			return nil, err
		}
		if chk.NumRows() == 0 {
			return collector, nil
		}
		collector.Count += int64(chk.NumRows())
		for row := it.Begin(); row != it.End(); row = it.Next() {
			datums := RowToDatums(row, s.RecordSet.Fields())
			newCols := make([]types.Datum, len(datums))
			// sizes are used to calculate the total size information. We calculate the sizes here because we need the
			// length of the original bytes instead of the collate key when it's a new collation string.
			sizes := make([]int64, 0, len(datums))
			for i := range datums {
				datums[i].Copy(&newCols[i])
				sizes = append(sizes, int64(len(datums[i].GetBytes())))
			}

			for i, val := range datums {
				// For string values, we use the collation key instead of the original value.
				if s.Collators[i] != nil && !val.IsNull() {
					decodedVal, err := tablecodec.DecodeColumnValue(val.GetBytes(), s.ColsFieldType[i], s.Sc.TimeZone)
					if err != nil {
						return nil, err
					}
					decodedVal.SetBytesAsString(s.Collators[i].Key(decodedVal.GetString()), decodedVal.Collation(), uint32(decodedVal.Length()))
					encodedKey, err := tablecodec.EncodeValue(s.Sc, nil, decodedVal)
					if err != nil {
						return nil, err
					}
					datums[i].SetBytes(encodedKey)
				}
			}
			err := collector.collectColumns(s.Sc, datums, sizes)
			if err != nil {
				return nil, err
			}
			err = collector.collectColumnGroups(s.Sc, datums, s.ColGroups, sizes)
			if err != nil {
				return nil, err
			}
			weight := s.Rng.Int63()
			item := &ReservoirRowSampleItem{
				Columns: newCols,
				Weight:  weight,
			}
			collector.sampleZippedRow(item)
		}
	}
}

func (s *ReservoirRowSampleCollector) collectColumns(sc *stmtctx.StatementContext, cols []types.Datum, sizes []int64) error {
	for i, col := range cols {
		if col.IsNull() {
			s.NullCount[i]++
			continue
		}
		// Minus one is to remove the flag byte.
		s.TotalSizes[i] += sizes[i] - 1
		err := s.FMSketches[i].InsertValue(sc, col)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ReservoirRowSampleCollector) collectColumnGroups(sc *stmtctx.StatementContext, cols []types.Datum, colGroups [][]int64, sizes []int64) error {
	colLen := len(cols)
	datumBuffer := make([]types.Datum, 0, len(cols))
	for i, group := range colGroups {
		datumBuffer = datumBuffer[:0]
		hasNull := true
		for _, c := range group {
			datumBuffer = append(datumBuffer, cols[c])
			hasNull = hasNull && cols[c].IsNull()
			s.TotalSizes[colLen+i] += sizes[c] - 1
		}
		// We don't maintain the null counts information for the multi-column group
		if hasNull && len(group) == 1 {
			s.NullCount[colLen+i]++
			continue
		}
		err := s.FMSketches[colLen+i].InsertRowValue(sc, datumBuffer)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ReservoirRowSampleCollector) sampleZippedRow(sample *ReservoirRowSampleItem) {
	if len(s.Samples) < s.MaxSampleSize {
		s.Samples = append(s.Samples, sample)
		if len(s.Samples) == s.MaxSampleSize {
			heap.Init(&s.Samples)
		}
		return
	}
	if s.Samples[0].Weight < sample.Weight {
		s.Samples[0] = sample
		heap.Fix(&s.Samples, 0)
	}
}

// ToProto converts the collector to proto struct.
func (s *ReservoirRowSampleCollector) ToProto() *tipb.RowSampleCollector {
	pbFMSketches := make([]*tipb.FMSketch, 0, len(s.FMSketches))
	for _, sketch := range s.FMSketches {
		pbFMSketches = append(pbFMSketches, FMSketchToProto(sketch))
	}
	collector := &tipb.RowSampleCollector{
		Samples:    RowSamplesToProto(s.Samples),
		NullCounts: s.NullCount,
		Count:      s.Count,
		FmSketch:   pbFMSketches,
		TotalSize:  s.TotalSizes,
	}
	return collector
}

// FromProto constructs the collector from the proto struct.
func (s *ReservoirRowSampleCollector) FromProto(pbCollector *tipb.RowSampleCollector) {
	s.baseCollector = &baseCollector{}
	s.Count = pbCollector.Count
	s.NullCount = pbCollector.NullCounts
	s.FMSketches = make([]*FMSketch, 0, len(pbCollector.FmSketch))
	for _, pbSketch := range pbCollector.FmSketch {
		s.FMSketches = append(s.FMSketches, FMSketchFromProto(pbSketch))
	}
	s.TotalSizes = pbCollector.TotalSize
	s.Samples = make(WeightedRowSampleHeap, 0, len(pbCollector.Samples))
	for _, pbSample := range pbCollector.Samples {
		data := make([]types.Datum, 0, len(pbSample.Row))
		for _, col := range pbSample.Row {
			b := make([]byte, len(col))
			copy(b, col)
			data = append(data, types.NewBytesDatum(b))
		}
		// The samples collected from regions are also organized by binary heap. So we can just copy the slice.
		// No need to maintain the heap again.
		s.Samples = append(s.Samples, &ReservoirRowSampleItem{
			Columns: data,
			Weight:  pbSample.Weight,
		})
	}
}

// MergeCollector merges the collectors to a final one.
func (s *ReservoirRowSampleCollector) MergeCollector(subCollector *ReservoirRowSampleCollector) {
	s.Count += subCollector.Count
	for i := range subCollector.FMSketches {
		s.FMSketches[i].MergeFMSketch(subCollector.FMSketches[i])
	}
	for i := range subCollector.NullCount {
		s.NullCount[i] += subCollector.NullCount[i]
	}
	for i := range subCollector.TotalSizes {
		s.TotalSizes[i] += subCollector.TotalSizes[i]
	}
	for _, sample := range subCollector.Samples {
		s.sampleZippedRow(sample)
	}
}

// RowSamplesToProto converts the samp slice to the pb struct.
func RowSamplesToProto(samples WeightedRowSampleHeap) []*tipb.RowSample {
	if len(samples) == 0 {
		return nil
	}
	rows := make([]*tipb.RowSample, 0, len(samples))
	colLen := len(samples[0].Columns)
	for _, sample := range samples {
		pbRow := &tipb.RowSample{
			Row:    make([][]byte, 0, colLen),
			Weight: sample.Weight,
		}
		for _, c := range sample.Columns {
			if c.IsNull() {
				pbRow.Row = append(pbRow.Row, []byte{codec.NilFlag})
				continue
			}
			pbRow.Row = append(pbRow.Row, c.GetBytes())
		}
		rows = append(rows, pbRow)
	}
	return rows
}
