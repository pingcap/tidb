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
	"unsafe"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
)

// RowSampleCollector implements the needed interface for a row-based sample collector.
type RowSampleCollector interface {
	MergeCollector(collector RowSampleCollector)
	sampleRow(row []types.Datum, rng *rand.Rand)
	Base() *baseCollector
}

type baseCollector struct {
	Samples    WeightedRowSampleHeap
	NullCount  []int64
	FMSketches []*FMSketch
	TotalSizes []int64
	Count      int64
	MemSize    int64
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

// EmptyReservoirSampleItemSize = (24 + 16 + 8) now.
const EmptyReservoirSampleItemSize = int64(unsafe.Sizeof(ReservoirRowSampleItem{}))

// MemUsage returns the memory usage of sample item.
func (i ReservoirRowSampleItem) MemUsage() (sum int64) {
	sum = EmptyReservoirSampleItemSize
	for _, col := range i.Columns {
		sum += col.MemUsage()
	}
	if i.Handle != nil {
		sum += i.Handle.MemUsage()
	}
	return sum
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

// RowSampleBuilder is used to construct the ReservoirRowSampleCollector to get the samples.
type RowSampleBuilder struct {
	Sc              *stmtctx.StatementContext
	RecordSet       sqlexec.RecordSet
	ColsFieldType   []*types.FieldType
	Collators       []collate.Collator
	ColGroups       [][]int64
	MaxSampleSize   int
	SampleRate      float64
	MaxFMSketchSize int
	Rng             *rand.Rand
}

// NewRowSampleCollector creates a collector from the given inputs.
func NewRowSampleCollector(maxSampleSize int, sampleRate float64, totalLen int) RowSampleCollector {
	if maxSampleSize > 0 {
		return NewReservoirRowSampleCollector(maxSampleSize, totalLen)
	}
	if sampleRate > 0 {
		return NewBernoulliRowSampleCollector(sampleRate, totalLen)
	}
	return nil
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
func (s *RowSampleBuilder) Collect() (RowSampleCollector, error) {
	collector := NewRowSampleCollector(s.MaxSampleSize, s.SampleRate, len(s.ColsFieldType)+len(s.ColGroups))
	for i := 0; i < len(s.ColsFieldType)+len(s.ColGroups); i++ {
		collector.Base().FMSketches = append(collector.Base().FMSketches, NewFMSketch(s.MaxFMSketchSize))
	}
	ctx := context.TODO()
	chk := s.RecordSet.NewChunk(nil)
	it := chunk.NewIterator4Chunk(chk)
	for {
		err := s.RecordSet.Next(ctx, chk)
		if err != nil {
			return nil, err
		}
		if chk.NumRows() == 0 {
			return collector, nil
		}
		collector.Base().Count += int64(chk.NumRows())
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
			err := collector.Base().collectColumns(s.Sc, datums, sizes)
			if err != nil {
				return nil, err
			}
			err = collector.Base().collectColumnGroups(s.Sc, datums, s.ColGroups, sizes)
			if err != nil {
				return nil, err
			}
			collector.sampleRow(newCols, s.Rng)
		}
	}
}

func (s *baseCollector) collectColumns(sc *stmtctx.StatementContext, cols []types.Datum, sizes []int64) error {
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

func (s *baseCollector) collectColumnGroups(sc *stmtctx.StatementContext, cols []types.Datum, colGroups [][]int64, sizes []int64) error {
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

// ToProto converts the collector to pb struct.
func (s *baseCollector) ToProto() *tipb.RowSampleCollector {
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

func (s *baseCollector) FromProto(pbCollector *tipb.RowSampleCollector, memTracker *memory.Tracker) {
	s.Count = pbCollector.Count
	s.NullCount = pbCollector.NullCounts
	s.FMSketches = make([]*FMSketch, 0, len(pbCollector.FmSketch))
	for _, pbSketch := range pbCollector.FmSketch {
		s.FMSketches = append(s.FMSketches, FMSketchFromProto(pbSketch))
	}
	s.TotalSizes = pbCollector.TotalSize
	sampleNum := len(pbCollector.Samples)
	s.Samples = make(WeightedRowSampleHeap, 0, sampleNum)
	// consume mandatory memory at the beginning, including all empty ReservoirRowSampleItems and all empty Datums of all sample rows, if exceeds, fast fail
	if len(pbCollector.Samples) > 0 {
		rowLen := len(pbCollector.Samples[0].Row)
		// 8 is the size of reference
		initMemSize := int64(sampleNum) * (int64(rowLen)*types.EmptyDatumSize + EmptyReservoirSampleItemSize + 8)
		s.MemSize += initMemSize
		memTracker.Consume(initMemSize)
	}
	bufferedMemSize := int64(0)
	for _, pbSample := range pbCollector.Samples {
		rowLen := len(pbSample.Row)
		data := make([]types.Datum, 0, rowLen)
		for _, col := range pbSample.Row {
			b := make([]byte, len(col))
			copy(b, col)
			data = append(data, types.NewBytesDatum(b))
		}
		// Directly copy the weight.
		sampleItem := &ReservoirRowSampleItem{Columns: data, Weight: pbSample.Weight}
		s.Samples = append(s.Samples, sampleItem)
		deltaSize := sampleItem.MemUsage() - EmptyReservoirSampleItemSize - int64(rowLen)*types.EmptyDatumSize
		memTracker.BufferedConsume(&bufferedMemSize, deltaSize)
		s.MemSize += deltaSize
	}
	memTracker.Consume(bufferedMemSize)
}

// Base implements the RowSampleCollector interface.
func (s *ReservoirRowSampleCollector) Base() *baseCollector {
	return s.baseCollector
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

func (s *ReservoirRowSampleCollector) sampleRow(row []types.Datum, rng *rand.Rand) {
	weight := rng.Int63()
	if len(s.Samples) < s.MaxSampleSize {
		s.Samples = append(s.Samples, &ReservoirRowSampleItem{
			Columns: row,
			Weight:  weight,
		})
		if len(s.Samples) == s.MaxSampleSize {
			heap.Init(&s.Samples)
		}
		return
	}
	if s.Samples[0].Weight < weight {
		s.Samples[0] = &ReservoirRowSampleItem{
			Columns: row,
			Weight:  weight,
		}
		heap.Fix(&s.Samples, 0)
	}
}

// MergeCollector merges the collectors to a final one.
func (s *ReservoirRowSampleCollector) MergeCollector(subCollector RowSampleCollector) {
	s.Count += subCollector.Base().Count
	for i, fms := range subCollector.Base().FMSketches {
		s.FMSketches[i].MergeFMSketch(fms)
	}
	for i, nullCount := range subCollector.Base().NullCount {
		s.NullCount[i] += nullCount
	}
	for i, totSize := range subCollector.Base().TotalSizes {
		s.TotalSizes[i] += totSize
	}
	oldSampleNum := len(s.Samples)
	for _, sample := range subCollector.Base().Samples {
		s.sampleZippedRow(sample)
	}
	subSampleNum := len(subCollector.Base().Samples)
	newSampleNum := len(s.Samples)
	totalSampleNum := oldSampleNum + subSampleNum
	if totalSampleNum == 0 {
		s.MemSize = 0
	} else {
		s.MemSize = (s.MemSize + subCollector.Base().MemSize) * int64(newSampleNum) / int64(totalSampleNum)
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

// BernoulliRowSampleCollector collects the samples from the source and organize the sample by row.
// It will maintain the following things:
//   Row samples.
//   FM sketches(To calculate the NDV).
//   Null counts.
//   The data sizes.
//   The number of rows.
// It uses the bernoulli sampling to collect the data.
type BernoulliRowSampleCollector struct {
	*baseCollector
	SampleRate float64
}

// NewBernoulliRowSampleCollector creates the new collector by the given inputs.
func NewBernoulliRowSampleCollector(sampleRate float64, totalLen int) *BernoulliRowSampleCollector {
	base := &baseCollector{
		Samples:    make(WeightedRowSampleHeap, 0, 8),
		NullCount:  make([]int64, totalLen),
		FMSketches: make([]*FMSketch, 0, totalLen),
		TotalSizes: make([]int64, totalLen),
	}
	return &BernoulliRowSampleCollector{
		baseCollector: base,
		SampleRate:    sampleRate,
	}
}

func (s *BernoulliRowSampleCollector) sampleRow(row []types.Datum, rng *rand.Rand) {
	if rng.Float64() > s.SampleRate {
		return
	}
	s.baseCollector.Samples = append(s.baseCollector.Samples, &ReservoirRowSampleItem{
		Columns: row,
		Weight:  0,
	})
}

// MergeCollector merges the collectors to a final one.
func (s *BernoulliRowSampleCollector) MergeCollector(subCollector RowSampleCollector) {
	s.Count += subCollector.Base().Count
	for i := range subCollector.Base().FMSketches {
		s.FMSketches[i].MergeFMSketch(subCollector.Base().FMSketches[i])
	}
	for i := range subCollector.Base().NullCount {
		s.NullCount[i] += subCollector.Base().NullCount[i]
	}
	for i := range subCollector.Base().TotalSizes {
		s.TotalSizes[i] += subCollector.Base().TotalSizes[i]
	}
	s.baseCollector.Samples = append(s.baseCollector.Samples, subCollector.Base().Samples...)
	s.MemSize += subCollector.Base().MemSize
}

// Base implements the interface RowSampleCollector.
func (s *BernoulliRowSampleCollector) Base() *baseCollector {
	return s.baseCollector
}
