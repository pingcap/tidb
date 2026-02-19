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
	"hash"
	"math/rand"
	"unsafe"

	"github.com/dolthub/swiss"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/memory"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
)

// RowSampleCollector implements the needed interface for a row-based sample collector.
type RowSampleCollector interface {
	MergeCollector(collector RowSampleCollector)
	sampleRow(row []types.Datum, rng *rand.Rand)
	Base() *baseCollector
	DestroyAndPutToPool()
}

type baseCollector struct {
	Samples           WeightedRowSampleHeap
	NullCount         []int64
	FMSketches        []*FMSketch
	F1Sketches        []*FMSketch
	HLLSketches       []*HLLSketch
	F1HLLSketches     []*HLLSketch
	f1Builders        []*f1Sketch
	TotalSizes        []int64
	Count             int64
	SketchSampleCount int64
	MemSize           int64
}

// ReservoirRowSampleCollector collects the samples from the source and organize the samples by row.
// It will maintain the following things:
//
//	Row samples.
//	FM sketches(To calculate the NDV).
//	Null counts.
//	The data sizes.
//	The number of rows.
//
// It uses weighted reservoir sampling(A-Res) to do the sampling.
type ReservoirRowSampleCollector struct {
	*baseCollector
	MaxSampleSize int
}

type f1Sketch struct {
	mask  uint64
	once  *swiss.Map[uint64, bool]
	multi *swiss.Map[uint64, bool]
}

func newF1Sketch() *f1Sketch {
	return &f1Sketch{
		mask:  0,
		once:  swiss.NewMap[uint64, bool](0),
		multi: swiss.NewMap[uint64, bool](0),
	}
}

func (s *f1Sketch) insertHashValue(hashVal uint64) {
	if (hashVal & s.mask) != 0 {
		return
	}
	if _, ok := s.multi.Get(hashVal); ok {
		return
	}
	if _, ok := s.once.Get(hashVal); ok {
		s.once.Delete(hashVal)
		s.multi.Put(hashVal, true)
	} else {
		s.once.Put(hashVal, true)
	}
}

func (s *f1Sketch) InsertValue(sc *stmtctx.StatementContext, value types.Datum) error {
	hashVal, err := hashDatum(sc, value)
	if err != nil {
		return err
	}
	s.insertHashValue(hashVal)
	return nil
}

func (s *f1Sketch) InsertRowValue(sc *stmtctx.StatementContext, values []types.Datum) error {
	hashVal, err := hashRow(sc, values)
	if err != nil {
		return err
	}
	s.insertHashValue(hashVal)
	return nil
}

func (s *f1Sketch) toFMSketch(maxSketchSize int) *FMSketch {
	if s == nil {
		return nil
	}
	sketch := NewFMSketch(maxSketchSize)
	sketch.mask = s.mask
	s.once.Iter(func(k uint64, _ bool) (stop bool) {
		sketch.hashset.Put(k, true)
		return false
	})
	return sketch
}

func (s *f1Sketch) toHLLSketch(b uint8) *HLLSketch {
	if s == nil {
		return nil
	}
	sketch := NewHLLSketch(b)
	s.once.Iter(func(k uint64, _ bool) (stop bool) {
		sketch.InsertHash(k)
		return false
	})
	return sketch
}

func (s *f1Sketch) reset() {
	if s == nil {
		return
	}
	s.once.Clear()
	s.multi.Clear()
	s.mask = 0
}

// ReservoirRowSampleItem is the item for the ReservoirRowSampleCollector. The weight is needed for the sampling algorithm.
type ReservoirRowSampleItem struct {
	Handle  kv.Handle
	Columns []types.Datum
	Weight  int64
}

// EmptyReservoirSampleItemSize = (24 + 16 + 8) now.
const EmptyReservoirSampleItemSize = int64(unsafe.Sizeof(ReservoirRowSampleItem{}))

const sketchSampleRate = 0.01

// MemUsage returns the memory usage of sample item.
func (i ReservoirRowSampleItem) MemUsage() (sum int64) {
	sum = EmptyReservoirSampleItemSize
	for _, col := range i.Columns {
		sum += col.MemUsage()
	}
	if i.Handle != nil {
		sum += int64(i.Handle.MemUsage())
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
func (h *WeightedRowSampleHeap) Push(i any) {
	*h = append(*h, i.(*ReservoirRowSampleItem))
}

// Pop implements the Heap interface.
func (h *WeightedRowSampleHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// RowSampleBuilder is used to construct the ReservoirRowSampleCollector to get the samples.
type RowSampleBuilder struct {
	RecordSet       sqlexec.RecordSet
	Sc              *stmtctx.StatementContext
	Rng             *rand.Rand
	ColsFieldType   []*types.FieldType
	Collators       []collate.Collator
	ColGroups       [][]int64
	MaxSampleSize   int
	SampleRate      float64
	MaxFMSketchSize int
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
		Samples:       make(WeightedRowSampleHeap, 0, maxSampleSize),
		NullCount:     make([]int64, totalLen),
		FMSketches:    make([]*FMSketch, 0, totalLen),
		F1Sketches:    make([]*FMSketch, 0, totalLen),
		HLLSketches:   make([]*HLLSketch, 0, totalLen),
		F1HLLSketches: make([]*HLLSketch, 0, totalLen),
		f1Builders:    make([]*f1Sketch, 0, totalLen),
		TotalSizes:    make([]int64, totalLen),
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
	for range len(s.ColsFieldType) + len(s.ColGroups) {
		collector.Base().FMSketches = append(collector.Base().FMSketches, NewFMSketch(s.MaxFMSketchSize))
		collector.Base().HLLSketches = append(collector.Base().HLLSketches, NewHLLSketch(DefaultHLLPrecision))
		collector.Base().f1Builders = append(collector.Base().f1Builders, newF1Sketch())
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
			break
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
					decodedVal, err := tablecodec.DecodeColumnValue(val.GetBytes(), s.ColsFieldType[i], s.Sc.TimeZone())
					if err != nil {
						return nil, err
					}
					decodedVal.SetBytesAsString(s.Collators[i].Key(decodedVal.GetString()), decodedVal.Collation(), uint32(decodedVal.Length()))
					encodedKey, err := tablecodec.EncodeValue(s.Sc.TimeZone(), nil, decodedVal)
					err = s.Sc.HandleError(err)
					if err != nil {
						return nil, err
					}
					datums[i].SetBytes(encodedKey)
				}
			}
			collectSketch := s.Rng.Float64() < sketchSampleRate
			err := collector.Base().collectColumns(s.Sc, datums, sizes, collectSketch)
			if err != nil {
				return nil, err
			}
			err = collector.Base().collectColumnGroups(s.Sc, datums, s.ColGroups, sizes, collectSketch)
			if err != nil {
				return nil, err
			}
			if collectSketch {
				collector.Base().SketchSampleCount++
			}
			collector.sampleRow(newCols, s.Rng)
		}
	}
	for i, group := range s.ColGroups {
		if len(group) != 1 {
			continue
		}
		// For the single-column group, its FMSketch is the same as that of the corresponding column. Hence, we don't
		// maintain its FMSketch in collectColumnGroups. We just copy the corresponding column's FMSketch after
		// iterating all rows. Also, we can directly copy TotalSize and NullCount.
		colIdx := group[0]
		colGroupIdx := len(s.ColsFieldType) + i
		collector.Base().FMSketches[colGroupIdx] = collector.Base().FMSketches[colIdx].Copy()
		if colIdx >= 0 && colIdx < int64(len(collector.Base().HLLSketches)) && collector.Base().HLLSketches[colIdx] != nil {
			collector.Base().HLLSketches[colGroupIdx] = collector.Base().HLLSketches[colIdx].Copy()
		} else {
			collector.Base().HLLSketches[colGroupIdx] = NewHLLSketch(DefaultHLLPrecision)
		}
		collector.Base().NullCount[colGroupIdx] = collector.Base().NullCount[colIdx]
		collector.Base().TotalSizes[colGroupIdx] = collector.Base().TotalSizes[colIdx]
	}
	collector.Base().BuildF1Sketches(s.ColGroups, s.MaxFMSketchSize)
	return collector, nil
}

func (s *baseCollector) destroyAndPutToPool() {
	for _, sketch := range s.FMSketches {
		sketch.DestroyAndPutToPool()
	}
	for _, sketch := range s.F1Sketches {
		sketch.DestroyAndPutToPool()
	}
	for i := range s.HLLSketches {
		s.HLLSketches[i] = nil
	}
	for i := range s.F1HLLSketches {
		s.F1HLLSketches[i] = nil
	}
	for _, builder := range s.f1Builders {
		builder.reset()
	}
}

func (s *baseCollector) collectColumns(sc *stmtctx.StatementContext, cols []types.Datum, sizes []int64, collectSketch bool) error {
	for i, col := range cols {
		if col.IsNull() {
			s.NullCount[i]++
			continue
		}
		// Minus one is to remove the flag byte.
		s.TotalSizes[i] += sizes[i] - 1
		if collectSketch {
			err := s.FMSketches[i].InsertValue(sc, col)
			if err != nil {
				return err
			}
			if i < len(s.HLLSketches) && s.HLLSketches[i] != nil {
				if err := s.HLLSketches[i].InsertValue(sc, col); err != nil {
					return err
				}
			}
			if i < len(s.f1Builders) && s.f1Builders[i] != nil {
				if err := s.f1Builders[i].InsertValue(sc, col); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *baseCollector) collectColumnGroups(sc *stmtctx.StatementContext, cols []types.Datum, colGroups [][]int64, sizes []int64, collectSketch bool) error {
	colLen := len(cols)
	datumBuffer := make([]types.Datum, 0, len(cols))
	for i, group := range colGroups {
		if len(group) == 1 {
			// For the single-column group, its FMSketch is the same as that of the corresponding column. Hence, we
			// don't need to maintain its FMSketch. We just copy the corresponding column's FMSketch after iterating
			// all rows. Also, we can directly copy TotalSize and NullCount.
			continue
		}
		// We don't maintain the null counts information for the multi-column group.
		datumBuffer = datumBuffer[:0]
		for _, c := range group {
			datumBuffer = append(datumBuffer, cols[c])
			if !cols[c].IsNull() {
				s.TotalSizes[colLen+i] += sizes[c] - 1
			}
		}
		if collectSketch {
			err := s.FMSketches[colLen+i].InsertRowValue(sc, datumBuffer)
			if err != nil {
				return err
			}
			if colLen+i < len(s.HLLSketches) && s.HLLSketches[colLen+i] != nil {
				if err := s.HLLSketches[colLen+i].InsertRowValue(sc, datumBuffer); err != nil {
					return err
				}
			}
			if colLen+i < len(s.f1Builders) && s.f1Builders[colLen+i] != nil {
				if err := s.f1Builders[colLen+i].InsertRowValue(sc, datumBuffer); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func hashDatum(sc *stmtctx.StatementContext, value types.Datum) (uint64, error) {
	bytes, err := codec.EncodeValue(sc.TimeZone(), nil, value)
	err = sc.HandleError(err)
	if err != nil {
		return 0, err
	}
	hashFunc := murmur3Pool.Get().(hash.Hash64)
	hashFunc.Reset()
	defer murmur3Pool.Put(hashFunc)
	_, err = hashFunc.Write(bytes)
	if err != nil {
		return 0, err
	}
	return hashFunc.Sum64(), nil
}

func hashRow(sc *stmtctx.StatementContext, values []types.Datum) (uint64, error) {
	hashFunc := murmur3Pool.Get().(hash.Hash64)
	hashFunc.Reset()
	defer murmur3Pool.Put(hashFunc)

	errCtx := sc.ErrCtx()
	b := make([]byte, 0, 8)
	for _, v := range values {
		b = b[:0]
		b, err := codec.EncodeValue(sc.TimeZone(), b, v)
		err = errCtx.HandleError(err)
		if err != nil {
			return 0, err
		}
		_, err = hashFunc.Write(b)
		if err != nil {
			return 0, err
		}
	}
	return hashFunc.Sum64(), nil
}

// BuildF1Sketches constructs F1 sketches from full data.
// F1 sketch contains the values that appear exactly once in the whole data set.
func (s *baseCollector) BuildF1Sketches(colGroups [][]int64, maxSketchSize int) {
	totalLen := len(s.NullCount)
	if totalLen == 0 {
		return
	}
	colLen := totalLen - len(colGroups)
	if colLen < 0 {
		colLen = 0
	}
	s.F1Sketches = make([]*FMSketch, totalLen)
	s.F1HLLSketches = make([]*HLLSketch, totalLen)
	for i := 0; i < colLen; i++ {
		if i < len(s.f1Builders) && s.f1Builders[i] != nil {
			s.F1Sketches[i] = s.f1Builders[i].toFMSketch(maxSketchSize)
			s.F1HLLSketches[i] = s.f1Builders[i].toHLLSketch(DefaultHLLPrecision)
		} else {
			s.F1Sketches[i] = NewFMSketch(maxSketchSize)
			s.F1HLLSketches[i] = NewHLLSketch(DefaultHLLPrecision)
		}
	}
	for i, group := range colGroups {
		idx := colLen + i
		if len(group) == 1 {
			colIdx := int(group[0])
			if colIdx >= 0 && colIdx < colLen && s.F1Sketches[colIdx] != nil {
				s.F1Sketches[idx] = s.F1Sketches[colIdx].Copy()
				if s.F1HLLSketches[colIdx] != nil {
					s.F1HLLSketches[idx] = s.F1HLLSketches[colIdx].Copy()
				} else {
					s.F1HLLSketches[idx] = NewHLLSketch(DefaultHLLPrecision)
				}
			} else {
				s.F1Sketches[idx] = NewFMSketch(maxSketchSize)
				s.F1HLLSketches[idx] = NewHLLSketch(DefaultHLLPrecision)
			}
			continue
		}
		if idx < len(s.f1Builders) && s.f1Builders[idx] != nil {
			s.F1Sketches[idx] = s.f1Builders[idx].toFMSketch(maxSketchSize)
			s.F1HLLSketches[idx] = s.f1Builders[idx].toHLLSketch(DefaultHLLPrecision)
		} else {
			s.F1Sketches[idx] = NewFMSketch(maxSketchSize)
			s.F1HLLSketches[idx] = NewHLLSketch(DefaultHLLPrecision)
		}
	}
}

// ToProto converts the collector to pb struct.
func (s *baseCollector) ToProto() *tipb.RowSampleCollector {
	pbFMSketches := make([]*tipb.FMSketch, 0, len(s.FMSketches))
	for _, sketch := range s.FMSketches {
		pbFMSketches = append(pbFMSketches, FMSketchToProto(sketch))
	}
	pbF1Sketches := make([]*tipb.FMSketch, 0, len(s.F1Sketches))
	for _, sketch := range s.F1Sketches {
		pbF1Sketches = append(pbF1Sketches, FMSketchToProto(sketch))
	}
	pbHLLSketches := make([]*tipb.HLLSketch, 0, len(s.HLLSketches))
	for _, sketch := range s.HLLSketches {
		pbHLLSketches = append(pbHLLSketches, HLLSketchToProto(sketch))
	}
	pbF1HLLSketches := make([]*tipb.HLLSketch, 0, len(s.F1HLLSketches))
	for _, sketch := range s.F1HLLSketches {
		pbF1HLLSketches = append(pbF1HLLSketches, HLLSketchToProto(sketch))
	}
	collector := &tipb.RowSampleCollector{
		Samples:           RowSamplesToProto(s.Samples),
		NullCounts:        s.NullCount,
		Count:             s.Count,
		FmSketch:          pbFMSketches,
		F1Sketch:          pbF1Sketches,
		HllSketch:         pbHLLSketches,
		HllF1Sketch:       pbF1HLLSketches,
		TotalSize:         s.TotalSizes,
		SketchSampleCount: s.SketchSampleCount,
	}
	return collector
}

func (s *baseCollector) FromProto(pbCollector *tipb.RowSampleCollector, memTracker *memory.Tracker) {
	s.Count = pbCollector.Count
	s.NullCount = pbCollector.NullCounts
	s.FMSketches = make([]*FMSketch, 0, len(pbCollector.FmSketch))
	s.f1Builders = nil
	s.SketchSampleCount = pbCollector.SketchSampleCount
	for _, pbSketch := range pbCollector.FmSketch {
		s.FMSketches = append(s.FMSketches, FMSketchFromProto(pbSketch))
	}
	s.F1Sketches = make([]*FMSketch, 0, len(pbCollector.F1Sketch))
	for _, pbSketch := range pbCollector.F1Sketch {
		s.F1Sketches = append(s.F1Sketches, FMSketchFromProto(pbSketch))
	}
	s.HLLSketches = make([]*HLLSketch, 0, len(pbCollector.HllSketch))
	for _, pbSketch := range pbCollector.HllSketch {
		s.HLLSketches = append(s.HLLSketches, HLLSketchFromProto(pbSketch))
	}
	s.F1HLLSketches = make([]*HLLSketch, 0, len(pbCollector.HllF1Sketch))
	for _, pbSketch := range pbCollector.HllF1Sketch {
		s.F1HLLSketches = append(s.F1HLLSketches, HLLSketchFromProto(pbSketch))
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
			data = append(data, types.NewBytesDatum(col))
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
	s.SketchSampleCount += subCollector.Base().SketchSampleCount
	for i, fms := range subCollector.Base().FMSketches {
		s.FMSketches[i].MergeFMSketch(fms)
	}
	for i, hll := range subCollector.Base().HLLSketches {
		if hll == nil {
			continue
		}
		if i >= len(s.HLLSketches) {
			s.HLLSketches = append(s.HLLSketches, make([]*HLLSketch, i+1-len(s.HLLSketches))...)
		}
		if s.HLLSketches[i] == nil {
			s.HLLSketches[i] = hll.Copy()
		} else {
			s.HLLSketches[i].Merge(hll)
		}
	}
	if len(subCollector.Base().F1Sketches) > 0 {
		if len(s.F1Sketches) < len(subCollector.Base().F1Sketches) {
			s.F1Sketches = append(s.F1Sketches, make([]*FMSketch, len(subCollector.Base().F1Sketches)-len(s.F1Sketches))...)
		}
		for i, f1 := range subCollector.Base().F1Sketches {
			if f1 == nil {
				continue
			}
			if s.F1Sketches[i] == nil {
				s.F1Sketches[i] = f1.Copy()
			} else {
				s.F1Sketches[i].MergeFMSketch(f1)
			}
		}
	}
	if len(subCollector.Base().F1HLLSketches) > 0 {
		if len(s.F1HLLSketches) < len(subCollector.Base().F1HLLSketches) {
			s.F1HLLSketches = append(s.F1HLLSketches, make([]*HLLSketch, len(subCollector.Base().F1HLLSketches)-len(s.F1HLLSketches))...)
		}
		for i, f1 := range subCollector.Base().F1HLLSketches {
			if f1 == nil {
				continue
			}
			if s.F1HLLSketches[i] == nil {
				s.F1HLLSketches[i] = f1.Copy()
			} else {
				s.F1HLLSketches[i].Merge(f1)
			}
		}
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

// DestroyAndPutToPool implements the interface RowSampleCollector.
func (s *ReservoirRowSampleCollector) DestroyAndPutToPool() {
	s.baseCollector.destroyAndPutToPool()
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
//
//	Row samples.
//	FM sketches(To calculate the NDV).
//	Null counts.
//	The data sizes.
//	The number of rows.
//
// It uses the bernoulli sampling to collect the data.
type BernoulliRowSampleCollector struct {
	*baseCollector
	SampleRate float64
}

// NewBernoulliRowSampleCollector creates the new collector by the given inputs.
func NewBernoulliRowSampleCollector(sampleRate float64, totalLen int) *BernoulliRowSampleCollector {
	base := &baseCollector{
		Samples:       make(WeightedRowSampleHeap, 0, 8),
		NullCount:     make([]int64, totalLen),
		FMSketches:    make([]*FMSketch, 0, totalLen),
		HLLSketches:   make([]*HLLSketch, 0, totalLen),
		F1HLLSketches: make([]*HLLSketch, 0, totalLen),
		f1Builders:    make([]*f1Sketch, 0, totalLen),
		TotalSizes:    make([]int64, totalLen),
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
	s.SketchSampleCount += subCollector.Base().SketchSampleCount
	for i := range subCollector.Base().FMSketches {
		s.FMSketches[i].MergeFMSketch(subCollector.Base().FMSketches[i])
	}
	for i, hll := range subCollector.Base().HLLSketches {
		if hll == nil {
			continue
		}
		if i >= len(s.HLLSketches) {
			s.HLLSketches = append(s.HLLSketches, make([]*HLLSketch, i+1-len(s.HLLSketches))...)
		}
		if s.HLLSketches[i] == nil {
			s.HLLSketches[i] = hll.Copy()
		} else {
			s.HLLSketches[i].Merge(hll)
		}
	}
	if len(subCollector.Base().F1Sketches) > 0 {
		if len(s.F1Sketches) < len(subCollector.Base().F1Sketches) {
			s.F1Sketches = append(s.F1Sketches, make([]*FMSketch, len(subCollector.Base().F1Sketches)-len(s.F1Sketches))...)
		}
		for i, f1 := range subCollector.Base().F1Sketches {
			if f1 == nil {
				continue
			}
			if s.F1Sketches[i] == nil {
				s.F1Sketches[i] = f1.Copy()
			} else {
				s.F1Sketches[i].MergeFMSketch(f1)
			}
		}
	}
	if len(subCollector.Base().F1HLLSketches) > 0 {
		if len(s.F1HLLSketches) < len(subCollector.Base().F1HLLSketches) {
			s.F1HLLSketches = append(s.F1HLLSketches, make([]*HLLSketch, len(subCollector.Base().F1HLLSketches)-len(s.F1HLLSketches))...)
		}
		for i, f1 := range subCollector.Base().F1HLLSketches {
			if f1 == nil {
				continue
			}
			if s.F1HLLSketches[i] == nil {
				s.F1HLLSketches[i] = f1.Copy()
			} else {
				s.F1HLLSketches[i].Merge(f1)
			}
		}
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

// DestroyAndPutToPool implements the interface RowSampleCollector.
func (s *BernoulliRowSampleCollector) DestroyAndPutToPool() {
	s.baseCollector.destroyAndPutToPool()
}
