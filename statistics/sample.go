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
	"context"
	"sort"
	"time"
	"unsafe"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/fastrand"
	"github.com/pingcap/tidb/util/sqlexec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/twmb/murmur3"
)

// SampleItem is an item of sampled column value.
type SampleItem struct {
	// Value is the sampled column value.
	Value types.Datum
	// Ordinal is original position of this item in SampleCollector before sorting. This
	// is used for computing correlation.
	Ordinal int
	// Handle is the handle of the sample in its key.
	// This property is used to calculate Ordinal in fast analyze.
	Handle kv.Handle
}

// EmptySampleItemSize is the size of empty SampleItem, 96 = 72 (datum) + 8 (int) + 16.
const EmptySampleItemSize = int64(unsafe.Sizeof(SampleItem{}))

// CopySampleItems returns a deep copy of SampleItem slice.
func CopySampleItems(items []*SampleItem) []*SampleItem {
	n := make([]*SampleItem, len(items))
	for i, item := range items {
		ni := *item
		n[i] = &ni
	}
	return n
}

// SortSampleItems shallow copies and sorts a slice of SampleItem.
func SortSampleItems(sc *stmtctx.StatementContext, items []*SampleItem) ([]*SampleItem, error) {
	sortedItems := make([]*SampleItem, len(items))
	copy(sortedItems, items)
	sorter := sampleItemSorter{items: sortedItems, sc: sc}
	sort.Stable(&sorter)
	return sortedItems, sorter.err
}

type sampleItemSorter struct {
	items []*SampleItem
	sc    *stmtctx.StatementContext
	err   error
}

func (s *sampleItemSorter) Len() int {
	return len(s.items)
}

func (s *sampleItemSorter) Less(i, j int) bool {
	var cmp int
	cmp, s.err = s.items[i].Value.Compare(s.sc, &s.items[j].Value, collate.GetBinaryCollator())
	if s.err != nil {
		return true
	}
	return cmp < 0
}

func (s *sampleItemSorter) Swap(i, j int) {
	s.items[i], s.items[j] = s.items[j], s.items[i]
}

// SampleCollector will collect Samples and calculate the count and ndv of an attribute.
type SampleCollector struct {
	Samples       []*SampleItem
	seenValues    int64 // seenValues is the current seen values.
	IsMerger      bool
	NullCount     int64
	Count         int64 // Count is the number of non-null rows.
	MaxSampleSize int64
	FMSketch      *FMSketch
	CMSketch      *CMSketch
	TopN          *TopN
	TotalSize     int64 // TotalSize is the total size of column.
	MemSize       int64 // major memory size of this sample collector.
}

// MergeSampleCollector merges two sample collectors.
func (c *SampleCollector) MergeSampleCollector(sc *stmtctx.StatementContext, rc *SampleCollector) {
	c.NullCount += rc.NullCount
	c.Count += rc.Count
	c.TotalSize += rc.TotalSize
	c.FMSketch.MergeFMSketch(rc.FMSketch)
	if rc.CMSketch != nil {
		err := c.CMSketch.MergeCMSketch(rc.CMSketch)
		terror.Log(errors.Trace(err))
	}
	for _, item := range rc.Samples {
		err := c.collect(sc, item.Value)
		terror.Log(errors.Trace(err))
	}
}

// SampleCollectorToProto converts SampleCollector to its protobuf representation.
func SampleCollectorToProto(c *SampleCollector) *tipb.SampleCollector {
	collector := &tipb.SampleCollector{
		NullCount: c.NullCount,
		Count:     c.Count,
		FmSketch:  FMSketchToProto(c.FMSketch),
		TotalSize: &c.TotalSize,
	}
	if c.CMSketch != nil {
		collector.CmSketch = CMSketchToProto(c.CMSketch, nil)
	}
	for _, item := range c.Samples {
		collector.Samples = append(collector.Samples, item.Value.GetBytes())
	}
	return collector
}

// MaxSampleValueLength defines the max length of the useful samples. If one sample value exceeds the max length, we drop it before building the stats.
const MaxSampleValueLength = mysql.MaxFieldVarCharLength / 2

// SampleCollectorFromProto converts SampleCollector from its protobuf representation.
func SampleCollectorFromProto(collector *tipb.SampleCollector) *SampleCollector {
	s := &SampleCollector{
		NullCount: collector.NullCount,
		Count:     collector.Count,
		FMSketch:  FMSketchFromProto(collector.FmSketch),
	}
	if collector.TotalSize != nil {
		s.TotalSize = *collector.TotalSize
	}
	s.CMSketch, s.TopN = CMSketchAndTopNFromProto(collector.CmSketch)
	for _, val := range collector.Samples {
		// When store the histogram bucket boundaries to kv, we need to limit the length of the value.
		if len(val) <= MaxSampleValueLength {
			item := &SampleItem{Value: types.NewBytesDatum(val)}
			s.Samples = append(s.Samples, item)
		}
	}
	return s
}

func (c *SampleCollector) collect(sc *stmtctx.StatementContext, d types.Datum) error {
	if !c.IsMerger {
		if d.IsNull() {
			c.NullCount++
			return nil
		}
		c.Count++
		if err := c.FMSketch.InsertValue(sc, d); err != nil {
			return errors.Trace(err)
		}
		if c.CMSketch != nil {
			c.CMSketch.InsertBytes(d.GetBytes())
		}
		// Minus one is to remove the flag byte.
		c.TotalSize += int64(len(d.GetBytes()) - 1)
	}
	c.seenValues++
	// The following code use types.CloneDatum(d) because d may have a deep reference
	// to the underlying slice, GC can't free them which lead to memory leak eventually.
	// TODO: Refactor the proto to avoid copying here.
	if len(c.Samples) < int(c.MaxSampleSize) {
		newItem := &SampleItem{}
		d.Copy(&newItem.Value)
		c.Samples = append(c.Samples, newItem)
	} else {
		shouldAdd := int64(fastrand.Uint64N(uint64(c.seenValues))) < c.MaxSampleSize
		if shouldAdd {
			idx := int(fastrand.Uint32N(uint32(c.MaxSampleSize)))
			newItem := &SampleItem{}
			d.Copy(&newItem.Value)
			// To keep the order of the elements, we use delete and append, not direct replacement.
			c.Samples = append(c.Samples[:idx], c.Samples[idx+1:]...)
			c.Samples = append(c.Samples, newItem)
		}
	}
	return nil
}

// CalcTotalSize is to calculate total size based on samples.
func (c *SampleCollector) CalcTotalSize() {
	c.TotalSize = 0
	for _, item := range c.Samples {
		c.TotalSize += int64(len(item.Value.GetBytes()))
	}
}

// SampleBuilder is used to build samples for columns.
// Also, if primary key is handle, it will directly build histogram for it.
type SampleBuilder struct {
	Sc              *stmtctx.StatementContext
	RecordSet       sqlexec.RecordSet
	ColLen          int // ColLen is the number of columns need to be sampled.
	PkBuilder       *SortedBuilder
	MaxBucketSize   int64
	MaxSampleSize   int64
	MaxFMSketchSize int64
	CMSketchDepth   int32
	CMSketchWidth   int32
	Collators       []collate.Collator
	ColsFieldType   []*types.FieldType
}

// CollectColumnStats collects sample from the result set using Reservoir Sampling algorithm,
// and estimates NDVs using FM Sketch during the collecting process.
// It returns the sample collectors which contain total count, null count, distinct values count and CM Sketch.
// It also returns the statistic builder for PK which contains the histogram.
// See https://en.wikipedia.org/wiki/Reservoir_sampling
func (s SampleBuilder) CollectColumnStats() ([]*SampleCollector, *SortedBuilder, error) {
	collectors := make([]*SampleCollector, s.ColLen)
	for i := range collectors {
		collectors[i] = &SampleCollector{
			MaxSampleSize: s.MaxSampleSize,
			FMSketch:      NewFMSketch(int(s.MaxFMSketchSize)),
		}
	}
	if s.CMSketchDepth > 0 && s.CMSketchWidth > 0 {
		for i := range collectors {
			collectors[i].CMSketch = NewCMSketch(s.CMSketchDepth, s.CMSketchWidth)
		}
	}
	ctx := context.TODO()
	req := s.RecordSet.NewChunk(nil)
	it := chunk.NewIterator4Chunk(req)
	for {
		err := s.RecordSet.Next(ctx, req)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if req.NumRows() == 0 {
			return collectors, s.PkBuilder, nil
		}
		if len(s.RecordSet.Fields()) == 0 {
			return nil, nil, errors.Errorf("collect column stats failed: record set has 0 field")
		}
		for row := it.Begin(); row != it.End(); row = it.Next() {
			datums := RowToDatums(row, s.RecordSet.Fields())
			if s.PkBuilder != nil {
				err = s.PkBuilder.Iterate(datums[0])
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
				datums = datums[1:]
			}
			for i, val := range datums {
				if s.Collators[i] != nil && !val.IsNull() {
					decodedVal, err := tablecodec.DecodeColumnValue(val.GetBytes(), s.ColsFieldType[i], s.Sc.TimeZone)
					if err != nil {
						return nil, nil, err
					}
					decodedVal.SetBytesAsString(s.Collators[i].Key(decodedVal.GetString()), decodedVal.Collation(), uint32(decodedVal.Length()))
					encodedKey, err := tablecodec.EncodeValue(s.Sc, nil, decodedVal)
					if err != nil {
						return nil, nil, err
					}
					val.SetBytes(encodedKey)
				}
				err = collectors[i].collect(s.Sc, val)
				if err != nil {
					return nil, nil, errors.Trace(err)
				}
			}
		}
	}
}

// RowToDatums converts row to datum slice.
func RowToDatums(row chunk.Row, fields []*ast.ResultField) []types.Datum {
	datums := make([]types.Datum, len(fields))
	for i, f := range fields {
		datums[i] = row.GetDatum(i, &f.Column.FieldType)
	}
	return datums
}

// ExtractTopN extracts the topn from the CM Sketch.
func (c *SampleCollector) ExtractTopN(numTop uint32, sc *stmtctx.StatementContext, tp *types.FieldType, timeZone *time.Location) error {
	if numTop == 0 {
		return nil
	}
	values := make([][]byte, 0, len(c.Samples))
	for _, sample := range c.Samples {
		values = append(values, sample.Value.GetBytes())
	}
	helper := newTopNHelper(values, numTop)
	cms := c.CMSketch
	c.TopN = NewTopN(int(helper.actualNumTop))
	// Process them decreasingly so we can handle most frequent values first and reduce the probability of hash collision
	// by small values.
	for i := uint32(0); i < helper.actualNumTop; i++ {
		h1, h2 := murmur3.Sum128(helper.sorted[i].data)
		realCnt := cms.queryHashValue(h1, h2)
		// Because the encode of topn is the new encode type. But analyze proto returns the old encode type for a sample datum,
		// we should decode it and re-encode it to get the correct bytes.
		d, err := tablecodec.DecodeColumnValue(helper.sorted[i].data, tp, timeZone)
		if err != nil {
			return err
		}
		data, err := tablecodec.EncodeValue(sc, nil, d)
		if err != nil {
			return err
		}
		cms.SubValue(h1, h2, realCnt)
		c.TopN.AppendTopN(data, realCnt)
	}
	c.TopN.Sort()
	return nil
}
