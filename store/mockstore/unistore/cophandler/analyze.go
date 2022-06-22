// Copyright 2019-present PingCAP, Inc.
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

package cophandler

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/twmb/murmur3"
)

// handleCopAnalyzeRequest handles coprocessor analyze request.
func handleCopAnalyzeRequest(dbReader *dbreader.DBReader, req *coprocessor.Request) *coprocessor.Response {
	resp := &coprocessor.Response{}
	if len(req.Ranges) == 0 {
		return resp
	}
	if req.GetTp() != kv.ReqTypeAnalyze {
		return resp
	}
	analyzeReq := new(tipb.AnalyzeReq)
	err := proto.Unmarshal(req.Data, analyzeReq)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	ranges, err := extractKVRanges(dbReader.StartKey, dbReader.EndKey, req.Ranges, false)
	if err != nil {
		resp.OtherError = err.Error()
		return resp
	}
	y.Assert(len(ranges) >= 1)
	if analyzeReq.Tp == tipb.AnalyzeType_TypeIndex {
		resp, err = handleAnalyzeIndexReq(dbReader, ranges, analyzeReq, req.StartTs)
	} else if analyzeReq.Tp == tipb.AnalyzeType_TypeCommonHandle {
		resp, err = handleAnalyzeCommonHandleReq(dbReader, ranges, analyzeReq, req.StartTs)
	} else if analyzeReq.Tp == tipb.AnalyzeType_TypeColumn {
		resp, err = handleAnalyzeColumnsReq(dbReader, ranges, analyzeReq, req.StartTs)
	} else if analyzeReq.Tp == tipb.AnalyzeType_TypeMixed {
		resp, err = handleAnalyzeMixedReq(dbReader, ranges, analyzeReq, req.StartTs)
	} else {
		resp, err = handleAnalyzeFullSamplingReq(dbReader, ranges, analyzeReq, req.StartTs)
	}
	if err != nil {
		resp = &coprocessor.Response{
			OtherError: err.Error(),
		}
	}
	return resp
}

func handleAnalyzeIndexReq(dbReader *dbreader.DBReader, rans []kv.KeyRange, analyzeReq *tipb.AnalyzeReq, startTS uint64) (*coprocessor.Response, error) {
	statsVer := int32(statistics.Version1)
	if analyzeReq.IdxReq.Version != nil {
		statsVer = *analyzeReq.IdxReq.Version
	}
	sctx := flagsToStatementContext(analyzeReq.Flags)
	processor := &analyzeIndexProcessor{
		sctx:         sctx,
		colLen:       int(analyzeReq.IdxReq.NumColumns),
		statsBuilder: statistics.NewSortedBuilder(sctx, analyzeReq.IdxReq.BucketSize, 0, types.NewFieldType(mysql.TypeBlob), int(statsVer)),
		statsVer:     statsVer,
	}
	if analyzeReq.IdxReq.TopNSize != nil {
		processor.topNCount = *analyzeReq.IdxReq.TopNSize
	}
	if analyzeReq.IdxReq.CmsketchDepth != nil && analyzeReq.IdxReq.CmsketchWidth != nil {
		processor.cms = statistics.NewCMSketch(*analyzeReq.IdxReq.CmsketchDepth, *analyzeReq.IdxReq.CmsketchWidth)
	}
	processor.fms = statistics.NewFMSketch(int(analyzeReq.IdxReq.SketchSize))
	for _, ran := range rans {
		err := dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, startTS, processor)
		if err != nil {
			return nil, err
		}
	}
	if statsVer >= statistics.Version2 {
		if processor.topNCurValuePair.Count != 0 {
			processor.topNValuePairs = append(processor.topNValuePairs, processor.topNCurValuePair)
		}
		sort.Slice(processor.topNValuePairs, func(i, j int) bool {
			if processor.topNValuePairs[i].Count > processor.topNValuePairs[j].Count {
				return true
			} else if processor.topNValuePairs[i].Count < processor.topNValuePairs[j].Count {
				return false
			}
			return bytes.Compare(processor.topNValuePairs[i].Encoded, processor.topNValuePairs[j].Encoded) < 0
		})
		if len(processor.topNValuePairs) > int(processor.topNCount) {
			processor.topNValuePairs = processor.topNValuePairs[:processor.topNCount]
		}
	}
	hg := statistics.HistogramToProto(processor.statsBuilder.Hist())
	var cm *tipb.CMSketch
	if processor.cms != nil {
		if statsVer >= statistics.Version2 {
			for _, valueCnt := range processor.topNValuePairs {
				h1, h2 := murmur3.Sum128(valueCnt.Encoded)
				processor.cms.SubValue(h1, h2, valueCnt.Count)
			}
		}
		cm = statistics.CMSketchToProto(processor.cms, &statistics.TopN{TopN: processor.topNValuePairs})
	}
	data, err := proto.Marshal(&tipb.AnalyzeIndexResp{Hist: hg, Cms: cm, Collector: &tipb.SampleCollector{FmSketch: statistics.FMSketchToProto(processor.fms)}})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &coprocessor.Response{Data: data}, nil
}

func handleAnalyzeCommonHandleReq(dbReader *dbreader.DBReader, rans []kv.KeyRange, analyzeReq *tipb.AnalyzeReq, startTS uint64) (*coprocessor.Response, error) {
	statsVer := statistics.Version1
	if analyzeReq.IdxReq.Version != nil {
		statsVer = int(*analyzeReq.IdxReq.Version)
	}
	processor := &analyzeCommonHandleProcessor{
		colLen:       int(analyzeReq.IdxReq.NumColumns),
		statsBuilder: statistics.NewSortedBuilder(flagsToStatementContext(analyzeReq.Flags), analyzeReq.IdxReq.BucketSize, 0, types.NewFieldType(mysql.TypeBlob), statsVer),
	}
	if analyzeReq.IdxReq.CmsketchDepth != nil && analyzeReq.IdxReq.CmsketchWidth != nil {
		processor.cms = statistics.NewCMSketch(*analyzeReq.IdxReq.CmsketchDepth, *analyzeReq.IdxReq.CmsketchWidth)
	}
	for _, ran := range rans {
		err := dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, startTS, processor)
		if err != nil {
			return nil, err
		}
	}
	hg := statistics.HistogramToProto(processor.statsBuilder.Hist())
	var cm *tipb.CMSketch
	if processor.cms != nil {
		cm = statistics.CMSketchToProto(processor.cms, nil)
	}
	data, err := proto.Marshal(&tipb.AnalyzeIndexResp{Hist: hg, Cms: cm})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &coprocessor.Response{Data: data}, nil
}

type analyzeIndexProcessor struct {
	skipVal

	sctx         *stmtctx.StatementContext
	colLen       int
	statsBuilder *statistics.SortedBuilder
	cms          *statistics.CMSketch
	fms          *statistics.FMSketch
	rowBuf       []byte

	statsVer         int32
	topNCount        int32
	topNValuePairs   []statistics.TopNMeta
	topNCurValuePair statistics.TopNMeta
}

func (p *analyzeIndexProcessor) Process(key, _ []byte) error {
	values, _, err := tablecodec.CutIndexKeyNew(key, p.colLen)
	if err != nil {
		return err
	}
	p.rowBuf = p.rowBuf[:0]

	for _, val := range values {
		p.rowBuf = append(p.rowBuf, val...)
		if p.cms != nil {
			p.cms.InsertBytes(p.rowBuf)
		}
	}

	if p.fms != nil {
		if err := p.fms.InsertValue(p.sctx, types.NewBytesDatum(safeCopy(p.rowBuf))); err != nil {
			return err
		}
	}

	if p.statsVer >= statistics.Version2 {
		if bytes.Equal(p.topNCurValuePair.Encoded, p.rowBuf) {
			p.topNCurValuePair.Count++
		} else {
			if p.topNCurValuePair.Count > 0 {
				p.topNValuePairs = append(p.topNValuePairs, p.topNCurValuePair)
			}
			p.topNCurValuePair.Encoded = safeCopy(p.rowBuf)
			p.topNCurValuePair.Count = 1
		}
	}
	rowData := safeCopy(p.rowBuf)
	err = p.statsBuilder.Iterate(types.NewBytesDatum(rowData))
	if err != nil {
		return err
	}
	return nil
}

type analyzeCommonHandleProcessor struct {
	skipVal

	colLen       int
	statsBuilder *statistics.SortedBuilder
	cms          *statistics.CMSketch
	rowBuf       []byte
}

func (p *analyzeCommonHandleProcessor) Process(key, value []byte) error {
	values, _, err := tablecodec.CutCommonHandle(key, p.colLen)
	if err != nil {
		return err
	}
	p.rowBuf = p.rowBuf[:0]
	for _, val := range values {
		p.rowBuf = append(p.rowBuf, val...)
		if p.cms != nil {
			p.cms.InsertBytes(p.rowBuf)
		}
	}
	rowData := safeCopy(p.rowBuf)
	err = p.statsBuilder.Iterate(types.NewBytesDatum(rowData))
	if err != nil {
		return err
	}
	return nil
}

type analyzeColumnsExec struct {
	skipVal
	reader  *dbreader.DBReader
	ranges  []kv.KeyRange
	curRan  int
	seekKey []byte
	endKey  []byte
	startTS uint64

	chk     *chunk.Chunk
	decoder *rowcodec.ChunkDecoder
	req     *chunk.Chunk
	evalCtx *evalContext
	fields  []*ast.ResultField
}

func buildBaseAnalyzeColumnsExec(dbReader *dbreader.DBReader, rans []kv.KeyRange, analyzeReq *tipb.AnalyzeReq, startTS uint64) (*analyzeColumnsExec, *statistics.SampleBuilder, int64, error) {
	sc := flagsToStatementContext(analyzeReq.Flags)
	sc.TimeZone = time.FixedZone("UTC", int(analyzeReq.TimeZoneOffset))
	evalCtx := &evalContext{sc: sc}
	columns := analyzeReq.ColReq.ColumnsInfo
	evalCtx.setColumnInfo(columns)
	if len(analyzeReq.ColReq.PrimaryColumnIds) > 0 {
		evalCtx.primaryCols = analyzeReq.ColReq.PrimaryColumnIds
	}
	decoder, err := newRowDecoder(evalCtx.columnInfos, evalCtx.fieldTps, evalCtx.primaryCols, evalCtx.sc.TimeZone)
	if err != nil {
		return nil, nil, -1, err
	}
	e := &analyzeColumnsExec{
		reader:  dbReader,
		seekKey: rans[0].StartKey,
		endKey:  rans[0].EndKey,
		ranges:  rans,
		curRan:  0,
		startTS: startTS,
		chk:     chunk.NewChunkWithCapacity(evalCtx.fieldTps, 1),
		decoder: decoder,
		evalCtx: evalCtx,
	}
	e.fields = make([]*ast.ResultField, len(columns))
	for i := range e.fields {
		rf := new(ast.ResultField)
		rf.Column = new(model.ColumnInfo)
		ft := types.FieldType{}
		ft.SetType(mysql.TypeBlob)
		ft.SetFlen(mysql.MaxBlobWidth)
		ft.SetCharset(charset.CharsetUTF8)
		ft.SetCollate(charset.CollationUTF8)
		rf.Column.FieldType = ft
		e.fields[i] = rf
	}

	pkID := int64(-1)
	numCols := len(columns)
	if columns[0].GetPkHandle() {
		pkID = columns[0].ColumnId
		columns = columns[1:]
		numCols--
	}
	collators := make([]collate.Collator, numCols)
	fts := make([]*types.FieldType, numCols)
	for i, col := range columns {
		ft := fieldTypeFromPBColumn(col)
		fts[i] = ft
		if ft.EvalType() == types.ETString {
			collators[i] = collate.GetCollator(ft.GetCollate())
		}
	}
	colReq := analyzeReq.ColReq
	builder := statistics.SampleBuilder{
		Sc:              sc,
		ColLen:          numCols,
		MaxBucketSize:   colReq.BucketSize,
		MaxFMSketchSize: colReq.SketchSize,
		MaxSampleSize:   colReq.SampleSize,
		Collators:       collators,
		ColsFieldType:   fts,
	}
	statsVer := statistics.Version1
	if analyzeReq.ColReq.Version != nil {
		statsVer = int(*analyzeReq.ColReq.Version)
	}
	if pkID != -1 {
		builder.PkBuilder = statistics.NewSortedBuilder(sc, builder.MaxBucketSize, pkID, types.NewFieldType(mysql.TypeBlob), statsVer)
	}
	if colReq.CmsketchWidth != nil && colReq.CmsketchDepth != nil {
		builder.CMSketchWidth = *colReq.CmsketchWidth
		builder.CMSketchDepth = *colReq.CmsketchDepth
	}
	return e, &builder, pkID, nil
}

func handleAnalyzeColumnsReq(dbReader *dbreader.DBReader, rans []kv.KeyRange, analyzeReq *tipb.AnalyzeReq, startTS uint64) (*coprocessor.Response, error) {
	recordSet, builder, pkID, err := buildBaseAnalyzeColumnsExec(dbReader, rans, analyzeReq, startTS)
	if err != nil {
		return nil, err
	}
	builder.RecordSet = recordSet
	collectors, pkBuilder, err := builder.CollectColumnStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	colResp := &tipb.AnalyzeColumnsResp{}
	if pkID != -1 {
		colResp.PkHist = statistics.HistogramToProto(pkBuilder.Hist())
	}
	for _, c := range collectors {
		colResp.Collectors = append(colResp.Collectors, statistics.SampleCollectorToProto(c))
	}
	data, err := proto.Marshal(colResp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &coprocessor.Response{Data: data}, nil
}

func handleAnalyzeFullSamplingReq(
	dbReader *dbreader.DBReader,
	rans []kv.KeyRange,
	analyzeReq *tipb.AnalyzeReq,
	startTS uint64,
) (*coprocessor.Response, error) {
	sc := flagsToStatementContext(analyzeReq.Flags)
	sc.TimeZone = time.FixedZone("UTC", int(analyzeReq.TimeZoneOffset))
	evalCtx := &evalContext{sc: sc}
	columns := analyzeReq.ColReq.ColumnsInfo
	evalCtx.setColumnInfo(columns)
	if len(analyzeReq.ColReq.PrimaryColumnIds) > 0 {
		evalCtx.primaryCols = analyzeReq.ColReq.PrimaryColumnIds
	}
	decoder, err := newRowDecoder(evalCtx.columnInfos, evalCtx.fieldTps, evalCtx.primaryCols, evalCtx.sc.TimeZone)
	if err != nil {
		return nil, err
	}
	e := &analyzeColumnsExec{
		reader:  dbReader,
		seekKey: rans[0].StartKey,
		endKey:  rans[0].EndKey,
		ranges:  rans,
		curRan:  0,
		startTS: startTS,
		chk:     chunk.NewChunkWithCapacity(evalCtx.fieldTps, 1),
		decoder: decoder,
		evalCtx: evalCtx,
	}
	e.fields = make([]*ast.ResultField, len(columns))
	for i := range e.fields {
		rf := new(ast.ResultField)
		rf.Column = new(model.ColumnInfo)
		ft := types.FieldType{}
		ft.SetType(mysql.TypeBlob)
		ft.SetFlen(mysql.MaxBlobWidth)
		ft.SetCharset(charset.CharsetUTF8)
		ft.SetCollate(charset.CollationUTF8)
		rf.Column.FieldType = ft
		e.fields[i] = rf
	}

	numCols := len(columns)
	collators := make([]collate.Collator, numCols)
	fts := make([]*types.FieldType, numCols)
	for i, col := range columns {
		ft := fieldTypeFromPBColumn(col)
		fts[i] = ft
		if ft.EvalType() == types.ETString {
			collators[i] = collate.GetCollator(ft.GetCollate())
		}
	}
	colGroups := make([][]int64, 0, len(analyzeReq.ColReq.ColumnGroups))
	for _, group := range analyzeReq.ColReq.ColumnGroups {
		colOffsets := make([]int64, len(group.ColumnOffsets))
		copy(colOffsets, group.ColumnOffsets)
		colGroups = append(colGroups, colOffsets)
	}
	colReq := analyzeReq.ColReq
	/* #nosec G404 */
	builder := &statistics.RowSampleBuilder{
		Sc:              sc,
		RecordSet:       e,
		ColsFieldType:   fts,
		Collators:       collators,
		ColGroups:       colGroups,
		MaxSampleSize:   int(colReq.SampleSize),
		MaxFMSketchSize: int(colReq.SketchSize),
		SampleRate:      colReq.GetSampleRate(),
		Rng:             rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	collector, err := builder.Collect()
	if err != nil {
		return nil, err
	}
	colResp := &tipb.AnalyzeColumnsResp{}
	colResp.RowCollector = collector.Base().ToProto()
	data, err := colResp.Marshal()
	if err != nil {
		return nil, err
	}
	return &coprocessor.Response{Data: data}, nil
}

// Fields implements the sqlexec.RecordSet Fields interface.
func (e *analyzeColumnsExec) Fields() []*ast.ResultField {
	return e.fields
}

func (e *analyzeColumnsExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	e.req = req
	err := e.reader.Scan(e.seekKey, e.endKey, math.MaxInt64, e.startTS, e)
	if err != nil {
		return err
	}
	if req.NumRows() < req.Capacity() {
		if e.curRan == len(e.ranges)-1 {
			e.seekKey = e.endKey
		} else {
			e.curRan++
			e.seekKey = e.ranges[e.curRan].StartKey
			e.endKey = e.ranges[e.curRan].EndKey
		}
	}
	return nil
}

func (e *analyzeColumnsExec) Process(key, value []byte) error {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}
	err = e.decoder.DecodeToChunk(value, handle, e.chk)
	if err != nil {
		return errors.Trace(err)
	}
	row := e.chk.GetRow(0)
	for i, tp := range e.evalCtx.fieldTps {
		d := row.GetDatum(i, tp)
		if d.IsNull() {
			e.req.AppendNull(i)
			continue
		}

		value, err := tablecodec.EncodeValue(e.evalCtx.sc, nil, d)
		if err != nil {
			return err
		}
		e.req.AppendBytes(i, value)
	}
	e.chk.Reset()
	if e.req.NumRows() == e.req.Capacity() {
		e.seekKey = kv.Key(key).PrefixNext()
		return dbreader.ErrScanBreak
	}
	return nil
}

func (e *analyzeColumnsExec) NewChunk(_ chunk.Allocator) *chunk.Chunk {
	fields := make([]*types.FieldType, 0, len(e.fields))
	for _, field := range e.fields {
		fields = append(fields, &field.Column.FieldType)
	}
	return chunk.NewChunkWithCapacity(fields, 1024)
}

// Close implements the sqlexec.RecordSet Close interface.
func (e *analyzeColumnsExec) Close() error {
	return nil
}

func handleAnalyzeMixedReq(dbReader *dbreader.DBReader, rans []kv.KeyRange, analyzeReq *tipb.AnalyzeReq, startTS uint64) (*coprocessor.Response, error) {
	statsVer := int32(statistics.Version1)
	if analyzeReq.IdxReq.Version != nil {
		statsVer = *analyzeReq.IdxReq.Version
	}
	colExec, builder, _, err := buildBaseAnalyzeColumnsExec(dbReader, rans, analyzeReq, startTS)
	if err != nil {
		return nil, err
	}
	sctx := flagsToStatementContext(analyzeReq.Flags)
	e := &analyzeMixedExec{
		sctx:               sctx,
		analyzeColumnsExec: *colExec,
		colLen:             int(analyzeReq.IdxReq.NumColumns),
		statsBuilder:       statistics.NewSortedBuilder(sctx, analyzeReq.IdxReq.BucketSize, 0, types.NewFieldType(mysql.TypeBlob), int(statsVer)),
		statsVer:           statsVer,
	}
	builder.RecordSet = e
	if analyzeReq.IdxReq.TopNSize != nil {
		e.topNCount = *analyzeReq.IdxReq.TopNSize
	}
	if analyzeReq.IdxReq.CmsketchDepth != nil && analyzeReq.IdxReq.CmsketchWidth != nil {
		e.cms = statistics.NewCMSketch(*analyzeReq.IdxReq.CmsketchDepth, *analyzeReq.IdxReq.CmsketchWidth)
	}
	e.fms = statistics.NewFMSketch(int(analyzeReq.IdxReq.SketchSize))
	collectors, _, err := builder.CollectColumnStats()
	if err != nil {
		return nil, errors.Trace(err)
	}
	// columns
	colResp := &tipb.AnalyzeColumnsResp{}
	for _, c := range collectors {
		colResp.Collectors = append(colResp.Collectors, statistics.SampleCollectorToProto(c))
	}
	// common handle
	if statsVer >= statistics.Version2 {
		if e.topNCurValuePair.Count != 0 {
			e.topNValuePairs = append(e.topNValuePairs, e.topNCurValuePair)
		}
		sort.Slice(e.topNValuePairs, func(i, j int) bool {
			if e.topNValuePairs[i].Count > e.topNValuePairs[j].Count {
				return true
			} else if e.topNValuePairs[i].Count < e.topNValuePairs[j].Count {
				return false
			}
			return bytes.Compare(e.topNValuePairs[i].Encoded, e.topNValuePairs[j].Encoded) < 0
		})
		if len(e.topNValuePairs) > int(e.topNCount) {
			e.topNValuePairs = e.topNValuePairs[:e.topNCount]
		}
	}
	hg := statistics.HistogramToProto(e.statsBuilder.Hist())
	var cm *tipb.CMSketch
	if e.cms != nil {
		if statsVer >= statistics.Version2 {
			for _, valueCnt := range e.topNValuePairs {
				h1, h2 := murmur3.Sum128(valueCnt.Encoded)
				e.cms.SubValue(h1, h2, valueCnt.Count)
			}
		}
		cm = statistics.CMSketchToProto(e.cms, &statistics.TopN{TopN: e.topNValuePairs})
	}
	fms := statistics.FMSketchToProto(e.fms)
	commonHandleResp := &tipb.AnalyzeIndexResp{Hist: hg, Cms: cm, Collector: &tipb.SampleCollector{FmSketch: fms}}
	resp := &tipb.AnalyzeMixedResp{
		ColumnsResp: colResp,
		IndexResp:   commonHandleResp,
	}
	data, err := proto.Marshal(resp)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &coprocessor.Response{Data: data}, nil
}

type analyzeMixedExec struct {
	analyzeColumnsExec

	sctx         *stmtctx.StatementContext
	colLen       int
	statsBuilder *statistics.SortedBuilder
	fms          *statistics.FMSketch
	cms          *statistics.CMSketch
	rowBuf       []byte

	statsVer         int32
	topNCount        int32
	topNValuePairs   []statistics.TopNMeta
	topNCurValuePair statistics.TopNMeta
}

func (e *analyzeMixedExec) Process(key, value []byte) error {
	// common handle
	values, _, err := tablecodec.CutCommonHandle(key, e.colLen)
	if err != nil {
		return err
	}
	e.rowBuf = e.rowBuf[:0]
	for _, val := range values {
		e.rowBuf = append(e.rowBuf, val...)
		if e.cms != nil {
			e.cms.InsertBytes(e.rowBuf)
		}
	}

	if e.fms != nil {
		if err := e.fms.InsertValue(e.sctx, types.NewBytesDatum(e.rowBuf)); err != nil {
			return err
		}
	}

	if e.statsVer >= statistics.Version2 {
		if bytes.Equal(e.topNCurValuePair.Encoded, e.rowBuf) {
			e.topNCurValuePair.Count++
		} else {
			if e.topNCurValuePair.Count > 0 {
				e.topNValuePairs = append(e.topNValuePairs, e.topNCurValuePair)
			}
			e.topNCurValuePair.Encoded = safeCopy(e.rowBuf)
			e.topNCurValuePair.Count = 1
		}
	}
	rowData := safeCopy(e.rowBuf)
	err = e.statsBuilder.Iterate(types.NewBytesDatum(rowData))
	if err != nil {
		return err
	}

	// columns
	err = e.analyzeColumnsExec.Process(key, value)
	return err
}

func (e *analyzeMixedExec) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	e.req = req
	err := e.reader.Scan(e.seekKey, e.endKey, math.MaxInt64, e.startTS, e)
	if err != nil {
		return err
	}
	if req.NumRows() < req.Capacity() {
		if e.curRan == len(e.ranges)-1 {
			e.seekKey = e.endKey
		} else {
			e.curRan++
			e.seekKey = e.ranges[e.curRan].StartKey
			e.endKey = e.ranges[e.curRan].EndKey
		}
	}
	return nil
}
