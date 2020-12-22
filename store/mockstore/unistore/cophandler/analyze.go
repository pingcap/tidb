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
// See the License for the specific language governing permissions and
// limitations under the License.

package cophandler

import (
	"bytes"
	"math"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ngaut/unistore/tikv/dbreader"
	"github.com/pingcap/badger/y"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/charset"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/statistics"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/twmb/murmur3"
	"golang.org/x/net/context"
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
	} else {
		resp, err = handleAnalyzeColumnsReq(dbReader, ranges, analyzeReq, req.StartTs)
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
	processor := &analyzeIndexProcessor{
		colLen:       int(analyzeReq.IdxReq.NumColumns),
		statsBuilder: statistics.NewSortedBuilder(flagsToStatementContext(analyzeReq.Flags), analyzeReq.IdxReq.BucketSize, 0, types.NewFieldType(mysql.TypeBlob)),
		statsVer:     statsVer,
	}
	if analyzeReq.IdxReq.TopNSize != nil {
		processor.topNCount = *analyzeReq.IdxReq.TopNSize
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
	if statsVer == statistics.Version2 {
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
		if statsVer == statistics.Version2 {
			for _, valueCnt := range processor.topNValuePairs {
				h1, h2 := murmur3.Sum128(valueCnt.Encoded)
				processor.cms.SubValue(h1, h2, valueCnt.Count)
			}
		}
		cm = statistics.CMSketchToProto(processor.cms, &statistics.TopN{TopN: processor.topNValuePairs})
	}
	data, err := proto.Marshal(&tipb.AnalyzeIndexResp{Hist: hg, Cms: cm})
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &coprocessor.Response{Data: data}, nil
}

func handleAnalyzeCommonHandleReq(dbReader *dbreader.DBReader, rans []kv.KeyRange, analyzeReq *tipb.AnalyzeReq, startTS uint64) (*coprocessor.Response, error) {
	processor := &analyzeCommonHandleProcessor{
		colLen:       int(analyzeReq.IdxReq.NumColumns),
		statsBuilder: statistics.NewSortedBuilder(flagsToStatementContext(analyzeReq.Flags), analyzeReq.IdxReq.BucketSize, 0, types.NewFieldType(mysql.TypeBlob)),
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

	colLen       int
	statsBuilder *statistics.SortedBuilder
	cms          *statistics.CMSketch
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
	if p.statsVer == statistics.Version2 {
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

func handleAnalyzeColumnsReq(dbReader *dbreader.DBReader, rans []kv.KeyRange, analyzeReq *tipb.AnalyzeReq, startTS uint64) (*coprocessor.Response, error) {
	sc := flagsToStatementContext(analyzeReq.Flags)
	sc.TimeZone = time.FixedZone("UTC", int(analyzeReq.TimeZoneOffset))
	evalCtx := &evalContext{sc: sc}
	columns := analyzeReq.ColReq.ColumnsInfo
	evalCtx.setColumnInfo(columns)
	if len(analyzeReq.ColReq.PrimaryColumnIds) > 0 {
		evalCtx.primaryCols = analyzeReq.ColReq.PrimaryColumnIds
	}
	decoder, err := evalCtx.newRowDecoder()
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
		rf.Column.FieldType = types.FieldType{Tp: mysql.TypeBlob, Flen: mysql.MaxBlobWidth, Charset: charset.CharsetUTF8, Collate: charset.CollationUTF8}
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
			collators[i] = collate.GetCollator(ft.Collate)
		}
	}
	colReq := analyzeReq.ColReq
	builder := statistics.SampleBuilder{
		Sc:              sc,
		RecordSet:       e,
		ColLen:          numCols,
		MaxBucketSize:   colReq.BucketSize,
		MaxFMSketchSize: colReq.SketchSize,
		MaxSampleSize:   colReq.SampleSize,
		Collators:       collators,
		ColsFieldType:   fts,
	}
	if pkID != -1 {
		builder.PkBuilder = statistics.NewSortedBuilder(sc, builder.MaxBucketSize, pkID, types.NewFieldType(mysql.TypeBlob))
	}
	if colReq.CmsketchWidth != nil && colReq.CmsketchDepth != nil {
		builder.CMSketchWidth = *colReq.CmsketchWidth
		builder.CMSketchDepth = *colReq.CmsketchDepth
	}
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
		return dbreader.ScanBreak
	}
	return nil
}

func (e *analyzeColumnsExec) NewChunk() *chunk.Chunk {
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
