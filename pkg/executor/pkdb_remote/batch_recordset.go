// Copyright 2024 PingCAP, Inc.
// Licensed under the Apache License, Version 2.0 (the "License");

package pkdbremote

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/executor/pkdb_remote/pb"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/metrics"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

var batchFieldTypePool = sync.Pool{
	New: func() any { return &types.FieldType{} },
}

var batchResultFieldPool = sync.Pool{
	New: func() any { return &resolve.ResultField{} },
}

var batchColumnInfoPool = sync.Pool{
	New: func() any { return &model.ColumnInfo{} },
}

var batchTableInfoPool = sync.Pool{
	New: func() any { return &model.TableInfo{} },
}

var batchStreamingRecordSetPool = sync.Pool{
	New: func() any { return &batchStreamingRecordSet{} },
}

const maxBatchChunkDecodeBufCap = 4 << 20 // 4MiB

var batchChunkDecodeBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 8<<10)
		return &b
	},
}

func acquireBatchChunkDecodeBuf(size int) *[]byte {
	if size <= 0 {
		b := make([]byte, 0)
		return &b
	}
	buf := batchChunkDecodeBufPool.Get().(*[]byte)
	if cap(*buf) < size {
		b := make([]byte, size)
		return &b
	}
	*buf = (*buf)[:size]
	return buf
}

func releaseBatchChunkDecodeBuf(buf *[]byte) {
	if buf == nil {
		return
	}
	if cap(*buf) > maxBatchChunkDecodeBufCap {
		return
	}
	*buf = (*buf)[:0]
	batchChunkDecodeBufPool.Put(buf)
}

// batchStreamingRecordSet reads results from a batch stream
type batchStreamingRecordSet struct {
	requestID       uint64
	pbColumnInfos   []*pb.ColumnInfo
	fields          []*resolve.ResultField
	fieldTypes      []*types.FieldType
	respCh          chan *pb.StreamResponse
	pr              *pendingRequest
	sw              *streamWrapper
	sctx            sessionctx.Context
	codec           *chunk.Codec
	rowBuffer       []*pb.Row
	bufferIdx       int
	chunkBuffer     *chunk.Chunk
	chunkRowIdx     int
	chunkDataBuf    *[]byte
	streamDone      bool
	streamErr       error
	forwardStart    time.Time
	firstRespTime   time.Time // Time when first response was received, used to calculate client consume duration
	firstResultTime time.Time
	closed          int32
	feedbackOnce    sync.Once
	feedback        *core.RemotePlanFeedback
	feedbackCfg     remotePlanFeedbackConfig
	remoteBytes     int64
	remoteRows      int64
	kvRequestCnt    uint64
	kvLocalFFICnt   uint64
	tikvScanBytes   int64
	mu              sync.Mutex
}

func newBatchStreamingRecordSet(requestID uint64, pbColumnInfos []*pb.ColumnInfo, respCh chan *pb.StreamResponse, pr *pendingRequest, sw *streamWrapper, sctx sessionctx.Context) *batchStreamingRecordSet {
	rs := acquireBatchStreamingRecordSet()
	rs.requestID = requestID
	rs.pbColumnInfos = pbColumnInfos
	rs.respCh = respCh
	rs.pr = pr
	rs.sw = sw
	rs.sctx = sctx
	if cap(rs.fieldTypes) < len(pbColumnInfos) {
		rs.fieldTypes = make([]*types.FieldType, len(pbColumnInfos))
	} else {
		rs.fieldTypes = rs.fieldTypes[:len(pbColumnInfos)]
	}
	if cap(rs.fields) < len(pbColumnInfos) {
		rs.fields = make([]*resolve.ResultField, len(pbColumnInfos))
	} else {
		rs.fields = rs.fields[:len(pbColumnInfos)]
	}
	for i, pbInfo := range pbColumnInfos {
		ft := acquireBatchFieldType(pbInfo)
		rs.fieldTypes[i] = ft
		rs.fields[i] = acquireBatchResultField(pbInfo, ft)
	}
	rs.codec = chunk.NewCodec(rs.fieldTypes)
	return rs
}

func acquireBatchStreamingRecordSet() *batchStreamingRecordSet {
	rs, _ := batchStreamingRecordSetPool.Get().(*batchStreamingRecordSet)
	if rs == nil {
		rs = &batchStreamingRecordSet{}
	}
	rs.pbColumnInfos = nil
	rs.respCh = nil
	rs.pr = nil
	rs.sw = nil
	rs.sctx = nil
	rs.codec = nil
	rs.rowBuffer = rs.rowBuffer[:0]
	rs.bufferIdx = 0
	rs.chunkBuffer = nil
	rs.chunkRowIdx = 0
	rs.chunkDataBuf = nil
	rs.streamDone = false
	rs.streamErr = nil
	rs.forwardStart = time.Time{}
	rs.firstRespTime = time.Time{}
	rs.firstResultTime = time.Time{}
	rs.closed = 0
	rs.feedbackOnce = sync.Once{}
	rs.feedback = nil
	rs.feedbackCfg = remotePlanFeedbackConfig{}
	rs.remoteBytes = 0
	rs.remoteRows = 0
	rs.kvRequestCnt = 0
	rs.kvLocalFFICnt = 0
	rs.tikvScanBytes = 0
	rs.mu = sync.Mutex{}
	return rs
}

func releaseBatchStreamingRecordSet(rs *batchStreamingRecordSet) {
	if rs == nil {
		return
	}
	rs.requestID = 0
	rs.pbColumnInfos = nil
	rs.respCh = nil
	rs.pr = nil
	rs.sw = nil
	rs.sctx = nil
	rs.codec = nil
	rs.rowBuffer = nil
	rs.bufferIdx = 0
	rs.chunkBuffer = nil
	rs.chunkRowIdx = 0
	rs.chunkDataBuf = nil
	rs.streamDone = false
	rs.streamErr = nil
	rs.forwardStart = time.Time{}
	rs.firstRespTime = time.Time{}
	rs.firstResultTime = time.Time{}
	rs.closed = 0
	rs.feedbackOnce = sync.Once{}
	rs.feedback = nil
	rs.feedbackCfg = remotePlanFeedbackConfig{}
	rs.remoteBytes = 0
	rs.remoteRows = 0
	rs.kvRequestCnt = 0
	rs.kvLocalFFICnt = 0
	rs.tikvScanBytes = 0
	rs.mu = sync.Mutex{}
	batchStreamingRecordSetPool.Put(rs)
}

func (s *batchStreamingRecordSet) attachRemotePlanFeedback(feedback *core.RemotePlanFeedback, cfg remotePlanFeedbackConfig) {
	if s == nil || feedback == nil || !cfg.enabled() {
		return
	}
	s.feedback = feedback
	s.feedbackCfg = cfg
}

func (s *batchStreamingRecordSet) maybeRecordRemotePlanFeedback(ctx context.Context) {
	if s == nil || s.feedback == nil || !s.feedbackCfg.enabled() || !s.streamDone || s.streamErr != nil {
		return
	}
	bytes := s.remoteBytes
	cfg := s.feedbackCfg
	scanBytes := s.tikvScanBytes
	shrinkGood := false
	shrinkKnown := cfg.noShrinkRatio > 0 && scanBytes > 0
	if shrinkKnown {
		// Treat as "no shrink" if result_bytes * 100 >= scan_bytes * noShrinkRatio.
		shrinkGood = bytes*100 < scanBytes*int64(cfg.noShrinkRatio)
	}
	localCallGood := false
	if cfg.minLocalCall > 0 {
		localCallGood = s.kvLocalFFICnt >= uint64(cfg.minLocalCall)
	}
	// We only mark "bad" when:
	// 1) we can judge shrink (scan bytes known) and it's not shrinking enough; AND
	// 2) TiKV local-call/FFI usage is not significant.
	bad := shrinkKnown && !shrinkGood && !localCallGood
	s.feedbackOnce.Do(func() {
		if s.feedback.RecordObservation(time.Time{}, bad, cfg.disableAfter, cfg.cooldown) {
			disabledUntil := time.Unix(0, s.feedback.DisabledUntilUnixNano())
			logutil.Logger(ctx).Debug("[remote] remote plan feedback disabled forwarding",
				zap.Int64("resultBytes", bytes),
				zap.Int64("tikvProcessedBytes", scanBytes),
				zap.Int32("noShrinkRatio", cfg.noShrinkRatio),
				zap.Int32("minLocalCallRequests", cfg.minLocalCall),
				zap.Uint64("kvRequestCount", s.kvRequestCnt),
				zap.Uint64("kvLocalCallRequestCount", s.kvLocalFFICnt),
				zap.Int32("disableAfter", cfg.disableAfter),
				zap.Duration("cooldown", cfg.cooldown),
				zap.Time("disabledUntil", disabledUntil))
		}
	})
}

func acquireBatchFieldType(pbInfo *pb.ColumnInfo) *types.FieldType {
	ft, _ := batchFieldTypePool.Get().(*types.FieldType)
	if ft == nil {
		ft = &types.FieldType{}
	}
	*ft = types.FieldType{}
	ft.SetType(byte(pbInfo.Type))
	ft.SetFlag(uint(pbInfo.Flag))
	ft.SetCharset(pbInfo.CharsetName)
	ft.SetCollate(pbInfo.Collate)
	if pbInfo.Flen != 0 {
		ft.SetFlen(int(pbInfo.Flen))
	} else {
		ft.SetFlen(int(pbInfo.ColumnLength))
	}
	ft.SetDecimal(int(pbInfo.Decimal))
	return ft
}

func releaseBatchFieldType(ft *types.FieldType) {
	if ft == nil {
		return
	}
	*ft = types.FieldType{}
	batchFieldTypePool.Put(ft)
}

func acquireBatchResultField(pbInfo *pb.ColumnInfo, ft *types.FieldType) *resolve.ResultField {
	rf, _ := batchResultFieldPool.Get().(*resolve.ResultField)
	if rf == nil {
		rf = &resolve.ResultField{}
	}
	*rf = resolve.ResultField{}
	rf.ColumnAsName = pmodel.NewCIStr(pbInfo.Name)
	rf.TableAsName = pmodel.NewCIStr(pbInfo.Table)
	rf.DBName = pmodel.NewCIStr(pbInfo.Schema)
	rf.EmptyOrgName = pbInfo.OrgName == ""

	ci, _ := batchColumnInfoPool.Get().(*model.ColumnInfo)
	if ci == nil {
		ci = &model.ColumnInfo{}
	}
	*ci = model.ColumnInfo{}
	ci.Name = pmodel.NewCIStr(pbInfo.OrgName)
	ci.FieldType = *ft
	rf.Column = ci

	ti, _ := batchTableInfoPool.Get().(*model.TableInfo)
	if ti == nil {
		ti = &model.TableInfo{}
	}
	*ti = model.TableInfo{}
	ti.Name = pmodel.NewCIStr(pbInfo.OrgTable)
	rf.Table = ti
	return rf
}

func releaseBatchResultField(rf *resolve.ResultField) {
	if rf == nil {
		return
	}
	if rf.Column != nil {
		*rf.Column = model.ColumnInfo{}
		batchColumnInfoPool.Put(rf.Column)
		rf.Column = nil
	}
	if rf.Table != nil {
		*rf.Table = model.TableInfo{}
		batchTableInfoPool.Put(rf.Table)
		rf.Table = nil
	}
	*rf = resolve.ResultField{}
	batchResultFieldPool.Put(rf)
}

func (s *batchStreamingRecordSet) Fields() []*resolve.ResultField { return s.fields }

func (s *batchStreamingRecordSet) Next(ctx context.Context, req *chunk.Chunk) error {
	req.Reset()
	if atomic.LoadInt32(&s.closed) == 1 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for req.NumRows() < req.Capacity() {
		if s.chunkBuffer != nil && s.chunkRowIdx < s.chunkBuffer.NumRows() {
			rowCopyStart := time.Now()
			for s.chunkRowIdx < s.chunkBuffer.NumRows() && req.NumRows() < req.Capacity() {
				req.AppendRow(s.chunkBuffer.GetRow(s.chunkRowIdx))
				s.chunkRowIdx++
			}
			metrics.RemotePlanRowCopyDuration.Observe(time.Since(rowCopyStart).Seconds())
			continue
		}
		if s.chunkBuffer != nil {
			s.releaseChunkBuffer()
		}
		if s.bufferIdx < len(s.rowBuffer) {
			row := s.rowBuffer[s.bufferIdx]
			s.bufferIdx++
			appendBatchRow(req, row, s.fieldTypes)
			continue
		}
		s.rowBuffer = s.rowBuffer[:0]
		s.bufferIdx = 0
		if s.streamDone {
			return s.streamErr
		}
		channelWaitStart := time.Now()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp, ok := <-s.respCh:
			metrics.RemotePlanChannelWaitDuration.Observe(time.Since(channelWaitStart).Seconds())
			if !ok {
				// Channel was closed, stream ended
				// Check if there was an error that caused the stream to close
				// This is important to detect truncated/failed requests
				s.streamDone = true
				if s.sw != nil {
					if closeErr := s.sw.GetCloseError(); closeErr != nil {
						s.streamErr = errors.Errorf("stream closed unexpectedly: %v", closeErr)
						return s.streamErr
					}
				}
				return nil
			}
			if resp == nil {
				// Shouldn't happen, but handle gracefully
				s.streamDone = true
				s.streamErr = errors.New("received nil response from stream")
				return s.streamErr
			}
			if resp.Err != "" {
				s.streamDone = true
				s.streamErr = errors.New(resp.Err)
				s.maybeRecordRemotePlanFeedback(ctx)
				return s.streamErr
			}
			if fb := resp.Feedback; fb != nil {
				s.applyRemoteExecFeedback(fb)
			}
			if chunkData := resp.Chunk; chunkData != nil && len(chunkData.Data) > 0 {
				s.remoteBytes += int64(len(chunkData.Data))
				if chunkData.NumRows > 0 {
					s.remoteRows += int64(chunkData.NumRows)
				}
				if s.codec != nil {
					decodeStart := time.Now()
					buf := acquireBatchChunkDecodeBuf(len(chunkData.Data))
					copy(*buf, chunkData.Data)
					decodedChunk, _ := s.codec.Decode((*buf)[:len(chunkData.Data)])
					metrics.RemotePlanChunkDecodeDuration.Observe(time.Since(decodeStart).Seconds())
					if decodedChunk != nil && decodedChunk.NumRows() > 0 {
						if s.firstResultTime.IsZero() && !s.firstRespTime.IsZero() {
							s.firstResultTime = time.Now()
							metrics.RemotePlanFirstResultWaitDuration.Observe(time.Since(s.firstRespTime).Seconds())
						}
						s.setChunkBuffer(decodedChunk, buf)
						if !resp.HasMore {
							s.streamDone = true
							s.maybeRecordRemotePlanFeedback(ctx)
						}
						continue
					}
					releaseBatchChunkDecodeBuf(buf)
				}
			}
			if rows := resp.Rows; len(rows) > 0 {
				if s.firstResultTime.IsZero() && !s.firstRespTime.IsZero() {
					s.firstResultTime = time.Now()
					metrics.RemotePlanFirstResultWaitDuration.Observe(time.Since(s.firstRespTime).Seconds())
				}
				s.remoteRows += int64(len(rows))
				for _, row := range rows {
					if row == nil {
						continue
					}
					for _, v := range row.RawValues {
						s.remoteBytes += int64(len(v))
					}
					if len(row.RawValues) == 0 {
						for _, v := range row.Values {
							s.remoteBytes += int64(len(v))
						}
					}
				}
				s.rowBuffer = append(s.rowBuffer, rows...)
			}
			if !resp.HasMore {
				if s.firstResultTime.IsZero() && !s.firstRespTime.IsZero() && len(resp.Rows) == 0 && (resp.Chunk == nil || len(resp.Chunk.Data) == 0) {
					s.firstResultTime = time.Now()
					metrics.RemotePlanFirstResultWaitDuration.Observe(time.Since(s.firstRespTime).Seconds())
				}
				s.streamDone = true
				s.maybeRecordRemotePlanFeedback(ctx)
				if len(resp.Rows) == 0 && (resp.Chunk == nil || len(resp.Chunk.Data) == 0) {
					return nil
				}
			}
		}
	}
	return nil
}

func (s *batchStreamingRecordSet) applyRemoteExecFeedback(fb *pb.RemoteExecFeedback) {
	if s == nil || fb == nil {
		return
	}
	s.kvRequestCnt = fb.KvRequestCount
	s.kvLocalFFICnt = fb.KvLocalCallRequestCount

	if fb.TikvProcessedKeysSize > uint64(math.MaxInt64) {
		s.tikvScanBytes = math.MaxInt64
	} else {
		s.tikvScanBytes = int64(fb.TikvProcessedKeysSize)
	}
}

func appendBatchRow(chk *chunk.Chunk, row *pb.Row, fieldTypes []*types.FieldType) {
	for i := 0; i < len(fieldTypes); i++ {
		if i < len(row.IsNull) && row.IsNull[i] {
			chk.AppendNull(i)
			continue
		}
		if len(row.RawValues) > 0 && i < len(row.RawValues) {
			appendBinaryValue(chk, i, fieldTypes[i], row.RawValues[i])
		} else if i < len(row.Values) {
			appendStringValue(chk, i, fieldTypes[i], row.Values[i])
		} else {
			chk.AppendNull(i)
		}
	}
}

func (s *batchStreamingRecordSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	initCap := variable.DefInitChunkSize
	maxChunkSize := variable.DefMaxChunkSize
	if s.sctx != nil {
		sessVars := s.sctx.GetSessionVars()
		if sessVars != nil {
			initCap = sessVars.InitChunkSize
			maxChunkSize = sessVars.MaxChunkSize
		}
	}

	if alloc != nil {
		return alloc.Alloc(s.fieldTypes, initCap, maxChunkSize)
	}
	return chunk.New(s.fieldTypes, initCap, maxChunkSize)
}

func (s *batchStreamingRecordSet) recordForwardMetrics() {
	if s.forwardStart.IsZero() {
		return
	}
	err := s.streamErr
	if err == nil && !s.streamDone {
		err = errors.New("remote stream not completed")
	}
	if err == nil {
		metrics.RemotePlanForwardSendOKDuration.Observe(time.Since(s.forwardStart).Seconds())
		metrics.RemotePlanForwardSendOKCounter.Inc()
		// Record client consume duration (time from first response to Close)
		if !s.firstRespTime.IsZero() {
			metrics.RemotePlanClientConsumeOKDuration.Observe(time.Since(s.firstRespTime).Seconds())
		}
		return
	}
	metrics.RemotePlanForwardSendErrDuration.Observe(time.Since(s.forwardStart).Seconds())
	metrics.RemotePlanForwardSendErrCounter.Inc()
	// Record client consume duration even on error
	if !s.firstRespTime.IsZero() {
		metrics.RemotePlanClientConsumeErrDuration.Observe(time.Since(s.firstRespTime).Seconds())
	}
}

func (s *batchStreamingRecordSet) Close() error {
	if !atomic.CompareAndSwapInt32(&s.closed, 0, 1) {
		return nil
	}

	s.recordForwardMetrics()
	s.maybeRecordRemotePlanFeedback(context.Background())
	s.releaseChunkBuffer()

	// Detach memory and disk tracker to prevent memory leak
	// This is critical: without this, the memory tracker attached in ResetContextOfStmt
	// will never be detached, causing memory to accumulate
	if s.sctx != nil {
		s.sctx.GetSessionVars().StmtCtx.DetachMemDiskTracker()
	}

	// Remove from pending requests to allow cleanup
	// This also prevents recvLoop from sending more responses to respCh
	if s.sw != nil && s.pr != nil {
		s.sw.pendingMu.Lock()
		_, existed := s.sw.pending[s.requestID]
		delete(s.sw.pending, s.requestID)
		s.sw.pendingMu.Unlock()

		// Close the done channel to signal completion
		// Use atomic operation to ensure it's only closed once
		s.pr.closeDone()

		// Decrement pending requests counter only if we actually removed it
		// (it might have already been removed by recvLoop when HasMore=false)
		if existed {
			metrics.RemotePlanBatchClientPendingRequests.Dec()
		}
	}

	// Drain remaining responses synchronously
	// Since we've removed from pending map, no new responses will be sent
	// We just need to drain what's already in the buffer
	for {
		select {
		case _, ok := <-s.respCh:
			if !ok {
				return s.finalize()
			}
			// Continue draining
		default:
			// Channel is empty, we're done
			return s.finalize()
		}
	}
}

func (s *batchStreamingRecordSet) finalize() error {
	if s.streamDone && s.streamErr == nil {
		releaseRespCh(s.respCh)
	}
	if err := s.releaseResources(); err != nil {
		return err
	}
	releaseBatchStreamingRecordSet(s)
	return nil
}

func (s *batchStreamingRecordSet) releaseResources() error {
	for i := range s.fieldTypes {
		if s.fieldTypes[i] != nil {
			releaseBatchFieldType(s.fieldTypes[i])
			s.fieldTypes[i] = nil
		}
	}
	for i := range s.fields {
		if s.fields[i] != nil {
			releaseBatchResultField(s.fields[i])
			s.fields[i] = nil
		}
	}
	s.fieldTypes = s.fieldTypes[:0]
	s.fields = s.fields[:0]
	return nil
}

func (s *batchStreamingRecordSet) setChunkBuffer(chk *chunk.Chunk, buf *[]byte) {
	s.releaseChunkBuffer()
	s.chunkBuffer = chk
	s.chunkRowIdx = 0
	s.chunkDataBuf = buf
}

func (s *batchStreamingRecordSet) releaseChunkBuffer() {
	if s.chunkDataBuf != nil {
		releaseBatchChunkDecodeBuf(s.chunkDataBuf)
		s.chunkDataBuf = nil
	}
	s.chunkBuffer = nil
	s.chunkRowIdx = 0
}
