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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package distsql

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/store/copr"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/execdetails"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikv"
)
type selRespChannelIter struct {
	channel    int
	loc        *time.Location
	rowLen     int
	fieldTypes []*types.FieldType
	encodeType tipb.EncodeType
	chkData    []tipb.Chunk

	// reserveChkSize indicates the reserved size for each chunk. (Only for default encoding)
	reserveChkSize int
	// curChkIdx indicates the index of the current chunk in chkData read currently.
	curChkIdx int
	// chk buffers the rows read from the current response
	chk *chunk.Chunk
	// offset indicates the read offset in iter.chk
	chkOffset int
}

func newSelRespChannelIter(result *selectResult, channel int) (*selRespChannelIter, error) {
	intest.Assert(result != nil && result.selectResp != nil && len(result.selectResp.IntermediateOutputs) == len(result.intermediateOutputTypes))

	var rowLen int
	var fieldTypes []*types.FieldType
	var encodeType tipb.EncodeType
	var chkData []tipb.Chunk
	intermediateOutputs := result.selectResp.IntermediateOutputs
	if intermediateOutputsLen := len(intermediateOutputs); channel < intermediateOutputsLen {
		fieldTypes = result.intermediateOutputTypes[channel]
		rowLen = len(fieldTypes)
		encodeType = intermediateOutputs[channel].GetEncodeType()
		chkData = intermediateOutputs[channel].GetChunks()
	} else if channel == intermediateOutputsLen {
		rowLen = result.rowLen
		fieldTypes = result.fieldTypes
		encodeType = result.selectResp.GetEncodeType()
		chkData = result.selectResp.GetChunks()
	} else {
		return nil, errors.Errorf(
			"invalid channel %d for selectResp with %d intermediate outputs",
			channel, intermediateOutputsLen,
		)
	}

	return &selRespChannelIter{
		channel:        channel,
		loc:            result.ctx.Location,
		rowLen:         rowLen,
		fieldTypes:     fieldTypes,
		encodeType:     encodeType,
		chkData:        chkData,
		reserveChkSize: vardef.DefInitChunkSize,
	}, nil
}

func (iter *selRespChannelIter) Channel() int {
	return iter.channel
}

func (iter *selRespChannelIter) Next() (SelectResultRow, error) {
	if iter.chk != nil && iter.chkOffset < iter.chk.NumRows() {
		iter.chkOffset++
		return SelectResultRow{
			ChannelIndex: iter.channel,
			Row:          iter.chk.GetRow(iter.chkOffset - 1),
		}, nil
	}

	if err := iter.nextChunk(); err != nil || iter.chk == nil {
		return SelectResultRow{}, err
	}

	iter.chkOffset = 1
	return SelectResultRow{
		ChannelIndex: iter.channel,
		Row:          iter.chk.GetRow(0),
	}, nil
}

func (iter *selRespChannelIter) nextChunk() error {
	iter.chk = nil
	for iter.curChkIdx < len(iter.chkData) {
		curData := &iter.chkData[iter.curChkIdx]
		if len(curData.RowsData) == 0 {
			iter.curChkIdx++
			continue
		}

		switch iter.encodeType {
		case tipb.EncodeType_TypeDefault:
			var err error
			newChk, leftRowsData, err := iter.fillChunkFromDefault(iter.chk, curData.RowsData)
			if err != nil {
				return err
			}
			iter.chk = newChk
			curData.RowsData = leftRowsData
			if newChk.NumRows() < newChk.RequiredRows() {
				continue
			}
		case tipb.EncodeType_TypeChunk:
			iter.chk = chunk.NewChunkWithCapacity(iter.fieldTypes, 0)
			chunk.NewDecoder(iter.chk, iter.fieldTypes).Reset(curData.RowsData)
			curData.RowsData = nil
		default:
			return errors.Errorf("unsupported encode type: %v", iter.encodeType)
		}

		if iter.chk.NumRows() > 0 {
			break
		}
	}
	return nil
}

func (iter *selRespChannelIter) fillChunkFromDefault(chk *chunk.Chunk, rowsData []byte) (*chunk.Chunk, []byte, error) {
	if chk == nil {
		chk = chunk.NewChunkWithCapacity(iter.fieldTypes, iter.reserveChkSize)
	}
	decoder := codec.NewDecoder(chk, iter.loc)
	for len(rowsData) > 0 && chk.NumRows() < chk.RequiredRows() {
		for i := range iter.rowLen {
			var err error
			rowsData, err = decoder.DecodeOne(rowsData, i, iter.fieldTypes[i])
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return chk, rowsData, nil
}

type selectResultIter struct {
	result                  *selectResult
	channels                []*selRespChannelIter
	intermediateOutputTypes [][]*types.FieldType
}

func newSelectResultIter(result *selectResult, intermediateOutputTypes [][]*types.FieldType) *selectResultIter {
	intest.Assert(result != nil && result.iter == nil)
	result.iter = &selectResultIter{
		result:                  result,
		intermediateOutputTypes: intermediateOutputTypes,
	}
	return result.iter
}

// Next implements the SelectResultIter interface.
func (iter *selectResultIter) Next(ctx context.Context) (SelectResultRow, error) {
	for {
		if r := iter.result; r.selectResp == nil {
			if err := r.fetchRespWithIntermediateResults(ctx, iter.intermediateOutputTypes); err != nil {
				return SelectResultRow{}, err
			}

			if r.selectResp == nil {
				return SelectResultRow{}, nil
			}

			if iter.channels == nil {
				iter.channels = make([]*selRespChannelIter, 0, len(iter.intermediateOutputTypes)+1)
			}

			for i := 0; i <= len(iter.intermediateOutputTypes); i++ {
				ch, err := newSelRespChannelIter(r, i)
				if err != nil {
					return SelectResultRow{}, err
				}
				iter.channels = append(iter.channels, ch)
			}
		}

		for len(iter.channels) > 0 {
			// here we read the channel in reverse order to make sure the "more complete" data should be read first.
			// For example, if a cop-request contains IndexLookUp, we should read the final rows first (with the biggest channel index),
			// and then read the index rows (with smaller channel index) that have not been looked up.
			lastPos := len(iter.channels) - 1
			channel := iter.channels[lastPos]
			row, err := channel.Next()
			if err != nil || !row.IsEmpty() {
				return row, err
			}
			iter.channels = iter.channels[:lastPos]
		}

		iter.result.selectResp = nil
	}
}

// Close implements the SelectResultIter interface.
func (iter *selectResultIter) Close() error {
	return iter.result.close()
}

// CopRuntimeStats is an interface uses to check whether the result has cop runtime stats.
type CopRuntimeStats interface {
	// GetCopRuntimeStats gets the cop runtime stats information.
	GetCopRuntimeStats() *copr.CopRuntimeStats
}

type selectResultRuntimeStats struct {
	copRespTime             execdetails.Percentile[execdetails.Duration]
	procKeys                execdetails.Percentile[execdetails.Int64]
	backoffSleep            map[string]time.Duration
	totalProcessTime        time.Duration
	totalWaitTime           time.Duration
	reqStat                 *tikv.RegionRequestRuntimeStats
	distSQLConcurrency      int
	extraConcurrency        int
	CoprCacheHitNum         int64
	storeBatchedNum         uint64
	storeBatchedFallbackNum uint64
	buildTaskDuration       time.Duration
	fetchRspDuration        time.Duration
}

func (s *selectResultRuntimeStats) mergeCopRuntimeStats(copStats *copr.CopRuntimeStats, respTime time.Duration) {
	s.copRespTime.Add(execdetails.Duration(respTime))
	procKeys := execdetails.Int64(0)
	if copStats.ScanDetail != nil {
		procKeys = execdetails.Int64(copStats.ScanDetail.ProcessedKeys)
	}
	s.procKeys.Add(procKeys)
	if len(copStats.BackoffSleep) > 0 {
		if s.backoffSleep == nil {
			s.backoffSleep = make(map[string]time.Duration)
		}
		for k, v := range copStats.BackoffSleep {
			s.backoffSleep[k] += v
		}
	}
	s.totalProcessTime += copStats.TimeDetail.ProcessTime
	s.totalWaitTime += copStats.TimeDetail.WaitTime
	if copStats.ReqStats != nil {
		if s.reqStat == nil {
			s.reqStat = copStats.ReqStats
		} else {
			s.reqStat.Merge(copStats.ReqStats)
		}
	}
	if copStats.CoprCacheHit {
		s.CoprCacheHitNum++
	}
}

func (s *selectResultRuntimeStats) Clone() execdetails.RuntimeStats {
	newRs := selectResultRuntimeStats{
		copRespTime:             execdetails.Percentile[execdetails.Duration]{},
		procKeys:                execdetails.Percentile[execdetails.Int64]{},
		backoffSleep:            make(map[string]time.Duration, len(s.backoffSleep)),
		reqStat:                 tikv.NewRegionRequestRuntimeStats(),
		distSQLConcurrency:      s.distSQLConcurrency,
		extraConcurrency:        s.extraConcurrency,
		CoprCacheHitNum:         s.CoprCacheHitNum,
		storeBatchedNum:         s.storeBatchedNum,
		storeBatchedFallbackNum: s.storeBatchedFallbackNum,
		buildTaskDuration:       s.buildTaskDuration,
		fetchRspDuration:        s.fetchRspDuration,
	}
	newRs.copRespTime.MergePercentile(&s.copRespTime)
	newRs.procKeys.MergePercentile(&s.procKeys)
	for k, v := range s.backoffSleep {
		newRs.backoffSleep[k] += v
	}
	newRs.totalProcessTime += s.totalProcessTime
	newRs.totalWaitTime += s.totalWaitTime
	newRs.reqStat = s.reqStat.Clone()
	return &newRs
}

func (s *selectResultRuntimeStats) Merge(rs execdetails.RuntimeStats) {
	other, ok := rs.(*selectResultRuntimeStats)
	if !ok {
		return
	}
	s.copRespTime.MergePercentile(&other.copRespTime)
	s.procKeys.MergePercentile(&other.procKeys)

	if len(other.backoffSleep) > 0 {
		if s.backoffSleep == nil {
			s.backoffSleep = make(map[string]time.Duration)
		}
		for k, v := range other.backoffSleep {
			s.backoffSleep[k] += v
		}
	}
	s.totalProcessTime += other.totalProcessTime
	s.totalWaitTime += other.totalWaitTime
	s.reqStat.Merge(other.reqStat)
	s.CoprCacheHitNum += other.CoprCacheHitNum
	if other.distSQLConcurrency > s.distSQLConcurrency {
		s.distSQLConcurrency = other.distSQLConcurrency
	}
	if other.extraConcurrency > s.extraConcurrency {
		s.extraConcurrency = other.extraConcurrency
	}
	s.storeBatchedNum += other.storeBatchedNum
	s.storeBatchedFallbackNum += other.storeBatchedFallbackNum
	s.buildTaskDuration += other.buildTaskDuration
	s.fetchRspDuration += other.fetchRspDuration
}

func (s *selectResultRuntimeStats) String() string {
	buf := bytes.NewBuffer(nil)
	reqStat := s.reqStat
	if s.copRespTime.Size() > 0 {
		size := s.copRespTime.Size()
		if size == 1 {
			fmt.Fprintf(buf, "cop_task: {num: 1, max: %v, proc_keys: %v", execdetails.FormatDuration(time.Duration(s.copRespTime.GetPercentile(0))), s.procKeys.GetPercentile(0))
		} else {
			vMax, vMin := s.copRespTime.GetMax(), s.copRespTime.GetMin()
			vP95 := s.copRespTime.GetPercentile(0.95)
			sum := s.copRespTime.Sum()
			vAvg := time.Duration(sum / float64(size))

			keyMax := s.procKeys.GetMax()
			keyP95 := s.procKeys.GetPercentile(0.95)
			fmt.Fprintf(buf, "cop_task: {num: %v, max: %v, min: %v, avg: %v, p95: %v", size,
				execdetails.FormatDuration(time.Duration(vMax.GetFloat64())), execdetails.FormatDuration(time.Duration(vMin.GetFloat64())),
				execdetails.FormatDuration(vAvg), execdetails.FormatDuration(time.Duration(vP95)))
			if keyMax > 0 {
				buf.WriteString(", max_proc_keys: ")
				buf.WriteString(strconv.FormatInt(int64(keyMax), 10))
				buf.WriteString(", p95_proc_keys: ")
				buf.WriteString(strconv.FormatInt(int64(keyP95), 10))
			}
		}
		if s.totalProcessTime > 0 {
			buf.WriteString(", tot_proc: ")
			buf.WriteString(execdetails.FormatDuration(s.totalProcessTime))
			if s.totalWaitTime > 0 {
				buf.WriteString(", tot_wait: ")
				buf.WriteString(execdetails.FormatDuration(s.totalWaitTime))
			}
		}
		if config.GetGlobalConfig().TiKVClient.CoprCache.CapacityMB > 0 {
			fmt.Fprintf(buf, ", copr_cache_hit_ratio: %v",
				strconv.FormatFloat(s.calcCacheHit(), 'f', 2, 64))
		} else {
			buf.WriteString(", copr_cache: disabled")
		}
		if s.buildTaskDuration > 0 {
			buf.WriteString(", build_task_duration: ")
			buf.WriteString(execdetails.FormatDuration(s.buildTaskDuration))
		}
		if s.distSQLConcurrency > 0 {
			buf.WriteString(", max_distsql_concurrency: ")
			buf.WriteString(strconv.FormatInt(int64(s.distSQLConcurrency), 10))
		}
		if s.extraConcurrency > 0 {
			buf.WriteString(", max_extra_concurrency: ")
			buf.WriteString(strconv.FormatInt(int64(s.extraConcurrency), 10))
		}
		if s.storeBatchedNum > 0 {
			buf.WriteString(", store_batch_num: ")
			buf.WriteString(strconv.FormatInt(int64(s.storeBatchedNum), 10))
		}
		if s.storeBatchedFallbackNum > 0 {
			buf.WriteString(", store_batch_fallback_num: ")
			buf.WriteString(strconv.FormatInt(int64(s.storeBatchedFallbackNum), 10))
		}
		buf.WriteString("}")
		if s.fetchRspDuration > 0 {
			buf.WriteString(", fetch_resp_duration: ")
			buf.WriteString(execdetails.FormatDuration(s.fetchRspDuration))
		}
	}

	rpcStatsStr := reqStat.String()
	if len(rpcStatsStr) > 0 {
		buf.WriteString(", rpc_info:{")
		buf.WriteString(rpcStatsStr)
		buf.WriteString("}")
	}

	if len(s.backoffSleep) > 0 {
		buf.WriteString(", backoff{")
		idx := 0
		for k, d := range s.backoffSleep {
			if idx > 0 {
				buf.WriteString(", ")
			}
			idx++
			fmt.Fprintf(buf, "%s: %s", k, execdetails.FormatDuration(d))
		}
		buf.WriteString("}")
	}
	return buf.String()
}

// Tp implements the RuntimeStats interface.
func (*selectResultRuntimeStats) Tp() int {
	return execdetails.TpSelectResultRuntimeStats
}

func (s *selectResultRuntimeStats) calcCacheHit() float64 {
	hit := s.CoprCacheHitNum
	tot := s.copRespTime.Size()
	if s.storeBatchedNum > 0 {
		tot += int(s.storeBatchedNum)
	}
	if tot == 0 {
		return 0
	}
	return float64(hit) / float64(tot)
}
