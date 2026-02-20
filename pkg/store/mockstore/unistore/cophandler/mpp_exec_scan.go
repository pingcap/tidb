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

package cophandler

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"slices"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/lockstore"
	"github.com/pingcap/tidb/pkg/store/mockstore/unistore/tikv/dbreader"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/rowcodec"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
)

type scanResult struct {
	chk              *chunk.Chunk
	lastProcessedKey kv.Key
	err              error
}

type tableScanExec struct {
	baseMPPExec

	kvRanges      []kv.KeyRange
	startTS       uint64
	dbReader      *dbreader.DBReader
	lockStore     *lockstore.MemStore
	resolvedLocks []uint64
	counts        []int64
	ndvs          []int64
	rowCnt        int64

	chk    *chunk.Chunk
	result chan scanResult
	done   chan struct{}
	wg     util.WaitGroupWrapper

	decoder *rowcodec.ChunkDecoder
	desc    bool

	// if ExtraPhysTblIDCol is requested, fill in the physical table id in this column position
	physTblIDColIdx *int
	// This is used to update the paging range result, updated in next().
	paging *coprocessor.KeyRange
}

func (e *tableScanExec) SkipValue() bool { return false }

func (e *tableScanExec) Process(key, value []byte, commitTS uint64) error {
	handle, err := tablecodec.DecodeRowKey(key)
	if err != nil {
		return errors.Trace(err)
	}

	err = e.decoder.DecodeToChunk(value, commitTS, handle, e.chk)
	if err != nil {
		return errors.Trace(err)
	}
	if e.physTblIDColIdx != nil {
		tblID := tablecodec.DecodeTableID(key)
		e.chk.AppendInt64(*e.physTblIDColIdx, tblID)
	}
	e.rowCnt++

	if e.chk.IsFull() {
		lastProcessed := kv.Key(slices.Clone(key)) // make a copy to avoid data race
		select {
		case e.result <- scanResult{chk: e.chk, lastProcessedKey: lastProcessed, err: nil}:
			e.chk = chunk.NewChunkWithCapacity(e.fieldTypes, DefaultBatchSize)
		case <-e.done:
			return dbreader.ErrScanBreak
		}
	}
	select {
	case <-e.done:
		return dbreader.ErrScanBreak
	default:
	}
	return nil
}

func (e *tableScanExec) open() error {
	var err error
	if e.lockStore != nil {
		for _, ran := range e.kvRanges {
			err = checkRangeLockForRange(e.lockStore, e.startTS, e.resolvedLocks, ran)
			if err != nil {
				return err
			}
		}
	}
	e.chk = chunk.NewChunkWithCapacity(e.fieldTypes, DefaultBatchSize)
	e.result = make(chan scanResult, 1)
	e.done = make(chan struct{})
	e.wg.Run(func() {
		// close the channel when done scanning, so that next() will got nil chunk
		defer close(e.result)
		var i int
		var ran kv.KeyRange
		for i, ran = range e.kvRanges {
			oldCnt := e.rowCnt
			if e.desc {
				err = e.dbReader.ReverseScan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e)
			} else {
				err = e.dbReader.Scan(ran.StartKey, ran.EndKey, math.MaxInt64, e.startTS, e)
			}
			if len(e.counts) != 0 {
				e.counts[i] += e.rowCnt - oldCnt
				e.ndvs[i] += e.rowCnt - oldCnt
			}
			if err != nil {
				e.result <- scanResult{err: err}
				return
			}
		}

		// handle the last chunk
		if e.chk != nil && e.chk.NumRows() > 0 {
			select {
			case e.result <- scanResult{chk: e.chk, err: nil}:
				return
			case <-e.done:
			}
			return
		}
	})

	return nil
}

func (e *tableScanExec) next() (*chunk.Chunk, error) {
	result := <-e.result
	// Update the range for coprocessor paging protocol.
	if e.paging != nil && result.err == nil {
		if e.desc {
			if result.lastProcessedKey != nil {
				*e.paging = coprocessor.KeyRange{Start: result.lastProcessedKey}
			} else {
				*e.paging = coprocessor.KeyRange{Start: e.kvRanges[len(e.kvRanges)-1].StartKey}
			}
		} else {
			if result.lastProcessedKey != nil {
				*e.paging = coprocessor.KeyRange{End: result.lastProcessedKey.Next()}
			} else {
				*e.paging = coprocessor.KeyRange{End: e.kvRanges[len(e.kvRanges)-1].EndKey}
			}
		}
	}

	if result.chk == nil || result.err != nil {
		return nil, result.err
	}
	e.execSummary.updateOnlyRows(result.chk.NumRows())
	return result.chk, nil
}

func (e *tableScanExec) stop() error {
	// just in case the channel is not initialized
	if e.done != nil {
		close(e.done)
	}
	e.wg.Wait()
	return nil
}

type indexScanExec struct {
	baseMPPExec

	startTS       uint64
	kvRanges      []kv.KeyRange
	desc          bool
	dbReader      *dbreader.DBReader
	lockStore     *lockstore.MemStore
	resolvedLocks []uint64
	counts        []int64
	ndvs          []int64
	prevVals      [][]byte
	rowCnt        int64
	ndvCnt        int64
	chk           *chunk.Chunk
	chkIdx        int
	chunks        []*chunk.Chunk

	colInfos   []rowcodec.ColInfo
	numIdxCols int
	hdlStatus  tablecodec.HandleStatus

	// if ExtraPhysTblIDCol is requested, fill in the physical table id in this column position
	physTblIDColIdx *int
	// if common handle key is requested, fill the common handle in this column
	commonHandleKeyIdx *int
	// This is used to update the paging range result, updated in next().
	paging                 *coprocessor.KeyRange
	chunkLastProcessedKeys []kv.Key
}

func (e *indexScanExec) SkipValue() bool { return false }

func (e *indexScanExec) isNewVals(values [][]byte) bool {
	for i := range e.numIdxCols {
		if !bytes.Equal(e.prevVals[i], values[i]) {
			return true
		}
	}
	return false
}

func (e *indexScanExec) Process(key, value []byte, _ uint64) error {
	decodedKey := key
	if !kv.Key(key).HasPrefix(tablecodec.TablePrefix()) {
		// If the key is in API V2, then ignore the prefix
		_, k, err := tikv.DecodeKey(key, kvrpcpb.APIVersion_V2)
		if err != nil {
			return errors.Trace(err)
		}
		decodedKey = k
		if !kv.Key(decodedKey).HasPrefix(tablecodec.TablePrefix()) {
			return errors.Errorf("invalid index key %q after decoded", key)
		}
	}
	values, err := tablecodec.DecodeIndexKV(decodedKey, value, e.numIdxCols, e.hdlStatus, e.colInfos)
	if err != nil {
		return err
	}
	e.rowCnt++
	if len(e.counts) > 0 && (len(e.prevVals[0]) == 0 || e.isNewVals(values)) {
		e.ndvCnt++
		for i := range e.numIdxCols {
			e.prevVals[i] = append(e.prevVals[i][:0], values[i]...)
		}
	}
	decoder := codec.NewDecoder(e.chk, e.sctx.GetSessionVars().StmtCtx.TimeZone())
	for i, value := range values {
		if i < len(e.fieldTypes) {
			_, err = decoder.DecodeOne(value, i, e.fieldTypes[i])
			if err != nil {
				return errors.Trace(err)
			}
		}
	}

	// If we need pid, it already filled by above loop. Because `DecodeIndexKV` func will return pid in `values`.
	// The following if statement is to fill in the tid when we needed it.
	if e.physTblIDColIdx != nil && *e.physTblIDColIdx >= len(values) {
		tblID := tablecodec.DecodeTableID(decodedKey)
		e.chk.AppendInt64(*e.physTblIDColIdx, tblID)
	}

	// If we need common handle key, we should fill it here.
	if e.commonHandleKeyIdx != nil && *e.commonHandleKeyIdx >= len(values) {
		h, err := tablecodec.DecodeIndexHandle(decodedKey, value, e.numIdxCols)
		if err != nil {
			return err
		}
		commonHandle, ok := h.(*kv.CommonHandle)
		if !ok {
			return errors.New("common handle expected")
		}
		e.chk.AppendBytes(*e.commonHandleKeyIdx, commonHandle.Encoded())
	}

	if e.chk.IsFull() {
		e.chunks = append(e.chunks, e.chk)
		if e.paging != nil {
			lastProcessed := kv.Key(slices.Clone(key)) // need a deep copy to store the key
			e.chunkLastProcessedKeys = append(e.chunkLastProcessedKeys, lastProcessed)
		}
		e.chk = chunk.NewChunkWithCapacity(e.fieldTypes, DefaultBatchSize)
	}
	return nil
}

func (e *indexScanExec) open() error {
	var err error
	for _, ran := range e.kvRanges {
		err = checkRangeLockForRange(e.lockStore, e.startTS, e.resolvedLocks, ran)
		if err != nil {
			return err
		}
	}
	e.chk = chunk.NewChunkWithCapacity(e.fieldTypes, DefaultBatchSize)
	for i, rg := range e.kvRanges {
		oldCnt := e.rowCnt
		e.ndvCnt = 0
		if e.desc {
			err = e.dbReader.ReverseScan(rg.StartKey, rg.EndKey, math.MaxInt64, e.startTS, e)
		} else {
			err = e.dbReader.Scan(rg.StartKey, rg.EndKey, math.MaxInt64, e.startTS, e)
		}
		if len(e.counts) != 0 {
			e.counts[i] += e.rowCnt - oldCnt
			e.ndvs[i] += e.ndvCnt
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	if e.chk.NumRows() != 0 {
		e.chunks = append(e.chunks, e.chk)
	}
	return nil
}

func (e *indexScanExec) next() (*chunk.Chunk, error) {
	if e.chkIdx < len(e.chunks) {
		e.chkIdx++
		e.execSummary.updateOnlyRows(e.chunks[e.chkIdx-1].NumRows())
		if e.paging != nil {
			if e.desc {
				if e.chkIdx == len(e.chunks) {
					*e.paging = coprocessor.KeyRange{Start: e.kvRanges[len(e.kvRanges)-1].StartKey}
				} else {
					*e.paging = coprocessor.KeyRange{Start: e.chunkLastProcessedKeys[e.chkIdx-1]}
				}
			} else {
				if e.chkIdx == len(e.chunks) {
					*e.paging = coprocessor.KeyRange{End: e.kvRanges[len(e.kvRanges)-1].EndKey}
				} else {
					*e.paging = coprocessor.KeyRange{End: e.chunkLastProcessedKeys[e.chkIdx-1].Next()}
				}
			}
		}
		return e.chunks[e.chkIdx-1], nil
	}

	if e.paging != nil {
		if e.desc {
			*e.paging = coprocessor.KeyRange{Start: e.kvRanges[len(e.kvRanges)-1].StartKey}
		} else {
			*e.paging = coprocessor.KeyRange{End: e.kvRanges[len(e.kvRanges)-1].EndKey}
		}
	}
	return nil, nil
}

type indexLookUpExec struct {
	baseMPPExec
	keyspaceID          uint32
	indexHandleOffsets  []uint32
	tblScanPB           *tipb.TableScan
	isCommonHandle      bool
	extraReaderProvider dbreader.ExtraDbReaderProvider
	buildTableScan      func(*dbreader.DBReader, []kv.KeyRange) (*tableScanExec, error)
	indexChunks         []*chunk.Chunk
}

func (e *indexLookUpExec) open() error {
	return e.children[0].open()
}

func (e *indexLookUpExec) stop() error {
	return e.children[0].stop()
}

func (e *indexLookUpExec) next() (ret *chunk.Chunk, _ error) {
	tblScans, counts, indexChk, err := e.fetchTableScans()
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(tblScans) == 0 && (indexChk == nil || indexChk.NumRows() == 0) {
		return nil, nil
	}

	if indexChk != nil {
		e.indexChunks = append(e.indexChunks, indexChk)
	}

	for i, tblScan := range tblScans {
		expectCnt := counts[i]
		err = func() error {
			err = tblScan.open()
			defer func() {
				err := tblScan.stop()
				if err != nil {
					panic(err)
				}
			}()

			readCnt := 0
			for {
				chk, err := tblScan.next()
				if err != nil {
					return err
				}

				if chk == nil || chk.NumRows() == 0 {
					break
				}

				if ret == nil {
					ret = chk
				} else {
					ret.Append(chk, 0, chk.NumRows())
				}
				readCnt += chk.NumRows()
				e.execSummary.updateOnlyRows(chk.NumRows())
				e.children[1].(*baseMPPExec).execSummary.updateOnlyRows(chk.NumRows())
			}

			if expectCnt != readCnt {
				panic(fmt.Sprintf("data may be inconsistency, expectCnt(%d) != readCnt(%d)", expectCnt, readCnt))
			}

			return nil
		}()

		if err != nil {
			return nil, err
		}
	}

	return ret, nil
}

func (e *indexLookUpExec) fetchTableScans() (tableScans []*tableScanExec, counts []int, indexChk *chunk.Chunk, err error) {
	var handleFilter func(kv.Handle) bool
	failpoint.InjectCall("inject-index-lookup-handle-filter", &handleFilter)
	type Handle struct {
		kv.Handle
		IndexOrder int
	}

	rowCnt := 0
	indexRows := make([]chunk.Row, 0, DefaultBatchSize)
	sortedHandles := make([]Handle, 0, DefaultBatchSize)
	for rowCnt < DefaultBatchSize {
		chk, err := e.children[0].next()
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		if chk == nil || chk.NumRows() == 0 {
			break
		}

		for i := range chk.NumRows() {
			row := chk.GetRow(i)
			indexRows = append(indexRows, row)
			handle, err := e.buildHandle(row)
			if err != nil {
				return nil, nil, nil, err
			}

			sortedHandles = append(sortedHandles, Handle{
				IndexOrder: rowCnt,
				Handle:     handle,
			})
			rowCnt++
		}
	}

	leftRows := make([]bool, len(indexRows))
	sort.Slice(sortedHandles, func(i, j int) bool {
		return sortedHandles[i].Compare(sortedHandles[j]) < 0
	})

	var curRegion dbreader.LocateExtraRegionResult
	var curKeys []kv.Key
	var curHandles []Handle
	endRegion := func() error {
		if len(curKeys) == 0 {
			return nil
		}

		defer func() {
			curRegion = dbreader.LocateExtraRegionResult{}
			curKeys = curKeys[:0]
			curHandles = curHandles[:0]
		}()

		ranges := make([]kv.KeyRange, 0)
		rangeStart := 0
		for i, h := range curHandles {
			if !e.isCommonHandle && i < len(curHandles)-1 && h.Next().Compare(curHandles[i+1]) == 0 {
				continue
			}
			ranges = append(ranges, kv.KeyRange{
				StartKey: curKeys[rangeStart],
				EndKey:   curKeys[i].Next(),
			})
			rangeStart = i + 1
		}

		reader, pbErr := e.extraReaderProvider.GetExtraDBReaderByRegion(dbreader.GetExtraDBReaderContext{
			Region: curRegion.Region,
			Peer:   curRegion.Peer,
			Ranges: ranges,
		})

		if pbErr != nil {
			for _, h := range curHandles {
				leftRows[h.IndexOrder] = true
			}
			logutil.BgLogger().Info("GetExtraDBReaderByRegion failed", zap.Any("err", pbErr))
		} else {
			tableScan, err := e.buildTableScan(reader, ranges)
			if err != nil {
				return err
			}
			tableScans = append(tableScans, tableScan)
			counts = append(counts, len(curHandles))
		}

		return nil
	}

	var codecV2 tikv.Codec
	if kerneltype.IsNextGen() {
		codecV2, err = tikv.NewCodecV2(tikv.ModeTxn, &keyspacepb.KeyspaceMeta{
			Id: e.keyspaceID,
		})
		if err != nil {
			return nil, nil, nil, err
		}
	}

	for _, h := range sortedHandles {
		if handleFilter != nil && !handleFilter(h.Handle) {
			leftRows[h.IndexOrder] = true
			continue
		}
		rowKey := tablecodec.EncodeRowKey(e.tblScanPB.TableId, h.Encoded())
		if codecV2 != nil {
			rowKey = codecV2.EncodeKey(rowKey)
		}
		mvccKey := codec.EncodeBytes(nil, rowKey)
		if curRegion.Found {
			if e.regionContainsKey(curRegion.Region, mvccKey) {
				curKeys = append(curKeys, rowKey)
				curHandles = append(curHandles, h)
				continue
			}

			if err = endRegion(); err != nil {
				return nil, nil, nil, err
			}
		}

		curRegion, err = e.extraReaderProvider.LocateExtraRegion(context.TODO(), mvccKey)
		if err != nil {
			return nil, nil, nil, err
		}

		if curRegion.Found {
			curKeys = append(curKeys, rowKey)
			curHandles = append(curHandles, h)
		} else {
			leftRows[h.IndexOrder] = true
		}
	}

	if err = endRegion(); err != nil {
		return nil, nil, nil, err
	}

	for i, left := range leftRows {
		if !left {
			continue
		}

		if indexChk == nil {
			indexChk = chunk.NewChunkWithCapacity(e.children[0].getFieldTypes(), DefaultBatchSize)
		}

		indexChk.AppendRow(indexRows[i])
	}

	return
}

func (e *indexLookUpExec) regionContainsKey(r *metapb.Region, key []byte) bool {
	return bytes.Compare(r.GetStartKey(), key) <= 0 &&
		(bytes.Compare(key, r.GetEndKey()) < 0 || len(r.GetEndKey()) == 0)
}

func (e *indexLookUpExec) buildHandle(row chunk.Row) (kv.Handle, error) {
	if e.isCommonHandle {
		return kv.NewCommonHandle(row.GetBytes(row.Len() - 1))
	}
	i := row.GetInt64(int(e.indexHandleOffsets[0]))
	return kv.IntHandle(i), nil
}

func (e *indexLookUpExec) takeIntermediateResults() (ret []*chunk.Chunk) {
	ret, e.indexChunks = e.indexChunks, nil
	return
}

func (e *indexLookUpExec) getIntermediateFieldTypes() []*types.FieldType {
	return e.children[0].getFieldTypes()
}
