// Copyright 2019 PingCAP, Inc.
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

package executor

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/cznic/mathutil"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/planner/core"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// SplitIndexRegionExec represents a split index regions executor.
type SplitIndexRegionExec struct {
	baseExecutor

	tableInfo      *model.TableInfo
	partitionNames []model.CIStr
	indexInfo      *model.IndexInfo
	lower          []types.Datum
	upper          []types.Datum
	num            int
	valueLists     [][]types.Datum
	splitIdxKeys   [][]byte

	done bool
	splitRegionResult
}

// nolint:structcheck
type splitRegionResult struct {
	splitRegions     int
	finishScatterNum int
}

// Open implements the Executor Open interface.
func (e *SplitIndexRegionExec) Open(ctx context.Context) (err error) {
	e.splitIdxKeys, err = e.getSplitIdxKeys()
	return err
}

// Next implements the Executor Next interface.
func (e *SplitIndexRegionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true
	if err := e.splitIndexRegion(ctx); err != nil {
		return err
	}

	appendSplitRegionResultToChunk(chk, e.splitRegions, e.finishScatterNum)
	return nil
}

// checkScatterRegionFinishBackOff is the back off time that used to check if a region has finished scattering before split region timeout.
const checkScatterRegionFinishBackOff = 50

// splitIndexRegion is used to split index regions.
func (e *SplitIndexRegionExec) splitIndexRegion(ctx context.Context) error {
	store := e.ctx.GetStore()
	s, ok := store.(kv.SplittableStore)
	if !ok {
		return nil
	}

	start := time.Now()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, e.ctx.GetSessionVars().GetSplitRegionTimeout())
	defer cancel()
	regionIDs, err := s.SplitRegions(ctxWithTimeout, e.splitIdxKeys, true, &e.tableInfo.ID)
	if err != nil {
		logutil.BgLogger().Warn("split table index region failed",
			zap.String("table", e.tableInfo.Name.L),
			zap.String("index", e.indexInfo.Name.L),
			zap.Error(err))
	}
	e.splitRegions = len(regionIDs)
	if e.splitRegions == 0 {
		return nil
	}

	if !e.ctx.GetSessionVars().WaitSplitRegionFinish {
		return nil
	}
	e.finishScatterNum = waitScatterRegionFinish(ctxWithTimeout, e.ctx, start, s, regionIDs, e.tableInfo.Name.L, e.indexInfo.Name.L)
	return nil
}

func (e *SplitIndexRegionExec) getSplitIdxKeys() ([][]byte, error) {
	// Split index regions by user specified value lists.
	if len(e.valueLists) > 0 {
		return e.getSplitIdxKeysFromValueList()
	}

	return e.getSplitIdxKeysFromBound()
}

func (e *SplitIndexRegionExec) getSplitIdxKeysFromValueList() (keys [][]byte, err error) {
	pi := e.tableInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, len(e.valueLists)+1)
		return e.getSplitIdxPhysicalKeysFromValueList(e.tableInfo.ID, keys)
	}

	// Split for all table partitions.
	if len(e.partitionNames) == 0 {
		keys = make([][]byte, 0, (len(e.valueLists)+1)*len(pi.Definitions))
		for _, p := range pi.Definitions {
			keys, err = e.getSplitIdxPhysicalKeysFromValueList(p.ID, keys)
			if err != nil {
				return nil, err
			}
		}
		return keys, nil
	}

	// Split for specified table partitions.
	keys = make([][]byte, 0, (len(e.valueLists)+1)*len(e.partitionNames))
	for _, name := range e.partitionNames {
		pid, err := tables.FindPartitionByName(e.tableInfo, name.L)
		if err != nil {
			return nil, err
		}
		keys, err = e.getSplitIdxPhysicalKeysFromValueList(pid, keys)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (e *SplitIndexRegionExec) getSplitIdxPhysicalKeysFromValueList(physicalID int64, keys [][]byte) ([][]byte, error) {
	keys = e.getSplitIdxPhysicalStartAndOtherIdxKeys(physicalID, keys)
	index := tables.NewIndex(physicalID, e.tableInfo, e.indexInfo)
	for _, v := range e.valueLists {
		idxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, v, kv.IntHandle(math.MinInt64), nil)
		if err != nil {
			return nil, err
		}
		keys = append(keys, idxKey)
	}
	return keys, nil
}

func (e *SplitIndexRegionExec) getSplitIdxPhysicalStartAndOtherIdxKeys(physicalID int64, keys [][]byte) [][]byte {
	// 1. Split in the start key for the index if the index is not the first index.
	// For the first index, splitting the start key can produce the region [tid, tid_i_1), which is useless.
	if len(e.tableInfo.Indices) > 0 && e.tableInfo.Indices[0].ID != e.indexInfo.ID {
		startKey := tablecodec.EncodeTableIndexPrefix(physicalID, e.indexInfo.ID)
		keys = append(keys, startKey)
	}

	// 2. Split in the end key.
	endKey := tablecodec.EncodeTableIndexPrefix(physicalID, e.indexInfo.ID+1)
	keys = append(keys, endKey)
	return keys
}

func (e *SplitIndexRegionExec) getSplitIdxKeysFromBound() (keys [][]byte, err error) {
	pi := e.tableInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, e.num)
		return e.getSplitIdxPhysicalKeysFromBound(e.tableInfo.ID, keys)
	}

	// Split for all table partitions.
	if len(e.partitionNames) == 0 {
		keys = make([][]byte, 0, e.num*len(pi.Definitions))
		for _, p := range pi.Definitions {
			keys, err = e.getSplitIdxPhysicalKeysFromBound(p.ID, keys)
			if err != nil {
				return nil, err
			}
		}
		return keys, nil
	}

	// Split for specified table partitions.
	keys = make([][]byte, 0, e.num*len(e.partitionNames))
	for _, name := range e.partitionNames {
		pid, err := tables.FindPartitionByName(e.tableInfo, name.L)
		if err != nil {
			return nil, err
		}
		keys, err = e.getSplitIdxPhysicalKeysFromBound(pid, keys)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (e *SplitIndexRegionExec) getSplitIdxPhysicalKeysFromBound(physicalID int64, keys [][]byte) ([][]byte, error) {
	keys = e.getSplitIdxPhysicalStartAndOtherIdxKeys(physicalID, keys)
	index := tables.NewIndex(physicalID, e.tableInfo, e.indexInfo)
	// Split index regions by lower, upper value and calculate the step by (upper - lower)/num.
	lowerIdxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, e.lower, kv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return nil, err
	}
	// Use math.MinInt64 as handle_id for the upper index key to avoid affecting calculate split point.
	// If use math.MaxInt64 here, test of `TestSplitIndex` will report error.
	upperIdxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, e.upper, kv.IntHandle(math.MinInt64), nil)
	if err != nil {
		return nil, err
	}

	if bytes.Compare(lowerIdxKey, upperIdxKey) >= 0 {
		lowerStr := datumSliceToString(e.lower)
		upperStr := datumSliceToString(e.upper)
		errMsg := fmt.Sprintf("Split index `%v` region lower value %v should less than the upper value %v",
			e.indexInfo.Name, lowerStr, upperStr)
		return nil, ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg)
	}
	return getValuesList(lowerIdxKey, upperIdxKey, e.num, keys), nil
}

// getValuesList is used to get `num` values between lower and upper value.
// To Simplify the explain, suppose lower and upper value type is int64, and lower=0, upper=100, num=10,
// then calculate the step=(upper-lower)/num=10, then the function should return 0+10, 10+10, 20+10... all together 9 (num-1) values.
// Then the function will return [10,20,30,40,50,60,70,80,90].
// The difference is the value type of upper, lower is []byte, So I use getUint64FromBytes to convert []byte to uint64.
func getValuesList(lower, upper []byte, num int, valuesList [][]byte) [][]byte {
	commonPrefixIdx := longestCommonPrefixLen(lower, upper)
	step := getStepValue(lower[commonPrefixIdx:], upper[commonPrefixIdx:], num)
	startV := getUint64FromBytes(lower[commonPrefixIdx:], 0)
	// To get `num` regions, only need to split `num-1` idx keys.
	buf := make([]byte, 8)
	for i := 0; i < num-1; i++ {
		value := make([]byte, 0, commonPrefixIdx+8)
		value = append(value, lower[:commonPrefixIdx]...)
		startV += step
		binary.BigEndian.PutUint64(buf, startV)
		value = append(value, buf...)
		valuesList = append(valuesList, value)
	}
	return valuesList
}

// longestCommonPrefixLen gets the longest common prefix byte length.
func longestCommonPrefixLen(s1, s2 []byte) int {
	l := mathutil.Min(len(s1), len(s2))
	i := 0
	for ; i < l; i++ {
		if s1[i] != s2[i] {
			break
		}
	}
	return i
}

// getStepValue gets the step of between the lower and upper value. step = (upper-lower)/num.
// Convert byte slice to uint64 first.
func getStepValue(lower, upper []byte, num int) uint64 {
	lowerUint := getUint64FromBytes(lower, 0)
	upperUint := getUint64FromBytes(upper, 0xff)
	return (upperUint - lowerUint) / uint64(num)
}

// getUint64FromBytes gets a uint64 from the `bs` byte slice.
// If len(bs) < 8, then padding with `pad`.
func getUint64FromBytes(bs []byte, pad byte) uint64 {
	buf := bs
	if len(buf) < 8 {
		buf = make([]byte, 0, 8)
		buf = append(buf, bs...)
		for i := len(buf); i < 8; i++ {
			buf = append(buf, pad)
		}
	}
	return binary.BigEndian.Uint64(buf)
}

func datumSliceToString(ds []types.Datum) string {
	str := "("
	for i, d := range ds {
		s, err := d.ToString()
		if err != nil {
			return fmt.Sprintf("%v", ds)
		}
		if i > 0 {
			str += ","
		}
		str += s
	}
	str += ")"
	return str
}

// SplitTableRegionExec represents a split table regions executor.
type SplitTableRegionExec struct {
	baseExecutor

	tableInfo      *model.TableInfo
	partitionNames []model.CIStr
	lower          []types.Datum
	upper          []types.Datum
	num            int
	handleCols     core.HandleCols
	valueLists     [][]types.Datum
	splitKeys      [][]byte

	done bool
	splitRegionResult
}

// Open implements the Executor Open interface.
func (e *SplitTableRegionExec) Open(ctx context.Context) (err error) {
	e.splitKeys, err = e.getSplitTableKeys()
	return err
}

// Next implements the Executor Next interface.
func (e *SplitTableRegionExec) Next(ctx context.Context, chk *chunk.Chunk) error {
	chk.Reset()
	if e.done {
		return nil
	}
	e.done = true

	if err := e.splitTableRegion(ctx); err != nil {
		return err
	}
	appendSplitRegionResultToChunk(chk, e.splitRegions, e.finishScatterNum)
	return nil
}

func (e *SplitTableRegionExec) splitTableRegion(ctx context.Context) error {
	store := e.ctx.GetStore()
	s, ok := store.(kv.SplittableStore)
	if !ok {
		return nil
	}

	start := time.Now()
	ctxWithTimeout, cancel := context.WithTimeout(ctx, e.ctx.GetSessionVars().GetSplitRegionTimeout())
	defer cancel()

	regionIDs, err := s.SplitRegions(ctxWithTimeout, e.splitKeys, true, &e.tableInfo.ID)
	if err != nil {
		logutil.BgLogger().Warn("split table region failed",
			zap.String("table", e.tableInfo.Name.L),
			zap.Error(err))
	}
	e.splitRegions = len(regionIDs)
	if e.splitRegions == 0 {
		return nil
	}

	if !e.ctx.GetSessionVars().WaitSplitRegionFinish {
		return nil
	}

	e.finishScatterNum = waitScatterRegionFinish(ctxWithTimeout, e.ctx, start, s, regionIDs, e.tableInfo.Name.L, "")
	return nil
}

func waitScatterRegionFinish(ctxWithTimeout context.Context, sctx sessionctx.Context, startTime time.Time, store kv.SplittableStore, regionIDs []uint64, tableName, indexName string) int {
	remainMillisecond := 0
	finishScatterNum := 0
	for _, regionID := range regionIDs {
		if isCtxDone(ctxWithTimeout) {
			// Do not break here for checking remain regions scatter finished with a very short backoff time.
			// Consider this situation -  Regions 1, 2, and 3 are to be split.
			// Region 1 times out before scattering finishes, while Region 2 and Region 3 have finished scattering.
			// In this case, we should return 2 Regions, instead of 0, have finished scattering.
			remainMillisecond = checkScatterRegionFinishBackOff
		} else {
			remainMillisecond = int((sctx.GetSessionVars().GetSplitRegionTimeout().Seconds() - time.Since(startTime).Seconds()) * 1000)
		}

		err := store.WaitScatterRegionFinish(ctxWithTimeout, regionID, remainMillisecond)
		if err == nil {
			finishScatterNum++
		} else {
			if len(indexName) == 0 {
				logutil.BgLogger().Warn("wait scatter region failed",
					zap.Uint64("regionID", regionID),
					zap.String("table", tableName),
					zap.Error(err))
			} else {
				logutil.BgLogger().Warn("wait scatter region failed",
					zap.Uint64("regionID", regionID),
					zap.String("table", tableName),
					zap.String("index", indexName),
					zap.Error(err))
			}
		}
	}
	return finishScatterNum
}

func appendSplitRegionResultToChunk(chk *chunk.Chunk, totalRegions, finishScatterNum int) {
	chk.AppendInt64(0, int64(totalRegions))
	if finishScatterNum > 0 && totalRegions > 0 {
		chk.AppendFloat64(1, float64(finishScatterNum)/float64(totalRegions))
	} else {
		chk.AppendFloat64(1, float64(0))
	}
}

func isCtxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

var minRegionStepValue = int64(1000)

func (e *SplitTableRegionExec) getSplitTableKeys() ([][]byte, error) {
	if len(e.valueLists) > 0 {
		return e.getSplitTableKeysFromValueList()
	}

	return e.getSplitTableKeysFromBound()
}

func (e *SplitTableRegionExec) getSplitTableKeysFromValueList() ([][]byte, error) {
	var keys [][]byte
	pi := e.tableInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, len(e.valueLists))
		return e.getSplitTablePhysicalKeysFromValueList(e.tableInfo.ID, keys)
	}

	// Split for all table partitions.
	if len(e.partitionNames) == 0 {
		keys = make([][]byte, 0, len(e.valueLists)*len(pi.Definitions))
		for _, p := range pi.Definitions {
			var err error
			keys, err = e.getSplitTablePhysicalKeysFromValueList(p.ID, keys)
			if err != nil {
				return nil, err
			}
		}
		return keys, nil
	}

	// Split for specified table partitions.
	keys = make([][]byte, 0, len(e.valueLists)*len(e.partitionNames))
	for _, name := range e.partitionNames {
		pid, err := tables.FindPartitionByName(e.tableInfo, name.L)
		if err != nil {
			return nil, err
		}
		keys, err = e.getSplitTablePhysicalKeysFromValueList(pid, keys)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (e *SplitTableRegionExec) getSplitTablePhysicalKeysFromValueList(physicalID int64, keys [][]byte) ([][]byte, error) {
	recordPrefix := tablecodec.GenTableRecordPrefix(physicalID)
	for _, v := range e.valueLists {
		handle, err := e.handleCols.BuildHandleByDatums(v)
		if err != nil {
			return nil, err
		}
		key := tablecodec.EncodeRecordKey(recordPrefix, handle)
		keys = append(keys, key)
	}
	return keys, nil
}

func (e *SplitTableRegionExec) getSplitTableKeysFromBound() ([][]byte, error) {
	var keys [][]byte
	pi := e.tableInfo.GetPartitionInfo()
	if pi == nil {
		keys = make([][]byte, 0, e.num)
		return e.getSplitTablePhysicalKeysFromBound(e.tableInfo.ID, keys)
	}

	// Split for all table partitions.
	if len(e.partitionNames) == 0 {
		keys = make([][]byte, 0, e.num*len(pi.Definitions))
		for _, p := range pi.Definitions {
			var err error
			keys, err = e.getSplitTablePhysicalKeysFromBound(p.ID, keys)
			if err != nil {
				return nil, err
			}
		}
		return keys, nil
	}

	// Split for specified table partitions.
	keys = make([][]byte, 0, e.num*len(e.partitionNames))
	for _, name := range e.partitionNames {
		pid, err := tables.FindPartitionByName(e.tableInfo, name.L)
		if err != nil {
			return nil, err
		}
		keys, err = e.getSplitTablePhysicalKeysFromBound(pid, keys)
		if err != nil {
			return nil, err
		}
	}
	return keys, nil
}

func (e *SplitTableRegionExec) calculateIntBoundValue() (lowerValue int64, step int64, err error) {
	isUnsigned := false
	if e.tableInfo.PKIsHandle {
		if pkCol := e.tableInfo.GetPkColInfo(); pkCol != nil {
			isUnsigned = mysql.HasUnsignedFlag(pkCol.Flag)
		}
	}
	if isUnsigned {
		lowerRecordID := e.lower[0].GetUint64()
		upperRecordID := e.upper[0].GetUint64()
		if upperRecordID <= lowerRecordID {
			errMsg := fmt.Sprintf("lower value %v should less than the upper value %v", lowerRecordID, upperRecordID)
			return 0, 0, ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg)
		}
		step = int64((upperRecordID - lowerRecordID) / uint64(e.num))
		lowerValue = int64(lowerRecordID)
	} else {
		lowerRecordID := e.lower[0].GetInt64()
		upperRecordID := e.upper[0].GetInt64()
		if upperRecordID <= lowerRecordID {
			errMsg := fmt.Sprintf("lower value %v should less than the upper value %v", lowerRecordID, upperRecordID)
			return 0, 0, ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg)
		}
		step = int64(uint64(upperRecordID-lowerRecordID) / uint64(e.num))
		lowerValue = lowerRecordID
	}
	if step < minRegionStepValue {
		errMsg := fmt.Sprintf("the region size is too small, expected at least %d, but got %d", step, minRegionStepValue)
		return 0, 0, ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg)
	}
	return lowerValue, step, nil
}

func (e *SplitTableRegionExec) getSplitTablePhysicalKeysFromBound(physicalID int64, keys [][]byte) ([][]byte, error) {
	recordPrefix := tablecodec.GenTableRecordPrefix(physicalID)
	// Split a separate region for index.
	containsIndex := len(e.tableInfo.Indices) > 0 && !(e.tableInfo.IsCommonHandle && len(e.tableInfo.Indices) == 1)
	if containsIndex {
		keys = append(keys, recordPrefix)
	}

	if e.handleCols.IsInt() {
		low, step, err := e.calculateIntBoundValue()
		if err != nil {
			return nil, err
		}
		recordID := low
		for i := 1; i < e.num; i++ {
			recordID += step
			key := tablecodec.EncodeRecordKey(recordPrefix, kv.IntHandle(recordID))
			keys = append(keys, key)
		}
		return keys, nil
	}
	lowerHandle, err := e.handleCols.BuildHandleByDatums(e.lower)
	if err != nil {
		return nil, err
	}
	upperHandle, err := e.handleCols.BuildHandleByDatums(e.upper)
	if err != nil {
		return nil, err
	}
	if lowerHandle.Compare(upperHandle) >= 0 {
		lowerStr := datumSliceToString(e.lower)
		upperStr := datumSliceToString(e.upper)
		errMsg := fmt.Sprintf("Split table `%v` region lower value %v should less than the upper value %v",
			e.tableInfo.Name.O, lowerStr, upperStr)
		return nil, ErrInvalidSplitRegionRanges.GenWithStackByArgs(errMsg)
	}
	low := tablecodec.EncodeRecordKey(recordPrefix, lowerHandle)
	up := tablecodec.EncodeRecordKey(recordPrefix, upperHandle)
	return getValuesList(low, up, e.num, keys), nil
}

// RegionMeta contains a region's peer detail
type regionMeta struct {
	region          *metapb.Region
	leaderID        uint64
	storeID         uint64 // storeID is the store ID of the leader region.
	start           string
	end             string
	scattering      bool
	writtenBytes    int64
	readBytes       int64
	approximateSize int64
	approximateKeys int64
}

func getPhysicalTableRegions(physicalTableID int64, tableInfo *model.TableInfo, tikvStore helper.Storage, s kv.SplittableStore, uniqueRegionMap map[uint64]struct{}) ([]regionMeta, error) {
	if uniqueRegionMap == nil {
		uniqueRegionMap = make(map[uint64]struct{})
	}
	// This is used to decode the int handle properly.
	var hasUnsignedIntHandle bool
	if pkInfo := tableInfo.GetPkColInfo(); pkInfo != nil {
		hasUnsignedIntHandle = mysql.HasUnsignedFlag(pkInfo.Flag)
	}
	// for record
	startKey, endKey := tablecodec.GetTableHandleKeyRange(physicalTableID)
	regionCache := tikvStore.GetRegionCache()
	recordRegionMetas, err := regionCache.LoadRegionsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
	if err != nil {
		return nil, err
	}
	recordPrefix := tablecodec.GenTableRecordPrefix(physicalTableID)
	tablePrefix := tablecodec.GenTablePrefix(physicalTableID)
	recordRegions, err := getRegionMeta(tikvStore, recordRegionMetas, uniqueRegionMap, tablePrefix, recordPrefix, nil, physicalTableID, 0, hasUnsignedIntHandle)
	if err != nil {
		return nil, err
	}

	regions := recordRegions
	// for indices
	for _, index := range tableInfo.Indices {
		if index.State != model.StatePublic {
			continue
		}
		startKey, endKey := tablecodec.GetTableIndexKeyRange(physicalTableID, index.ID)
		regionMetas, err := regionCache.LoadRegionsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
		if err != nil {
			return nil, err
		}
		indexPrefix := tablecodec.EncodeTableIndexPrefix(physicalTableID, index.ID)
		indexRegions, err := getRegionMeta(tikvStore, regionMetas, uniqueRegionMap, tablePrefix, recordPrefix, indexPrefix, physicalTableID, index.ID, hasUnsignedIntHandle)
		if err != nil {
			return nil, err
		}
		regions = append(regions, indexRegions...)
	}
	err = checkRegionsStatus(s, regions)
	if err != nil {
		return nil, err
	}
	return regions, nil
}

func getPhysicalIndexRegions(physicalTableID int64, indexInfo *model.IndexInfo, tikvStore helper.Storage, s kv.SplittableStore, uniqueRegionMap map[uint64]struct{}) ([]regionMeta, error) {
	if uniqueRegionMap == nil {
		uniqueRegionMap = make(map[uint64]struct{})
	}

	startKey, endKey := tablecodec.GetTableIndexKeyRange(physicalTableID, indexInfo.ID)
	regionCache := tikvStore.GetRegionCache()
	regions, err := regionCache.LoadRegionsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
	if err != nil {
		return nil, err
	}
	recordPrefix := tablecodec.GenTableRecordPrefix(physicalTableID)
	tablePrefix := tablecodec.GenTablePrefix(physicalTableID)
	indexPrefix := tablecodec.EncodeTableIndexPrefix(physicalTableID, indexInfo.ID)
	indexRegions, err := getRegionMeta(tikvStore, regions, uniqueRegionMap, tablePrefix, recordPrefix, indexPrefix, physicalTableID, indexInfo.ID, false)
	if err != nil {
		return nil, err
	}
	err = checkRegionsStatus(s, indexRegions)
	if err != nil {
		return nil, err
	}
	return indexRegions, nil
}

func checkRegionsStatus(store kv.SplittableStore, regions []regionMeta) error {
	for i := range regions {
		scattering, err := store.CheckRegionInScattering(regions[i].region.Id)
		if err != nil {
			return err
		}
		regions[i].scattering = scattering
	}
	return nil
}

func decodeRegionsKey(regions []regionMeta, tablePrefix, recordPrefix, indexPrefix []byte,
	physicalTableID, indexID int64, hasUnsignedIntHandle bool) {
	d := &regionKeyDecoder{
		physicalTableID:      physicalTableID,
		tablePrefix:          tablePrefix,
		recordPrefix:         recordPrefix,
		indexPrefix:          indexPrefix,
		indexID:              indexID,
		hasUnsignedIntHandle: hasUnsignedIntHandle,
	}
	for i := range regions {
		regions[i].start = d.decodeRegionKey(regions[i].region.StartKey)
		regions[i].end = d.decodeRegionKey(regions[i].region.EndKey)
	}
}

type regionKeyDecoder struct {
	physicalTableID      int64
	tablePrefix          []byte
	recordPrefix         []byte
	indexPrefix          []byte
	indexID              int64
	hasUnsignedIntHandle bool
}

func (d *regionKeyDecoder) decodeRegionKey(key []byte) string {
	if len(d.indexPrefix) > 0 && bytes.HasPrefix(key, d.indexPrefix) {
		return fmt.Sprintf("t_%d_i_%d_%x", d.physicalTableID, d.indexID, key[len(d.indexPrefix):])
	} else if len(d.recordPrefix) > 0 && bytes.HasPrefix(key, d.recordPrefix) {
		if len(d.recordPrefix) == len(key) {
			return fmt.Sprintf("t_%d_r", d.physicalTableID)
		}
		isIntHandle := len(key)-len(d.recordPrefix) == 8
		if isIntHandle {
			_, handle, err := codec.DecodeInt(key[len(d.recordPrefix):])
			if err == nil {
				if d.hasUnsignedIntHandle {
					return fmt.Sprintf("t_%d_r_%d", d.physicalTableID, uint64(handle))
				}
				return fmt.Sprintf("t_%d_r_%d", d.physicalTableID, handle)
			}
		}
		return fmt.Sprintf("t_%d_r_%x", d.physicalTableID, key[len(d.recordPrefix):])
	}
	if len(d.tablePrefix) > 0 && bytes.HasPrefix(key, d.tablePrefix) {
		key = key[len(d.tablePrefix):]
		// Has index prefix.
		if !bytes.HasPrefix(key, []byte("_i")) {
			return fmt.Sprintf("t_%d_%x", d.physicalTableID, key)
		}
		key = key[2:]
		// try to decode index ID.
		if _, indexID, err := codec.DecodeInt(key); err == nil {
			return fmt.Sprintf("t_%d_i_%d_%x", d.physicalTableID, indexID, key[8:])
		}
		return fmt.Sprintf("t_%d_i__%x", d.physicalTableID, key)
	}
	// Has table prefix.
	if bytes.HasPrefix(key, []byte("t")) {
		key = key[1:]
		// try to decode table ID.
		if _, tableID, err := codec.DecodeInt(key); err == nil {
			return fmt.Sprintf("t_%d_%x", tableID, key[8:])
		}
		return fmt.Sprintf("t_%x", key)
	}
	return fmt.Sprintf("%x", key)
}

func getRegionMeta(tikvStore helper.Storage, regionMetas []*tikv.Region, uniqueRegionMap map[uint64]struct{},
	tablePrefix, recordPrefix, indexPrefix []byte, physicalTableID, indexID int64,
	hasUnsignedIntHandle bool) ([]regionMeta, error) {
	regions := make([]regionMeta, 0, len(regionMetas))
	for _, r := range regionMetas {
		if _, ok := uniqueRegionMap[r.GetID()]; ok {
			continue
		}
		uniqueRegionMap[r.GetID()] = struct{}{}
		regions = append(regions, regionMeta{
			region:   r.GetMeta(),
			leaderID: r.GetLeaderPeerID(),
			storeID:  r.GetLeaderStoreID(),
		})
	}
	regions, err := getRegionInfo(tikvStore, regions)
	if err != nil {
		return regions, err
	}
	decodeRegionsKey(regions, tablePrefix, recordPrefix, indexPrefix, physicalTableID, indexID, hasUnsignedIntHandle)
	return regions, nil
}

func getRegionInfo(store helper.Storage, regions []regionMeta) ([]regionMeta, error) {
	// check pd server exists.
	etcd, ok := store.(kv.EtcdBackend)
	if !ok {
		return regions, nil
	}
	pdHosts, err := etcd.EtcdAddrs()
	if err != nil {
		return regions, err
	}
	if len(pdHosts) == 0 {
		return regions, nil
	}
	tikvHelper := &helper.Helper{
		Store:       store,
		RegionCache: store.GetRegionCache(),
	}
	for i := range regions {
		regionInfo, err := tikvHelper.GetRegionInfoByID(regions[i].region.Id)
		if err != nil {
			return nil, err
		}
		regions[i].writtenBytes = regionInfo.WrittenBytes
		regions[i].readBytes = regionInfo.ReadBytes
		regions[i].approximateSize = regionInfo.ApproximateSize
		regions[i].approximateKeys = regionInfo.ApproximateKeys
	}
	return regions, nil
}
