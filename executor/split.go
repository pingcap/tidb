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
	"strconv"

	"github.com/cznic/mathutil"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/table"
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

	tableInfo  *model.TableInfo
	indexInfo  *model.IndexInfo
	lower      []types.Datum
	upper      []types.Datum
	num        int
	valueLists [][]types.Datum
}

type splitableStore interface {
	SplitRegionAndScatter(splitKey kv.Key) (uint64, error)
	WaitScatterRegionFinish(regionID uint64) error
	CheckScatterRegionFinished(regionID uint64) (bool, error)
}

// Next implements the Executor Next interface.
func (e *SplitIndexRegionExec) Next(ctx context.Context, _ *chunk.RecordBatch) error {
	store := e.ctx.GetStore()
	s, ok := store.(splitableStore)
	if !ok {
		return nil
	}
	splitIdxKeys, err := e.getSplitIdxKeys()
	if err != nil {
		return err
	}
	regionIDs := make([]uint64, 0, len(splitIdxKeys))
	for _, idxKey := range splitIdxKeys {
		regionID, err := s.SplitRegionAndScatter(idxKey)
		if err != nil {
			logutil.Logger(context.Background()).Warn("split table index region failed",
				zap.String("table", e.tableInfo.Name.L),
				zap.String("index", e.indexInfo.Name.L),
				zap.Error(err))
			continue
		}
		regionIDs = append(regionIDs, regionID)

	}
	if !e.ctx.GetSessionVars().WaitTableSplitFinish {
		return nil
	}
	for _, regionID := range regionIDs {
		err := s.WaitScatterRegionFinish(regionID)
		if err != nil {
			logutil.Logger(context.Background()).Warn("wait scatter region failed",
				zap.Uint64("regionID", regionID),
				zap.String("table", e.tableInfo.Name.L),
				zap.String("index", e.indexInfo.Name.L),
				zap.Error(err))
		}
	}
	return nil
}

func (e *SplitIndexRegionExec) getSplitIdxKeys() ([][]byte, error) {
	var idxKeys [][]byte
	if e.num > 0 {
		idxKeys = make([][]byte, 0, e.num)
	} else {
		idxKeys = make([][]byte, 0, len(e.valueLists)+1)
	}
	// Split in the start of the index key.
	startIdxKey := tablecodec.EncodeTableIndexPrefix(e.tableInfo.ID, e.indexInfo.ID)
	idxKeys = append(idxKeys, startIdxKey)

	index := tables.NewIndex(e.tableInfo.ID, e.tableInfo, e.indexInfo)
	// Split index regions by user specified value lists.
	if len(e.valueLists) > 0 {
		for _, v := range e.valueLists {
			idxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, v, math.MinInt64, nil)
			if err != nil {
				return nil, err
			}
			idxKeys = append(idxKeys, idxKey)
		}
		return idxKeys, nil
	}
	// Split index regions by lower, upper value and calculate the step by (upper - lower)/num.
	lowerIdxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, e.lower, math.MinInt64, nil)
	if err != nil {
		return nil, err
	}
	// Use math.MinInt64 as handle_id for the upper index key to avoid affecting calculate split point.
	// If use math.MaxInt64 here, test of `TestSplitIndex` will report error.
	upperIdxKey, _, err := index.GenIndexKey(e.ctx.GetSessionVars().StmtCtx, e.upper, math.MinInt64, nil)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(lowerIdxKey, upperIdxKey) >= 0 {
		lowerStr, err1 := datumSliceToString(e.lower)
		upperStr, err2 := datumSliceToString(e.upper)
		if err1 != nil || err2 != nil {
			return nil, errors.Errorf("Split index `%v` region lower value %v should less than the upper value %v", e.indexInfo.Name, e.lower, e.upper)
		}
		return nil, errors.Errorf("Split index `%v` region lower value %v should less than the upper value %v", e.indexInfo.Name, lowerStr, upperStr)
	}
	return getValuesList(lowerIdxKey, upperIdxKey, e.num, idxKeys), nil
}

// getValuesList is used to get `num` values between lower and upper value.
// To Simplify the explain, suppose lower and upper value type is int64, and lower=0, upper=100, num=10,
// then calculate the step=(upper-lower)/num=10, then the function should return 0+10, 10+10, 20+10... all together 9 (num-1) values.
// Then the function will return [10,20,30,40,50,60,70,80,90].
// The difference is the value type of upper,lower is []byte, So I use getUint64FromBytes to convert []byte to uint64.
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

func datumSliceToString(ds []types.Datum) (string, error) {
	str := "("
	for i, d := range ds {
		s, err := d.ToString()
		if err != nil {
			return str, err
		}
		if i > 0 {
			str += ","
		}
		str += s
	}
	str += ")"
	return str, nil
}

// SplitTableRegionExec represents a split table regions executor.
type SplitTableRegionExec struct {
	baseExecutor

	tableInfo  *model.TableInfo
	lower      types.Datum
	upper      types.Datum
	num        int
	valueLists [][]types.Datum
}

// Next implements the Executor Next interface.
func (e *SplitTableRegionExec) Next(ctx context.Context, _ *chunk.RecordBatch) error {
	store := e.ctx.GetStore()
	s, ok := store.(splitableStore)
	if !ok {
		return nil
	}
	splitKeys, err := e.getSplitTableKeys()
	if err != nil {
		return err
	}
	regionIDs := make([]uint64, 0, len(splitKeys))
	for _, key := range splitKeys {
		regionID, err := s.SplitRegionAndScatter(key)
		if err != nil {
			logutil.Logger(context.Background()).Warn("split table region failed",
				zap.String("table", e.tableInfo.Name.L),
				zap.Error(err))
			continue
		}
		regionIDs = append(regionIDs, regionID)
	}
	if !e.ctx.GetSessionVars().WaitTableSplitFinish {
		return nil
	}
	for _, regionID := range regionIDs {
		err := s.WaitScatterRegionFinish(regionID)
		if err != nil {
			logutil.Logger(context.Background()).Warn("wait scatter region failed",
				zap.Uint64("regionID", regionID),
				zap.String("table", e.tableInfo.Name.L),
				zap.Error(err))
		}
	}
	return nil
}

var minRegionStepValue = uint64(1000)

func (e *SplitTableRegionExec) getSplitTableKeys() ([][]byte, error) {
	var keys [][]byte
	if e.num > 0 {
		keys = make([][]byte, 0, e.num)
	} else {
		keys = make([][]byte, 0, len(e.valueLists))
	}
	recordPrefix := tablecodec.GenTableRecordPrefix(e.tableInfo.ID)
	if len(e.valueLists) > 0 {
		for _, v := range e.valueLists {
			key := tablecodec.EncodeRecordKey(recordPrefix, v[0].GetInt64())
			keys = append(keys, key)
		}
		return keys, nil
	}
	isUnsigned := false
	if e.tableInfo.PKIsHandle {
		if pkCol := e.tableInfo.GetPkColInfo(); pkCol != nil {
			isUnsigned = mysql.HasUnsignedFlag(pkCol.Flag)
		}
	}
	var step uint64
	var lowerValue int64
	if isUnsigned {
		lowerRecordID := e.lower.GetUint64()
		upperRecordID := e.upper.GetUint64()
		if upperRecordID <= lowerRecordID {
			return nil, errors.Errorf("Split table `%s` region lower value %v should less than the upper value %v", e.tableInfo.Name, lowerRecordID, upperRecordID)
		}
		step = (upperRecordID - lowerRecordID) / uint64(e.num)
		lowerValue = int64(lowerRecordID)
	} else {
		lowerRecordID := e.lower.GetInt64()
		upperRecordID := e.upper.GetInt64()
		if upperRecordID <= lowerRecordID {
			return nil, errors.Errorf("Split table `%s` region lower value %v should less than the upper value %v", e.tableInfo.Name, lowerRecordID, upperRecordID)
		}
		step = uint64(upperRecordID-lowerRecordID) / uint64(e.num)
		lowerValue = lowerRecordID
	}
	if step < minRegionStepValue {
		return nil, errors.Errorf("Split table `%s` region step value should more than %v, step %v is invalid", e.tableInfo.Name, minRegionStepValue, step)
	}

	recordID := lowerValue
	for i := 1; i < e.num; i++ {
		recordID += int64(step)
		key := tablecodec.EncodeRecordKey(recordPrefix, recordID)
		keys = append(keys, key)
	}
	return keys, nil
}

// SplitTableRegionStatusExec represents a split regions status executor.
type SplitTableRegionStatusExec struct {
	baseExecutor

	table     table.Table
	indexInfo *model.IndexInfo
	cursor    int
	regions   []regionMeta
}

// RegionMeta contains a region's peer detail
type regionMeta struct {
	region        *metapb.Region
	leader        *metapb.Peer
	start         string
	end           string
	scatterFinish bool
}

// Open implements the Executor Open interface.
func (e *SplitTableRegionStatusExec) Open(ctx context.Context) error {
	store := e.ctx.GetStore()
	tikvStore, ok := store.(tikv.Storage)
	if !ok {
		return nil
	}
	splitStore, ok := store.(splitableStore)
	if !ok {
		return nil
	}

	regionCache := tikvStore.GetRegionCache()
	var regions []regionMeta
	var err error
	if info := e.table.Meta().GetPartitionInfo(); info != nil {
		regions, err = e.getPartitionTableRegions(info, e.table.(table.PartitionedTable), regionCache, splitStore)
	} else {
		regions, err = e.getCommonTableRegions(regionCache, splitStore)
	}
	if err != nil {
		return err
	}
	e.regions = regions
	e.cursor = 0
	return nil
}

func (e *SplitTableRegionStatusExec) getPartitionTableRegions(info *model.PartitionInfo, tbl table.PartitionedTable, regionCache *tikv.RegionCache, splitStore splitableStore) ([]regionMeta, error) {
	var regions []regionMeta
	var err error
	for _, def := range info.Definitions {
		pid := def.ID
		partition := tbl.GetPartition(pid)
		partition.GetPhysicalID()
		var partitionRegions []regionMeta
		if e.indexInfo != nil {
			partitionRegions, err = e.getIndexRegions(partition.GetPhysicalID(), regionCache, splitStore)

		} else {
			partitionRegions, err = e.getTableRegions(partition.GetPhysicalID(), regionCache, splitStore)
		}
		if err != nil {
			return nil, err
		}
		regions = append(regions, partitionRegions...)
	}
	return regions, nil
}

func (e *SplitTableRegionStatusExec) getCommonTableRegions(regionCache *tikv.RegionCache, splitStore splitableStore) ([]regionMeta, error) {
	var regions []regionMeta
	var err error
	if e.indexInfo != nil {
		regions, err = e.getIndexRegions(e.table.Meta().ID, regionCache, splitStore)

	} else {
		regions, err = e.getTableRegions(e.table.Meta().ID, regionCache, splitStore)
	}
	return regions, err
}

// Next implements the Executor Next interface.
func (e *SplitTableRegionStatusExec) Next(ctx context.Context, req *chunk.RecordBatch) error {
	store := e.ctx.GetStore()
	if _, ok := store.(tikv.Storage); !ok {
		return nil
	}

	req.GrowAndReset(e.maxChunkSize)
	if e.cursor >= len(e.regions) {
		return nil
	}
	numCurBatch := mathutil.Min(req.Capacity(), len(e.regions)-e.cursor)
	for i := e.cursor; i < e.cursor+numCurBatch; i++ {
		req.AppendUint64(0, e.regions[i].region.Id)
		req.AppendString(1, e.regions[i].start)
		req.AppendString(2, e.regions[i].end)
		req.AppendUint64(3, e.regions[i].leader.Id)
		peers := ""
		for i, peer := range e.regions[i].region.Peers {
			if i > 0 {
				peers += ", "
			}
			peers += strconv.FormatUint(peer.Id, 10)
		}
		req.AppendString(4, peers)
		if e.regions[i].scatterFinish {
			req.AppendInt64(5, 1)
		} else {
			req.AppendInt64(5, 0)
		}
	}
	e.cursor += numCurBatch
	return nil
}

func (e *SplitTableRegionStatusExec) getTableRegions(physicalTableID int64, regionCache *tikv.RegionCache, s splitableStore) ([]regionMeta, error) {
	uniqueRegionMap := make(map[uint64]struct{})
	// for record
	startKey, endKey := tablecodec.GetTableHandleKeyRange(physicalTableID)
	recordRegionIDs, err := regionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
	if err != nil {
		return nil, err
	}
	recordRegions, err := getRegionMeta(regionCache, recordRegionIDs, uniqueRegionMap)
	if err != nil {
		return nil, err
	}

	recordPrefix := tablecodec.GenTableRecordPrefix(physicalTableID)
	tablePrefix := tablecodec.GenTablePrefix(physicalTableID)
	decodeRegionsKey(recordRegions, tablePrefix, recordPrefix, nil, physicalTableID, 0)
	regions := recordRegions

	// for indices
	for _, index := range e.table.Meta().Indices {
		if index.State != model.StatePublic {
			continue
		}
		startKey, endKey := tablecodec.GetTableIndexKeyRange(physicalTableID, index.ID)
		rIDs, err := regionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
		if err != nil {
			return nil, err
		}
		indexRegions, err := getRegionMeta(regionCache, rIDs, uniqueRegionMap)
		if err != nil {
			return nil, err
		}
		indexPrefix := tablecodec.EncodeTableIndexPrefix(physicalTableID, index.ID)
		decodeRegionsKey(indexRegions, tablePrefix, recordPrefix, indexPrefix, physicalTableID, index.ID)
		regions = append(regions, indexRegions...)
	}
	err = checkRegionsStatus(s, regions)
	if err != nil {
		return nil, err
	}
	return regions, nil
}

func (e *SplitTableRegionStatusExec) getIndexRegions(physicalTableID int64, regionCache *tikv.RegionCache, s splitableStore) ([]regionMeta, error) {
	uniqueRegionMap := make(map[uint64]struct{})
	recordPrefix := tablecodec.GenTableRecordPrefix(physicalTableID)
	tablePrefix := tablecodec.GenTablePrefix(physicalTableID)
	indexPrefix := tablecodec.EncodeTableIndexPrefix(physicalTableID, e.indexInfo.ID)

	startKey, endKey := tablecodec.GetTableIndexKeyRange(physicalTableID, e.indexInfo.ID)
	rIDs, err := regionCache.ListRegionIDsInKeyRange(tikv.NewBackoffer(context.Background(), 500), startKey, endKey)
	if err != nil {
		return nil, err
	}
	indexRegions, err := getRegionMeta(regionCache, rIDs, uniqueRegionMap)
	if err != nil {
		return nil, err
	}
	decodeRegionsKey(indexRegions, tablePrefix, recordPrefix, indexPrefix, physicalTableID, e.indexInfo.ID)
	err = checkRegionsStatus(s, indexRegions)
	if err != nil {
		return nil, err
	}
	return indexRegions, nil
}

func checkRegionsStatus(store splitableStore, regions []regionMeta) error {
	for i := range regions {
		scatterFinish, err := store.CheckScatterRegionFinished(regions[i].region.Id)
		if err != nil {
			return err
		}
		regions[i].scatterFinish = scatterFinish
	}
	return nil
}

func decodeRegionsKey(regions []regionMeta, tablePrefix, recordPrefix, indexPrefix []byte, physicalTableID, indexID int64) {
	d := &regionKeyDecoder{
		physicalTableID: physicalTableID,
		tablePrefix:     tablePrefix,
		recordPrefix:    recordPrefix,
		indexPrefix:     indexPrefix,
		indexID:         indexID,
	}
	for i := range regions {
		regions[i].start = d.decodeRegionKey(regions[i].region.StartKey)
		regions[i].end = d.decodeRegionKey(regions[i].region.EndKey)
	}
}

type regionKeyDecoder struct {
	physicalTableID int64
	tablePrefix     []byte
	recordPrefix    []byte
	indexPrefix     []byte
	indexID         int64
}

func (d *regionKeyDecoder) decodeRegionKey(key []byte) string {
	if len(d.indexPrefix) > 0 && bytes.HasPrefix(key, d.indexPrefix) {
		return fmt.Sprintf("t_%d_i_%d_%x", d.physicalTableID, d.indexID, key[len(d.indexPrefix):])
	} else if len(d.recordPrefix) > 0 && bytes.HasPrefix(key, d.recordPrefix) {
		_, handle, err := codec.DecodeInt(key[len(d.recordPrefix):])
		if err == nil {
			return fmt.Sprintf("t_%d_r_%d", d.physicalTableID, strconv.FormatInt(handle, 10))
		}
	}
	if len(d.tablePrefix) > 0 && bytes.HasPrefix(key, d.tablePrefix) {
		return fmt.Sprintf("t_%d_%x", d.physicalTableID, key[len(d.tablePrefix):])
	}
	return fmt.Sprintf("%x", key)
}

func getRegionMeta(regionCache *tikv.RegionCache, regionIDs []uint64, uniqueRegionMap map[uint64]struct{}) ([]regionMeta, error) {
	regions := make([]regionMeta, 0, len(regionIDs))
	for _, regionID := range regionIDs {
		meta, leader, err := regionCache.PDClient().GetRegionByID(context.TODO(), regionID)
		if err != nil {
			return nil, errors.Trace(err)
		}

		if meta == nil {
			return nil, errors.Errorf("region not found for regionID %q", regionID)
		}
		if _, ok := uniqueRegionMap[meta.Id]; ok {
			continue
		}
		uniqueRegionMap[meta.Id] = struct{}{}
		regions = append(regions, regionMeta{
			region: meta,
			leader: leader,
		})
	}
	return regions, nil
}
