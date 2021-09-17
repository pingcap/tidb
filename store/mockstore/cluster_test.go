// Copyright 2016-present, PingCAP, Inc.
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

package mockstore

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

func TestClusterSplit(t *testing.T) {
	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", nil)
	require.NoError(t, err)
	testutils.BootstrapWithSingleStore(cluster)
	mvccStore := rpcClient.MvccStore

	store, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.NoError(t, err)
	defer store.Close()

	txn, err := store.Begin()
	require.NoError(t, err)

	// Mock inserting many rows in a table.
	tblID := int64(1)
	idxID := int64(2)
	colID := int64(3)
	handle := int64(1)
	sc := &stmtctx.StatementContext{TimeZone: time.UTC}
	for i := 0; i < 1000; i++ {
		rowKey := tablecodec.EncodeRowKeyWithHandle(tblID, kv.IntHandle(handle))
		colValue := types.NewStringDatum(strconv.Itoa(int(handle)))
		// TODO: Should use session's TimeZone instead of UTC.
		rd := rowcodec.Encoder{Enable: true}
		rowValue, err1 := tablecodec.EncodeRow(sc, []types.Datum{colValue}, []int64{colID}, nil, nil, &rd)
		require.NoError(t, err1)
		txn.Set(rowKey, rowValue)

		encodedIndexValue, err1 := codec.EncodeKey(sc, nil, []types.Datum{colValue, types.NewIntDatum(handle)}...)
		require.NoError(t, err1)
		idxKey := tablecodec.EncodeIndexSeekKey(tblID, idxID, encodedIndexValue)
		txn.Set(idxKey, []byte{'0'})
		handle++
	}
	err = txn.Commit(context.Background())
	require.NoError(t, err)

	// Split Table into 10 regions.
	tableStart := tablecodec.GenTableRecordPrefix(tblID)
	cluster.SplitKeys(tableStart, tableStart.PrefixNext(), 10)

	// 10 table regions and first region and last region.
	regions := cluster.GetAllRegions()
	require.Len(t, regions, 12)

	allKeysMap := make(map[string]bool)
	recordPrefix := tablecodec.GenTableRecordPrefix(tblID)
	for _, region := range regions {
		startKey := toRawKey(region.Meta.StartKey)
		endKey := toRawKey(region.Meta.EndKey)
		if !bytes.HasPrefix(startKey, recordPrefix) {
			continue
		}
		pairs := mvccStore.Scan(startKey, endKey, math.MaxInt64, math.MaxUint64, kvrpcpb.IsolationLevel_SI, nil)
		if len(pairs) > 0 {
			require.Len(t, pairs, 100)
		}
		for _, pair := range pairs {
			allKeysMap[string(pair.Key)] = true
		}
	}
	require.Len(t, allKeysMap, 1000)

	indexStart := tablecodec.EncodeTableIndexPrefix(tblID, idxID)
	cluster.SplitKeys(indexStart, indexStart.PrefixNext(), 10)

	allIndexMap := make(map[string]bool)
	indexPrefix := tablecodec.EncodeTableIndexPrefix(tblID, idxID)
	regions = cluster.GetAllRegions()
	for _, region := range regions {
		startKey := toRawKey(region.Meta.StartKey)
		endKey := toRawKey(region.Meta.EndKey)
		if !bytes.HasPrefix(startKey, indexPrefix) {
			continue
		}
		pairs := mvccStore.Scan(startKey, endKey, math.MaxInt64, math.MaxUint64, kvrpcpb.IsolationLevel_SI, nil)
		if len(pairs) > 0 {
			require.Len(t, pairs, 100)
		}
		for _, pair := range pairs {
			allIndexMap[string(pair.Key)] = true
		}
	}
	require.Len(t, allIndexMap, 1000)
}

func toRawKey(k []byte) []byte {
	if len(k) == 0 {
		return nil
	}
	_, k, err := codec.DecodeBytes(k, nil)
	if err != nil {
		panic(err)
	}
	return k
}
