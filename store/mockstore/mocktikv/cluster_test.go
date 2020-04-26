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
// See the License for the specific language governing permissions and
// limitations under the License.

package mocktikv_test

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/rowcodec"
)

var _ = Suite(&testClusterSuite{})

type testClusterSuite struct {
	store kv.Storage
}

func (s *testClusterSuite) TestClusterSplit(c *C) {
	cluster := mocktikv.NewCluster()
	mocktikv.BootstrapWithSingleStore(cluster)
	mvccStore := mocktikv.MustNewMVCCStore()
	store, err := mockstore.NewMockTikvStore(
		mockstore.WithCluster(cluster),
		mockstore.WithMVCCStore(mvccStore),
	)
	c.Assert(err, IsNil)

	txn, err := store.Begin()
	c.Assert(err, IsNil)

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
		c.Assert(err1, IsNil)
		txn.Set(rowKey, rowValue)

		encodedIndexValue, err1 := codec.EncodeKey(sc, nil, []types.Datum{colValue, types.NewIntDatum(handle)}...)
		c.Assert(err1, IsNil)
		idxKey := tablecodec.EncodeIndexSeekKey(tblID, idxID, encodedIndexValue)
		txn.Set(idxKey, []byte{'0'})
		handle++
	}
	err = txn.Commit(context.Background())
	c.Assert(err, IsNil)

	// Split Table into 10 regions.
	cluster.SplitTable(mvccStore, tblID, 10)

	// 10 table regions and first region and last region.
	regions := cluster.GetAllRegions()
	c.Assert(regions, HasLen, 12)

	allKeysMap := make(map[string]bool)
	recordPrefix := tablecodec.GenTableRecordPrefix(tblID)
	for _, region := range regions {
		startKey := mocktikv.MvccKey(region.Meta.StartKey).Raw()
		endKey := mocktikv.MvccKey(region.Meta.EndKey).Raw()
		if !bytes.HasPrefix(startKey, recordPrefix) {
			continue
		}
		pairs := mvccStore.Scan(startKey, endKey, math.MaxInt64, math.MaxUint64, kvrpcpb.IsolationLevel_SI, nil)
		if len(pairs) > 0 {
			c.Assert(pairs, HasLen, 100)
		}
		for _, pair := range pairs {
			allKeysMap[string(pair.Key)] = true
		}
	}
	c.Assert(allKeysMap, HasLen, 1000)

	cluster.SplitIndex(mvccStore, tblID, idxID, 10)

	allIndexMap := make(map[string]bool)
	indexPrefix := tablecodec.EncodeTableIndexPrefix(tblID, idxID)
	regions = cluster.GetAllRegions()
	for _, region := range regions {
		startKey := mocktikv.MvccKey(region.Meta.StartKey).Raw()
		endKey := mocktikv.MvccKey(region.Meta.EndKey).Raw()
		if !bytes.HasPrefix(startKey, indexPrefix) {
			continue
		}
		pairs := mvccStore.Scan(startKey, endKey, math.MaxInt64, math.MaxUint64, kvrpcpb.IsolationLevel_SI, nil)
		if len(pairs) > 0 {
			c.Assert(pairs, HasLen, 100)
		}
		for _, pair := range pairs {
			allIndexMap[string(pair.Key)] = true
		}
	}
	c.Assert(allIndexMap, HasLen, 1000)
}
