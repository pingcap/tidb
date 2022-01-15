// Copyright 2020 PingCAP, Inc.
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

package unistore

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/stretchr/testify/require"
)

func TestRawHandler(t *testing.T) {
	h := newRawHandler()
	ctx := context.Background()
	keys := make([][]byte, 10)
	vals := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = []byte(fmt.Sprintf("key%d", i))
		vals[i] = []byte(fmt.Sprintf("val%d", i))
	}
	putResp, _ := h.RawPut(ctx, &kvrpcpb.RawPutRequest{Key: keys[0], Value: vals[0]})
	require.NotNil(t, putResp)
	getResp, _ := h.RawGet(ctx, &kvrpcpb.RawGetRequest{Key: keys[0]})
	require.NotNil(t, getResp)
	require.Equal(t, 0, bytes.Compare(getResp.Value, vals[0]))
	delResp, _ := h.RawDelete(ctx, &kvrpcpb.RawDeleteRequest{Key: keys[0]})
	require.NotNil(t, delResp)

	batchPutReq := &kvrpcpb.RawBatchPutRequest{Pairs: []*kvrpcpb.KvPair{
		{Key: keys[1], Value: vals[1]},
		{Key: keys[3], Value: vals[3]},
		{Key: keys[5], Value: vals[5]},
	}}
	batchPutResp, _ := h.RawBatchPut(ctx, batchPutReq)
	require.NotNil(t, batchPutResp)
	batchGetResp, _ := h.RawBatchGet(ctx, &kvrpcpb.RawBatchGetRequest{Keys: [][]byte{keys[1], keys[3], keys[5]}})
	require.NotNil(t, batchGetResp)
	require.Equal(t, batchPutReq.Pairs, batchGetResp.Pairs)
	batchDelResp, _ := h.RawBatchDelete(ctx, &kvrpcpb.RawBatchDeleteRequest{Keys: [][]byte{keys[1], keys[3], keys[5]}})
	require.NotNil(t, batchDelResp)

	batchPutReq.Pairs = []*kvrpcpb.KvPair{
		{Key: keys[6], Value: vals[6]},
		{Key: keys[7], Value: vals[7]},
		{Key: keys[8], Value: vals[8]},
	}
	batchPutResp, _ = h.RawBatchPut(ctx, batchPutReq)
	require.NotNil(t, batchPutResp)

	scanReq := &kvrpcpb.RawScanRequest{StartKey: keys[0], EndKey: keys[9], Limit: 2}
	scanResp, _ := h.RawScan(ctx, scanReq)
	require.NotNil(t, batchPutResp)
	require.Len(t, scanResp.Kvs, 2)
	require.Equal(t, scanResp.Kvs, batchPutReq.Pairs[:2])

	delRangeResp, _ := h.RawDeleteRange(ctx, &kvrpcpb.RawDeleteRangeRequest{StartKey: keys[0], EndKey: keys[9]})
	require.NotNil(t, delRangeResp)

	scanResp, _ = h.RawScan(ctx, scanReq)
	require.Equal(t, 0, len(scanResp.Kvs))
}
