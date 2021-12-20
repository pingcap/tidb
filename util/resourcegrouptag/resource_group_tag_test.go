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

package resourcegrouptag

import (
	"crypto/sha256"
	"math/rand"
	"testing"

	"github.com/pingcap/kvproto/pkg/coprocessor"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikvrpc"
)

func TestResourceGroupTagEncoding(t *testing.T) {
	sqlDigest := parser.NewDigest(nil)
	tag := EncodeResourceGroupTag(sqlDigest, nil, tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown)
	require.Len(t, tag, 2)

	decodedSQLDigest, err := DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Len(t, decodedSQLDigest, 0)

	sqlDigest = parser.NewDigest([]byte{'a', 'a'})
	tag = EncodeResourceGroupTag(sqlDigest, nil, tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown)
	// version(1) + prefix(1) + length(1) + content(2hex -> 1byte) + label(2)
	require.Len(t, tag, 6)

	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)

	sqlDigest = parser.NewDigest(genRandHex(64))
	tag = EncodeResourceGroupTag(sqlDigest, nil, tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)

	sqlDigest = parser.NewDigest(genRandHex(510))
	tag = EncodeResourceGroupTag(sqlDigest, nil, tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown)
	decodedSQLDigest, err = DecodeResourceGroupTag(tag)
	require.NoError(t, err)
	require.Equal(t, sqlDigest.Bytes(), decodedSQLDigest)
}

func TestResourceGroupTagEncodingPB(t *testing.T) {
	digest1 := genDigest("abc")
	digest2 := genDigest("abcdefg")
	// Test for protobuf
	resourceTag := &tipb.ResourceGroupTag{
		SqlDigest:  digest1,
		PlanDigest: digest2,
	}
	buf, err := resourceTag.Marshal()
	require.NoError(t, err)
	require.Len(t, buf, 68)

	tag := &tipb.ResourceGroupTag{}
	err = tag.Unmarshal(buf)
	require.NoError(t, err)
	require.Equal(t, digest1, tag.SqlDigest)
	require.Equal(t, digest2, tag.PlanDigest)

	// Test for protobuf sql_digest only
	resourceTag = &tipb.ResourceGroupTag{
		SqlDigest: digest1,
	}
	buf, err = resourceTag.Marshal()
	require.NoError(t, err)
	require.Len(t, buf, 34)

	tag = &tipb.ResourceGroupTag{}
	err = tag.Unmarshal(buf)
	require.NoError(t, err)
	require.Equal(t, digest1, tag.SqlDigest)
	require.Nil(t, tag.PlanDigest)
}

func TestGetResourceGroupLabelByKey(t *testing.T) {
	var label tipb.ResourceGroupTagLabel
	// tablecodec.EncodeRowKey(0, []byte{})
	label = GetResourceGroupLabelByKey([]byte{116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 114})
	require.Equal(t, tipb.ResourceGroupTagLabel_ResourceGroupTagLabelRow, label)
	// tablecodec.EncodeIndexSeekKey(0, 0, []byte{}))
	label = GetResourceGroupLabelByKey([]byte{116, 128, 0, 0, 0, 0, 0, 0, 0, 95, 105, 128, 0, 0, 0, 0, 0, 0, 0})
	require.Equal(t, tipb.ResourceGroupTagLabel_ResourceGroupTagLabelIndex, label)
	label = GetResourceGroupLabelByKey([]byte(""))
	require.Equal(t, tipb.ResourceGroupTagLabel_ResourceGroupTagLabelUnknown, label)
}

func TestGetFirstKeyFromRequest(t *testing.T) {
	var testK1 = []byte("TEST-1")
	var testK2 = []byte("TEST-2")
	var req *tikvrpc.Request

	require.Nil(t, GetFirstKeyFromRequest(nil))

	req = &tikvrpc.Request{Req: (*kvrpcpb.GetRequest)(nil)}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.GetRequest{Key: nil}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.GetRequest{Key: testK1}}
	require.Equal(t, testK1, GetFirstKeyFromRequest(req))

	req = &tikvrpc.Request{Req: (*kvrpcpb.BatchGetRequest)(nil)}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.BatchGetRequest{Keys: nil}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.BatchGetRequest{Keys: [][]byte{}}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.BatchGetRequest{Keys: [][]byte{testK2, testK1}}}
	require.Equal(t, testK2, GetFirstKeyFromRequest(req))

	req = &tikvrpc.Request{Req: (*kvrpcpb.ScanRequest)(nil)}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.ScanRequest{StartKey: nil}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.ScanRequest{StartKey: testK1}}
	require.Equal(t, testK1, GetFirstKeyFromRequest(req))

	req = &tikvrpc.Request{Req: (*kvrpcpb.PrewriteRequest)(nil)}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.PrewriteRequest{Mutations: nil}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.PrewriteRequest{Mutations: []*kvrpcpb.Mutation{}}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.PrewriteRequest{Mutations: []*kvrpcpb.Mutation{{Key: testK2}, {Key: testK1}}}}
	require.Equal(t, testK2, GetFirstKeyFromRequest(req))

	req = &tikvrpc.Request{Req: (*kvrpcpb.CommitRequest)(nil)}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.CommitRequest{Keys: nil}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.CommitRequest{Keys: [][]byte{}}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.CommitRequest{Keys: [][]byte{testK1, testK1}}}
	require.Equal(t, testK1, GetFirstKeyFromRequest(req))

	req = &tikvrpc.Request{Req: (*kvrpcpb.BatchRollbackRequest)(nil)}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.BatchRollbackRequest{Keys: nil}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.BatchRollbackRequest{Keys: [][]byte{}}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &kvrpcpb.BatchRollbackRequest{Keys: [][]byte{testK2, testK1}}}
	require.Equal(t, testK2, GetFirstKeyFromRequest(req))

	req = &tikvrpc.Request{Req: (*coprocessor.Request)(nil)}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &coprocessor.Request{Ranges: nil}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &coprocessor.Request{Ranges: []*coprocessor.KeyRange{}}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &coprocessor.Request{Ranges: []*coprocessor.KeyRange{{Start: testK1}}}}
	require.Equal(t, testK1, GetFirstKeyFromRequest(req))

	req = &tikvrpc.Request{Req: (*coprocessor.BatchRequest)(nil)}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &coprocessor.BatchRequest{Regions: nil}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &coprocessor.BatchRequest{Regions: []*coprocessor.RegionInfo{}}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &coprocessor.BatchRequest{Regions: []*coprocessor.RegionInfo{{Ranges: nil}}}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &coprocessor.BatchRequest{Regions: []*coprocessor.RegionInfo{{Ranges: []*coprocessor.KeyRange{}}}}}
	require.Nil(t, GetFirstKeyFromRequest(req))
	req = &tikvrpc.Request{Req: &coprocessor.BatchRequest{Regions: []*coprocessor.RegionInfo{{Ranges: []*coprocessor.KeyRange{{Start: testK2}}}}}}
	require.Equal(t, testK2, GetFirstKeyFromRequest(req))
}

func genRandHex(length int) []byte {
	const chars = "0123456789abcdef"
	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = chars[rand.Intn(len(chars))]
	}
	return res
}

func genDigest(str string) []byte {
	hasher := sha256.New()
	hasher.Write(hack.Slice(str))
	return hasher.Sum(nil)
}
