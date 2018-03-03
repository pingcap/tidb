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
// See the License for the specific language governing permissions and
// limitations under the License.

package distsql

import (
	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/charset"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tipb/go-tipb"
	"golang.org/x/net/context"
)

func (s *testSuite) TestSelect(c *C) {
	keyRanges := []kv.KeyRange{
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x6},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xa},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc},
		},
		{
			StartKey: kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64},
			EndKey:   kv.Key{0x74, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf, 0x5f, 0x72, 0x80, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x65},
		},
	}

	request, err := (&RequestBuilder{}).SetKeyRanges(keyRanges).
		SetDAGRequest(&tipb.DAGRequest{}).
		SetDesc(false).
		SetKeepOrder(false).
		SetPriority(kv.PriorityNormal).
		SetFromSessionVars(variable.NewSessionVars()).
		Build()
	c.Assert(err, IsNil)

	/// 4 int64 types.
	colTypes := []*types.FieldType{
		{
			Tp:      mysql.TypeLonglong,
			Flen:    mysql.MaxIntWidth,
			Decimal: 0,
			Flag:    mysql.BinaryFlag,
			Charset: charset.CharsetBin,
			Collate: charset.CollationBin,
		},
	}
	colTypes = append(colTypes, colTypes[0])
	colTypes = append(colTypes, colTypes[0])
	colTypes = append(colTypes, colTypes[0])

	response, err := Select(context.TODO(), s.sctx, request, colTypes)
	c.Assert(err, IsNil)
	result, ok := response.(*selectResult)
	c.Assert(ok, IsTrue)
	c.Assert(result.label, Equals, "dag")
	c.Assert(result.rowLen, Equals, len(colTypes))

	response.Fetch(context.TODO())

	chk := chunk.NewChunk(colTypes)
	err = response.NextChunk(context.TODO(), chk)
	c.Assert(err, NotNil)
	c.Assert(chk.NumRows(), Equals, 0)
}

type mockStore struct{ client *mockClient }
type mockClient struct{}
type mockResponse struct{ count int }
type mockResultSubset struct{ data []byte }

// functions for mockStore.
func (m *mockStore) GetClient() kv.Client                                    { return m.client }
func (m *mockStore) GetOracle() oracle.Oracle                                { return nil }
func (m *mockStore) Begin() (kv.Transaction, error)                          { return nil, nil }
func (m *mockStore) BeginWithStartTS(startTS uint64) (kv.Transaction, error) { return m.Begin() }
func (m *mockStore) GetSnapshot(ver kv.Version) (kv.Snapshot, error)         { return nil, nil }
func (m *mockStore) Close() error                                            { return nil }
func (m *mockStore) UUID() string                                            { return "mock" }
func (m *mockStore) CurrentVersion() (kv.Version, error)                     { return kv.Version{}, nil }
func (m *mockStore) SupportDeleteRange() bool                                { return false }

// functions for mockClient.
func (c *mockClient) Send(ctx context.Context, req *kv.Request) kv.Response { return &mockResponse{} }
func (c *mockClient) IsRequestTypeSupported(reqType, subType int64) bool    { return true }

// functions for mockResponse.
func (resp *mockResponse) Close() error { return nil }
func (resp *mockResponse) Next(ctx context.Context) (kv.ResultSubset, error) {
	resp.count++
	if resp.count == 100 {
		return nil, errors.New("error happened")
	}

	respPB := &tipb.SelectResponse{
		Chunks:       []tipb.Chunk{},
		OutputCounts: []int64{1},
	}
	respBytes, err := respPB.Marshal()
	if err != nil {
		panic(err)
	}
	return &mockResultSubset{respBytes}, nil
}

// functions for mockResultSubset.
func (r *mockResultSubset) GetData() []byte { return r.data }
