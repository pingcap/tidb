// Copyright 2016 PingCAP, Inc.
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
	"errors"
	"runtime"
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
	"github.com/pingcap/tipb/go-tipb"
	goctx "golang.org/x/net/context"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testDistsqlSuite{})

type testDistsqlSuite struct{}

// TODO: add more tests.
func (s *testDistsqlSuite) TestColumnToProto(c *C) {
	defer testleak.AfterTest(c)()
	// Make sure the Flag is set in tipb.ColumnInfo
	tp := types.NewFieldType(mysql.TypeLong)
	tp.Flag = 10
	tp.Collate = "utf8_bin"
	col := &model.ColumnInfo{
		FieldType: *tp,
	}
	pc := columnToProto(col)
	c.Assert(pc.GetFlag(), Equals, int32(10))
	ntp := FieldTypeFromPBColumn(pc)
	c.Assert(ntp, DeepEquals, tp)

	cols := []*model.ColumnInfo{col, col}
	pcs := ColumnsToProto(cols, false)
	for _, v := range pcs {
		c.Assert(v.GetFlag(), Equals, int32(10))
	}
	pcs = ColumnsToProto(cols, true)
	for _, v := range pcs {
		c.Assert(v.GetFlag(), Equals, int32(10))
	}
}

func (s *testDistsqlSuite) TestIndexToProto(c *C) {
	defer testleak.AfterTest(c)()
	cols := []*model.ColumnInfo{
		{
			ID:     1,
			Name:   model.NewCIStr("col1"),
			Offset: 1,
		},
		{
			ID:     2,
			Name:   model.NewCIStr("col2"),
			Offset: 2,
		},
	}
	cols[0].Flag |= mysql.PriKeyFlag

	idxCols := []*model.IndexColumn{
		{
			Name:   model.NewCIStr("col1"),
			Offset: 1,
			Length: 1,
		},
		{
			Name:   model.NewCIStr("col1"),
			Offset: 1,
			Length: 1,
		},
	}

	idxInfos := []*model.IndexInfo{
		{
			ID:      1,
			Name:    model.NewCIStr("idx1"),
			Table:   model.NewCIStr("test"),
			Columns: idxCols,
			Unique:  true,
			Primary: true,
		},
		{
			ID:      2,
			Name:    model.NewCIStr("idx2"),
			Table:   model.NewCIStr("test"),
			Columns: idxCols,
			Unique:  true,
			Primary: true,
		},
	}

	tbInfo := model.TableInfo{
		ID:         1,
		Name:       model.NewCIStr("test"),
		Columns:    cols,
		Indices:    idxInfos,
		PKIsHandle: true,
	}

	pIdx := IndexToProto(&tbInfo, idxInfos[0])
	c.Assert(pIdx.TableId, Equals, int64(1))
	c.Assert(pIdx.IndexId, Equals, int64(1))
	c.Assert(pIdx.Unique, Equals, true)
}

// For issue 1791
func (s *testDistsqlSuite) TestGoroutineLeak(c *C) {
	var sr SelectResult
	countBefore := runtime.NumGoroutine()

	sr = &selectResult{
		resp:    &mockResponse{},
		results: make(chan resultWithErr, 5),
		closed:  make(chan struct{}),
	}
	go sr.Fetch(goctx.TODO())
	for {
		// mock test will generate some partial result then return error
		_, err := sr.Next()
		if err != nil {
			// close selectResult on error, partialResult's fetch goroutine may leak
			sr.Close()
			break
		}
	}

	tick := 10 * time.Millisecond
	totalSleep := time.Duration(0)
	for totalSleep < 3*time.Second {
		time.Sleep(tick)
		totalSleep += tick
		countAfter := runtime.NumGoroutine()

		if countAfter-countBefore < 5 {
			return
		}
	}

	c.Error("distsql goroutine leak!")
}

type mockResponse struct {
	count int
}

func (resp *mockResponse) Next() ([]byte, error) {
	resp.count++
	if resp.count == 100 {
		return nil, errors.New("error happened")
	}
	return mockSubresult(), nil
}

func (resp *mockResponse) Close() error {
	return nil
}

func mockSubresult() []byte {
	resp := new(tipb.SelectResponse)
	b, err := resp.Marshal()
	if err != nil {
		panic(err)
	}
	return b
}
