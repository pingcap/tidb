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

package statistics

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

func TestT(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&testStatisticsSuite{})

type testStatisticsSuite struct {
	count   int64
	samples []types.Datum
}

type dataTable struct {
	count   int64
	samples []types.Datum
}

func (s *testStatisticsSuite) SetUpSuite(c *C) {
	s.count = 100000
	samples := make([]types.Datum, 10000)
	start := 1000 // 1000 values is null
	for i := start; i < len(samples); i++ {
		samples[i].SetInt64(int64(i))
	}
	for i := start; i < len(samples); i += 3 {
		samples[i].SetInt64(samples[i].GetInt64() + 1)
	}
	for i := start; i < len(samples); i += 5 {
		samples[i].SetInt64(samples[i].GetInt64() + 2)
	}
	err := types.SortDatums(samples)
	c.Check(err, IsNil)
	s.samples = samples
}

func (s *testStatisticsSuite) TestEstimateNDV(c *C) {
	ndv, err := estimateNDV(s.count, s.samples)
	c.Check(err, IsNil)
	c.Check(ndv, Equals, int64(49792))
}

func (s *testStatisticsSuite) TestTable(c *C) {
	tblInfo := &model.TableInfo{
		ID: 1,
	}
	columns := []*model.ColumnInfo{
		{
			ID:        2,
			FieldType: *types.NewFieldType(mysql.TypeLonglong),
		},
	}
	tblInfo.Columns = columns
	timestamp := int64(10)
	bucketCount := int64(256)
	t, err := NewTable(tblInfo, timestamp, s.count, bucketCount, [][]types.Datum{s.samples})
	c.Check(err, IsNil)
	str := t.String()
	log.Debug(str)
	c.Check(len(str), Greater, 0)

	tpb, err := t.ToPB()
	c.Check(err, IsNil)
	data, err := proto.Marshal(tpb)
	c.Check(err, IsNil)
	ntpb := &TablePB{}
	err = proto.Unmarshal(data, ntpb)
	c.Check(err, IsNil)
	nt, err := TableFromPB(tblInfo, ntpb)
	c.Check(err, IsNil)
	c.Check(nt.String(), Equals, str)
}
