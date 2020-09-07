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
// See the License for the specific language governing permissions and
// limitations under the License.

package plancodec

import (
	"testing"

	. "github.com/pingcap/check"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testPlanIDSuite{})

type testPlanIDSuite struct{}

func (s *testPlanIDSuite) TestPlanIDChanged(c *C) {
	// Attention: for compatibility, shouldn't modify the below test, you can only add test when add new plan ID.
	c.Assert(typeSelID, Equals, 1)
	c.Assert(typeSetID, Equals, 2)
	c.Assert(typeProjID, Equals, 3)
	c.Assert(typeAggID, Equals, 4)
	c.Assert(typeStreamAggID, Equals, 5)
	c.Assert(typeHashAggID, Equals, 6)
	c.Assert(typeShowID, Equals, 7)
	c.Assert(typeJoinID, Equals, 8)
	c.Assert(typeUnionID, Equals, 9)
	c.Assert(typeTableScanID, Equals, 10)
	c.Assert(typeMemTableScanID, Equals, 11)
	c.Assert(typeUnionScanID, Equals, 12)
	c.Assert(typeIdxScanID, Equals, 13)
	c.Assert(typeSortID, Equals, 14)
	c.Assert(typeTopNID, Equals, 15)
	c.Assert(typeLimitID, Equals, 16)
	c.Assert(typeHashJoinID, Equals, 17)
	c.Assert(typeMergeJoinID, Equals, 18)
	c.Assert(typeIndexJoinID, Equals, 19)
	c.Assert(typeIndexMergeJoinID, Equals, 20)
	c.Assert(typeIndexHashJoinID, Equals, 21)
	c.Assert(typeApplyID, Equals, 22)
	c.Assert(typeMaxOneRowID, Equals, 23)
	c.Assert(typeExistsID, Equals, 24)
	c.Assert(typeDualID, Equals, 25)
	c.Assert(typeLockID, Equals, 26)
	c.Assert(typeInsertID, Equals, 27)
	c.Assert(typeUpdateID, Equals, 28)
	c.Assert(typeDeleteID, Equals, 29)
	c.Assert(typeIndexLookUpID, Equals, 30)
	c.Assert(typeTableReaderID, Equals, 31)
	c.Assert(typeIndexReaderID, Equals, 32)
	c.Assert(typeWindowID, Equals, 33)
	c.Assert(typeTiKVSingleGatherID, Equals, 34)
	c.Assert(typeIndexMergeID, Equals, 35)
	c.Assert(typePointGet, Equals, 36)
	c.Assert(typeShowDDLJobs, Equals, 37)
	c.Assert(typeBatchPointGet, Equals, 38)
	c.Assert(typeClusterMemTableReader, Equals, 39)
	c.Assert(typeDataSourceID, Equals, 40)
}
