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

package core

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"
)

// LogicalOptimize exports the `logicalOptimize` function for test packages and
// doesn't affect the normal package and access control of Golang (tricky ^_^)
var LogicalOptimize = logicalOptimize

var _ = Suite(&testPlannerFunctionSuite{})

type testPlannerFunctionSuite struct {
}

func testDecimalConvert(lDec, lLen, rDec, rLen int, lConvert, rConvert bool, cDec, cLen int, c *C) {
	lType := types.NewFieldType(mysql.TypeNewDecimal)
	lType.Decimal = lDec
	lType.Flen = lLen

	rType := types.NewFieldType(mysql.TypeNewDecimal)
	rType.Decimal = rDec
	rType.Flen = rLen

	cType, lCon, rCon := negotiateCommonType(lType, rType)
	c.Assert(cType.Tp, Equals, mysql.TypeNewDecimal)
	c.Assert(cType.Decimal, Equals, cDec)
	c.Assert(cType.Flen, Equals, cLen)
	c.Assert(lConvert, Equals, lCon)
	c.Assert(rConvert, Equals, rCon)
}

func (t *testPlannerFunctionSuite) TestMPPDecimalConvert(c *C) {
	testDecimalConvert(5, 9, 5, 8, false, false, 5, 9, c)
	testDecimalConvert(5, 8, 5, 9, false, false, 5, 9, c)
	testDecimalConvert(0, 8, 0, 11, true, false, 0, 11, c)
	testDecimalConvert(0, 16, 0, 11, false, false, 0, 16, c)
	testDecimalConvert(5, 9, 4, 9, true, true, 5, 10, c)
	testDecimalConvert(5, 8, 4, 9, true, true, 5, 10, c)
	testDecimalConvert(5, 9, 4, 8, false, true, 5, 9, c)
	testDecimalConvert(10, 16, 0, 11, true, true, 10, 21, c)
	testDecimalConvert(5, 19, 0, 20, false, true, 5, 25, c)
	testDecimalConvert(20, 20, 0, 60, true, true, 20, 65, c)
	testDecimalConvert(20, 40, 0, 60, false, true, 20, 65, c)
	testDecimalConvert(0, 40, 0, 60, false, false, 0, 60, c)
}
