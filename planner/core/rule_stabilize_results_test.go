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

package core_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testutil"
)

var _ = Suite(&testRuleStabilizeRestuls{})

type testRuleStabilizeRestuls struct {
	store kv.Storage
	dom   *domain.Domain

	testData testutil.TestData
}

func (s *testRuleStabilizeRestuls) SetUpTest(c *C) {
	var err error
	s.store, s.dom, err = newStoreWithBootstrap()
	c.Assert(err, IsNil)

	s.testData, err = testutil.LoadTestSuiteData("testdata", "stable_result_mode_suite")
	c.Assert(err, IsNil)
}

func (s *testRuleStabilizeRestuls) TestStableResultMode(c *C) {
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("set tidb_enable_stable_result_mode=1")
	tk.MustExec("create table t (a int primary key, b int, c int, d int, key(b))")

	var input []string
	var output []struct {
		Plan []string
	}
	s.testData.GetTestCases(c, &input, &output)
	c.Assert(len(input), Equals, len(output))
	for i := range input {
		tk.MustQuery("explain " + input[i]).Check(testkit.Rows(output[i].Plan...))
	}
}
