// Copyright 2017 PingCAP, Inc.
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

package executor_test

import (
	"fmt"
	"strings"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
	"github.com/pingcap/tidb/util/types"
)

func (s *testSuite) TestAnalyzeTable(c *C) {
	defer func() {
		testleak.AfterTest(c)()
	}()
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create index ind_a on t1 (a)")
	tk.MustExec("insert into t1 (a) values (1)")
	result := tk.MustQuery("explain select * from t1 where t1.a = 1")
	rowStr := fmt.Sprintf("%s", result.Rows())
	c.Check(strings.Split(rowStr, "{")[0], Equals, "[[IndexScan_5 ")
	tk.MustExec("analyze table t1")
	result = tk.MustQuery("explain select * from t1 where t1.a = 1")
	rowStr = fmt.Sprintf("%s", result.Rows())
	c.Check(strings.Split(rowStr, "{")[0], Equals, "[[TableScan_4 ")

	tk.MustExec("drop table if exists t1")
	tk.MustExec("create table t1 (a int)")
	tk.MustExec("create index ind_a on t1 (a)")
	tk.MustExec("insert into t1 (a) values (1)")
	tk.MustExec("analyze table t1 index ind_a")
	result = tk.MustQuery("explain select * from t1 where t1.a = 1")
	rowStr = fmt.Sprintf("%s", result.Rows())
	c.Check(strings.Split(rowStr, "{")[0], Equals, "[[TableScan_4 ")
}

type recordSet struct {
	data   []types.Datum
	count  int
	cursor int
}

func (r *recordSet) Fields() ([]*ast.ResultField, error) {
	return nil, nil
}

func (r *recordSet) Next() (*ast.Row, error) {
	if r.cursor == r.count {
		return nil, nil
	}
	r.cursor++
	return &ast.Row{Data: []types.Datum{r.data[r.cursor-1]}}, nil
}

func (r *recordSet) Close() error {
	r.cursor = 0
	return nil
}

func (s *testSuite) TestCollectSamplesAndEstimateNDVs(c *C) {
	count := 10000
	rs := &recordSet{
		data:   make([]types.Datum, count),
		count:  count,
		cursor: 0,
	}
	start := 1000 // 1000 values is null
	for i := start; i < rs.count; i++ {
		rs.data[i].SetInt64(int64(i))
	}
	for i := start; i < rs.count; i += 3 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 1)
	}
	for i := start; i < rs.count; i += 5 {
		rs.data[i].SetInt64(rs.data[i].GetInt64() + 2)
	}

	cnt, _, ndvs, err := executor.CollectSamplesAndEstimateNDVs(rs, 1)
	c.Assert(err, IsNil)
	c.Assert(cnt, Equals, int64(rs.count))
	c.Assert(ndvs[0], Equals, int64(6624))
}
