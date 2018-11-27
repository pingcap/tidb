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

package executor_test

import (
	"context"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
)

func (s *testSuite) TestIndexLookupJoinHang(c *C) {
	tk := testkit.NewTestKitWithInit(c, s.store)
	tk.MustExec("create table idxJoinOuter (a int unsigned)")
	tk.MustExec("create table idxJoinInner (a int unsigned unique)")
	tk.MustExec("insert idxJoinOuter values (1), (1), (1), (1), (1)")
	tk.MustExec("insert idxJoinInner values (1)")
	tk.Se.GetSessionVars().IndexJoinBatchSize = 1
	tk.Se.GetSessionVars().IndexLookupJoinConcurrency = 1

	rs, err := tk.Exec("select /*+ TIDB_INLJ(i)*/ * from idxJoinOuter o left join idxJoinInner i on o.a = i.a where o.a in (1, 2) and (i.a - 3) > 0")
	c.Assert(err, IsNil)
	chk := rs.NewChunk()
	for i := 0; i < 5; i++ {
		rs.Next(context.Background(), chk)
	}
	rs.Close()
}
