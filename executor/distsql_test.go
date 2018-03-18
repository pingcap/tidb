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
	"bytes"
	"fmt"
	"runtime/pprof"
	"strings"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/testkit"
	"golang.org/x/net/context"
)

// TestIndexDoubleReadClose checks that when a index double read returns before reading all the rows, the goroutine doesn't
// leak. For testing distsql with multiple regions, we need to manually split a mock TiKV.
func (s *testSuite) TestIndexDoubleReadClose(c *C) {
	if _, ok := s.store.GetClient().(*tikv.CopClient); !ok {
		// Make sure the store is tikv store.
		return
	}
	originSize := atomic.LoadInt32(&executor.LookupTableTaskChannelSize)
	atomic.StoreInt32(&executor.LookupTableTaskChannelSize, 1)
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("set @@tidb_index_lookup_size = '10'")
	tk.MustExec("use test")
	tk.MustExec("create table dist (id int primary key, c_idx int, c_col int, index (c_idx))")

	// Insert 100 rows.
	var values []string
	for i := 0; i < 100; i++ {
		values = append(values, fmt.Sprintf("(%d, %d, %d)", i, i, i))
	}
	tk.MustExec("insert dist values " + strings.Join(values, ","))

	rs, err := tk.Exec("select * from dist where c_idx between 0 and 100")
	c.Assert(err, IsNil)
	chk := rs.NewChunk()
	err = rs.NextChunk(context.Background(), chk)
	c.Assert(err, IsNil)
	c.Assert(err, IsNil)
	keyword := "pickAndExecTask"
	rs.Close()
	time.Sleep(time.Millisecond * 50)
	c.Check(checkGoroutineExists(keyword), IsFalse)
	atomic.StoreInt32(&executor.LookupTableTaskChannelSize, originSize)
}

func checkGoroutineExists(keyword string) bool {
	buf := new(bytes.Buffer)
	profile := pprof.Lookup("goroutine")
	profile.WriteTo(buf, 1)
	str := buf.String()
	return strings.Contains(str, keyword)
}

func (s *testSuite) TestCopClientSend(c *C) {
	c.Skip("not stable")
	if _, ok := s.store.GetClient().(*tikv.CopClient); !ok {
		// Make sure the store is tikv store.
		return
	}
	tk := testkit.NewTestKit(c, s.store)
	tk.MustExec("use test")
	tk.MustExec("create table copclient (id int primary key)")

	// Insert 1000 rows.
	var values []string
	for i := 0; i < 1000; i++ {
		values = append(values, fmt.Sprintf("(%d)", i))
	}
	tk.MustExec("insert copclient values " + strings.Join(values, ","))

	// Get table ID for split.
	dom := domain.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("copclient"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the table.
	s.cluster.SplitTable(s.mvccStore, tblID, 100)

	ctx := context.Background()
	// Send coprocessor request when the table split.
	rs, err := tk.Exec("select sum(id) from copclient")
	c.Assert(err, IsNil)
	defer rs.Close()
	chk := rs.NewChunk()
	err = rs.NextChunk(ctx, chk)
	c.Assert(err, IsNil)
	c.Assert(chk.GetRow(0).GetMyDecimal(0).String(), Equals, "499500")

	// Split one region.
	key := tablecodec.EncodeRowKeyWithHandle(tblID, 500)
	region, _ := s.cluster.GetRegionByKey([]byte(key))
	peerID := s.cluster.AllocID()
	s.cluster.Split(region.GetId(), s.cluster.AllocID(), key, []uint64{peerID}, peerID)

	// Check again.
	rs, err = tk.Exec("select sum(id) from copclient")
	c.Assert(err, IsNil)
	chk = rs.NewChunk()
	err = rs.NextChunk(ctx, chk)
	c.Assert(err, IsNil)
	c.Assert(chk.GetRow(0).GetMyDecimal(0).String(), Equals, "499500")
	rs.Close()

	// Check there is no goroutine leak.
	rs, err = tk.Exec("select * from copclient order by id")
	c.Assert(err, IsNil)
	chk = rs.NewChunk()
	err = rs.NextChunk(ctx, chk)
	c.Assert(err, IsNil)
	rs.Close()
	keyword := "(*copIterator).work"
	c.Check(checkGoroutineExists(keyword), IsFalse)
}

func (s *testSuite) TestGetLackHandles(c *C) {
	expectedHandles := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	handlesMap := make(map[int64]struct{})
	for _, h := range expectedHandles {
		handlesMap[h] = struct{}{}
	}

	// expected handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	// obtained handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	diffHandles := executor.GetLackHandles(expectedHandles, handlesMap)
	c.Assert(diffHandles, HasLen, 0)
	c.Assert(handlesMap, HasLen, 0)

	// expected handles 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
	// obtained handles 2, 3, 4, 6, 7, 8, 9
	retHandles := []int64{2, 3, 4, 6, 7, 8, 9}
	handlesMap = make(map[int64]struct{})
	handlesMap[1] = struct{}{}
	handlesMap[5] = struct{}{}
	handlesMap[10] = struct{}{}
	diffHandles = executor.GetLackHandles(expectedHandles, handlesMap)
	c.Assert(retHandles, DeepEquals, diffHandles)
}
