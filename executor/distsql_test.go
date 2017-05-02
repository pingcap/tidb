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
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/executor"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/testkit"
)

// TestIndexDoubleReadClose checks that when a index double read returns before reading all the rows, the goroutine doesn't
// leak. For testing distsql with multiple regions, we need to manually split a mock TiKV.
func (s *testSuite) TestIndexDoubleReadClose(c *C) {
	if _, ok := s.store.GetClient().(*tikv.CopClient); !ok {
		// Make sure the store is tikv store.
		return
	}
	originSize := executor.LookupTableTaskChannelSize
	executor.LookupTableTaskChannelSize = 1
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

	rss, err := tk.Se.Execute("select * from dist where c_idx between 0 and 100")
	c.Assert(err, IsNil)
	rs := rss[0]
	_, err = rs.Next()
	c.Assert(err, IsNil)
	keyword := "pickAndExecTask"
	c.Check(checkGoroutineExists(keyword), IsTrue)
	rs.Close()
	time.Sleep(time.Millisecond * 50)
	c.Check(checkGoroutineExists(keyword), IsFalse)
	executor.LookupTableTaskChannelSize = originSize
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
	dom := sessionctx.GetDomain(tk.Se)
	is := dom.InfoSchema()
	tbl, err := is.TableByName(model.NewCIStr("test"), model.NewCIStr("copclient"))
	c.Assert(err, IsNil)
	tblID := tbl.Meta().ID

	// Split the table.
	cli := tikv.GetMockTiKVClient(s.store)
	cli.Cluster.SplitTable(cli.MvccStore, tblID, 100)

	// Send coprocessor request when the table split.
	rss, err := tk.Se.Execute("select sum(id) from copclient")
	c.Assert(err, IsNil)
	rs := rss[0]
	defer rs.Close()
	row, err := rs.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data[0].GetMysqlDecimal().String(), Equals, "499500")

	// Split one region.
	key := tablecodec.EncodeRowKeyWithHandle(tblID, 500)
	region, _ := cli.Cluster.GetRegionByKey([]byte(key))
	peerID := cli.Cluster.AllocID()
	cli.Cluster.Split(region.GetId(), cli.Cluster.AllocID(), key, []uint64{peerID}, peerID)

	// Check again.
	rss, err = tk.Se.Execute("select sum(id) from copclient")
	c.Assert(err, IsNil)
	rs = rss[0]
	row, err = rs.Next()
	c.Assert(err, IsNil)
	c.Assert(row.Data[0].GetMysqlDecimal().String(), Equals, "499500")
	rs.Close()

	// Check there is no goroutine leak.
	rss, err = tk.Se.Execute("select * from copclient order by id")
	c.Assert(err, IsNil)
	rs = rss[0]
	_, err = rs.Next()
	c.Assert(err, IsNil)
	rs.Close()
	keyword := "(*copIterator).work"
	c.Check(checkGoroutineExists(keyword), IsFalse)
}
