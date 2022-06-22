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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package binloginfo_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/table/tables"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/external"
	pumpcli "github.com/pingcap/tidb/tidb-binlog/pump_client"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/util/collate"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockBinlogPump struct {
	mu struct {
		sync.Mutex
		payloads [][]byte
		mockFail bool
	}
}

func (p *mockBinlogPump) WriteBinlog(ctx context.Context, req *binlog.WriteBinlogReq) (*binlog.WriteBinlogResp, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.mu.mockFail {
		return &binlog.WriteBinlogResp{}, errors.New("mock fail")
	}
	p.mu.payloads = append(p.mu.payloads, req.Payload)
	return &binlog.WriteBinlogResp{}, nil
}

// PullBinlogs implements PumpServer interface.
func (p *mockBinlogPump) PullBinlogs(req *binlog.PullBinlogReq, srv binlog.Pump_PullBinlogsServer) error {
	return nil
}

type binlogSuite struct {
	store  kv.Storage
	serv   *grpc.Server
	pump   *mockBinlogPump
	client *pumpcli.PumpsClient
	ddl    ddl.DDL
}

const maxRecvMsgSize = 64 * 1024

func createBinlogSuite(t *testing.T) (s *binlogSuite, clean func()) {
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/store/driver/txn/mockSyncBinlogCommit", `return(true)`))

	s = new(binlogSuite)
	store, cleanStore := testkit.CreateMockStore(t)
	s.store = store
	unixFile := "/tmp/mock-binlog-pump" + strconv.FormatInt(time.Now().UnixNano(), 10)
	l, err := net.Listen("unix", unixFile)
	require.NoError(t, err)
	s.serv = grpc.NewServer(grpc.MaxRecvMsgSize(maxRecvMsgSize))
	s.pump = new(mockBinlogPump)
	binlog.RegisterPumpServer(s.serv, s.pump)
	go func() {
		err := s.serv.Serve(l)
		require.NoError(t, err)
	}()
	opt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("unix", addr, timeout)
	})
	clientCon, err := grpc.Dial(unixFile, opt, grpc.WithInsecure())
	require.NoError(t, err)
	require.NotNil(t, clientCon)

	tk := testkit.NewTestKit(t, s.store)
	sessionDomain := domain.GetDomain(tk.Session().(sessionctx.Context))
	s.ddl = sessionDomain.DDL()

	s.client = binloginfo.MockPumpsClient(binlog.NewPumpClient(clientCon))
	s.ddl.SetBinlogClient(s.client)

	clean = func() {
		clientCon.Close()
		err = s.ddl.Stop()
		require.NoError(t, err)
		s.serv.Stop()
		err = os.Remove(unixFile)
		if err != nil {
			require.EqualError(t, err, fmt.Sprintf("remove %v: no such file or directory", unixFile))
		}
		cleanStore()
		require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/store/driver/txn/mockSyncBinlogCommit"))
	}

	return
}

func TestBinlog(t *testing.T) {
	s, clean := createBinlogSuite(t)
	defer clean()

	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().BinlogClient = s.client
	pump := s.pump
	tk.MustExec("drop table if exists local_binlog")
	ddlQuery := "create table local_binlog (id int unique key, name varchar(10)) shard_row_id_bits=1"
	binlogDDLQuery := "create table local_binlog (id int unique key, name varchar(10)) /*T! shard_row_id_bits=1 */"
	tk.MustExec(ddlQuery)
	var matched bool // got matched pre DDL and commit DDL
	for i := 0; i < 10; i++ {
		preDDL, commitDDL, _ := getLatestDDLBinlog(t, pump, binlogDDLQuery)
		if preDDL != nil && commitDDL != nil {
			if preDDL.DdlJobId == commitDDL.DdlJobId {
				require.Equal(t, preDDL.StartTs, commitDDL.StartTs)
				require.Greater(t, commitDDL.CommitTs, commitDDL.StartTs)
				matched = true
				break
			}
		}
		time.Sleep(time.Millisecond * 10)
	}
	require.True(t, matched)

	tk.MustExec("insert local_binlog values (1, 'abc'), (2, 'cde')")
	prewriteVal := getLatestBinlogPrewriteValue(t, pump)
	require.Greater(t, prewriteVal.SchemaVersion, int64(0))
	require.Greater(t, prewriteVal.Mutations[0].TableId, int64(0))
	expected := [][]types.Datum{
		{types.NewIntDatum(1), types.NewCollationStringDatum("abc", mysql.DefaultCollationName)},
		{types.NewIntDatum(2), types.NewCollationStringDatum("cde", mysql.DefaultCollationName)},
	}
	gotRows := mutationRowsToRows(t, prewriteVal.Mutations[0].InsertedRows, 2, 4)
	require.Equal(t, expected, gotRows)

	tk.MustExec("update local_binlog set name = 'xyz' where id = 2")
	prewriteVal = getLatestBinlogPrewriteValue(t, pump)
	oldRow := [][]types.Datum{
		{types.NewIntDatum(2), types.NewCollationStringDatum("cde", mysql.DefaultCollationName)},
	}
	newRow := [][]types.Datum{
		{types.NewIntDatum(2), types.NewCollationStringDatum("xyz", mysql.DefaultCollationName)},
	}
	gotRows = mutationRowsToRows(t, prewriteVal.Mutations[0].UpdatedRows, 1, 3)
	require.Equal(t, oldRow, gotRows)

	gotRows = mutationRowsToRows(t, prewriteVal.Mutations[0].UpdatedRows, 7, 9)
	require.Equal(t, newRow, gotRows)

	tk.MustExec("delete from local_binlog where id = 1")
	prewriteVal = getLatestBinlogPrewriteValue(t, pump)
	gotRows = mutationRowsToRows(t, prewriteVal.Mutations[0].DeletedRows, 1, 3)
	expected = [][]types.Datum{
		{types.NewIntDatum(1), types.NewCollationStringDatum("abc", mysql.DefaultCollationName)},
	}
	require.Equal(t, expected, gotRows)

	// Test table primary key is not integer.
	tk.Session().GetSessionVars().EnableClusteredIndex = variable.ClusteredIndexDefModeIntOnly
	tk.MustExec("create table local_binlog2 (name varchar(64) primary key, age int)")
	tk.MustExec("insert local_binlog2 values ('abc', 16), ('def', 18)")
	tk.MustExec("delete from local_binlog2 where name = 'def'")
	prewriteVal = getLatestBinlogPrewriteValue(t, pump)
	require.Equal(t, binlog.MutationType_DeleteRow, prewriteVal.Mutations[0].Sequence[0])

	expected = [][]types.Datum{
		{types.NewStringDatum("def"), types.NewIntDatum(18), types.NewIntDatum(-1), types.NewIntDatum(2)},
	}
	gotRows = mutationRowsToRows(t, prewriteVal.Mutations[0].DeletedRows, 1, 3, 4, 5)
	require.Equal(t, expected, gotRows)

	// Test Table don't have primary key.
	tk.MustExec("create table local_binlog3 (c1 int, c2 int)")
	tk.MustExec("insert local_binlog3 values (1, 2), (1, 3), (2, 3)")
	tk.MustExec("update local_binlog3 set c1 = 3 where c1 = 2")
	prewriteVal = getLatestBinlogPrewriteValue(t, pump)

	// The encoded update row is [oldColID1, oldColVal1, oldColID2, oldColVal2, -1, handle,
	// 		newColID1, newColVal2, newColID2, newColVal2, -1, handle]
	gotRows = mutationRowsToRows(t, prewriteVal.Mutations[0].UpdatedRows, 7, 9)
	expected = [][]types.Datum{
		{types.NewIntDatum(3), types.NewIntDatum(3)},
	}
	require.Equal(t, expected, gotRows)
	expected = [][]types.Datum{
		{types.NewIntDatum(-1), types.NewIntDatum(3), types.NewIntDatum(-1), types.NewIntDatum(3)},
	}
	gotRows = mutationRowsToRows(t, prewriteVal.Mutations[0].UpdatedRows, 4, 5, 10, 11)
	require.Equal(t, expected, gotRows)

	tk.MustExec("delete from local_binlog3 where c1 = 3 and c2 = 3")
	prewriteVal = getLatestBinlogPrewriteValue(t, pump)
	require.Equal(t, binlog.MutationType_DeleteRow, prewriteVal.Mutations[0].Sequence[0])
	gotRows = mutationRowsToRows(t, prewriteVal.Mutations[0].DeletedRows, 1, 3, 4, 5)
	expected = [][]types.Datum{
		{types.NewIntDatum(3), types.NewIntDatum(3), types.NewIntDatum(-1), types.NewIntDatum(3)},
	}
	require.Equal(t, expected, gotRows)

	// Test Mutation Sequence.
	tk.MustExec("create table local_binlog4 (c1 int primary key, c2 int)")
	tk.MustExec("insert local_binlog4 values (1, 1), (2, 2), (3, 2)")
	tk.MustExec("begin")
	tk.MustExec("delete from local_binlog4 where c1 = 1")
	tk.MustExec("insert local_binlog4 values (1, 1)")
	tk.MustExec("update local_binlog4 set c2 = 3 where c1 = 3")
	tk.MustExec("commit")
	prewriteVal = getLatestBinlogPrewriteValue(t, pump)
	require.Equal(t, []binlog.MutationType{
		binlog.MutationType_DeleteRow,
		binlog.MutationType_Insert,
		binlog.MutationType_Update,
	}, prewriteVal.Mutations[0].Sequence)

	// Test statement rollback.
	tk.MustExec("create table local_binlog5 (c1 int primary key)")
	tk.MustExec("begin")
	tk.MustExec("insert into local_binlog5 value (1)")
	// This statement execute fail and should not write binlog.
	_, err := tk.Exec("insert into local_binlog5 value (4),(3),(1),(2)")
	require.Error(t, err)
	tk.MustExec("commit")
	prewriteVal = getLatestBinlogPrewriteValue(t, pump)
	require.Equal(t, []binlog.MutationType{
		binlog.MutationType_Insert,
	}, prewriteVal.Mutations[0].Sequence)

	checkBinlogCount(t, pump)

	pump.mu.Lock()
	originBinlogLen := len(pump.mu.payloads)
	pump.mu.Unlock()
	tk.MustExec("set @@global.autocommit = 0")
	tk.MustExec("set @@global.autocommit = 1")
	pump.mu.Lock()
	newBinlogLen := len(pump.mu.payloads)
	pump.mu.Unlock()
	require.Equal(t, originBinlogLen, newBinlogLen)
}

func TestMaxRecvSize(t *testing.T) {
	s, clean := createBinlogSuite(t)
	defer clean()

	info := &binloginfo.BinlogInfo{
		Data: &binlog.Binlog{
			Tp:            binlog.BinlogType_Prewrite,
			PrewriteValue: make([]byte, maxRecvMsgSize+1),
		},
		Client: s.client,
	}
	binlogWR := info.WriteBinlog(1)
	err := binlogWR.GetError()
	require.Error(t, err)
	require.Falsef(t, terror.ErrCritical.Equal(err), fmt.Sprintf("%v", err))
}

func getLatestBinlogPrewriteValue(t *testing.T, pump *mockBinlogPump) *binlog.PrewriteValue {
	var bin *binlog.Binlog
	pump.mu.Lock()
	for i := len(pump.mu.payloads) - 1; i >= 0; i-- {
		payload := pump.mu.payloads[i]
		bin = new(binlog.Binlog)
		err := bin.Unmarshal(payload)
		require.NoError(t, err)
		if bin.Tp == binlog.BinlogType_Prewrite {
			break
		}
	}
	pump.mu.Unlock()
	require.NotNil(t, bin)
	preVal := new(binlog.PrewriteValue)
	err := preVal.Unmarshal(bin.PrewriteValue)
	require.NoError(t, err)
	return preVal
}

func getLatestDDLBinlog(t *testing.T, pump *mockBinlogPump, ddlQuery string) (preDDL, commitDDL *binlog.Binlog, offset int) {
	pump.mu.Lock()
	for i := len(pump.mu.payloads) - 1; i >= 0; i-- {
		payload := pump.mu.payloads[i]
		bin := new(binlog.Binlog)
		err := bin.Unmarshal(payload)
		require.NoError(t, err)
		if bin.Tp == binlog.BinlogType_Commit && bin.DdlJobId > 0 {
			commitDDL = bin
		}
		if bin.Tp == binlog.BinlogType_Prewrite && bin.DdlJobId != 0 {
			preDDL = bin
		}
		if preDDL != nil && commitDDL != nil {
			offset = i
			break
		}
	}
	pump.mu.Unlock()
	require.Greater(t, preDDL.DdlJobId, int64(0))
	require.Greater(t, preDDL.StartTs, int64(0))
	require.Equal(t, int64(0), preDDL.CommitTs)
	formatted, err := binloginfo.FormatAndAddTiDBSpecificComment(ddlQuery)
	require.NoError(t, err, "ddlQuery: %s", ddlQuery)
	require.Equal(t, formatted, string(preDDL.DdlQuery))
	return
}

func checkBinlogCount(t *testing.T, pump *mockBinlogPump) {
	var bin *binlog.Binlog
	prewriteCount := 0
	ddlCount := 0
	pump.mu.Lock()
	length := len(pump.mu.payloads)
	for i := length - 1; i >= 0; i-- {
		payload := pump.mu.payloads[i]
		bin = new(binlog.Binlog)
		err := bin.Unmarshal(payload)
		require.NoError(t, err)
		if bin.Tp == binlog.BinlogType_Prewrite {
			if bin.DdlJobId != 0 {
				ddlCount++
			} else {
				prewriteCount++
			}
		}
	}
	pump.mu.Unlock()
	require.Greater(t, ddlCount, 0)
	match := false
	for i := 0; i < 10; i++ {
		pump.mu.Lock()
		length = len(pump.mu.payloads)
		pump.mu.Unlock()
		if (prewriteCount+ddlCount)*2 == length {
			match = true
			break
		}
		time.Sleep(time.Millisecond * 10)
	}
	require.True(t, match)
}

func mutationRowsToRows(t *testing.T, mutationRows [][]byte, columnValueOffsets ...int) [][]types.Datum {
	var rows = make([][]types.Datum, 0)
	for _, mutationRow := range mutationRows {
		datums, err := codec.Decode(mutationRow, 5)
		require.NoError(t, err)
		for i := range datums {
			if datums[i].Kind() == types.KindBytes {
				datums[i].SetBytesAsString(datums[i].GetBytes(), mysql.DefaultCollationName, collate.DefaultLen)
			}
		}
		row := make([]types.Datum, 0, len(columnValueOffsets))
		for _, colOff := range columnValueOffsets {
			row = append(row, datums[colOff])
		}
		rows = append(rows, row)
	}
	return rows
}

func TestBinlogForSequence(t *testing.T) {
	s, clean := createBinlogSuite(t)
	defer clean()

	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	s.pump.mu.Lock()
	s.pump.mu.payloads = s.pump.mu.payloads[:0]
	s.pump.mu.Unlock()
	tk.Session().GetSessionVars().BinlogClient = s.client

	tk.MustExec("drop sequence if exists seq")
	// the default start = 1, increment = 1.
	tk.MustExec("create sequence seq cache 3")
	// trigger the sequence cache allocation.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("1"))
	sequenceTable := external.GetTableByName(t, tk, "test", "seq")
	tc, ok := sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round := tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(3), end)
	require.Equal(t, int64(0), round)

	// Check the sequence binlog.
	// Got matched pre DDL and commit DDL.
	ok = mustGetDDLBinlog(s, "select setval(`test`.`seq`, 3)", t)
	require.True(t, ok)

	// Invalidate the current sequence cache.
	tk.MustQuery("select setval(seq, 5)").Check(testkit.Rows("5"))
	// trigger the next sequence cache allocation.
	tk.MustQuery("select nextval(seq)").Check(testkit.Rows("6"))
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(8), end)
	require.Equal(t, int64(0), round)
	ok = mustGetDDLBinlog(s, "select setval(`test`.`seq`, 8)", t)
	require.True(t, ok)

	tk.MustExec("create database test2")
	tk.MustExec("use test2")
	tk.MustExec("drop sequence if exists seq2")
	tk.MustExec("create sequence seq2 start 1 increment -2 cache 3 minvalue -10 maxvalue 10 cycle")
	// trigger the sequence cache allocation.
	tk.MustQuery("select nextval(seq2)").Check(testkit.Rows("1"))
	sequenceTable = external.GetTableByName(t, tk, "test2", "seq2")
	tc, ok = sequenceTable.(*tables.TableCommon)
	require.Equal(t, true, ok)
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(-3), end)
	require.Equal(t, int64(0), round)
	ok = mustGetDDLBinlog(s, "select setval(`test2`.`seq2`, -3)", t)
	require.True(t, ok)

	tk.MustQuery("select setval(seq2, -100)").Check(testkit.Rows("-100"))
	// trigger the sequence cache allocation.
	tk.MustQuery("select nextval(seq2)").Check(testkit.Rows("10"))
	_, end, round = tc.GetSequenceCommon().GetSequenceBaseEndRound()
	require.Equal(t, int64(6), end)
	require.Equal(t, int64(1), round)
	ok = mustGetDDLBinlog(s, "select setval(`test2`.`seq2`, 6)", t)
	require.True(t, ok)

	// Test dml txn is independent from sequence txn.
	tk.MustExec("drop sequence if exists seq")
	tk.MustExec("create sequence seq cache 3")
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (a int default next value for seq)")
	// sequence txn commit first then the dml txn.
	tk.MustExec("insert into t values(-1),(default),(-1),(default)")
	// binlog list like [... ddl prewrite(offset), ddl commit, dml prewrite, dml commit]
	_, _, offset := getLatestDDLBinlog(t, s.pump, "select setval(`test2`.`seq`, 3)")
	s.pump.mu.Lock()
	require.Equal(t, len(s.pump.mu.payloads)-1, offset+3)
	s.pump.mu.Unlock()
}

// Sometimes this test doesn't clean up fail, let the function name begin with 'Z'
// so it runs last and would not disrupt other tests.
func TestZIgnoreError(t *testing.T) {
	s, clean := createBinlogSuite(t)
	defer clean()

	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().BinlogClient = s.client
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t (id int)")

	binloginfo.SetIgnoreError(true)
	s.pump.mu.Lock()
	s.pump.mu.mockFail = true
	s.pump.mu.Unlock()

	tk.MustExec("insert into t values (1)")
	tk.MustExec("insert into t values (1)")

	// Clean up.
	s.pump.mu.Lock()
	s.pump.mu.mockFail = false
	s.pump.mu.Unlock()
	binloginfo.DisableSkipBinlogFlag()
	binloginfo.SetIgnoreError(false)
}

func TestPartitionedTable(t *testing.T) {
	s, clean := createBinlogSuite(t)
	defer clean()

	// This test checks partitioned table write binlog with table ID, rather than partition ID.
	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().BinlogClient = s.client
	tk.MustExec("drop table if exists t")
	tk.MustExec(`create table t (id int) partition by range (id) (
			partition p0 values less than (1),
			partition p1 values less than (4),
			partition p2 values less than (7),
			partition p3 values less than (10))`)
	tids := make([]int64, 0, 10)
	for i := 0; i < 10; i++ {
		tk.MustExec("insert into t values (?)", i)
		prewriteVal := getLatestBinlogPrewriteValue(t, s.pump)
		tids = append(tids, prewriteVal.Mutations[0].TableId)
	}
	require.Equal(t, 10, len(tids))
	for i := 1; i < 10; i++ {
		require.Equal(t, tids[0], tids[i])
	}
}

func TestPessimisticLockThenCommit(t *testing.T) {
	s, clean := createBinlogSuite(t)
	defer clean()

	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().BinlogClient = s.client
	tk.MustExec("drop table if exists t")
	tk.MustExec("create table t(a int, b int)")
	tk.MustExec("begin pessimistic")
	tk.MustExec("insert into t select 1, 1")
	tk.MustExec("commit")
	prewriteVal := getLatestBinlogPrewriteValue(t, s.pump)
	require.Equal(t, 1, len(prewriteVal.Mutations))
}

func TestDeleteSchema(t *testing.T) {
	store, clean := testkit.CreateMockStore(t)
	defer clean()

	tk := testkit.NewTestKit(t, store)
	tk.MustExec("use test")
	tk.MustExec("CREATE TABLE `b1` (`id` int(11) NOT NULL AUTO_INCREMENT, `job_id` varchar(50) NOT NULL, `split_job_id` varchar(30) DEFAULT NULL, PRIMARY KEY (`id`), KEY `b1` (`job_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;")
	tk.MustExec("CREATE TABLE `b2` (`id` int(11) NOT NULL AUTO_INCREMENT, `job_id` varchar(50) NOT NULL, `batch_class` varchar(20) DEFAULT NULL, PRIMARY KEY (`id`), UNIQUE KEY `bu` (`job_id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	tk.MustExec("insert into b2 (job_id, batch_class) values (2, 'TEST');")
	tk.MustExec("insert into b1 (job_id) values (2);")

	// This test cover a bug that the final schema and the binlog row inconsistent.
	// The final schema of this SQL should be the schema of table b1, rather than the schema of join result.
	tk.MustExec("delete from b1 where job_id in (select job_id from b2 where batch_class = 'TEST') or split_job_id in (select job_id from b2 where batch_class = 'TEST');")
	tk.MustExec("delete b1 from b2 right join b1 on b1.job_id = b2.job_id and batch_class = 'TEST';")
}

func TestFormatAndAddTiDBSpecificComment(t *testing.T) {
	testCase := []struct {
		input  string
		result string
	}{
		{
			"create table t1 (id int ) shard_row_id_bits=2;",
			"CREATE TABLE `t1` (`id` INT) /*T! SHARD_ROW_ID_BITS = 2 */;",
		},
		{
			"create table t1 (id int ) shard_row_id_bits=2 pre_split_regions=2;",
			"CREATE TABLE `t1` (`id` INT) /*T! SHARD_ROW_ID_BITS = 2 */ /*T! PRE_SPLIT_REGIONS = 2 */;",
		},
		{
			"create table t1 (id int ) shard_row_id_bits=2     pre_split_regions=2;",
			"CREATE TABLE `t1` (`id` INT) /*T! SHARD_ROW_ID_BITS = 2 */ /*T! PRE_SPLIT_REGIONS = 2 */;",
		},
		{
			"create table t1 (id int ) shard_row_id_bits=2 engine=innodb pre_split_regions=2;",
			"CREATE TABLE `t1` (`id` INT) /*T! SHARD_ROW_ID_BITS = 2 */ ENGINE = innodb /*T! PRE_SPLIT_REGIONS = 2 */;",
		},
		{
			"create table t1 (id int ) pre_split_regions=2 shard_row_id_bits=2;",
			"CREATE TABLE `t1` (`id` INT) /*T! PRE_SPLIT_REGIONS = 2 */ /*T! SHARD_ROW_ID_BITS = 2 */;",
		},
		{
			"create table t6 (id int ) shard_row_id_bits=2 shard_row_id_bits=3 pre_split_regions=2;",
			"CREATE TABLE `t6` (`id` INT) /*T! SHARD_ROW_ID_BITS = 2 */ /*T! SHARD_ROW_ID_BITS = 3 */ /*T! PRE_SPLIT_REGIONS = 2 */;",
		},
		{
			"create table t1 (id int primary key auto_random(2));",
			"CREATE TABLE `t1` (`id` INT PRIMARY KEY /*T![auto_rand] AUTO_RANDOM(2) */);",
		},
		{
			"create table t1 (id int primary key auto_random);",
			"CREATE TABLE `t1` (`id` INT PRIMARY KEY /*T![auto_rand] AUTO_RANDOM */);",
		},
		{
			"create table t1 (id int auto_random ( 4 ) primary key);",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM(4) */ PRIMARY KEY);",
		},
		{
			"create table t1 (id int  auto_random  (   4    ) primary key);",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM(4) */ PRIMARY KEY);",
		},
		{
			"create table t1 (id int auto_random ( 3 ) primary key) auto_random_base = 100;",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM(3) */ PRIMARY KEY) /*T![auto_rand_base] AUTO_RANDOM_BASE = 100 */;",
		},
		{
			"create table t1 (id int auto_random primary key) auto_random_base = 50;",
			"CREATE TABLE `t1` (`id` INT /*T![auto_rand] AUTO_RANDOM */ PRIMARY KEY) /*T![auto_rand_base] AUTO_RANDOM_BASE = 50 */;",
		},
		{
			"create table t1 (id int auto_increment key) auto_id_cache 100;",
			"CREATE TABLE `t1` (`id` INT AUTO_INCREMENT PRIMARY KEY) /*T![auto_id_cache] AUTO_ID_CACHE = 100 */;",
		},
		{
			"create table t1 (id int auto_increment unique) auto_id_cache 10;",
			"CREATE TABLE `t1` (`id` INT AUTO_INCREMENT UNIQUE KEY) /*T![auto_id_cache] AUTO_ID_CACHE = 10 */;",
		},
		{
			"create table t1 (id int) auto_id_cache = 5;",
			"CREATE TABLE `t1` (`id` INT) /*T![auto_id_cache] AUTO_ID_CACHE = 5 */;",
		},
		{
			"create table t1 (id int) auto_id_cache=5;",
			"CREATE TABLE `t1` (`id` INT) /*T![auto_id_cache] AUTO_ID_CACHE = 5 */;",
		},
		{
			"create table t1 (id int) /*T![auto_id_cache] auto_id_cache=5 */ ;",
			"CREATE TABLE `t1` (`id` INT) /*T![auto_id_cache] AUTO_ID_CACHE = 5 */;",
		},
		{
			"create table t1 (id int, a varchar(255), primary key (a, b) clustered);",
			"CREATE TABLE `t1` (`id` INT,`a` VARCHAR(255),PRIMARY KEY(`a`, `b`) /*T![clustered_index] CLUSTERED */);",
		},
		{
			"create table t1(id int, v int, primary key(a) clustered);",
			"CREATE TABLE `t1` (`id` INT,`v` INT,PRIMARY KEY(`a`) /*T![clustered_index] CLUSTERED */);",
		},
		{
			"create table t1(id int primary key clustered, v int);",
			"CREATE TABLE `t1` (`id` INT PRIMARY KEY /*T![clustered_index] CLUSTERED */,`v` INT);",
		},
		{
			"alter table t add primary key(a) clustered;",
			"ALTER TABLE `t` ADD PRIMARY KEY(`a`) /*T![clustered_index] CLUSTERED */;",
		},
		{
			"create table t1 (id int, a varchar(255), primary key (a, b) nonclustered);",
			"CREATE TABLE `t1` (`id` INT,`a` VARCHAR(255),PRIMARY KEY(`a`, `b`) /*T![clustered_index] NONCLUSTERED */);",
		},
		{
			"create table t1 (id int, a varchar(255), primary key (a, b) /*T![clustered_index] nonclustered */);",
			"CREATE TABLE `t1` (`id` INT,`a` VARCHAR(255),PRIMARY KEY(`a`, `b`) /*T![clustered_index] NONCLUSTERED */);",
		},
		{
			"create table clustered_test(id int)",
			"CREATE TABLE `clustered_test` (`id` INT);",
		},
		{
			"create database clustered_test",
			"CREATE DATABASE `clustered_test`;",
		},
		{
			"create database clustered",
			"CREATE DATABASE `clustered`;",
		},
		{
			"create table clustered (id int)",
			"CREATE TABLE `clustered` (`id` INT);",
		},
		{
			"create table t1 (id int, a varchar(255) key clustered);",
			"CREATE TABLE `t1` (`id` INT,`a` VARCHAR(255) PRIMARY KEY /*T![clustered_index] CLUSTERED */);",
		},
		{
			"alter table t force auto_increment = 12;",
			"ALTER TABLE `t` /*T![force_inc] FORCE */ AUTO_INCREMENT = 12;",
		},
		{
			"alter table t force, auto_increment = 12;",
			"ALTER TABLE `t` FORCE /* AlterTableForce is not supported */ , AUTO_INCREMENT = 12;",
		},
		{
			// https://github.com/pingcap/tiflow/issues/3755
			"create table cdc_test (id varchar(10) primary key ,c1 varchar(10)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin/*!90000  SHARD_ROW_ID_BITS=4 PRE_SPLIT_REGIONS=3 */",
			"CREATE TABLE `cdc_test` (`id` VARCHAR(10) PRIMARY KEY,`c1` VARCHAR(10)) ENGINE = InnoDB DEFAULT CHARACTER SET = UTF8MB4 DEFAULT COLLATE = UTF8MB4_BIN /*T! SHARD_ROW_ID_BITS = 4 */ /*T! PRE_SPLIT_REGIONS = 3 */;",
		},
		{
			"create table clustered (id int);  create table t1 (id int, a varchar(255) key clustered);  ",
			"CREATE TABLE `clustered` (`id` INT);CREATE TABLE `t1` (`id` INT,`a` VARCHAR(255) PRIMARY KEY /*T![clustered_index] CLUSTERED */);",
		},
		{
			"",
			"",
		},
		{
			";;",
			"",
		},
		{
			"alter table t cache",
			"ALTER TABLE `t` CACHE;",
		},
		{
			"alter table t nocache",
			"ALTER TABLE `t` NOCACHE;",
		},
	}
	for _, ca := range testCase {
		re, err := binloginfo.FormatAndAddTiDBSpecificComment(ca.input)
		require.Equal(t, ca.result, re)
		require.NoError(t, err, "Unexpected error for AddTiDBSpecificComment, test input: %s", ca.input)
	}
}

func mustGetDDLBinlog(s *binlogSuite, ddlQuery string, t *testing.T) (matched bool) {
	for i := 0; i < 10; i++ {
		preDDL, commitDDL, _ := getLatestDDLBinlog(t, s.pump, ddlQuery)
		if preDDL != nil && commitDDL != nil {
			if preDDL.DdlJobId == commitDDL.DdlJobId {
				require.Equal(t, preDDL.StartTs, commitDDL.StartTs)
				require.Greater(t, commitDDL.CommitTs, commitDDL.StartTs)
				matched = true
				break
			}
		}
		time.Sleep(time.Millisecond * 30)
	}
	return
}

func TestTempTableBinlog(t *testing.T) {
	s, clean := createBinlogSuite(t)
	defer clean()

	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().BinlogClient = s.client
	tk.MustExec("begin")
	tk.MustExec("drop table if exists temp_table")
	ddlQuery := "create global temporary table temp_table(id int) on commit delete rows"
	tk.MustExec(ddlQuery)
	ok := mustGetDDLBinlog(s, ddlQuery, t)
	require.True(t, ok)

	tk.MustExec("insert temp_table value(1)")
	tk.MustExec("update temp_table set id=id+1")
	tk.MustExec("commit")
	prewriteVal := getLatestBinlogPrewriteValue(t, s.pump)
	require.Equal(t, 0, len(prewriteVal.Mutations))

	tk.MustExec("begin")
	tk.MustExec("delete from temp_table")
	tk.MustExec("commit")
	prewriteVal = getLatestBinlogPrewriteValue(t, s.pump)
	require.Equal(t, 0, len(prewriteVal.Mutations))

	ddlQuery = "truncate table temp_table"
	tk.MustExec(ddlQuery)
	ok = mustGetDDLBinlog(s, ddlQuery, t)
	require.True(t, ok)

	ddlQuery = "drop table if exists temp_table"
	tk.MustExec(ddlQuery)
	ok = mustGetDDLBinlog(s, ddlQuery, t)
	require.True(t, ok)

	// for local temporary table
	latestNonLocalTemporaryTableDDL := ddlQuery
	tk.MustExec("create temporary table l_temp_table(id int)")
	// create temporary table do not write to bin log, so the latest ddl binlog is the previous one
	ok = mustGetDDLBinlog(s, latestNonLocalTemporaryTableDDL, t)
	require.True(t, ok)

	tk.MustExec("insert l_temp_table value(1)")
	prewriteVal = getLatestBinlogPrewriteValue(t, s.pump)
	require.Equal(t, 0, len(prewriteVal.Mutations))

	tk.MustExec("update l_temp_table set id=id+1")
	prewriteVal = getLatestBinlogPrewriteValue(t, s.pump)
	require.Equal(t, 0, len(prewriteVal.Mutations))

	tk.MustExec("delete from l_temp_table")
	prewriteVal = getLatestBinlogPrewriteValue(t, s.pump)
	require.Equal(t, 0, len(prewriteVal.Mutations))

	tk.MustExec("begin")
	tk.MustExec("insert l_temp_table value(1)")
	tk.MustExec("update l_temp_table set id=id+1")
	tk.MustExec("commit")
	prewriteVal = getLatestBinlogPrewriteValue(t, s.pump)
	require.Equal(t, 0, len(prewriteVal.Mutations))

	tk.MustExec("begin")
	tk.MustExec("delete from l_temp_table")
	tk.MustExec("commit")
	prewriteVal = getLatestBinlogPrewriteValue(t, s.pump)
	require.Equal(t, 0, len(prewriteVal.Mutations))

	tk.MustExec("truncate table l_temp_table")
	ok = mustGetDDLBinlog(s, latestNonLocalTemporaryTableDDL, t)
	require.True(t, ok)

	tk.MustExec("drop table l_temp_table")
	ok = mustGetDDLBinlog(s, latestNonLocalTemporaryTableDDL, t)
	require.True(t, ok)
}

func TestAlterTableCache(t *testing.T) {
	s, clean := createBinlogSuite(t)
	defer clean()

	// Don't write binlog for 'ALTER TABLE t CACHE|NOCACHE'.
	// Cached table is regarded as normal table.

	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().BinlogClient = s.client
	tk.MustExec("drop table if exists t")
	ddlQuery := "create table t (id int)"
	tk.MustExec(ddlQuery)

	tk.MustExec(`alter table t cache`)
	// The latest DDL is still the previous one.
	getLatestDDLBinlog(t, s.pump, ddlQuery)

	tk.MustExec("insert into t values (?)", 666)
	prewriteVal := getLatestBinlogPrewriteValue(t, s.pump)
	require.Equal(t, 1, len(prewriteVal.Mutations))

	tk.MustExec(`alter table t nocache`)
	getLatestDDLBinlog(t, s.pump, ddlQuery)
}

func TestIssue28292(t *testing.T) {
	s, clean := createBinlogSuite(t)
	defer clean()

	tk := testkit.NewTestKit(t, s.store)
	tk.MustExec("use test")
	tk.Session().GetSessionVars().BinlogClient = s.client
	tk.MustExec("set @@tidb_txn_mode = 'pessimistic'")
	tk.MustExec(`CREATE TABLE xxx (
machine_id int(11) DEFAULT NULL,
date datetime DEFAULT NULL,
code int(11) DEFAULT NULL,
value decimal(20,3) DEFAULT NULL,
KEY stat_data_index1 (machine_id,date,code)
) PARTITION BY RANGE ( TO_DAYS(date) ) (
PARTITION p0 VALUES LESS THAN (TO_DAYS('2021-09-04')),
PARTITION p1 VALUES LESS THAN (TO_DAYS('2021-09-19')),
PARTITION p2 VALUES LESS THAN (TO_DAYS('2021-10-04')),
PARTITION p3 VALUES LESS THAN (TO_DAYS('2021-10-19')),
PARTITION p4 VALUES LESS THAN (TO_DAYS('2021-11-04')))`)

	tk.MustExec("INSERT INTO xxx value(123, '2021-09-22 00:00:00', 666, 123.24)")
	tk.MustExec("BEGIN")
	// No panic.
	tk.MustExec("DELETE FROM xxx WHERE machine_id = 123 and date = '2021-09-22 00:00:00'")
	tk.MustExec("COMMIT")
}
