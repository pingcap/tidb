// Copyright 2021 PingCAP, Inc.
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

//go:build !codes

package testkit

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/session"
	sessiontypes "github.com/pingcap/tidb/pkg/session/types"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit/testenv"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/pingcap/tidb/pkg/util/metricsutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	"go.uber.org/atomic"
	"google.golang.org/grpc"
)

var testKitIDGenerator atomic.Uint64

// TestKit is a utility to run sql test.
type TestKit struct {
	require *require.Assertions
	assert  *assert.Assertions
	t       testing.TB
	store   kv.Storage
	session sessiontypes.Session
	alloc   chunk.Allocator
}

// NewTestKit returns a new *TestKit.
func NewTestKit(t testing.TB, store kv.Storage) *TestKit {
	if _, ok := t.(*testing.B); !ok {
		// Don't check `intest.InTest` for benchmark. We should allow to run benchmarks without `intest` tag, because some assert may have significant performance
		// impact.
		require.True(t, intest.InTest, "you should add --tags=intest when to test, see https://pingcap.github.io/tidb-dev-guide/get-started/setup-an-ide.html for help")
	}
	testenv.SetGOMAXPROCSForTest()
	tk := &TestKit{
		require: require.New(t),
		assert:  assert.New(t),
		t:       t,
		store:   store,
		alloc:   chunk.NewAllocator(),
	}
	tk.RefreshSession()

	dom, _ := session.GetDomain(store)
	sm := dom.InfoSyncer().GetSessionManager()
	if sm != nil {
		mockSm, ok := sm.(*MockSessionManager)
		if ok {
			mockSm.mu.Lock()
			if mockSm.Conn == nil {
				mockSm.Conn = make(map[uint64]sessiontypes.Session)
			}
			mockSm.Conn[tk.session.GetSessionVars().ConnectionID] = tk.session
			mockSm.mu.Unlock()
		}
		tk.session.SetSessionManager(sm)
	}

	return tk
}

// NewTestKitWithSession returns a new *TestKit.
func NewTestKitWithSession(t testing.TB, store kv.Storage, se sessiontypes.Session) *TestKit {
	return &TestKit{
		require: require.New(t),
		assert:  assert.New(t),
		t:       t,
		store:   store,
		session: se,
		alloc:   chunk.NewAllocator(),
	}
}

// RefreshSession set a new session for the testkit
func (tk *TestKit) RefreshSession() {
	tk.session = NewSession(tk.t, tk.store)

	if intest.InTest {
		if rand.Intn(10) >= 3 { // 70% chance to run infoschema v2
			tk.MustExec("set @@global.tidb_schema_cache_size = 1024 * 1024 * 1024")
		}
	}

	// enforce sysvar cache loading, ref loadCommonGlobalVariableIfNeeded
	tk.MustExec("select 3")
}

// SetSession set the session of testkit
func (tk *TestKit) SetSession(session sessiontypes.Session) {
	tk.session = session
	// enforce sysvar cache loading, ref loadCommonGlobalVariableIfNeeded
	tk.MustExec("select 3")
}

// Session return the session associated with the testkit
func (tk *TestKit) Session() sessiontypes.Session {
	return tk.session
}

// MustExec executes a sql statement and asserts nil error.
func (tk *TestKit) MustExec(sql string, args ...any) {
	defer func() {
		if tk.alloc != nil {
			tk.alloc.Reset()
		}
	}()
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	tk.MustExecWithContext(ctx, sql, args...)
}

// MustExecWithContext executes a sql statement and asserts nil error.
func (tk *TestKit) MustExecWithContext(ctx context.Context, sql string, args ...any) {
	res, err := tk.ExecWithContext(ctx, sql, args...)
	comment := fmt.Sprintf("sql:%s, %v, error stack %v", sql, args, errors.ErrorStack(err))
	tk.require.NoError(err, comment)

	if res != nil {
		tk.require.NoError(res.Close())
	}
}

// MustQuery query the statements and returns result rows.
// If expected result is set it asserts the query result equals expected result.
func (tk *TestKit) MustQuery(sql string, args ...any) *Result {
	defer func() {
		if tk.alloc != nil {
			tk.alloc.Reset()
		}
	}()
	return tk.MustQueryWithContext(context.Background(), sql, args...)
}

// EventuallyMustQueryAndCheck query the statements and assert that
// result rows.lt will equal the expected results in waitFor time, periodically checking equality each tick.
// Note: retry can't ignore error of the statements. If statements returns error, it will break out.
func (tk *TestKit) EventuallyMustQueryAndCheck(sql string, args []any,
	expected [][]any, waitFor time.Duration, tick time.Duration) {
	defer func() {
		if tk.alloc != nil {
			tk.alloc.Reset()
		}
	}()
	tk.require.Eventually(func() bool {
		res := tk.MustQueryWithContext(context.Background(), sql, args...)
		return res.Equal(expected)
	}, waitFor, tick)
}

// MustQueryWithContext query the statements and returns result rows.
func (tk *TestKit) MustQueryWithContext(ctx context.Context, sql string, args ...any) *Result {
	comment := fmt.Sprintf("sql:%s, args:%v", sql, args)
	rs, err := tk.ExecWithContext(ctx, sql, args...)
	tk.require.NoError(err, comment)
	tk.require.NotNil(rs, comment)
	return tk.ResultSetToResultWithCtx(ctx, rs, comment)
}

// EventuallyMustIndexLookup checks whether the plan for the sql is IndexLookUp.
func (tk *TestKit) EventuallyMustIndexLookup(sql string, args ...any) *Result {
	require.Eventually(tk.t, func() bool {
		ok, _ := tk.hasPlan(sql, "IndexLookUp", args...)
		return ok
	}, 3*time.Second, 100*time.Millisecond)
	return tk.MustQuery(sql, args...)
}

// MustIndexLookup checks whether the plan for the sql is IndexLookUp.
func (tk *TestKit) MustIndexLookup(sql string, args ...any) *Result {
	tk.MustHavePlan(sql, "IndexLookUp", args...)
	return tk.MustQuery(sql, args...)
}

// MustPartition checks if the result execution plan must read specific partitions.
func (tk *TestKit) MustPartition(sql string, partitions string, args ...any) *Result {
	rs := tk.MustQuery("explain "+sql, args...)
	ok := len(partitions) == 0
	for i := range rs.rows {
		if len(partitions) == 0 && strings.Contains(rs.rows[i][3], "partition:") {
			ok = false
		}
		// The data format is "table: t1, partition: p0,p1,p2"
		if len(partitions) != 0 && strings.HasSuffix(rs.rows[i][3], "partition:"+partitions) {
			ok = true
		}
	}
	tk.require.True(ok)
	return tk.MustQuery(sql, args...)
}

// MustPartitionByList checks if the result execution plan must read specific partitions by list.
func (tk *TestKit) MustPartitionByList(sql string, partitions []string, args ...any) *Result {
	rs := tk.MustQuery("explain "+sql, args...)
	ok := len(partitions) == 0
	for i := range rs.rows {
		if ok {
			tk.require.NotContains(rs.rows[i][3], "partition:")
		}
		for index, partition := range partitions {
			if !ok && strings.Contains(rs.rows[i][3], "partition:"+partition) {
				partitions = append(partitions[:index], partitions[index+1:]...)
			}
		}
	}
	if !ok {
		tk.require.Len(partitions, 0)
	}
	return tk.MustQuery(sql, args...)
}

// QueryToErr executes a sql statement and discard results.
func (tk *TestKit) QueryToErr(sql string, args ...any) error {
	comment := fmt.Sprintf("sql:%s, args:%v", sql, args)
	res, err := tk.Exec(sql, args...)
	tk.require.NoError(err, comment)
	tk.require.NotNil(res, comment)
	_, resErr := session.GetRows4Test(context.Background(), tk.session, res)
	tk.require.NoError(res.Close())
	return resErr
}

// ResultSetToResult converts sqlexec.RecordSet to testkit.Result.
// It is used to check results of execute statement in binary mode.
func (tk *TestKit) ResultSetToResult(rs sqlexec.RecordSet, comment string) *Result {
	return tk.ResultSetToResultWithCtx(context.Background(), rs, comment)
}

// ResultSetToResultWithCtx converts sqlexec.RecordSet to testkit.Result.
func (tk *TestKit) ResultSetToResultWithCtx(ctx context.Context, rs sqlexec.RecordSet, comment string) *Result {
	rows, err := session.ResultSetToStringSlice(ctx, tk.session, rs)
	tk.require.NoError(err, comment)
	return &Result{rows: rows, comment: comment, assert: tk.assert, require: tk.require}
}

func (tk *TestKit) hasPlan(sql string, plan string, args ...any) (bool, *Result) {
	rs := tk.MustQuery("explain "+sql, args...)
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][0], plan) {
			return true, rs
		}
	}
	return false, rs
}

// MustHavePlan checks if the result execution plan contains specific plan.
func (tk *TestKit) MustHavePlan(sql string, plan string, args ...any) {
	has, rs := tk.hasPlan(sql, plan, args...)
	tk.require.True(has, fmt.Sprintf("%s doesn't have plan %s, full plan %v", sql, plan, rs.Rows()))
}

// MustNotHavePlan checks if the result execution plan contains specific plan.
func (tk *TestKit) MustNotHavePlan(sql string, plan string, args ...any) {
	has, rs := tk.hasPlan(sql, plan, args...)
	tk.require.False(has, fmt.Sprintf("%s shouldn't have plan %s, full plan %v", sql, plan, rs.Rows()))
}

// HasTiFlashPlan checks if the result execution plan contains TiFlash plan.
func (tk *TestKit) HasTiFlashPlan(sql string, args ...any) bool {
	rs := tk.MustQuery("explain "+sql, args...)
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][2], "tiflash") {
			return true
		}
	}
	return false
}

// HasPlanForLastExecution checks if the execution plan of the last execution contains specific plan.
func (tk *TestKit) HasPlanForLastExecution(plan string) bool {
	connID := tk.session.GetSessionVars().ConnectionID
	rs := tk.MustQuery(fmt.Sprintf("explain for connection %d", connID))
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][0], plan) {
			return true
		}
	}
	return false
}

// HasKeywordInOperatorInfo checks if the result execution plan contains specific keyword in the operator info.
func (tk *TestKit) HasKeywordInOperatorInfo(sql string, keyword string, args ...any) bool {
	rs := tk.MustQuery("explain "+sql, args...)
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][4], keyword) {
			return true
		}
	}
	return false
}

// NotHasKeywordInOperatorInfo checks if the result execution plan doesn't contain specific keyword in the operator info.
func (tk *TestKit) NotHasKeywordInOperatorInfo(sql string, keyword string, args ...any) bool {
	rs := tk.MustQuery("explain "+sql, args...)
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][4], keyword) {
			return false
		}
	}
	return true
}

// HasPlan4ExplainFor checks if the result execution plan contains specific plan.
func (tk *TestKit) HasPlan4ExplainFor(result *Result, plan string) bool {
	for i := range result.rows {
		if strings.Contains(result.rows[i][0], plan) {
			return true
		}
	}
	return false
}

// Exec executes a sql statement using the prepared stmt API
func (tk *TestKit) Exec(sql string, args ...any) (sqlexec.RecordSet, error) {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnOthers)
	return tk.ExecWithContext(ctx, sql, args...)
}

// ExecWithContext executes a sql statement using the prepared stmt API
func (tk *TestKit) ExecWithContext(ctx context.Context, sql string, args ...any) (rs sqlexec.RecordSet, err error) {
	defer tk.Session().GetSessionVars().ClearAlloc(&tk.alloc, err != nil)
	if len(args) == 0 {
		sc := tk.session.GetSessionVars().StmtCtx
		prevWarns := sc.GetWarnings()
		var stmts []ast.StmtNode
		if len(stmts) == 0 {
			var err error
			stmts, err = tk.session.Parse(ctx, sql)
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		warns := sc.GetWarnings()
		parserWarns := warns[len(prevWarns):]
		tk.Session().GetSessionVars().SetAlloc(tk.alloc)
		var rs0 sqlexec.RecordSet
		for i, stmt := range stmts {
			var rs sqlexec.RecordSet
			var err error
			if s, ok := stmt.(*ast.NonTransactionalDMLStmt); ok {
				rs, err = session.HandleNonTransactionalDML(ctx, s, tk.Session())
			} else {
				rs, err = tk.Session().ExecuteStmt(ctx, stmt)
			}
			if i == 0 {
				rs0 = rs
				if len(stmts) > 1 && rs != nil {
					// The result of the first statement will be drained and closed later. To avoid leaking
					// resource on the statement context, we'll need to store the result and close the
					// resultSet before executing other statements.
					rs0 = buildRowsRecordSet(ctx, rs)
					// the `rs` can be closed safely now
					terror.Call(rs.Close)
				}
			} else if rs != nil {
				// other statements are executed, but the `ResultSet` is not returned, so close them here
				terror.Call(rs.Close)
			}
			if err != nil {
				tk.session.GetSessionVars().StmtCtx.AppendError(err)
				return rs, errors.Trace(err)
			}
		}
		if len(parserWarns) > 0 {
			tk.session.GetSessionVars().StmtCtx.AppendWarnings(parserWarns)
		}
		return rs0, nil
	}

	stmtID, _, _, err := tk.session.PrepareStmt(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	params := expression.Args2Expressions4Test(args...)
	tk.Session().GetSessionVars().SetAlloc(tk.alloc)
	rs, err = tk.session.ExecutePreparedStmt(ctx, stmtID, params)
	if err != nil {
		return rs, errors.Trace(err)
	}
	err = tk.session.DropPreparedStmt(stmtID)
	if err != nil {
		return rs, errors.Trace(err)
	}
	return rs, nil
}

// ExecToErr executes a sql statement and discard results.
func (tk *TestKit) ExecToErr(sql string, args ...any) error {
	res, err := tk.Exec(sql, args...)
	if res != nil {
		tk.require.NoError(res.Close())
	}
	return err
}

// MustExecToErr executes a sql statement and must return Error.
func (tk *TestKit) MustExecToErr(sql string, args ...any) {
	res, err := tk.Exec(sql, args...)
	if res != nil {
		tk.require.NoError(res.Close())
	}
	tk.require.Error(err)
}

// NewSession creates a new session environment for test.
func NewSession(t testing.TB, store kv.Storage) sessiontypes.Session {
	se, err := session.CreateSession4Test(store)
	require.NoError(t, err)
	se.SetConnectionID(testKitIDGenerator.Inc())
	return se
}

// RefreshConnectionID refresh the connection ID for session of the testkit
func (tk *TestKit) RefreshConnectionID() {
	if tk.session != nil {
		tk.session.SetConnectionID(testKitIDGenerator.Inc())
	}
}

// MustGetErrCode executes a sql statement and assert it's error code.
func (tk *TestKit) MustGetErrCode(sql string, errCode int) {
	_, err := tk.Exec(sql)
	tk.require.Errorf(err, "sql: %s", sql)
	originErr := errors.Cause(err)
	tErr, ok := originErr.(*terror.Error)
	tk.require.Truef(ok, "sql: %s, expect type 'terror.Error', but obtain '%T': %v", sql, originErr, originErr)
	sqlErr := terror.ToSQLError(tErr)
	tk.require.Equalf(errCode, int(sqlErr.Code), "sql: %s, Assertion failed, origin err:\n  %v", sql, sqlErr)
}

// MustGetErrMsg executes a sql statement and assert its error message.
func (tk *TestKit) MustGetErrMsg(sql string, errStr string) {
	err := tk.ExecToErr(sql)
	tk.require.EqualError(err, errStr)
}

// MustGetDBError executes a sql statement and assert its terror.
func (tk *TestKit) MustGetDBError(sql string, dberr *terror.Error) {
	err := tk.ExecToErr(sql)
	tk.require.Truef(terror.ErrorEqual(err, dberr), "err %v", err)
}

// MustContainErrMsg executes a sql statement and assert its error message containing errStr.
func (tk *TestKit) MustContainErrMsg(sql string, errStr any) {
	err := tk.ExecToErr(sql)
	tk.require.Error(err)
	tk.require.Contains(err.Error(), errStr)
}

// MustMatchErrMsg executes a sql statement and assert its error message matching errRx.
func (tk *TestKit) MustMatchErrMsg(sql string, errRx any) {
	err := tk.ExecToErr(sql)
	tk.require.Error(err)
	tk.require.Regexp(errRx, err.Error())
}

// MustUseIndex checks if the result execution plan contains specific index(es).
func (tk *TestKit) MustUseIndex(sql string, index string, args ...any) bool {
	rs := tk.MustQuery("explain "+sql, args...)
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][3], "index:"+index) {
			return true
		}
	}
	return false
}

// MustUseIndex4ExplainFor checks if the result execution plan contains specific index(es).
func (tk *TestKit) MustUseIndex4ExplainFor(result *Result, index string) bool {
	for i := range result.rows {
		// It depends on whether we enable to collect the execution info.
		if strings.Contains(result.rows[i][3], "index:"+index) {
			return true
		}
		if strings.Contains(result.rows[i][4], "index:"+index) {
			return true
		}
	}
	return false
}

// CheckExecResult checks the affected rows and the insert id after executing MustExec.
func (tk *TestKit) CheckExecResult(affectedRows, insertID int64) {
	tk.require.Equal(int64(tk.Session().AffectedRows()), affectedRows)
	tk.require.Equal(int64(tk.Session().LastInsertID()), insertID)
}

// MustPointGet checks whether the plan for the sql is Point_Get.
func (tk *TestKit) MustPointGet(sql string, args ...any) *Result {
	rs := tk.MustQuery("explain "+sql, args...)
	tk.require.Len(rs.rows, 1)
	tk.require.Contains(rs.rows[0][0], "Point_Get", "plan %v", rs.rows[0][0])
	return tk.MustQuery(sql, args...)
}

// UsedPartitions returns the partition names that will be used or all/dual.
func (tk *TestKit) UsedPartitions(sql string, args ...any) *Result {
	rs := tk.MustQuery("explain "+sql, args...)
	var usedPartitions [][]string
	for i := range rs.rows {
		index := strings.Index(rs.rows[i][3], "partition:")
		if index != -1 {
			p := rs.rows[i][3][index+len("partition:"):]
			partitions := strings.Split(strings.SplitN(p, " ", 2)[0], ",")
			usedPartitions = append(usedPartitions, partitions)
		}
	}
	comment := fmt.Sprintf("sql:%s, args:%v", sql, args)
	return &Result{rows: usedPartitions, comment: comment, assert: tk.assert, require: tk.require}
}

// WithPruneMode run test case under prune mode.
func WithPruneMode(tk *TestKit, mode variable.PartitionPruneMode, f func()) {
	tk.MustExec("set @@tidb_partition_prune_mode=`" + string(mode) + "`")
	tk.MustExec("set global tidb_partition_prune_mode=`" + string(mode) + "`")
	f()
}

func containGlobal(rs *Result) bool {
	partitionNameCol := 2
	for i := range rs.rows {
		if strings.Contains(rs.rows[i][partitionNameCol], "global") {
			return true
		}
	}
	return false
}

// MustNoGlobalStats checks if there is no global stats.
func (tk *TestKit) MustNoGlobalStats(table string) bool {
	if containGlobal(tk.MustQuery("show stats_meta where table_name like '" + table + "'")) {
		return false
	}
	if containGlobal(tk.MustQuery("show stats_buckets where table_name like '" + table + "'")) {
		return false
	}
	if containGlobal(tk.MustQuery("show stats_histograms where table_name like '" + table + "'")) {
		return false
	}
	return true
}

// CheckLastMessage checks last message after executing MustExec
func (tk *TestKit) CheckLastMessage(msg string) {
	tk.require.Equal(tk.Session().LastMessage(), msg)
}

// RequireEqual checks if actual is equal to the expected
func (tk *TestKit) RequireEqual(expected any, actual any, msgAndArgs ...any) {
	tk.require.Equal(expected, actual, msgAndArgs...)
}

// RequireNotEqual checks if actual is not equal to the expected
func (tk *TestKit) RequireNotEqual(expected any, actual any, msgAndArgs ...any) {
	tk.require.NotEqual(expected, actual, msgAndArgs...)
}

// RequireNoError checks if error happens
func (tk *TestKit) RequireNoError(err error, msgAndArgs ...any) {
	tk.require.NoError(err, msgAndArgs)
}

// RegionProperityClient is to get region properties.
type RegionProperityClient struct {
	tikv.Client
	mu struct {
		sync.Mutex
		failedOnce bool
		count      int64
	}
}

// SendRequest is to mock send request.
func (c *RegionProperityClient) SendRequest(ctx context.Context, addr string, req *tikvrpc.Request, timeout time.Duration) (*tikvrpc.Response, error) {
	if req.Type == tikvrpc.CmdDebugGetRegionProperties {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.mu.count++
		// Mock failure once.
		if !c.mu.failedOnce {
			c.mu.failedOnce = true
			return &tikvrpc.Response{}, nil
		}
	}
	return c.Client.SendRequest(ctx, addr, req, timeout)
}

// MockPumpClient is a mock pump client.
type MockPumpClient struct{}

// WriteBinlog is a mock method.
func (m MockPumpClient) WriteBinlog(ctx context.Context, in *binlog.WriteBinlogReq, opts ...grpc.CallOption) (*binlog.WriteBinlogResp, error) {
	return &binlog.WriteBinlogResp{}, nil
}

// PullBinlogs is a mock method.
func (m MockPumpClient) PullBinlogs(ctx context.Context, in *binlog.PullBinlogReq, opts ...grpc.CallOption) (binlog.Pump_PullBinlogsClient, error) {
	return nil, nil
}

var _ sqlexec.RecordSet = &rowsRecordSet{}

type rowsRecordSet struct {
	fields []*ast.ResultField
	rows   []chunk.Row

	idx int

	// this error is stored here to return in the future
	err error
}

func (r *rowsRecordSet) Fields() []*ast.ResultField {
	return r.fields
}

func (r *rowsRecordSet) Next(ctx context.Context, req *chunk.Chunk) error {
	if r.err != nil {
		return r.err
	}

	req.Reset()
	for r.idx < len(r.rows) {
		if req.IsFull() {
			return nil
		}
		req.AppendRow(r.rows[r.idx])
		r.idx++
	}
	return nil
}

func (r *rowsRecordSet) NewChunk(alloc chunk.Allocator) *chunk.Chunk {
	fields := make([]*types.FieldType, 0, len(r.fields))
	for _, field := range r.fields {
		fields = append(fields, &field.Column.FieldType)
	}
	if alloc != nil {
		return alloc.Alloc(fields, 0, 1024)
	}
	return chunk.New(fields, 1024, 1024)
}

func (r *rowsRecordSet) Close() error {
	// do nothing
	return nil
}

// buildRowsRecordSet builds a `rowsRecordSet` from any `RecordSet` by draining all rows from it.
// It's used to store the result temporarily. After building a new RecordSet, the original rs (in the argument) can be
// closed safely.
func buildRowsRecordSet(ctx context.Context, rs sqlexec.RecordSet) sqlexec.RecordSet {
	rows, err := session.GetRows4Test(ctx, nil, rs)
	if err != nil {
		return &rowsRecordSet{
			fields: rs.Fields(),
			err:    err,
		}
	}

	return &rowsRecordSet{
		fields: rs.Fields(),
		rows:   rows,
		idx:    0,
	}
}

// MockTiDBStatusPort mock the TiDB server status port to have metrics.
func MockTiDBStatusPort(ctx context.Context, b *testing.B, port string) *util.WaitGroupWrapper {
	var wg util.WaitGroupWrapper
	err := metricsutil.RegisterMetrics()
	terror.MustNil(err)
	router := mux.NewRouter()
	router.Handle("/metrics", promhttp.Handler())
	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)
	serverMux.HandleFunc("/debug/pprof/", pprof.Index)
	serverMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	serverMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	serverMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	serverMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	statusListener, err := net.Listen("tcp", "0.0.0.0:"+port)
	require.NoError(b, err)
	statusServer := &http.Server{Handler: serverMux}
	wg.RunWithLog(func() {
		if err := statusServer.Serve(statusListener); err != nil {
			b.Logf("status server serve failed: %v", err)
		}
	})
	wg.RunWithLog(func() {
		<-ctx.Done()
		_ = statusServer.Close()
	})

	return &wg
}
