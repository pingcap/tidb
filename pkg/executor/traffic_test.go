// Copyright 2024 PingCAP, Inc.
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

package executor

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/planner/util/coretestsdk"
	"github.com/pingcap/tidb/pkg/privilege"
	"github.com/pingcap/tidb/pkg/sessionctx/stmtctx"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
	tmock "github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestTrafficForm(t *testing.T) {
	tests := []struct {
		sql          string
		method       string
		path         string
		form         url.Values
		hasStartTime bool
	}{
		{
			sql:    "traffic capture to '/tmp' duration='1s' encryption_method='aes' compress=false",
			method: http.MethodPost,
			path:   capturePath,
			form: url.Values{
				"output":         []string{"/tmp"},
				"duration":       []string{"1s"},
				"encrypt-method": []string{"aes"},
				"compress":       []string{"false"},
			},
			hasStartTime: true,
		},
		{
			sql:    "traffic capture to '/tmp' duration='1s'",
			method: http.MethodPost,
			path:   capturePath,
			form: url.Values{
				"output":   []string{"/tmp"},
				"duration": []string{"1s"},
			},
			hasStartTime: true,
		},
		{
			sql:    "traffic replay from '/tmp' user='root' password='123456' speed=1.0 read_only=true",
			method: http.MethodPost,
			path:   replayPath,
			form: url.Values{
				"input":    []string{"/tmp"},
				"username": []string{"root"},
				"password": []string{"123456"},
				"speed":    []string{"1.0"},
				"readonly": []string{"true"},
			},
			hasStartTime: true,
		},
		{
			sql:    "traffic replay from '/tmp' user='root'",
			method: http.MethodPost,
			path:   replayPath,
			form: url.Values{
				"input":    []string{"/tmp"},
				"username": []string{"root"},
			},
			hasStartTime: true,
		},
		{
			sql:    "cancel traffic jobs",
			method: http.MethodPost,
			path:   cancelPath,
			form:   url.Values{},
		},
		{
			sql:    "show traffic jobs",
			method: http.MethodGet,
			path:   showPath,
			form:   url.Values{},
		},
	}

	suite := newTrafficTestSuite(t, 10)
	ctx := context.TODO()
	httpHandler := &mockHTTPHandler{t: t, httpOK: true}
	server, port := runServer(t, httpHandler)
	defer server.Close()
	ctx = fillCtxWithTiProxyAddr(ctx, []int{port})
	for i, test := range tests {
		executor := suite.build(ctx, test.sql)
		require.NoError(t, executor.Open(ctx))
		chk := exec.NewFirstChunk(executor)
		require.NoError(t, executor.Next(ctx, chk), "case %d", i)
		require.Equal(t, test.method, httpHandler.getMethod(), "case %d", i)
		require.Equal(t, test.path, httpHandler.getPath(), "case %d", i)
		actualForm := httpHandler.getForm()
		if test.hasStartTime {
			require.NotEmpty(t, actualForm.Get("start-time"), "case %d", i)
			test.form.Add("start-time", actualForm.Get("start-time"))
		}
		require.Equal(t, test.form, actualForm, "case %d", i)
		require.EqualValues(t, 0, suite.stmtCtx().WarningCount(), "case %d", i)
	}
}

func TestTrafficError(t *testing.T) {
	suite := newTrafficTestSuite(t, 10)
	ctx := context.TODO()
	exec := suite.build(ctx, "traffic capture to 'test://tmp  ?' duration='1s'")

	// no tiproxy
	m := make(map[string]*infosync.TiProxyServerInfo)
	tempCtx := context.WithValue(ctx, tiproxyAddrKey, m)
	require.ErrorContains(t, exec.Next(tempCtx, nil), "no tiproxy server found")

	// invalid file path
	m["127.0.0.1:0"] = &infosync.TiProxyServerInfo{IP: "127.0.0.1", StatusPort: "0"}
	require.ErrorContains(t, exec.Next(tempCtx, nil), "parse output path failed")

	// can't connect to s3
	replayCtx, cancel := context.WithCancel(tempCtx)
	cancel()
	exec = suite.build(replayCtx, "traffic replay from 's3://bucket/tmp' user='root' password='123456'")
	require.ErrorContains(t, exec.Next(replayCtx, nil), "context canceled")

	// tiproxy no response
	exec = suite.build(tempCtx, "traffic capture to '/tmp' duration='1s'")
	require.ErrorContains(t, exec.Next(tempCtx, nil), "dial tcp")

	// tiproxy responds with error
	httpHandler := &mockHTTPHandler{t: t, httpOK: false, resp: "mock error"}
	server, port := runServer(t, httpHandler)
	defer server.Close()
	tempCtx = fillCtxWithTiProxyAddr(ctx, []int{port})
	err := exec.Next(tempCtx, nil)
	require.ErrorContains(t, errors.Cause(err), "mock error")
}

func TestCapturePath(t *testing.T) {
	tiproxyNum := 3
	handlers := make([]*mockHTTPHandler, 0, tiproxyNum)
	servers := make([]*http.Server, 0, tiproxyNum)
	ports := make([]int, 0, tiproxyNum)
	for range tiproxyNum {
		httpHandler := &mockHTTPHandler{t: t, httpOK: true}
		handlers = append(handlers, httpHandler)
		server, port := runServer(t, httpHandler)
		servers = append(servers, server)
		ports = append(ports, port)
	}
	defer func() {
		for _, server := range servers {
			server.Close()
		}
	}()

	ctx := context.TODO()
	tempCtx := fillCtxWithTiProxyAddr(ctx, ports)
	suite := newTrafficTestSuite(t, 10)
	prefix, suffix := "s3://bucket/tmp", "access-key=minioadmin&secret-access-key=minioadmin&endpoint=http://minio:8000&force-path-style=true"
	exec := suite.build(ctx, fmt.Sprintf("traffic capture to '%s?%s' duration='1s'", prefix, suffix))
	require.NoError(t, exec.Next(tempCtx, nil))

	paths := make([]string, 0, tiproxyNum)
	expectedPaths := make([]string, 0, tiproxyNum)
	for i := range tiproxyNum {
		httpHandler := handlers[i]
		output := httpHandler.getForm().Get("output")
		require.True(t, strings.HasPrefix(output, prefix), output)
		require.True(t, strings.HasSuffix(output, suffix), output)
		paths = append(paths, output[len(prefix)+1:len(output)-len(suffix)-1])
		expectedPaths = append(expectedPaths, fmt.Sprintf("tiproxy-%d", i))
	}
	sort.Strings(paths)
	require.Equal(t, expectedPaths, paths)
}

func TestReplayPath(t *testing.T) {
	tiproxyNum := 2
	handlers := make([]*mockHTTPHandler, 0, tiproxyNum)
	servers := make([]*http.Server, 0, tiproxyNum)
	ports := make([]int, 0, tiproxyNum)
	for range tiproxyNum {
		httpHandler := &mockHTTPHandler{t: t, httpOK: true}
		handlers = append(handlers, httpHandler)
		server, port := runServer(t, httpHandler)
		servers = append(servers, server)
		ports = append(ports, port)
	}
	defer func() {
		for _, server := range servers {
			server.Close()
		}
	}()

	tests := []struct {
		paths     []string
		formPaths []string
		warn      string
		err       string
	}{
		{
			paths:     []string{},
			err:       "no replay files found",
			formPaths: []string{},
		},
		{
			paths:     []string{"tiproxy-0/meta", "tiproxy-0/traffic-1.log", "tiproxy-0/traffic-2.log"},
			formPaths: []string{"tiproxy-0"},
			warn:      "tiproxy instances number (2) is greater than input paths number (1)",
		},
		{
			paths:     []string{"tiproxy-0/meta", "tiproxy-1/meta", "tiproxy-2"},
			formPaths: []string{"tiproxy-0", "tiproxy-1"},
		},
		{
			paths:     []string{"tiproxy-0/meta", "tiproxy-0/traffic-1.log", "tiproxy-1/meta", "tiproxy-1/traffic-1.log"},
			formPaths: []string{"tiproxy-0", "tiproxy-1"},
		},
		{
			paths:     []string{"tiproxy-0/meta", "tiproxy-1/meta", "tiproxy-2/meta"},
			formPaths: []string{},
			err:       "tiproxy instances number (2) is less than input paths number (3)",
		},
	}
	ctx := context.TODO()
	store := &mockExternalStorage{}
	ctx = fillCtxWithTiProxyAddr(ctx, ports)
	ctx = context.WithValue(ctx, trafficStoreKey, store)
	prefix, suffix := "s3://bucket/tmp", "access-key=minioadmin&secret-access-key=minioadmin&endpoint=http://minio:8000&force-path-style=true"
	for i, test := range tests {
		store.paths = test.paths
		suite := newTrafficTestSuite(t, 10)
		exec := suite.build(ctx, fmt.Sprintf("traffic replay from '%s?%s' user='root'", prefix, suffix))
		for j := range tiproxyNum {
			handlers[j].reset()
		}
		err := exec.Next(ctx, nil)
		if test.err != "" {
			require.ErrorContains(t, err, test.err, "case %d", i)
		} else {
			require.NoError(t, err, "case %d", i)
			warnings := suite.stmtCtx().GetWarnings()
			if test.warn != "" {
				require.Len(t, warnings, 1, "case %d", i)
				require.ErrorContains(t, warnings[0].Err, test.warn, "case %d", i)
			} else {
				require.Len(t, warnings, 0, "case %d", i)
			}
		}

		formPaths := make([]string, 0, len(test.formPaths))
		for j := range tiproxyNum {
			httpHandler := handlers[j]
			if httpHandler.getMethod() != "" {
				form := httpHandler.getForm()
				require.NotEmpty(t, form, "case %d", i)
				input := form.Get("input")
				require.True(t, strings.HasPrefix(input, prefix), input)
				require.True(t, strings.HasSuffix(input, suffix), input)
				formPaths = append(formPaths, input[len(prefix)+1:len(input)-len(suffix)-1])
			}
		}
		sort.Strings(formPaths)
		require.Equal(t, test.formPaths, formPaths, "case %d", i, "case %d", i)
	}
}

func TestTrafficShow(t *testing.T) {
	suite := newTrafficTestSuite(t, 2)
	ctx := context.TODO()
	fields := trafficJobFields()

	handlers := make([]*mockHTTPHandler, 0, 2)
	servers := make([]*http.Server, 0, 2)
	ports := make([]int, 0, 2)
	for range 2 {
		httpHandler := &mockHTTPHandler{t: t, httpOK: true}
		handlers = append(handlers, httpHandler)
		server, port := runServer(t, httpHandler)
		servers = append(servers, server)
		ports = append(ports, port)
	}
	defer func() {
		for _, server := range servers {
			server.Close()
		}
	}()
	if strconv.Itoa(ports[0]) > strconv.Itoa(ports[1]) {
		ports[0], ports[1] = ports[1], ports[0]
		handlers[0], handlers[1] = handlers[1], handlers[0]
	}
	ctx = fillCtxWithTiProxyAddr(ctx, ports)

	marshaledTime1, marshaledTime2 := "2020-01-01T00:00:00Z", "2020-01-01T01:00:00Z"
	marshaledCaptureJob := `{
		"type": "capture",
		"status": "canceled",
		"start_time": "%s",
		"end_time": "2020-01-01T02:01:01Z",
		"progress": "50%%",
		"error": "mock error",
		"output": "/tmp/traffic",
		"duration": "1m",
		"compress": true,
		"encryption_method": ""
	}`
	marshaledReplayJob := `{
		"type": "replay",
		"status": "running",
		"start_time": "%s",
		"progress": "50%%",
		"input": "s3://bucket/tmp&access-key=xxx&secret-access-key=xxx",
		"username": "root",
		"speed": 1,
		"read_only": true
	}`
	showTime1, showTime2 := "2020-01-01 00:00:00.000000", "2020-01-01 01:00:00.000000"
	showCaptureResult := "%s, 2020-01-01 02:01:01.000000, 127.0.0.1:%d, capture, 50%%, canceled, mock error, OUTPUT=\"/tmp/traffic\", DURATION=\"1m\", COMPRESS=true, ENCRYPTION_METHOD=\"\"\n"
	showReplayResult := "%s, NULL, 127.0.0.1:%d, replay, 50%%, running, , INPUT=\"s3://bucket/tmp&access-key=xxx&secret-access-key=xxx\", USER=\"root\", SPEED=1.000000, READ_ONLY=false\n"
	tests := []struct {
		resp []string
		chks []string
	}{
		{
			resp: []string{"[]", "[]"},
			chks: []string{},
		},
		{
			resp: []string{fmt.Sprintf("[%s]", fmt.Sprintf(marshaledCaptureJob, marshaledTime1)), "[]"},
			chks: []string{fmt.Sprintf(showCaptureResult, showTime1, ports[0])},
		},
		{
			resp: []string{fmt.Sprintf("[%s]", fmt.Sprintf(marshaledReplayJob, marshaledTime1)), "[]"},
			chks: []string{fmt.Sprintf(showReplayResult, showTime1, ports[0])},
		},
		{
			resp: []string{fmt.Sprintf("[%s]", fmt.Sprintf(marshaledCaptureJob, marshaledTime1)), fmt.Sprintf("[%s]", fmt.Sprintf(marshaledCaptureJob, marshaledTime1))},
			chks: []string{fmt.Sprintf("%s%s", fmt.Sprintf(showCaptureResult, showTime1, ports[0]), fmt.Sprintf(showCaptureResult, showTime1, ports[1]))},
		},
		{
			resp: []string{fmt.Sprintf("[%s,%s]", fmt.Sprintf(marshaledCaptureJob, marshaledTime1), fmt.Sprintf(marshaledReplayJob, marshaledTime2)),
				fmt.Sprintf("[%s,%s]", fmt.Sprintf(marshaledCaptureJob, marshaledTime1), fmt.Sprintf(marshaledReplayJob, marshaledTime2))},
			chks: []string{fmt.Sprintf("%s%s", fmt.Sprintf(showReplayResult, showTime2, ports[0]), fmt.Sprintf(showReplayResult, showTime2, ports[1])),
				fmt.Sprintf("%s%s", fmt.Sprintf(showCaptureResult, showTime1, ports[0]), fmt.Sprintf(showCaptureResult, showTime1, ports[1]))},
		},
	}

	for i, test := range tests {
		for j := range test.resp {
			handlers[j].setResponse(test.resp[j])
		}
		executor := suite.build(ctx, "show traffic jobs")
		require.NoError(t, executor.Open(ctx), "case %d", i)
		chk := chunk.New(fields, 2, 2)
		for j := range test.chks {
			require.NoError(t, executor.Next(ctx, chk), "case %d, %d", i, j)
			require.Equal(t, test.chks[j], chk.ToString(fields), "case %d, %d", i, j)
		}
		require.NoError(t, executor.Next(ctx, chk), "case %d", i)
		require.Equal(t, 0, chk.NumRows(), "case %d", i)
	}
}

func TestTrafficPrivilege(t *testing.T) {
	suite := newTrafficTestSuite(t, 10)
	ctx := context.TODO()
	httpHandler := &mockHTTPHandler{t: t, httpOK: true}
	server, port := runServer(t, httpHandler)
	defer server.Close()
	ctx = fillCtxWithTiProxyAddr(ctx, []int{port})
	mgr := &mockPrivManager{}
	privilege.BindPrivilegeManager(suite.execBuilder.ctx, mgr)

	cancelTests := []struct {
		privs []bool
		form  url.Values
	}{
		{
			privs: []bool{true, false},
			form:  url.Values{"type": []string{"capture"}},
		},
		{
			privs: []bool{false, true},
			form:  url.Values{"type": []string{"replay"}},
		},
		{
			privs: []bool{true, true},
			form:  url.Values{},
		},
	}
	for _, test := range cancelTests {
		httpHandler.reset()
		mgr.On("RequestDynamicVerification", []*auth.RoleIdentity{}, "TRAFFIC_CAPTURE_ADMIN", false).Return(test.privs[0]).Once()
		mgr.On("RequestDynamicVerification", []*auth.RoleIdentity{}, "TRAFFIC_REPLAY_ADMIN", false).Return(test.privs[1]).Once()
		exec := suite.build(ctx, "cancel traffic jobs")
		require.NoError(t, exec.Next(ctx, nil))
		require.Equal(t, test.form, httpHandler.getForm(), "privs %v", test.privs)
	}

	showTests := []struct {
		privs []bool
		types []string
	}{
		{
			privs: []bool{true, false},
			types: []string{"capture"},
		},
		{
			privs: []bool{false, true},
			types: []string{"replay"},
		},
		{
			privs: []bool{true, true},
			types: []string{"capture", "replay"},
		},
	}
	marshaledJob := `[{
		"start_time": "2020-01-01T02:01:01Z",
		"type": "capture"
	},{
		"start_time": "2020-01-01T02:01:01Z",
		"type": "replay"
	}]`
	httpHandler.setResponse(marshaledJob)
	fields := trafficJobFields()
	for _, test := range showTests {
		mgr.On("RequestDynamicVerification", []*auth.RoleIdentity{}, "TRAFFIC_CAPTURE_ADMIN", false).Return(test.privs[0]).Once()
		mgr.On("RequestDynamicVerification", []*auth.RoleIdentity{}, "TRAFFIC_REPLAY_ADMIN", false).Return(test.privs[1]).Once()
		exec := suite.build(ctx, "show traffic jobs")
		require.NoError(t, exec.Open(ctx))
		chk := chunk.New(fields, 2, 2)
		jobs := make([]string, 0, 2)
		require.NoError(t, exec.Next(ctx, chk))
		for j := range chk.NumRows() {
			jobs = append(jobs, chk.Column(3).GetString(j))
		}
		sort.Strings(jobs)
		require.Equal(t, test.types, jobs)
	}
}

type trafficTestSuite struct {
	t           *testing.T
	parser      *parser.Parser
	planBuilder *plannercore.PlanBuilder
	execBuilder *MockExecutorBuilder
}

func newTrafficTestSuite(t *testing.T, chunkSize int) *trafficTestSuite {
	parser := parser.New()
	sctx := mock.NewContext()
	sctx.GetSessionVars().MaxChunkSize = chunkSize
	is := infoschema.MockInfoSchema([]*model.TableInfo{coretestsdk.MockSignedTable(), coretestsdk.MockUnsignedTable()})
	planBuilder, _ := plannercore.NewPlanBuilder().Init(sctx, nil, hint.NewQBHintHandler(nil))
	execBuilder := NewMockExecutorBuilderForTest(sctx, is, nil)
	return &trafficTestSuite{
		t:           t,
		parser:      parser,
		planBuilder: planBuilder,
		execBuilder: execBuilder,
	}
}

func (suite *trafficTestSuite) build(ctx context.Context, sql string) exec.Executor {
	stmt, err := suite.parser.ParseOneStmt(sql, "", "")
	require.NoError(suite.t, err)
	p, err := suite.planBuilder.Build(ctx, resolve.NewNodeW(stmt))
	require.NoError(suite.t, err)
	executor := suite.execBuilder.build(p)
	require.NotEmpty(suite.t, executor)
	return executor
}

func (suite *trafficTestSuite) stmtCtx() *stmtctx.StatementContext {
	return suite.execBuilder.ctx.GetSessionVars().StmtCtx
}

type mockHTTPHandler struct {
	t *testing.T
	sync.Mutex
	form   url.Values
	method string
	path   string
	resp   string
	httpOK bool
}

func (handler *mockHTTPHandler) setResponse(resp string) {
	handler.Lock()
	defer handler.Unlock()
	handler.resp = resp
}

func (handler *mockHTTPHandler) getForm() url.Values {
	handler.Lock()
	defer handler.Unlock()
	return handler.form
}

func (handler *mockHTTPHandler) getMethod() string {
	handler.Lock()
	defer handler.Unlock()
	return handler.method
}

func (handler *mockHTTPHandler) getPath() string {
	handler.Lock()
	defer handler.Unlock()
	return handler.path
}

func (handler *mockHTTPHandler) reset() {
	handler.Lock()
	defer handler.Unlock()
	handler.form = nil
	handler.method = ""
	handler.path = ""
}

func (handler *mockHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler.Lock()
	defer handler.Unlock()
	handler.method = r.Method
	handler.path = r.URL.Path
	require.NoError(handler.t, r.ParseForm())
	handler.form = r.PostForm
	if handler.httpOK {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	resp := handler.resp
	if len(resp) == 0 && r.Method == http.MethodGet {
		resp = "[]"
	}
	_, err := w.Write([]byte(resp))
	require.NoError(handler.t, err)
}

func runServer(t *testing.T, handler http.Handler) (*http.Server, int) {
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	server := &http.Server{Addr: listener.Addr().String(), Handler: handler}
	go server.Serve(listener)
	return server, listener.Addr().(*net.TCPAddr).Port
}

func fillCtxWithTiProxyAddr(ctx context.Context, ports []int) context.Context {
	m := make(map[string]*infosync.TiProxyServerInfo)
	for _, port := range ports {
		addr := fmt.Sprintf("127.0.0.1:%d", port)
		m[addr] = &infosync.TiProxyServerInfo{IP: "127.0.0.1", StatusPort: strconv.Itoa(port)}
	}
	return context.WithValue(ctx, tiproxyAddrKey, m)
}

func trafficJobFields() []*types.FieldType {
	return []*types.FieldType{
		types.NewFieldType(mysql.TypeDatetime),
		types.NewFieldType(mysql.TypeDate),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeVarchar),
		types.NewFieldType(mysql.TypeVarchar),
	}
}

type mockPrivManager struct {
	tmock.Mock
	privilege.Manager
}

func (m *mockPrivManager) RequestDynamicVerification(activeRoles []*auth.RoleIdentity, privName string, grantable bool) bool {
	return m.Called(activeRoles, privName, grantable).Bool(0)
}

var _ objstore.Storage = (*mockExternalStorage)(nil)

type mockExternalStorage struct {
	objstore.Storage
	paths []string
}

func (s *mockExternalStorage) WalkDir(ctx context.Context, _ *objstore.WalkOption, fn func(string, int64) error) error {
	for _, path := range s.paths {
		if err := fn(path, 0); err != nil {
			return err
		}
	}
	return nil
}
