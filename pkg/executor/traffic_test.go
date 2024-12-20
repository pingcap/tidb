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
	"strconv"
	"sync"
	"testing"

	"github.com/pingcap/tidb/pkg/domain/infosync"
	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
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
			path:   "/api/traffic/capture",
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
			path:   "/api/traffic/capture",
			form: url.Values{
				"output":   []string{"/tmp"},
				"duration": []string{"1s"},
			},
			hasStartTime: true,
		},
		{
			sql:    "traffic replay from '/tmp' user='root' password='123456' speed=1.0 read_only=true",
			method: http.MethodPost,
			path:   "/api/traffic/replay",
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
			path:   "/api/traffic/replay",
			form: url.Values{
				"input":    []string{"/tmp"},
				"username": []string{"root"},
			},
			hasStartTime: true,
		},
		{
			sql:    "cancel traffic jobs",
			method: http.MethodPost,
			path:   "/api/traffic/cancel",
			form:   url.Values{},
		},
		{
			sql:    "show traffic jobs",
			method: http.MethodGet,
			path:   "/api/traffic/show",
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
	}
}

func TestTrafficError(t *testing.T) {
	suite := newTrafficTestSuite(t, 10)
	ctx := context.TODO()
	exec := suite.build(ctx, "cancel traffic jobs")

	// no tiproxy
	m := make(map[string]*infosync.TiProxyServerInfo)
	tempCtx := context.WithValue(ctx, tiproxyAddrKey, m)
	require.ErrorContains(t, exec.Next(tempCtx, nil), "no tiproxy server found")

	// tiproxy no response
	m["127.0.0.1:0"] = &infosync.TiProxyServerInfo{IP: "127.0.0.1", StatusPort: "0"}
	require.ErrorContains(t, exec.Next(tempCtx, nil), "dial tcp")

	// tiproxy responds with error
	httpHandler := &mockHTTPHandler{t: t, httpOK: false}
	server, port := runServer(t, httpHandler)
	defer server.Close()
	tempCtx = fillCtxWithTiProxyAddr(ctx, []int{port})
	require.ErrorContains(t, exec.Next(tempCtx, nil), "500 Internal Server Error")
}

func TestTrafficShow(t *testing.T) {
	suite := newTrafficTestSuite(t, 2)
	ctx := context.TODO()
	fields := trafficJobFields()

	handlers := make([]*mockHTTPHandler, 0, 2)
	servers := make([]*http.Server, 0, 2)
	ports := make([]int, 0, 2)
	for i := 0; i < 2; i++ {
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
	marshaledJob := `{
		"type": "capture",
		"status": "canceled",
		"start_time": "%s",
		"end_time": "2020-01-01T02:01:01Z",
		"progress": "50%%",
		"error": "mock error"
	}`
	showTime1, showTime2 := "2020-01-01 00:00:00.000000", "2020-01-01 01:00:00.000000"
	showResult := "%s, 2020-01-01 02:01:01.000000, 127.0.0.1:%d, capture, 50%%, canceled, mock error\n"
	tests := []struct {
		resp []string
		chks []string
	}{
		{
			resp: []string{"[]", "[]"},
			chks: []string{},
		},
		{
			resp: []string{fmt.Sprintf("[%s]", fmt.Sprintf(marshaledJob, marshaledTime1)), "[]"},
			chks: []string{fmt.Sprintf(showResult, showTime1, ports[0])},
		},
		{
			resp: []string{fmt.Sprintf("[%s]", fmt.Sprintf(marshaledJob, marshaledTime1)), fmt.Sprintf("[%s]", fmt.Sprintf(marshaledJob, marshaledTime1))},
			chks: []string{fmt.Sprintf("%s%s", fmt.Sprintf(showResult, showTime1, ports[0]), fmt.Sprintf(showResult, showTime1, ports[1]))},
		},
		{
			resp: []string{fmt.Sprintf("[%s,%s]", fmt.Sprintf(marshaledJob, marshaledTime1), fmt.Sprintf(marshaledJob, marshaledTime2)),
				fmt.Sprintf("[%s,%s]", fmt.Sprintf(marshaledJob, marshaledTime1), fmt.Sprintf(marshaledJob, marshaledTime2))},
			chks: []string{fmt.Sprintf("%s%s", fmt.Sprintf(showResult, showTime2, ports[0]), fmt.Sprintf(showResult, showTime2, ports[1])),
				fmt.Sprintf("%s%s", fmt.Sprintf(showResult, showTime1, ports[0]), fmt.Sprintf(showResult, showTime1, ports[1]))},
		},
	}

	for _, test := range tests {
		for j := range test.resp {
			handlers[j].setResponse(test.resp[j])
		}
		executor := suite.build(ctx, "show traffic jobs")
		require.NoError(t, executor.Open(ctx))
		chk := chunk.New(fields, 2, 2)
		for j := 0; j < len(test.chks); j++ {
			require.NoError(t, executor.Next(ctx, chk))
			require.Equal(t, test.chks[j], chk.ToString(fields))
		}
		require.NoError(t, executor.Next(ctx, chk))
		require.Equal(t, 0, chk.NumRows())
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
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable(), plannercore.MockUnsignedTable()})
	planBuilder, _ := plannercore.NewPlanBuilder().Init(sctx, nil, hint.NewQBHintHandler(nil))
	execBuilder := NewMockExecutorBuilderForTest(sctx, is)
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

func (handler *mockHTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	handler.Lock()
	defer handler.Unlock()
	handler.method = r.Method
	handler.path = r.URL.Path
	require.NoError(handler.t, r.ParseForm())
	handler.form = r.PostForm
	if handler.httpOK {
		w.WriteHeader(http.StatusOK)
		resp := handler.resp
		if len(resp) == 0 && r.Method == http.MethodGet {
			resp = "[]"
		}
		_, err := w.Write([]byte(resp))
		require.NoError(handler.t, err)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
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
		types.NewFieldType(mysql.TypeDate),
		types.NewFieldType(mysql.TypeDate),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
		types.NewFieldType(mysql.TypeString),
	}
}
