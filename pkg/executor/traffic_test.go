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
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	plannercore "github.com/pingcap/tidb/pkg/planner/core"
	"github.com/pingcap/tidb/pkg/planner/core/resolve"
	"github.com/pingcap/tidb/pkg/util/hint"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
)

func TestTrafficForm(t *testing.T) {
	tests := []struct {
		sql    string
		method string
		path   string
		form   url.Values
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
		},
		{
			sql:    "traffic capture to '/tmp' duration='1s'",
			method: http.MethodPost,
			path:   "/api/traffic/capture",
			form: url.Values{
				"output":   []string{"/tmp"},
				"duration": []string{"1s"},
			},
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
		},
		{
			sql:    "traffic replay from '/tmp' user='root'",
			method: http.MethodPost,
			path:   "/api/traffic/replay",
			form: url.Values{
				"input":    []string{"/tmp"},
				"username": []string{"root"},
			},
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

	parser := parser.New()
	sctx := mock.NewContext()
	ctx := context.TODO()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable(), plannercore.MockUnsignedTable()})
	builder, _ := plannercore.NewPlanBuilder().Init(sctx, nil, hint.NewQBHintHandler(nil))
	httpHandler := &mockHTTPHandler{t: t, httpOK: true}
	server, port := runServer(t, httpHandler)
	defer server.Close()
	ctx = fillCtxWithTiProxyAddr(ctx, []int{port})
	for _, test := range tests {
		stmt, err := parser.ParseOneStmt(test.sql, "", "")
		require.NoError(t, err)
		p, err := builder.Build(ctx, resolve.NewNodeW(stmt))
		require.NoError(t, err)
		executorBuilder := NewMockExecutorBuilderForTest(sctx, is)
		exec := executorBuilder.build(p)
		require.NotEmpty(t, exec)
		require.NoError(t, exec.Next(ctx, nil))
		require.Equal(t, test.method, httpHandler.getMethod())
		require.Equal(t, test.path, httpHandler.getPath())
		require.Equal(t, test.form, httpHandler.getForm())
	}
}

func TestTrafficError(t *testing.T) {
	sctx := mock.NewContext()
	ctx := context.TODO()
	is := infoschema.MockInfoSchema([]*model.TableInfo{plannercore.MockSignedTable(), plannercore.MockUnsignedTable()})
	builder, _ := plannercore.NewPlanBuilder().Init(sctx, nil, hint.NewQBHintHandler(nil))
	stmt, err := parser.New().ParseOneStmt("cancel traffic jobs", "", "")
	require.NoError(t, err)
	p, err := builder.Build(ctx, resolve.NewNodeW(stmt))
	require.NoError(t, err)
	executorBuilder := NewMockExecutorBuilderForTest(sctx, is)
	exec := executorBuilder.build(p)
	require.NotEmpty(t, exec)

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

type mockHTTPHandler struct {
	t *testing.T
	sync.Mutex
	form   url.Values
	method string
	path   string
	httpOK bool
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
