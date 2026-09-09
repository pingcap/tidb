// Copyright 2026 PingCAP, Inc.
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

package tikvhandler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/stretchr/testify/require"
)

func TestParseFlashReplicaReloadQuery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    bool
		wantErr string
	}{
		{name: "omitted", want: false},
		{name: "false", query: "?reload=false", want: false},
		{name: "true", query: "?reload=true", want: true},
		{name: "one", query: "?reload=1", want: true},
		{name: "zero", query: "?reload=0", want: false},
		{name: "invalid", query: "?reload=maybe", wantErr: "invalid reload query value"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/tiflash/replica"+tt.query, nil)
			got, err := parseFlashReplicaReloadQuery(req)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestFlashReplicaSummarySysvar(t *testing.T) {
	sv := variable.GetSysVar(vardef.TiDBColumnarStorageEnabled)
	require.NotNil(t, sv)
	require.Equal(t, vardef.ScopeGlobal, sv.Scope)
	require.Equal(t, variable.BoolToOnOff(vardef.DefTiDBColumnarStorageEnabled), sv.Value)
}

func TestDXFTaskCleanupBatchSizeHandler(t *testing.T) {
	restore := proto.SetTaskCleanupBatchSizeForTest(proto.DefaultTaskCleanupBatchSize)
	defer restore()
	h := NewDXFTaskCleanupBatchSizeHandler()

	get := httptest.NewRecorder()
	h.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/dxf/schedule/task_cleanup_batch_size", nil))
	require.Equal(t, http.StatusOK, get.Code)
	require.Contains(t, get.Body.String(), `"task_cleanup_batch_size": 20`)
	require.Contains(t, get.Body.String(), `"persistence": "memory_only"`)

	for _, value := range []string{"", "0", "1001"} {
		post := httptest.NewRecorder()
		h.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/dxf/schedule/task_cleanup_batch_size?value="+value, nil))
		require.Equal(t, http.StatusBadRequest, post.Code)
	}

	post := httptest.NewRecorder()
	h.ServeHTTP(post, httptest.NewRequest(http.MethodPost, "/dxf/schedule/task_cleanup_batch_size?value=128", nil))
	require.Equal(t, http.StatusOK, post.Code)
	require.Contains(t, post.Body.String(), `"task_cleanup_batch_size": 128`)
	require.Equal(t, 128, proto.GetTaskCleanupBatchSize())
}
