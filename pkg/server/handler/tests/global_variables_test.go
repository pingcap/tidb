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

package tests

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/server/handler/tikvhandler"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/sem"
	semv2 "github.com/pingcap/tidb/pkg/util/sem/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestGlobalVariables(t *testing.T) {
	ts := createBasicHTTPHandlerTestSuite()
	ts.startServer(t)
	defer ts.stopServer(t)

	resp, err := ts.FetchStatus("/variables/global")
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	if !kerneltype.IsNextGen() {
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
		return
	}
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	var values map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&values))

	fetch := func() map[string]string {
		resp, err := ts.FetchStatus("/variables/global")
		require.NoError(t, err)
		defer func() { require.NoError(t, resp.Body.Close()) }()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		var result map[string]string
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
		return result
	}
	tk := testkit.NewTestKit(t, ts.store)
	rows := tk.MustQuery("SHOW GLOBAL VARIABLES").Rows()
	require.Len(t, values, len(rows))
	for _, row := range rows {
		require.Contains(t, values, row[0])
	}
	require.NotContains(t, values, vardef.Timestamp)
	require.Equal(t, vardef.Off, values[vardef.ValidatePasswordEnable])
	require.Equal(t, "MEDIUM", values[vardef.ValidatePasswordPolicy])
	require.NotEmpty(t, values[vardef.Version])

	t.Run("current values", func(t *testing.T) {
		tk.MustExec("SET GLOBAL max_execution_time = 1234")
		tk.MustExec("SET SESSION max_execution_time = 5678")
		require.Equal(t, "1234", fetch()[vardef.MaxExecutionTime])
		tk.MustExec("SET GLOBAL max_execution_time = 4321")
		require.Equal(t, "4321", fetch()[vardef.MaxExecutionTime])
	})

	t.Run("GET only", func(t *testing.T) {
		resp, err := ts.PostStatus("/variables/global", "application/json", nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, resp.Body.Close()) }()
		require.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
	})

	t.Run("sensitive values are masked", func(t *testing.T) {
		originalRedactMode := errors.RedactLogEnabled.Load()
		t.Cleanup(func() { tk.MustExec("SET GLOBAL tidb_redact_log = '" + originalRedactMode + "'") })
		names := []string{
			vardef.TiDBExpEmbedJinaAIAPIKey, vardef.TiDBExpEmbedOpenAIAPIKey,
			vardef.TiDBExpEmbedCohereAPIKey, vardef.TiDBExpEmbedHuggingFaceAPIKey,
			vardef.TiDBExpEmbedNvidiaNIMAPIKey, vardef.TiDBExpEmbedGeminiAPIKey,
			vardef.AuthenticationLDAPSASLBindRootPWD, vardef.AuthenticationLDAPSimpleBindRootPWD,
			vardef.TiDBConfig, vardef.TiDBTraceEvent, vardef.InitConnect,
			vardef.ValidatePasswordDictionary, "init_slave",
		}
		oldNoop := vardef.EnableNoopVariables.Swap(true)
		t.Cleanup(func() { vardef.EnableNoopVariables.Store(oldNoop) })
		var called atomic.Int64
		for _, name := range names {
			original := variable.GetSysVar(name)
			t.Cleanup(func() { variable.RegisterSysVar(original) })
			replacement := *original
			replacement.GetGlobal = func(context.Context, *variable.SessionVars) (string, error) {
				called.Add(1)
				return "credential-that-must-be-masked", nil
			}
			variable.RegisterSysVar(&replacement)
		}
		for _, mode := range []string{vardef.Off, vardef.On, vardef.Marker} {
			tk.MustExec("SET GLOBAL tidb_redact_log = '" + mode + "'")
			result := fetch()
			for _, name := range names {
				require.Equal(t, vardef.MaskPwd, result[name], name)
			}
		}
		require.Equal(t, int64(3*len(names)), called.Load())
	})

	t.Run("noop visibility", func(t *testing.T) {
		original := vardef.EnableNoopVariables.Load()
		t.Cleanup(func() { vardef.EnableNoopVariables.Store(original) })
		for _, enabled := range []bool{false, true} {
			vardef.EnableNoopVariables.Store(enabled)
			result := fetch()
			_, exists := result["init_slave"]
			require.Equal(t, enabled, exists)
		}
	})

	t.Run("custom sensitive variables", func(t *testing.T) {
		const name = "test_http_sensitive_variable"
		defer variable.UnregisterSysVar(name)
		for _, value := range []string{"", "x", "sk-long-secret-suffix", "s3://user:secret@bucket/?token=secret%zz"} {
			variable.RegisterSysVar(&variable.SysVar{
				Name: name, Scope: vardef.ScopeGlobal, Value: "default-secret", IsSensitive: true,
				GetGlobal: func(context.Context, *variable.SessionVars) (string, error) {
					return value, nil
				},
			})
			expected := vardef.MaskPwd
			if value == "" {
				expected = ""
			}
			require.Equal(t, expected, fetch()[name])
		}
	})

	t.Run("cloud storage URI redaction", func(t *testing.T) {
		original := vardef.CloudStorageURI.Load()
		t.Cleanup(func() { vardef.CloudStorageURI.Store(original) })
		for _, test := range []struct {
			uri      string
			expected string
		}{
			{"", ""},
			{"s3://bucket/path", "s3://bucket/path"},
			{"s3://bucket/path?access-key=key&secret-access-key=secret&session-token=token&region=us-east-1", "s3://bucket/path?access-key=xxxxxx&region=us-east-1&secret-access-key=xxxxxx&session-token=xxxxxx"},
			{"ks3://bucket/path?Access_Key=key&secret_access_key=secret", "ks3://bucket/path?Access_Key=xxxxxx&secret_access_key=xxxxxx"},
			{"oss://bucket/path?access-key=key&secret-access-key=secret", "oss://bucket/path?access-key=xxxxxx&secret-access-key=xxxxxx"},
			{"azure://bucket/path?account-key=key&encryption-key=secret&sas-token=token", "azure://bucket/path?account-key=xxxxxx&encryption-key=xxxxxx&sas-token=xxxxxx"},
			{"azblob://bucket/path?account-key=key&encryption-key=secret&sas-token=token", "azblob://bucket/path?account-key=xxxxxx&encryption-key=xxxxxx&sas-token=xxxxxx"},
			{"azure://bucket/path?account-name=acct&endpoint=https%3A%2F%2Facct.blob.core.windows.net%2F%3Fsig%3Dsecret&sas-token=token", "azure://bucket/path?account-name=acct&endpoint=xxxxxx&sas-token=xxxxxx"},
			{"azblob://bucket/path?EndPoint=https%3A%2F%2Facct.blob.core.windows.net%2F%3Fsig%3Dsecret&sas_token=token", "azblob://bucket/path?EndPoint=xxxxxx&sas_token=xxxxxx"},
		} {
			vardef.CloudStorageURI.Store(test.uri)
			require.Equal(t, test.expected, fetch()[vardef.TiDBCloudStorageURI])
		}
	})

	t.Run("security enhanced mode", func(t *testing.T) {
		originalHostname := variable.GetSysVar(vardef.Hostname)
		defer variable.RegisterSysVar(originalHostname)
		sem.Enable()
		defer sem.Disable()
		result := fetch()
		require.Equal(t, vardef.MaskPwd, result[vardef.TiDBConfig])
		require.Contains(t, result, vardef.TiDBGeneralLog)
		require.Contains(t, result, vardef.MaxExecutionTime)
	})

	t.Run("security enhanced mode v2", func(t *testing.T) {
		require.NoError(t, semv2.EnableBy(&semv2.Config{
			TiDBVersion: "v0.0.0",
			RestrictedVariables: []semv2.VariableRestriction{
				{Name: vardef.MaxExecutionTime, Hidden: true},
				{Name: vardef.TiDBConfig, Hidden: true},
			},
		}))
		defer semv2.Disable()
		result := fetch()
		require.Contains(t, result, vardef.MaxExecutionTime)
		require.Equal(t, vardef.MaskPwd, result[vardef.TiDBConfig])
		require.Contains(t, result, vardef.Version)
	})

	t.Run("safe getter failure", func(t *testing.T) {
		core, recorded := observer.New(zap.ErrorLevel)
		restore := log.ReplaceGlobals(zap.New(core), &log.ZapProperties{
			Core:  core,
			Level: zap.NewAtomicLevelAt(zap.ErrorLevel),
		})
		defer restore()
		const name = "test_http_failing_global_variable"
		defer variable.UnregisterSysVar(name)
		for _, sensitive := range []bool{false, true} {
			recorded.TakeAll()
			variable.RegisterSysVar(&variable.SysVar{
				Name: name, Scope: vardef.ScopeGlobal, IsSensitive: sensitive,
				GetGlobal: func(context.Context, *variable.SessionVars) (string, error) {
					return "credential-in-value-must-not-leak", errors.New("credential-in-error-must-not-leak")
				},
			})
			resp, err := ts.FetchStatus("/variables/global")
			require.NoError(t, err)
			defer func() { require.NoError(t, resp.Body.Close()) }()
			require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			require.NotContains(t, string(body), "credential-in-")
			require.NotContains(t, string(body), vardef.MaxExecutionTime)
			logs := recorded.FilterMessage("unable to read global variable").All()
			require.Len(t, logs, 1)
			require.Equal(t, name, logs[0].ContextMap()["name"])
			require.Equal(t, "credential-in-error-must-not-leak", logs[0].ContextMap()["error"])
			require.NotContains(t, logs[0].ContextMap(), "value")
		}
	})

	t.Run("request cancellation", func(t *testing.T) {
		const name = "test_http_cancel_global_variable"
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		variable.RegisterSysVar(&variable.SysVar{
			Name: name, Scope: vardef.ScopeGlobal,
			GetGlobal: func(ctx context.Context, _ *variable.SessionVars) (string, error) {
				_, hasDeadline := ctx.Deadline()
				require.True(t, hasDeadline)
				cancel()
				require.ErrorIs(t, ctx.Err(), context.Canceled)
				return "", ctx.Err()
			},
		})
		defer variable.UnregisterSysVar(name)
		req := httptest.NewRequest(http.MethodGet, "/variables/global", nil).WithContext(ctx)
		resp := httptest.NewRecorder()
		tikvhandler.NewGlobalVariablesHandler(ts.server.NewTikvHandlerTool()).ServeHTTP(resp, req)
		require.Equal(t, http.StatusInternalServerError, resp.Code)
		require.NotContains(t, resp.Body.String(), vardef.MaxExecutionTime)
	})
}
