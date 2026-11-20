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
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/server/handler"
	"github.com/pingcap/tidb/pkg/store/helper"
	"github.com/stretchr/testify/require"
	tikvcfg "github.com/tikv/client-go/v2/config"
)

type securityTestStore struct {
	helper.Storage
	calls  int
	err    error
	policy tikvcfg.Security
	update func(context.Context, tikvcfg.Security) error
}

func (s *securityTestStore) UpdateKVServerSecurity(ctx context.Context, security tikvcfg.Security) error {
	if s.update != nil {
		return s.update(ctx, security)
	}
	s.calls++
	s.policy = security
	return s.err
}

func TestKVServerSecuritySettingsFastOff(t *testing.T) {
	defer config.RestoreFunc()()
	for _, lateSuccess := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel-preflight", true: "late-success"}[lateSuccess], func(t *testing.T) {
			cfg := config.NewConfig()
			cfg.Security.ClusterSSLCA = "test-ca.pem"
			cfg.Security.KVServerVerifyCNMode = "enforce"
			cfg.Security.KVServerVerifyCN = []string{"trusted-server"}
			config.StoreGlobalConfig(cfg)
			entered, resume, canceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			defer release()
			store := &securityTestStore{update: func(ctx context.Context, sec tikvcfg.Security) error {
				if sec.KVServerVerifyCNMode == "off" {
					return nil
				}
				close(entered)
				select {
				case <-ctx.Done():
					close(canceled)
				case <-resume:
				}
				if lateSuccess {
					// The client may have committed just before off, then returned late.
					<-resume
					return nil
				}
				return ctx.Err()
			}}
			h := NewSettingsHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: store}})
			post := func(mode, cn string, done chan *httptest.ResponseRecorder) {
				form := url.Values{"kv_server_verify_cn_mode": {mode}, "kv_server_verify_cn": {cn}}
				req := httptest.NewRequest(http.MethodPost, "/settings", strings.NewReader(form.Encode()))
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, req)
				done <- rec
			}
			enabled, disabled := make(chan *httptest.ResponseRecorder, 1), make(chan *httptest.ResponseRecorder, 1)
			go post("enforce", `["updated-server"]`, enabled)
			<-entered
			go post("off", `[]`, disabled)
			select {
			case rec := <-disabled:
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			case <-time.After(time.Second):
				release()
				<-enabled
				<-disabled
				t.Fatal("off waited for the previous preflight")
			}
			select {
			case <-canceled:
			case <-time.After(time.Second):
				t.Error("off did not cancel the previous request")
			}
			release()
			rec := <-enabled
			require.NotEqual(t, http.StatusOK, rec.Code, "superseded update must not report success")
			require.Equal(t, "off", config.GetGlobalConfig().Security.KVServerVerifyCNMode)
			require.Equal(t, "off", tikvcfg.GetGlobalConfig().Security.KVServerVerifyCNMode)
			require.Empty(t, config.GetGlobalConfig().Security.KVServerVerifyCN)
		})
	}
}

func TestKVServerSecuritySettingsQueuedCancellation(t *testing.T) {
	require.NoError(t, kvServerSecurityUpdateSem.Acquire(context.Background(), 1))
	unlock := sync.OnceFunc(func() { kvServerSecurityUpdateSem.Release(1) })
	defer unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- applyKVServerSecurity(ctx, tikvcfg.Security{KVServerVerifyCNMode: "enforce"}, func(context.Context, tikvcfg.Security) error {
			t.Error("canceled queued request must not update the store")
			return nil
		})
	}()
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("queued settings update did not honor cancellation")
	}
}

func TestKVServerSecuritySettings(t *testing.T) {
	defer config.RestoreFunc()()
	for _, tc := range []struct {
		name, mode, cn string
		fail           bool
		calls          int
		ok             bool
	}{
		{"enable", "enforce", `["trusted-server"]`, false, 1, true},
		{"disable", "off", `[]`, false, 1, true},
		{"preflight-rejected", "enforce", `["denied"]`, true, 1, false},
		{"empty-list", "enforce", `[]`, false, 0, false},
		{"invalid-mode", "on", `["trusted-server"]`, false, 0, false},
		{"invalid-json", "enforce", `trusted-server`, false, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := config.NewConfig()
			cfg.Security.ClusterSSLCA = "test-ca.pem"
			config.StoreGlobalConfig(cfg)
			store := &securityTestStore{}
			if tc.fail {
				store.err = errors.New("TLS preflight rejected")
			}
			h := NewSettingsHandler(&handler.TikvHandlerTool{Helper: helper.Helper{Store: store}})
			form := url.Values{"kv_server_verify_cn_mode": {tc.mode}, "kv_server_verify_cn": {tc.cn}}
			req := httptest.NewRequest(http.MethodPost, "/settings", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, tc.calls, store.calls)
			if tc.ok {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				require.Equal(t, tc.mode, config.GetGlobalConfig().Security.KVServerVerifyCNMode)
				require.Equal(t, store.policy.KVServerVerifyCN, tikvcfg.GetGlobalConfig().Security.KVServerVerifyCN)
			} else {
				require.NotEqual(t, http.StatusOK, rec.Code)
				require.Equal(t, "off", config.GetGlobalConfig().Security.KVServerVerifyCNMode)
				require.Empty(t, config.GetGlobalConfig().Security.KVServerVerifyCN)
			}
		})
	}
}
