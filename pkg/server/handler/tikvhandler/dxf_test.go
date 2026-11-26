// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tikvhandler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/pingcap/tidb/pkg/dxf/framework/schstatus"
	"github.com/stretchr/testify/require"
)

func TestDXFNodesHandler(t *testing.T) {
	t.Run("read error", func(t *testing.T) {
		h := &DXFNodesHandler{
			getNodes: func(context.Context) ([]proto.ManagedNode, error) {
				return nil, errors.New("injected read error")
			},
		}
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dxf/nodes", nil))

		require.Equal(t, http.StatusInternalServerError, recorder.Code)
		require.Equal(t, "injected read error", recorder.Body.String())
	})

	t.Run("canceled request", func(t *testing.T) {
		var observedErr error
		h := &DXFNodesHandler{
			getNodes: func(ctx context.Context) ([]proto.ManagedNode, error) {
				observedErr = ctx.Err()
				return []proto.ManagedNode{}, nil
			},
		}
		reqCtx, cancel := context.WithCancel(context.Background())
		cancel()
		req := httptest.NewRequest(http.MethodGet, "/dxf/nodes", nil).WithContext(reqCtx)
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, req)

		require.ErrorIs(t, observedErr, context.Canceled)
		require.Equal(t, http.StatusOK, recorder.Code)
		require.JSONEq(t, "[]", recorder.Body.String())
	})
}

func TestParsePauseScaleInFlag(t *testing.T) {
	_, _, err := parsePauseScaleInFlag(&http.Request{})
	require.ErrorContains(t, err, "invalid action")
	_, _, err = parsePauseScaleInFlag(&http.Request{Form: map[string][]string{
		"action": {"pause_scale_in"}, "ttl": {"invalid"},
	}})
	require.ErrorContains(t, err, "invalid ttl")
	flag, ttlFlag, err := parsePauseScaleInFlag(&http.Request{Form: map[string][]string{"action": {"pause_scale_in"}}})
	require.NoError(t, err)
	require.EqualValues(t, schstatus.PauseScaleInFlag, flag)
	require.True(t, ttlFlag.Enabled)
	require.Equal(t, dxfOperationDefaultTTL, ttlFlag.TTL)
	require.WithinRange(t, ttlFlag.ExpireTime, time.Now().Add(ttlFlag.TTL-time.Minute), time.Now().Add(ttlFlag.TTL))
	flag, ttlFlag, err = parsePauseScaleInFlag(&http.Request{Form: map[string][]string{
		"action": {"pause_scale_in"}, "ttl": {"5h"},
	}})
	require.NoError(t, err)
	require.EqualValues(t, schstatus.PauseScaleInFlag, flag)
	require.True(t, ttlFlag.Enabled)
	require.Equal(t, 5*time.Hour, ttlFlag.TTL)
	require.WithinRange(t, ttlFlag.ExpireTime, time.Now().Add(ttlFlag.TTL-time.Minute), time.Now().Add(ttlFlag.TTL))
}
