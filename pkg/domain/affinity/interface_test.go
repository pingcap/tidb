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

package affinity

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"
	pdhttp "github.com/tikv/pd/client/http"
)

type retryAffinityManager struct {
	Manager
	operation func(context.Context) error
}

func (m *retryAffinityManager) CreateAffinityGroupsIfNotExists(ctx context.Context, _ map[string][]pdhttp.AffinityGroupKeyRange) error {
	return m.operation(ctx)
}

func (m *retryAffinityManager) DeleteAffinityGroups(ctx context.Context, _ []string) error {
	return m.operation(ctx)
}

func TestAffinityRetryNilContext(t *testing.T) {
	InitManager(nil)
	require.NoError(t, CreateGroupsIfNotExists(nil, map[string][]pdhttp.AffinityGroupKeyRange{"g1": {}}))
	require.NoError(t, DeleteGroupsWithRetry(nil, []string{"g1"}))
}

func TestAffinityCreateRetry(t *testing.T) {
	groups := map[string][]pdhttp.AffinityGroupKeyRange{"g1": {{StartKey: []byte("a"), EndKey: []byte("b")}}}
	for _, tc := range []struct {
		name  string
		errs  []error
		calls int
	}{
		{"503_then_success", []error{pdStatusErr(http.StatusServiceUnavailable)}, 2},
		{"network_then_success", []error{errors.New("connection reset")}, 2},
		{"client_error", []error{pdStatusErr(http.StatusBadRequest)}, 1},
		{"retry_limit", []error{pdStatusErr(503), pdStatusErr(503), pdStatusErr(503), pdStatusErr(503)}, maxRetryTimes + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			manager = &retryAffinityManager{operation: func(context.Context) error {
				calls++
				if calls <= len(tc.errs) {
					return tc.errs[calls-1]
				}
				return nil
			}}
			t.Cleanup(func() { InitManager(nil) })
			err := CreateGroupsIfNotExists(context.Background(), groups)
			if tc.calls > len(tc.errs) {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.errs[tc.calls-1])
			}
			require.Equal(t, tc.calls, calls)
		})
	}
}

func TestAffinityRetryCancelledContext(t *testing.T) {
	for _, deletion := range []bool{false, true} {
		for _, cancelBeforeCall := range []bool{false, true} {
			t.Run(fmt.Sprintf("delete=%v/cancel_before=%v", deletion, cancelBeforeCall), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if cancelBeforeCall {
					cancel()
				}
				calls := 0
				manager = &retryAffinityManager{operation: func(context.Context) error {
					calls++
					cancel()
					return pdStatusErr(http.StatusServiceUnavailable)
				}}
				t.Cleanup(func() { InitManager(nil) })
				var err error
				if deletion {
					err = DeleteGroupsWithRetry(ctx, []string{"g1"})
				} else {
					err = CreateGroupsIfNotExists(ctx, map[string][]pdhttp.AffinityGroupKeyRange{"g1": {}})
				}
				require.ErrorIs(t, err, context.Canceled)
				if cancelBeforeCall {
					require.Zero(t, calls)
				} else {
					require.Equal(t, 1, calls)
				}
			})
		}
	}
}
