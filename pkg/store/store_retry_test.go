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

package store

import (
	"errors"
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type startupRetryTestStorage struct {
	kv.Storage
}

type startupRetryTestDriver struct {
	open func(string) (kv.Storage, error)
}

func (d *startupRetryTestDriver) Open(path string) (kv.Storage, error) {
	return d.open(path)
}

func TestNewStoreRetryOnPDLeaderChange(t *testing.T) {
	// This is the error returned during PD leader initialization in lr-test #15.
	leaderErr := status.Error(codes.Unknown,
		"[PD:tso:ErrGenerateTimestamp]generate timestamp failed, requested pd is not leader of cluster")
	permanentErr := errors.New("invalid storage configuration")
	otherTSOErr := status.Error(codes.Unknown,
		"[PD:tso:ErrGenerateTimestamp]generate timestamp failed, tso count should be positive")
	// getTSFailedErr reproduces the TSO-layer wrapping captured in the failing
	// PR CI logs (2177 occurrences of "get TSO failed, rpc error: ...
	// [PD:tso:ErrGenerateTimestamp]"). noLeaderFoundErr is not a captured log
	// line: it is the plain message produced by the pinned pd/client
	// service-discovery init (servicediscovery/pd_service_discovery.go:852,
	// returned once initRetry is exhausted during pd.NewClient), and it covers
	// matching without an RFC prefix.
	getTSFailedErr := fmt.Errorf("get TSO failed, %w", leaderErr)
	noLeaderFoundErr := errors.New("no leader found")

	tests := []struct {
		name       string
		openErrors []error
		wantCalls  int
		wantErr    error
	}{
		{
			name:       "transient leader error then success",
			openErrors: []error{leaderErr, nil},
			wantCalls:  2,
		},
		{
			name:       "wrapped leader error then success",
			openErrors: []error{fmt.Errorf("initialize PD oracle: %w", leaderErr), nil},
			wantCalls:  2,
		},
		{
			name:       "persistent leader error exhausts retries",
			openErrors: []error{leaderErr, leaderErr, leaderErr},
			wantCalls:  3,
			wantErr:    leaderErr,
		},
		{
			// The TSO layer reports the same leader change with an extra
			// "get TSO failed" prefix and a wrapped status value.
			name:       "get TSO failed wrapping leader status",
			openErrors: []error{getTSFailedErr, nil},
			wantCalls:  2,
		},
		{
			// A plain "no leader found" from service discovery has no RFC code;
			// it is still a leader change and must be retried.
			name:       "plain no leader found then success",
			openErrors: []error{noLeaderFoundErr, nil},
			wantCalls:  2,
		},
		{
			name:       "permanent storage error fails immediately",
			openErrors: []error{permanentErr},
			wantCalls:  1,
			wantErr:    permanentErr,
		},
		{
			name:       "other TSO error fails immediately",
			openErrors: []error{otherTSOErr},
			wantCalls:  1,
			wantErr:    otherTSOErr,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const maxRetries = 3
			scheme := fmt.Sprintf("pdstartupretry%d", i)
			path := scheme + "://test-store"
			expectedStore := &startupRetryTestStorage{}
			calls := 0
			driver := &startupRetryTestDriver{open: func(gotPath string) (kv.Storage, error) {
				require.Equal(t, path, gotPath)
				calls++
				if calls <= len(tt.openErrors) && tt.openErrors[calls-1] != nil {
					return nil, tt.openErrors[calls-1]
				}
				return expectedStore, nil
			}}
			require.NoError(t, Register(scheme, driver))
			t.Cleanup(func() {
				storesLock.Lock()
				defer storesLock.Unlock()
				delete(stores, scheme)
			})

			got, err := newStoreWithRetry(path, maxRetries)
			require.Equal(t, tt.wantCalls, calls)
			if tt.wantErr != nil {
				require.Nil(t, got)
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				require.Same(t, expectedStore, got)
			}
		})
	}
}
