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

package mpp

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/stretchr/testify/require"
)

type recoveryErrorCoordinator struct {
	kv.MppCoordinator
	err error
}

func (c *recoveryErrorCoordinator) Next(context.Context) (kv.ResultSubset, error) {
	return nil, c.err
}

type testRecoveryHandler struct {
	recover func(context.Context, *RecoveryInfo) error
}

func (*testRecoveryHandler) chooseHandlerImpl(error) bool { return true }

func (h *testRecoveryHandler) doRecovery(ctx context.Context, info *RecoveryInfo) error {
	return h.recover(ctx, info)
}

func TestMPPRecoveryCancellation(t *testing.T) {
	for _, mode := range []string{"cancel", "deadline", "http_error"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if mode == "deadline" {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			}
			defer cancel()

			mppErr := errors.New("Memory limit exceeded")
			handler := &testRecoveryHandler{recover: func(recoveryCtx context.Context, info *RecoveryInfo) error {
				require.Same(t, ctx, recoveryCtx)
				require.Same(t, mppErr, info.MPPErr)
				require.Equal(t, 2, info.NodeCnt)
				if mode == "cancel" {
					cancel()
				}
				return errors.New("autoscaler request failed")
			}}
			retryer := &ExecutorWithRetry{
				ctx:     context.Background(),
				coord:   &recoveryErrorCoordinator{err: mppErr},
				nodeCnt: 2,
				mppErrRecovery: &RecoveryHandler{
					enable:         true,
					maxRecoveryCnt: 3,
					holder:         &mppResultHolder{capacity: 1},
					handlers:       []handlerImpl{handler},
				},
			}
			err := retryer.nextWithRecovery(ctx)
			if mode == "http_error" {
				require.Same(t, mppErr, err)
			} else {
				require.ErrorIs(t, err, ctx.Err())
			}
			require.Equal(t, uint32(1), retryer.mppErrRecovery.RecoveryCnt())
		})
	}
}
