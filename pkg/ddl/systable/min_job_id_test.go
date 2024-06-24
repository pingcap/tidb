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

package systable

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestRefreshMinJobID(t *testing.T) {
	loopRetryIntBak := refreshInterval
	refreshInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		refreshInterval = loopRetryIntBak
	})
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mgr := mock.NewMockManager(ctrl)
	ctx := context.Background()

	refresher := NewMinJobIDRefresher(mgr)
	// success
	start := time.Now()
	mgr.EXPECT().GetMinJobID(gomock.Any(), int64(0)).Return(int64(1), nil)
	refresher.Refresh(ctx)
	require.EqualValues(t, 1, refresher.currMinJobID)
	require.GreaterOrEqual(t, refresher.lastRefreshTime, start)
	require.True(t, ctrl.Satisfied())
	// not refresh too fast
	refreshInterval = time.Hour
	refresher.Refresh(ctx)
	require.True(t, ctrl.Satisfied())
	// ignore refresh error
	refresher.lastRefreshTime = time.Time{}
	mgr.EXPECT().GetMinJobID(gomock.Any(), int64(1)).Return(int64(0), errors.New("mock err"))
	refresher.Refresh(ctx)
	require.EqualValues(t, 1, refresher.currMinJobID)
	require.True(t, ctrl.Satisfied())
	// success again
	refresher.lastRefreshTime = time.Time{}
	mgr.EXPECT().GetMinJobID(gomock.Any(), int64(1)).Return(int64(100), nil)
	refresher.Refresh(ctx)
	require.EqualValues(t, 100, refresher.currMinJobID)
	require.GreaterOrEqual(t, refresher.lastRefreshTime, start)
	require.True(t, ctrl.Satisfied())
}
