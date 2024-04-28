// Copyright 2023 PingCAP, Inc.
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

package planner_test

import (
	"context"
	"testing"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/planner"
	"github.com/pingcap/tidb/pkg/disttask/framework/storage"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/mock/gomock"
)

func TestPlanner(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	ctx = util.WithInternalSourceType(ctx, kv.InternalDistTask)

	store := testkit.CreateMockStore(t)
	gtk := testkit.NewTestKit(t, store)
	pool := pools.NewResourcePool(func() (pools.Resource, error) {
		return gtk.Session(), nil
	}, 1, 1, time.Second)
	defer pool.Close()
	mgr := storage.NewTaskManager(pool)
	storage.SetTaskManager(mgr)
	p := &planner.Planner{}
	pCtx := planner.PlanCtx{
		Ctx:        ctx,
		SessionCtx: gtk.Session(),
		TaskKey:    "1",
		TaskType:   "example",
		ThreadCnt:  1,
	}
	mockLogicalPlan := mock.NewMockLogicalPlan(ctrl)
	mockLogicalPlan.EXPECT().ToTaskMeta().Return([]byte("mock"), nil)
	taskID, err := p.Run(pCtx, mockLogicalPlan)
	require.NoError(t, err)
	require.Equal(t, int64(1), taskID)
}
