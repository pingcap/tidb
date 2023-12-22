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
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/mock"
	"github.com/pingcap/tidb/pkg/disttask/framework/planner"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestPhysicalPlan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPipelineSpec := mock.NewMockPipelineSpec(ctrl)

	plan := &planner.PhysicalPlan{}
	planCtx := planner.PlanCtx{}
	plan.AddProcessor(planner.ProcessorSpec{Pipeline: mockPipelineSpec, Step: 1})
	mockPipelineSpec.EXPECT().ToSubtaskMeta(gomock.Any()).Return([]byte("mock"), nil)
	subtaskMetas, err := plan.ToSubtaskMetas(planCtx, 1)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("mock")}, subtaskMetas)
}
