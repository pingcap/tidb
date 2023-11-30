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

package importinto

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/disttask/framework/proto"
	"github.com/stretchr/testify/require"
)

func TestImportScheduler(t *testing.T) {
	ctx := context.Background()
	scheduler := newImportScheduler(
		ctx,
		":4000",
		&proto.Task{
			ID: 1,
		},
		nil,
	).(*importScheduler)

	require.NotNil(t, scheduler.BaseScheduler.Extension)
	require.True(t, scheduler.IsIdempotent(&proto.Subtask{}))

	for _, step := range []proto.Step{
		StepImport,
		StepEncodeAndSort,
		StepMergeSort,
		StepWriteAndIngest,
		StepPostProcess,
	} {
		exe, err := scheduler.GetSubtaskExecutor(ctx, &proto.Task{Step: step, Meta: []byte("{}")}, nil)
		require.NoError(t, err)
		require.NotNil(t, exe)
	}
	_, err := scheduler.GetSubtaskExecutor(ctx, &proto.Task{Step: proto.StepInit, Meta: []byte("{}")}, nil)
	require.Error(t, err)
	_, err = scheduler.GetSubtaskExecutor(ctx, &proto.Task{Step: StepImport, Meta: []byte("")}, nil)
	require.Error(t, err)
}
