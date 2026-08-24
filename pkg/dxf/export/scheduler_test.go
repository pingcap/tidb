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

package export

import (
	"testing"

	"github.com/pingcap/tidb/pkg/dxf/framework/proto"
	"github.com/stretchr/testify/require"
)

func TestGetNextStep(t *testing.T) {
	s := &exportScheduler{}
	// StepInit -> Dump -> Done.
	require.Equal(t, proto.ExportStepDump, s.GetNextStep(&proto.TaskBase{Step: proto.StepInit}))
	require.Equal(t, proto.StepDone, s.GetNextStep(&proto.TaskBase{Step: proto.ExportStepDump}))
	require.Equal(t, proto.StepDone, s.GetNextStep(&proto.TaskBase{Step: proto.StepDone}))
}

func TestTaskKey(t *testing.T) {
	require.Equal(t, "export/100/42", TaskKey(100, 42))
	require.NotEqual(t, TaskKey(100, 42), TaskKey(100, 43))
}
