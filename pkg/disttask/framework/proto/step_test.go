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

package proto

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStep(t *testing.T) {
	// backfill
	require.Equal(t, "init", Step2Str(Backfill, StepInit))
	require.Equal(t, "read-index", Step2Str(Backfill, BackfillStepReadIndex))
	require.Equal(t, "merge-sort", Step2Str(Backfill, BackfillStepMergeSort))
	require.Equal(t, "write&ingest", Step2Str(Backfill, BackfillStepWriteAndIngest))
	require.Equal(t, "done", Step2Str(Backfill, StepDone))
	require.Equal(t, "unknown step 111", Step2Str(Backfill, 111))

	// import into
	require.Equal(t, "init", Step2Str(ImportInto, StepInit))
	require.Equal(t, "import", Step2Str(ImportInto, ImportStepImport))
	require.Equal(t, "post-process", Step2Str(ImportInto, ImportStepPostProcess))
	require.Equal(t, "merge-sort", Step2Str(ImportInto, ImportStepMergeSort))
	require.Equal(t, "encode&sort", Step2Str(ImportInto, ImportStepEncodeAndSort))
	require.Equal(t, "write&ingest", Step2Str(ImportInto, ImportStepWriteAndIngest))
	require.Equal(t, "done", Step2Str(ImportInto, StepDone))
	require.Equal(t, "unknown step 123", Step2Str(ImportInto, 123))

	// example type
	require.Equal(t, "init", Step2Str(TaskTypeExample, StepInit))
	require.Equal(t, "one", Step2Str(TaskTypeExample, StepOne))
	require.Equal(t, "two", Step2Str(TaskTypeExample, StepTwo))
	require.Equal(t, "done", Step2Str(TaskTypeExample, StepDone))
	require.Equal(t, "unknown step 333", Step2Str(TaskTypeExample, 333))

	// unknown type
	require.Equal(t, "unknown type 123", Step2Str(TaskType("123"), 123))
}
