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

package ddl

import "github.com/pingcap/tidb/pkg/disttask/framework/proto"

// Steps of Add Index, each step is represented by one or multiple subtasks.
// the initial step is StepInit(-1)
// steps are processed in the following order:
// - local sort:
// StepInit -> StepReadIndex -> StepWriteAndIngest -> StepDone
// - global sort:
// StepInit -> StepReadIndex -> StepMergeSort -> StepWriteAndIngest -> StepDone
const (
	StepReadIndex proto.Step = 1
	// StepMergeSort only used in global sort, it will merge sorted kv from global storage, so we can have better
	// read performance during StepWriteAndIngest with global sort.
	// depends on how much kv files are overlapped.
	// When kv files overlapped less than MergeSortOverlapThreshold, there‘re no subtasks.
	StepMergeSort proto.Step = 2

	// StepWriteAndIngest write sorted kv into TiKV and ingest it.
	StepWriteAndIngest proto.Step = 3
)
