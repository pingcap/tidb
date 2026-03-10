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
	"fmt"
	"strings"
)

// Step is the step of task.
type Step int64

// TaskStep is the step of task.
// DO NOT change the value of the constants, will break backward compatibility.
// successfully task MUST go from StepInit to business steps, then StepDone.
const (
	StepInit Step = -1
	StepDone Step = -2

	unknownStepPrefix = "unknown step"
)

// Step2Str converts step to string.
// it's too bad that we define step as int 🙃.
func Step2Str(t TaskType, s Step) string {
	// StepInit and StepDone are special steps, we don't check task type for them.
	if s == StepInit {
		return "init"
	} else if s == StepDone {
		return "done"
	}
	switch t {
	case Backfill:
		return backfillStep2Str(s)
	case ImportInto:
		return importIntoStep2Str(s)
	case TaskTypeExample:
		return exampleStep2Str(s)
	}
	return fmt.Sprintf("unknown type %s", t)
}

// IsValidStep returns whether the step is valid for the task type.
func IsValidStep(t TaskType, s Step) bool {
	str := Step2Str(t, s)
	return !strings.Contains(str, unknownStepPrefix)
}

// Steps of example task type.
const (
	StepOne   Step = 1
	StepTwo   Step = 2
	StepThree Step = 3
)

func exampleStep2Str(s Step) string {
	switch s {
	case StepOne:
		return "one"
	case StepTwo:
		return "two"
	case StepThree:
		return "three"
	default:
		return unknownStepStr(s)
	}
}

// Steps of IMPORT INTO, each step is represented by one or multiple subtasks.
// the initial step is StepInit(-1)
// steps are processed in the following order:
//
//   - local sort:
//     StepInit
//     -> ImportStepImport
//     -> ImportStepPostProcess
//     -> StepDone
//   - global sort:
//     StepInit
//     -> ImportStepEncodeAndSort
//     -> ImportStepMergeSort (optional)
//     -> ImportStepWriteAndIngest
//     -> ImportStepCollectConflicts (optional)
//     -> ImportStepConflictResolution (optional)
//     -> ImportStepPostProcess
//     -> StepDone
const (
	// ImportStepImport we sort source data and ingest it into TiKV in this step.
	ImportStepImport Step = 1
	// ImportStepPostProcess we verify checksum and add index in this step.
	ImportStepPostProcess Step = 2
	// ImportStepEncodeAndSort encode source data and write sorted kv into global storage.
	ImportStepEncodeAndSort Step = 3
	// ImportStepMergeSort merge sorted kv from global storage, so we can have better
	// read performance during ImportStepWriteAndIngest.
	// depends on how much kv files are overlapped, there's might 0 subtasks
	// in this step.
	ImportStepMergeSort Step = 4
	// ImportStepWriteAndIngest write sorted kv into TiKV and ingest it.
	ImportStepWriteAndIngest Step = 5
	// ImportStepCollectConflicts collect conflicts info, this step won't mutate
	// downstream data, so is idempotent, and we can collect a correct checksum
	// for the conflicted rows. if we do this together with ImportStepConflictResolution,
	// once the step retry in the middle, we can't get a correct checksum.
	// this step also need to do deduplication for the conflicted rows due to
	// multiple unique indexes to avoid repeated collection, currently, we do it
	// in memory, so if there are too many conflicts, we will skip the later
	// checksum step as we don't know the exact checksum.
	ImportStepCollectConflicts Step = 6
	// ImportStepConflictResolution resolve detected conflicts.
	// during other steps of global sort, we will detect conflicts and record them
	// in external storage, if any conflicts are detected, we will resolve them
	// here. so there might be 0 subtasks in this step.
	ImportStepConflictResolution Step = 7
)

func importIntoStep2Str(s Step) string {
	switch s {
	case ImportStepImport:
		return "import"
	case ImportStepPostProcess:
		return "post-process"
	case ImportStepEncodeAndSort:
		return "encode"
	case ImportStepMergeSort:
		return "merge-sort"
	case ImportStepWriteAndIngest:
		return "ingest"
	case ImportStepCollectConflicts:
		return "collect-conflicts"
	case ImportStepConflictResolution:
		return "conflict-resolution"
	default:
		return unknownStepStr(s)
	}
}

// Steps of Add Index, each step is represented by one or multiple subtasks.
// the initial step is StepInit(-1)
// steps are processed in the following order:
// - local sort:
// StepInit -> BackfillStepReadIndex -> StepDone
// - global sort:
// StepInit -> BackfillStepReadIndex -> BackfillStepMergeSort -> BackfillStepWriteAndIngest -> StepDone
const (
	BackfillStepReadIndex Step = 1
	// BackfillStepMergeSort only used in global sort, it will merge sorted kv from global storage, so we can have better
	// read performance during BackfillStepWriteAndIngest with global sort.
	// depends on how much kv files are overlapped.
	// When kv files overlapped less than MergeSortOverlapThreshold, there‘re no subtasks.
	BackfillStepMergeSort Step = 2

	// BackfillStepWriteAndIngest write sorted kv into TiKV and ingest it.
	BackfillStepWriteAndIngest Step = 3

	// BackfillStepMergeTempIndex is the step to merge temp index into the original index.
	BackfillStepMergeTempIndex Step = 4
)

// StepStr convert proto.Step to string.
func backfillStep2Str(s Step) string {
	switch s {
	case BackfillStepReadIndex:
		return "read-index"
	case BackfillStepMergeSort:
		return "merge-sort"
	case BackfillStepWriteAndIngest:
		return "ingest"
	case BackfillStepMergeTempIndex:
		return "merge-temp-index"
	default:
		return unknownStepStr(s)
	}
}

func unknownStepStr(s Step) string {
	return fmt.Sprintf("%s %d", unknownStepPrefix, s)
}
