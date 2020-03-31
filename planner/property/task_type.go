// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package property

// TaskType is the type of execution task.
type TaskType int

const (
	// RootTaskType stands for the tasks that executed in the TiDB layer.
	RootTaskType TaskType = iota

	// CopSingleReadTaskType stands for the a TableScan or IndexScan tasks
	// executed in the coprocessor layer.
	CopSingleReadTaskType

	// CopDoubleReadTaskType stands for the a IndexLookup tasks executed in the
	// coprocessor layer.
	CopDoubleReadTaskType

	// CopTiFlashLocalReadTaskType stands for flash coprocessor that read data locally,
	// and only a part of the data is read in one cop task
	CopTiFlashLocalReadTaskType

	// CopTiFlashGlobalReadTaskType stands for flash coprocessor that read data globally
	// and all the data of given table will be read in one cop task
	CopTiFlashGlobalReadTaskType
)

// String implements fmt.Stringer interface.
func (t TaskType) String() string {
	switch t {
	case RootTaskType:
		return "rootTask"
	case CopSingleReadTaskType:
		return "copSingleReadTask"
	case CopDoubleReadTaskType:
		return "copDoubleReadTask"
	case CopTiFlashLocalReadTaskType:
		return "copTiFlashLocalReadTask"
	case CopTiFlashGlobalReadTaskType:
		return "copTiFlashRemoteReadTask"
	}
	return "UnknownTaskType"
}
