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

// Package workloadbasedlearning implements the Workload-Based Learning Optimizer.
// The Workload-Based Learning Optimizer introduces a new module in TiDB that leverages captured workload history to
// enhance the database query optimizer.
// By learning from historical data, this module helps the optimizer make smarter decisions, such as identify hot and cold tables,
// analyze resource consumption, etc.
// The workload analysis results can be used to directly suggest a better path,
// or to indirectly influence the cost model and stats so that the optimizer can select the best plan more intelligently and adaptively.
package workloadbasedlearning

// Handle The entry point for all workload-based learning related tasks
type Handle struct {
}

// NewWorkloadBasedLearningHandle Create a new WorkloadBasedLearningHandle
// WorkloadBasedLearningHandle is Singleton pattern
func NewWorkloadBasedLearningHandle() *Handle {
	return &Handle{}
}

// HandleReadTableCost Start a new round of analysis of all historical read queries.
func (Handle *Handle) HandleReadTableCost() {

}
