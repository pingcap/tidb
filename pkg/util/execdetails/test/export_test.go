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

package execdetails_test

import (
	execdetails "github.com/pingcap/tidb/pkg/util/execdetails"
)

// Type aliases for testing
type RUV2Weights = execdetails.ExportRUV2Weights
type ExecDetails = execdetails.ExportExecDetails
type CopExecDetails = execdetails.ExportCopExecDetails
type RUV2Metrics = execdetails.ExportRUV2Metrics
type CopRuntimeStats = execdetails.ExportCopRuntimeStats
type RuntimeStatsColl = execdetails.ExportRuntimeStatsColl

// Helper functions for testing
func newRuntimeStatsColl(reuse *RuntimeStatsColl) *RuntimeStatsColl {
	return execdetails.ExportNewRuntimeStatsColl(reuse)
}

func newRUV2Metrics() *RUV2Metrics {
	return execdetails.ExportNewRUV2Metrics()
}

func getZeroTimeDetail() string {
	return execdetails.ExportZeroTimeDetail().String()
}
