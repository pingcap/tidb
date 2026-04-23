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

package execdetails

import "github.com/tikv/client-go/v2/util"

// ExportRUV2Weights exports RUV2Weights type for testing
type ExportRUV2Weights = RUV2Weights

// ExportExecDetails exports ExecDetails type for testing
type ExportExecDetails = ExecDetails

// ExportCopExecDetails exports CopExecDetails type for testing
type ExportCopExecDetails = CopExecDetails

// ExportRUV2Metrics exports RUV2Metrics type for testing
type ExportRUV2Metrics = RUV2Metrics

// ExportCopRuntimeStats exports CopRuntimeStats type for testing
type ExportCopRuntimeStats = CopRuntimeStats

// ExportBasicCopRuntimeStats exports basicCopRuntimeStats type for testing
type ExportBasicCopRuntimeStats = basicCopRuntimeStats

// ExportRuntimeStatsColl exports RuntimeStatsColl type for testing
type ExportRuntimeStatsColl = RuntimeStatsColl

// ExportNewRuntimeStatsColl exports NewRuntimeStatsColl function for testing
func ExportNewRuntimeStatsColl(reuse *RuntimeStatsColl) *RuntimeStatsColl {
	return NewRuntimeStatsColl(reuse)
}

// ExportNewRUV2Metrics exports NewRUV2Metrics function for testing
func ExportNewRUV2Metrics() *RUV2Metrics {
	return NewRUV2Metrics()
}

// ExportZeroTimeDetail exports zeroTimeDetail for testing
func ExportZeroTimeDetail() util.TimeDetail {
	return zeroTimeDetail
}

// ExportFormatRUV2Summary exports FormatRUV2Summary function for testing
func ExportFormatRUV2Summary(metrics *RUV2Metrics, weights RUV2Weights, tiKVRU, tiFlashRU float64) (total string, detail string) {
	return FormatRUV2Summary(metrics, weights, tiKVRU, tiFlashRU)
}

// ExportFormatRUV2Total exports FormatRUV2Total function for testing
func ExportFormatRUV2Total(metrics *RUV2Metrics, weights RUV2Weights, tiKVRU, tiFlashRU float64) string {
	return FormatRUV2Total(metrics, weights, tiKVRU, tiFlashRU)
}

// ExportFormatRUV2Metrics exports FormatRUV2Metrics function for testing
func ExportFormatRUV2Metrics(metrics *RUV2Metrics, weights RUV2Weights, tiKVRU, tiFlashRU float64) string {
	return FormatRUV2Metrics(metrics, weights, tiKVRU, tiFlashRU)
}

// ExportRuntimeStatsWithCommit exports RuntimeStatsWithCommit type for testing
type ExportRuntimeStatsWithCommit = RuntimeStatsWithCommit

// ExportRURuntimeStats exports RURuntimeStats type for testing
type ExportRURuntimeStats = RURuntimeStats

// ExportConcurrencyInfo exports ConcurrencyInfo type for testing
type ExportConcurrencyInfo = ConcurrencyInfo

// ExportNewConcurrencyInfo exports NewConcurrencyInfo function for testing
func ExportNewConcurrencyInfo(worker string, concurrency int) *ConcurrencyInfo {
	return NewConcurrencyInfo(worker, concurrency)
}

// ExportGetStats exports the stats field from CopRuntimeStats for testing
func ExportGetStats(cop *CopRuntimeStats) *basicCopRuntimeStats {
	return &cop.stats
}

// ExportGetScanDetail exports the scanDetail field from CopRuntimeStats for testing
func ExportGetScanDetail(cop *CopRuntimeStats) util.ScanDetail {
	return cop.scanDetail
}

// ExportGetCopStats exports GetCopStats method from RuntimeStatsColl for testing
func ExportGetCopStats(coll *RuntimeStatsColl, planID int) *CopRuntimeStats {
	return coll.GetCopStats(planID)
}
