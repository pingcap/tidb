// Copyright 2020 PingCAP, Inc.
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

package telemetry

import (
	"context"
	"time"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/sessionctx"
)

type telemetryData struct {
	Hardware           []*clusterHardwareItem  `json:"hardware"`
	Instances          []*clusterInfoItem      `json:"instances"`
	TelemetryHostExtra *telemetryHostExtraInfo `json:"hostExtra"`
	ReportTimestamp    int64                   `json:"reportTimestamp"`
	TrackingID         string                  `json:"trackingId"`
	FeatureUsage       *featureUsage           `json:"featureUsage"`
	WindowedStats      []*windowData           `json:"windowedStats"`
	SlowQueryStats     *slowQueryStats         `json:"slowQueryStats"`
}

func generateTelemetryData(sctx sessionctx.Context, trackingID string) telemetryData {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTelemetry)
	r := telemetryData{
		ReportTimestamp: time.Now().Unix(),
		TrackingID:      trackingID,
	}
	if h, err := getClusterHardware(ctx, sctx); err == nil {
		r.Hardware = h
	}
	if i, err := getClusterInfo(ctx, sctx); err == nil {
		r.Instances = i
	}
	if f, err := getFeatureUsage(ctx, sctx); err == nil {
		r.FeatureUsage = f
	}
	if s, err := getSlowQueryStats(); err == nil {
		r.SlowQueryStats = s
	}

	r.WindowedStats = getWindowData()
	r.TelemetryHostExtra = getTelemetryHostExtraInfo()
	return r
}

func postReportTelemetryData() {
	postReportTxnUsage()
	postReportCTEUsage()
	postReportAccountLockUsage()
	postReportMultiSchemaChangeUsage()
	postReportExchangePartitionUsage()
	postReportTablePartitionUsage()
	postReportSlowQueryStats()
	postReportNonTransactionalCounter()
	PostSavepointCount()
	postReportLazyPessimisticUniqueCheckSetCount()
	postReportDDLUsage()
	postReportIndexMergeUsage()
	postStoreBatchUsage()
	postReportAggressiveLockingUsageCounter()
}

// PostReportTelemetryDataForTest is for test.
func PostReportTelemetryDataForTest() {
	postReportTablePartitionUsage()
}
