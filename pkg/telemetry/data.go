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

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/sessionctx"
)

type telemetryData struct {
	ReportTimestamp int64         `json:"reportTimestamp"`
	FeatureUsage    *featureUsage `json:"featureUsage"`
	WindowedStats   []*windowData `json:"windowedStats"`
}

func generateTelemetryData(sctx sessionctx.Context) telemetryData {
	ctx := kv.WithInternalSourceType(context.Background(), kv.InternalTxnTelemetry)
	r := telemetryData{
		ReportTimestamp: time.Now().Unix(),
	}
	if f, err := getFeatureUsage(ctx, sctx); err == nil {
		r.FeatureUsage = f
	}

	r.WindowedStats = getWindowData()
	return r
}

func postReportTelemetryData() {
	postReportTxnUsage()
	postReportCTEUsage()
	postReportAccountLockUsage()
	postReportMultiSchemaChangeUsage()
	postReportExchangePartitionUsage()
	postReportTablePartitionUsage()
	postReportNonTransactionalCounter()
	PostSavepointCount()
	postReportLazyPessimisticUniqueCheckSetCount()
	postReportDDLUsage()
	postReportIndexMergeUsage()
	postStoreBatchUsage()
	postReportFairLockingUsageCounter()
}

// PostReportTelemetryDataForTest is for test.
func PostReportTelemetryDataForTest() {
	postReportTablePartitionUsage()
}
