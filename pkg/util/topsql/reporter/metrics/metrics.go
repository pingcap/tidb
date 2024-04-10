// Copyright 2021 PingCAP, Inc.
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

package reporter

import (
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// reporter metrics vars
var (
	IgnoreExceedSQLCounter              prometheus.Counter
	IgnoreExceedPlanCounter             prometheus.Counter
	IgnoreCollectChannelFullCounter     prometheus.Counter
	IgnoreCollectStmtChannelFullCounter prometheus.Counter
	IgnoreReportChannelFullCounter      prometheus.Counter
	ReportAllDurationSuccHistogram      prometheus.Observer
	ReportAllDurationFailedHistogram    prometheus.Observer
	ReportRecordDurationSuccHistogram   prometheus.Observer
	ReportRecordDurationFailedHistogram prometheus.Observer
	ReportSQLDurationSuccHistogram      prometheus.Observer
	ReportSQLDurationFailedHistogram    prometheus.Observer
	ReportPlanDurationSuccHistogram     prometheus.Observer
	ReportPlanDurationFailedHistogram   prometheus.Observer
	TopSQLReportRecordCounterHistogram  prometheus.Observer
	TopSQLReportSQLCountHistogram       prometheus.Observer
	TopSQLReportPlanCountHistogram      prometheus.Observer
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init topsql reporter metrics vars.
func InitMetricsVars() {
	IgnoreExceedSQLCounter = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_exceed_sql")
	IgnoreExceedPlanCounter = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_exceed_plan")
	IgnoreCollectChannelFullCounter = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_collect_channel_full")
	IgnoreCollectStmtChannelFullCounter = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_collect_stmt_channel_full")
	IgnoreReportChannelFullCounter = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_report_channel_full")
	ReportAllDurationSuccHistogram = metrics.TopSQLReportDurationHistogram.WithLabelValues("all", metrics.LblOK)
	ReportAllDurationFailedHistogram = metrics.TopSQLReportDurationHistogram.WithLabelValues("all", metrics.LblError)
	ReportRecordDurationSuccHistogram = metrics.TopSQLReportDurationHistogram.WithLabelValues("record", metrics.LblOK)
	ReportRecordDurationFailedHistogram = metrics.TopSQLReportDurationHistogram.WithLabelValues("record", metrics.LblError)
	ReportSQLDurationSuccHistogram = metrics.TopSQLReportDurationHistogram.WithLabelValues("sql", metrics.LblOK)
	ReportSQLDurationFailedHistogram = metrics.TopSQLReportDurationHistogram.WithLabelValues("sql", metrics.LblError)
	ReportPlanDurationSuccHistogram = metrics.TopSQLReportDurationHistogram.WithLabelValues("plan", metrics.LblOK)
	ReportPlanDurationFailedHistogram = metrics.TopSQLReportDurationHistogram.WithLabelValues("plan", metrics.LblError)
	TopSQLReportRecordCounterHistogram = metrics.TopSQLReportDataHistogram.WithLabelValues("record")
	TopSQLReportSQLCountHistogram = metrics.TopSQLReportDataHistogram.WithLabelValues("sql")
	TopSQLReportPlanCountHistogram = metrics.TopSQLReportDataHistogram.WithLabelValues("plan")
}
