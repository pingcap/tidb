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

import "github.com/pingcap/tidb/metrics"

var (
	ignoreExceedSQLCounter              = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_exceed_sql")
	ignoreExceedPlanCounter             = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_exceed_plan")
	ignoreCollectChannelFullCounter     = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_collect_channel_full")
	ignoreCollectStmtChannelFullCounter = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_collect_stmt_channel_full")
	ignoreReportChannelFullCounter      = metrics.TopSQLIgnoredCounter.WithLabelValues("ignore_report_channel_full")
	reportAllDurationSuccHistogram      = metrics.TopSQLReportDurationHistogram.WithLabelValues("all", metrics.LblOK)
	reportAllDurationFailedHistogram    = metrics.TopSQLReportDurationHistogram.WithLabelValues("all", metrics.LblError)
	reportRecordDurationSuccHistogram   = metrics.TopSQLReportDurationHistogram.WithLabelValues("record", metrics.LblOK)
	reportRecordDurationFailedHistogram = metrics.TopSQLReportDurationHistogram.WithLabelValues("record", metrics.LblError)
	reportSQLDurationSuccHistogram      = metrics.TopSQLReportDurationHistogram.WithLabelValues("sql", metrics.LblOK)
	reportSQLDurationFailedHistogram    = metrics.TopSQLReportDurationHistogram.WithLabelValues("sql", metrics.LblError)
	reportPlanDurationSuccHistogram     = metrics.TopSQLReportDurationHistogram.WithLabelValues("plan", metrics.LblOK)
	reportPlanDurationFailedHistogram   = metrics.TopSQLReportDurationHistogram.WithLabelValues("plan", metrics.LblError)
	topSQLReportRecordCounterHistogram  = metrics.TopSQLReportDataHistogram.WithLabelValues("record")
	topSQLReportSQLCountHistogram       = metrics.TopSQLReportDataHistogram.WithLabelValues("sql")
	topSQLReportPlanCountHistogram      = metrics.TopSQLReportDataHistogram.WithLabelValues("plan")
)
