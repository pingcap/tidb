// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import (
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/prometheus/client_golang/prometheus"
)

// server metrics vars
var (
	QueryTotalCountOk  []prometheus.Counter
	QueryTotalCountErr []prometheus.Counter

	DisconnectNormal            prometheus.Counter
	DisconnectByClientWithError prometheus.Counter
	DisconnectErrorUndetermined prometheus.Counter

	ConnIdleDurationHistogramNotInTxn prometheus.Observer
	ConnIdleDurationHistogramInTxn    prometheus.Observer

	AffectedRowsCounterInsert  prometheus.Counter
	AffectedRowsCounterUpdate  prometheus.Counter
	AffectedRowsCounterDelete  prometheus.Counter
	AffectedRowsCounterReplace prometheus.Counter

	InPacketBytes  prometheus.Counter
	OutPacketBytes prometheus.Counter
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init server metrics vars.
func InitMetricsVars() {
	QueryTotalCountOk = []prometheus.Counter{
		mysql.ComSleep:            metrics.QueryTotalCounter.WithLabelValues("Sleep", "OK", "default"),
		mysql.ComQuit:             metrics.QueryTotalCounter.WithLabelValues("Quit", "OK", "default"),
		mysql.ComInitDB:           metrics.QueryTotalCounter.WithLabelValues("InitDB", "OK", "default"),
		mysql.ComQuery:            metrics.QueryTotalCounter.WithLabelValues("Query", "OK", "default"),
		mysql.ComPing:             metrics.QueryTotalCounter.WithLabelValues("Ping", "OK", "default"),
		mysql.ComFieldList:        metrics.QueryTotalCounter.WithLabelValues("FieldList", "OK", "default"),
		mysql.ComStmtPrepare:      metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "OK", "default"),
		mysql.ComStmtExecute:      metrics.QueryTotalCounter.WithLabelValues("StmtExecute", "OK", "default"),
		mysql.ComStmtFetch:        metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "OK", "default"),
		mysql.ComStmtClose:        metrics.QueryTotalCounter.WithLabelValues("StmtClose", "OK", "default"),
		mysql.ComStmtSendLongData: metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "OK", "default"),
		mysql.ComStmtReset:        metrics.QueryTotalCounter.WithLabelValues("StmtReset", "OK", "default"),
		mysql.ComSetOption:        metrics.QueryTotalCounter.WithLabelValues("SetOption", "OK", "default"),
	}
	QueryTotalCountErr = []prometheus.Counter{
		mysql.ComSleep:            metrics.QueryTotalCounter.WithLabelValues("Sleep", "Error", "default"),
		mysql.ComQuit:             metrics.QueryTotalCounter.WithLabelValues("Quit", "Error", "default"),
		mysql.ComInitDB:           metrics.QueryTotalCounter.WithLabelValues("InitDB", "Error", "default"),
		mysql.ComQuery:            metrics.QueryTotalCounter.WithLabelValues("Query", "Error", "default"),
		mysql.ComPing:             metrics.QueryTotalCounter.WithLabelValues("Ping", "Error", "default"),
		mysql.ComFieldList:        metrics.QueryTotalCounter.WithLabelValues("FieldList", "Error", "default"),
		mysql.ComStmtPrepare:      metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "Error", "default"),
		mysql.ComStmtExecute:      metrics.QueryTotalCounter.WithLabelValues("StmtExecute", "Error", "default"),
		mysql.ComStmtFetch:        metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "Error", "default"),
		mysql.ComStmtClose:        metrics.QueryTotalCounter.WithLabelValues("StmtClose", "Error", "default"),
		mysql.ComStmtSendLongData: metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "Error", "default"),
		mysql.ComStmtReset:        metrics.QueryTotalCounter.WithLabelValues("StmtReset", "Error", "default"),
		mysql.ComSetOption:        metrics.QueryTotalCounter.WithLabelValues("SetOption", "Error", "default"),
	}

	DisconnectNormal = metrics.DisconnectionCounter.WithLabelValues(metrics.LblOK)
	DisconnectByClientWithError = metrics.DisconnectionCounter.WithLabelValues(metrics.LblError)
	DisconnectErrorUndetermined = metrics.DisconnectionCounter.WithLabelValues("undetermined")

	ConnIdleDurationHistogramNotInTxn = metrics.ConnIdleDurationHistogram.WithLabelValues("0")
	ConnIdleDurationHistogramInTxn = metrics.ConnIdleDurationHistogram.WithLabelValues("1")

	AffectedRowsCounterInsert = metrics.AffectedRowsCounter.WithLabelValues("Insert")
	AffectedRowsCounterUpdate = metrics.AffectedRowsCounter.WithLabelValues("Update")
	AffectedRowsCounterDelete = metrics.AffectedRowsCounter.WithLabelValues("Delete")
	AffectedRowsCounterReplace = metrics.AffectedRowsCounter.WithLabelValues("Replace")

	InPacketBytes = metrics.PacketIOCounter.WithLabelValues("In")
	OutPacketBytes = metrics.PacketIOCounter.WithLabelValues("Out")
}
