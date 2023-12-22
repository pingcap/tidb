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

	QueryTotalCountSQLTypeBreakdownOk  map[string]prometheus.Counter
	QueryTotalCountSQLTypeBreakdownErr map[string]prometheus.Counter

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
		mysql.ComSleep:            metrics.QueryTotalCounter.WithLabelValues("Sleep", "OK", "Other"),
		mysql.ComQuit:             metrics.QueryTotalCounter.WithLabelValues("Quit", "OK", "Other"),
		mysql.ComInitDB:           metrics.QueryTotalCounter.WithLabelValues("InitDB", "OK", "Other"),
		mysql.ComQuery:            metrics.QueryTotalCounter.WithLabelValues("Query", "OK", "Other"),
		mysql.ComPing:             metrics.QueryTotalCounter.WithLabelValues("Ping", "OK", "Other"),
		mysql.ComFieldList:        metrics.QueryTotalCounter.WithLabelValues("FieldList", "OK", "Other"),
		mysql.ComStmtPrepare:      metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "OK", "Other"),
		mysql.ComStmtExecute:      metrics.QueryTotalCounter.WithLabelValues("StmtExecute", "OK", "Other"),
		mysql.ComStmtFetch:        metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "OK", "Other"),
		mysql.ComStmtClose:        metrics.QueryTotalCounter.WithLabelValues("StmtClose", "OK", "Other"),
		mysql.ComStmtSendLongData: metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "OK", "Other"),
		mysql.ComStmtReset:        metrics.QueryTotalCounter.WithLabelValues("StmtReset", "OK", "Other"),
		mysql.ComSetOption:        metrics.QueryTotalCounter.WithLabelValues("SetOption", "OK", "Other"),
	}
	QueryTotalCountErr = []prometheus.Counter{
		mysql.ComSleep:            metrics.QueryTotalCounter.WithLabelValues("Sleep", "Error", "Other"),
		mysql.ComQuit:             metrics.QueryTotalCounter.WithLabelValues("Quit", "Error", "Other"),
		mysql.ComInitDB:           metrics.QueryTotalCounter.WithLabelValues("InitDB", "Error", "Other"),
		mysql.ComQuery:            metrics.QueryTotalCounter.WithLabelValues("Query", "Error", "Other"),
		mysql.ComPing:             metrics.QueryTotalCounter.WithLabelValues("Ping", "Error", "Other"),
		mysql.ComFieldList:        metrics.QueryTotalCounter.WithLabelValues("FieldList", "Error", "Other"),
		mysql.ComStmtPrepare:      metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "Error", "Other"),
		mysql.ComStmtExecute:      metrics.QueryTotalCounter.WithLabelValues("StmtExecute", "Error", "Other"),
		mysql.ComStmtFetch:        metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "Error", "Other"),
		mysql.ComStmtClose:        metrics.QueryTotalCounter.WithLabelValues("StmtClose", "Error", "Other"),
		mysql.ComStmtSendLongData: metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "Error", "Other"),
		mysql.ComStmtReset:        metrics.QueryTotalCounter.WithLabelValues("StmtReset", "Error", "Other"),
		mysql.ComSetOption:        metrics.QueryTotalCounter.WithLabelValues("SetOption", "Error", "Other"),
	}

	QueryTotalCountSQLTypeBreakdownOk = map[string]prometheus.Counter{
		"Insert":  metrics.QueryTotalCounter.WithLabelValues("Query", "OK", "Insert"),
		"Replace": metrics.QueryTotalCounter.WithLabelValues("Query", "OK", "Replace"),
		"Delete":  metrics.QueryTotalCounter.WithLabelValues("Query", "OK", "Delete"),
		"Update":  metrics.QueryTotalCounter.WithLabelValues("Query", "OK", "Update"),
		"Select":  metrics.QueryTotalCounter.WithLabelValues("Query", "OK", "Select"),
	}

	QueryTotalCountSQLTypeBreakdownErr = map[string]prometheus.Counter{
		"Insert":  metrics.QueryTotalCounter.WithLabelValues("Query", "Error", "Insert"),
		"Replace": metrics.QueryTotalCounter.WithLabelValues("Query", "Error", "Replace"),
		"Delete":  metrics.QueryTotalCounter.WithLabelValues("Query", "Error", "Delete"),
		"Update":  metrics.QueryTotalCounter.WithLabelValues("Query", "Error", "Update"),
		"Select":  metrics.QueryTotalCounter.WithLabelValues("Query", "Error", "Select"),
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
