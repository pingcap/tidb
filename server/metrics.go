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

package server

import (
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	queryTotalCountOk  []prometheus.Counter
	queryTotalCountErr []prometheus.Counter

	disconnectNormal            prometheus.Counter
	disconnectByClientWithError prometheus.Counter
	disconnectErrorUndetermined prometheus.Counter

	connIdleDurationHistogramNotInTxn prometheus.Observer
	connIdleDurationHistogramInTxn    prometheus.Observer

	affectedRowsCounterInsert  prometheus.Counter
	affectedRowsCounterUpdate  prometheus.Counter
	affectedRowsCounterDelete  prometheus.Counter
	affectedRowsCounterReplace prometheus.Counter

	readPacketBytes  prometheus.Counter
	writePacketBytes prometheus.Counter
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init server metrics vars.
func InitMetricsVars() {
	queryTotalCountOk = []prometheus.Counter{
		mysql.ComSleep:            metrics.QueryTotalCounter.WithLabelValues("Sleep", "OK"),
		mysql.ComQuit:             metrics.QueryTotalCounter.WithLabelValues("Quit", "OK"),
		mysql.ComInitDB:           metrics.QueryTotalCounter.WithLabelValues("InitDB", "OK"),
		mysql.ComQuery:            metrics.QueryTotalCounter.WithLabelValues("Query", "OK"),
		mysql.ComPing:             metrics.QueryTotalCounter.WithLabelValues("Ping", "OK"),
		mysql.ComFieldList:        metrics.QueryTotalCounter.WithLabelValues("FieldList", "OK"),
		mysql.ComStmtPrepare:      metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "OK"),
		mysql.ComStmtExecute:      metrics.QueryTotalCounter.WithLabelValues("StmtExecute", "OK"),
		mysql.ComStmtFetch:        metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "OK"),
		mysql.ComStmtClose:        metrics.QueryTotalCounter.WithLabelValues("StmtClose", "OK"),
		mysql.ComStmtSendLongData: metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "OK"),
		mysql.ComStmtReset:        metrics.QueryTotalCounter.WithLabelValues("StmtReset", "OK"),
		mysql.ComSetOption:        metrics.QueryTotalCounter.WithLabelValues("SetOption", "OK"),
	}
	queryTotalCountErr = []prometheus.Counter{
		mysql.ComSleep:            metrics.QueryTotalCounter.WithLabelValues("Sleep", "Error"),
		mysql.ComQuit:             metrics.QueryTotalCounter.WithLabelValues("Quit", "Error"),
		mysql.ComInitDB:           metrics.QueryTotalCounter.WithLabelValues("InitDB", "Error"),
		mysql.ComQuery:            metrics.QueryTotalCounter.WithLabelValues("Query", "Error"),
		mysql.ComPing:             metrics.QueryTotalCounter.WithLabelValues("Ping", "Error"),
		mysql.ComFieldList:        metrics.QueryTotalCounter.WithLabelValues("FieldList", "Error"),
		mysql.ComStmtPrepare:      metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "Error"),
		mysql.ComStmtExecute:      metrics.QueryTotalCounter.WithLabelValues("StmtExecute", "Error"),
		mysql.ComStmtFetch:        metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "Error"),
		mysql.ComStmtClose:        metrics.QueryTotalCounter.WithLabelValues("StmtClose", "Error"),
		mysql.ComStmtSendLongData: metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "Error"),
		mysql.ComStmtReset:        metrics.QueryTotalCounter.WithLabelValues("StmtReset", "Error"),
		mysql.ComSetOption:        metrics.QueryTotalCounter.WithLabelValues("SetOption", "Error"),
	}

	disconnectNormal = metrics.DisconnectionCounter.WithLabelValues(metrics.LblOK)
	disconnectByClientWithError = metrics.DisconnectionCounter.WithLabelValues(metrics.LblError)
	disconnectErrorUndetermined = metrics.DisconnectionCounter.WithLabelValues("undetermined")

	connIdleDurationHistogramNotInTxn = metrics.ConnIdleDurationHistogram.WithLabelValues("0")
	connIdleDurationHistogramInTxn = metrics.ConnIdleDurationHistogram.WithLabelValues("1")

	affectedRowsCounterInsert = metrics.AffectedRowsCounter.WithLabelValues("Insert")
	affectedRowsCounterUpdate = metrics.AffectedRowsCounter.WithLabelValues("Update")
	affectedRowsCounterDelete = metrics.AffectedRowsCounter.WithLabelValues("Delete")
	affectedRowsCounterReplace = metrics.AffectedRowsCounter.WithLabelValues("Replace")

	readPacketBytes = metrics.PacketIOCounter.WithLabelValues("read")
	writePacketBytes = metrics.PacketIOCounter.WithLabelValues("write")
}
