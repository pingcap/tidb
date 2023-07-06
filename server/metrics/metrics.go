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
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/mysql"
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

	ReadPacketBytes  prometheus.Counter
	WritePacketBytes prometheus.Counter
)

func init() {
	InitMetricsVars()
}

// InitMetricsVars init server metrics vars.
func InitMetricsVars() {
	QueryTotalCountOk = []prometheus.Counter{
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
	QueryTotalCountErr = []prometheus.Counter{
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

	DisconnectNormal = metrics.DisconnectionCounter.WithLabelValues(metrics.LblOK)
	DisconnectByClientWithError = metrics.DisconnectionCounter.WithLabelValues(metrics.LblError)
	DisconnectErrorUndetermined = metrics.DisconnectionCounter.WithLabelValues("undetermined")

	ConnIdleDurationHistogramNotInTxn = metrics.ConnIdleDurationHistogram.WithLabelValues("0")
	ConnIdleDurationHistogramInTxn = metrics.ConnIdleDurationHistogram.WithLabelValues("1")

	AffectedRowsCounterInsert = metrics.AffectedRowsCounter.WithLabelValues("Insert")
	AffectedRowsCounterUpdate = metrics.AffectedRowsCounter.WithLabelValues("Update")
	AffectedRowsCounterDelete = metrics.AffectedRowsCounter.WithLabelValues("Delete")
	AffectedRowsCounterReplace = metrics.AffectedRowsCounter.WithLabelValues("Replace")

	ReadPacketBytes = metrics.PacketIOCounter.WithLabelValues("read")
	WritePacketBytes = metrics.PacketIOCounter.WithLabelValues("write")
}
