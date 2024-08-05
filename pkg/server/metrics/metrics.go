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
	"strconv"

	"github.com/pingcap/tidb/pkg/domain/resourcegroup"
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

// CmdToString convert command type to string.
func CmdToString(cmd byte) string {
	switch cmd {
	case mysql.ComSleep:
		return "Sleep"
	case mysql.ComQuit:
		return "Quit"
	case mysql.ComInitDB:
		return "InitDB"
	case mysql.ComQuery:
		return "Query"
	case mysql.ComPing:
		return "Ping"
	case mysql.ComFieldList:
		return "FieldList"
	case mysql.ComStmtPrepare:
		return "StmtPrepare"
	case mysql.ComStmtExecute:
		return "StmtExecute"
	case mysql.ComStmtFetch:
		return "StmtFetch"
	case mysql.ComStmtClose:
		return "StmtClose"
	case mysql.ComStmtSendLongData:
		return "StmtSendLongData"
	case mysql.ComStmtReset:
		return "StmtReset"
	case mysql.ComSetOption:
		return "SetOption"
	}
	return strconv.Itoa(int(cmd))
}

// InitMetricsVars init server metrics vars.
func InitMetricsVars() {
	QueryTotalCountOk = []prometheus.Counter{
		mysql.ComSleep:            metrics.QueryTotalCounter.WithLabelValues("Sleep", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComQuit:             metrics.QueryTotalCounter.WithLabelValues("Quit", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComInitDB:           metrics.QueryTotalCounter.WithLabelValues("InitDB", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComQuery:            metrics.QueryTotalCounter.WithLabelValues("Query", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComPing:             metrics.QueryTotalCounter.WithLabelValues("Ping", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComFieldList:        metrics.QueryTotalCounter.WithLabelValues("FieldList", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtPrepare:      metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtExecute:      metrics.QueryTotalCounter.WithLabelValues("StmtExecute", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtFetch:        metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtClose:        metrics.QueryTotalCounter.WithLabelValues("StmtClose", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtSendLongData: metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtReset:        metrics.QueryTotalCounter.WithLabelValues("StmtReset", "OK", resourcegroup.DefaultResourceGroupName),
		mysql.ComSetOption:        metrics.QueryTotalCounter.WithLabelValues("SetOption", "OK", resourcegroup.DefaultResourceGroupName),
	}
	QueryTotalCountErr = []prometheus.Counter{
		mysql.ComSleep:            metrics.QueryTotalCounter.WithLabelValues("Sleep", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComQuit:             metrics.QueryTotalCounter.WithLabelValues("Quit", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComInitDB:           metrics.QueryTotalCounter.WithLabelValues("InitDB", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComQuery:            metrics.QueryTotalCounter.WithLabelValues("Query", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComPing:             metrics.QueryTotalCounter.WithLabelValues("Ping", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComFieldList:        metrics.QueryTotalCounter.WithLabelValues("FieldList", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtPrepare:      metrics.QueryTotalCounter.WithLabelValues("StmtPrepare", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtExecute:      metrics.QueryTotalCounter.WithLabelValues("StmtExecute", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtFetch:        metrics.QueryTotalCounter.WithLabelValues("StmtFetch", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtClose:        metrics.QueryTotalCounter.WithLabelValues("StmtClose", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtSendLongData: metrics.QueryTotalCounter.WithLabelValues("StmtSendLongData", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComStmtReset:        metrics.QueryTotalCounter.WithLabelValues("StmtReset", "Error", resourcegroup.DefaultResourceGroupName),
		mysql.ComSetOption:        metrics.QueryTotalCounter.WithLabelValues("SetOption", "Error", resourcegroup.DefaultResourceGroupName),
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
