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
<<<<<<< HEAD:server/metrics/metrics.go
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/mysql"
=======
	"github.com/pingcap/tidb/pkg/domain/resourcegroup"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/parser/mysql"
>>>>>>> e80385270c9 (metrics: add connection and fail metrics  by `resource group name` (#49424)):pkg/server/metrics/metrics.go
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

	ReadPacketBytes = metrics.PacketIOCounter.WithLabelValues("read")
	WritePacketBytes = metrics.PacketIOCounter.WithLabelValues("write")
}
