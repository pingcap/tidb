// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

const (
	namespace = "tidb"
	subsystem = "server"
)

var (
	hotTblIdxFlowBytesDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, "hot_table_index_flow_bytes"),
		"The total flow bytes for hot table/index.",
		[]string{"schema", "name", "index", "rw"}, nil,
	)
	hotTblIdxRegionCountDesc = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, subsystem, "hot_table_index_region_count"),
		"The hot region count for hot table/index.",
		[]string{"schema", "name", "index", "rw"}, nil,
	)
)

type tidbPromsExporter struct {
	*tikvHandlerTool
	totalScrapes prometheus.Counter
	errorScrapes prometheus.Counter
}

func newTiDBPromsExporter(handler *tikvHandlerTool) *tidbPromsExporter {
	return &tidbPromsExporter{
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "tidb_exporter_scrapes_total",
			Help:      "Total number of times TiDB hot region was scraped for metrics.",
		}),
		errorScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "tidb_exporter_scrapes_error",
			Help:      "Total number of times TiDB hot region was scraped failed for metrics.",
		}),
		tikvHandlerTool: handler,
	}
}

// Describe implements prometheus.Collector.
func (e *tidbPromsExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.totalScrapes.Desc()
	ch <- e.errorScrapes.Desc()
}

// Collect implements prometheus.Collector.
func (e *tidbPromsExporter) Collect(ch chan<- prometheus.Metric) {
	e.scrapeHotTableIndex(ch)
	ch <- e.totalScrapes
	ch <- e.errorScrapes
}

var hotTableIndexCfg = []struct {
	endpoint string
	op       string
}{
	{hotRead, "r"},
	{hotWrite, "w"},
}

func (e *tidbPromsExporter) scrapeHotTableIndex(mChan chan<- prometheus.Metric) {
	for _, task := range hotTableIndexCfg {
		hotReadInfo, err := e.scrapeHotInfo(task.endpoint)
		if err != nil {
			log.Error(err)
			e.errorScrapes.Inc()
			return
		}
		for idx, m := range hotReadInfo {
			mChan <- prometheus.MustNewConstMetric(
				hotTblIdxFlowBytesDesc, prometheus.CounterValue, float64(m.FlowBytes),
				idx.DbName, idx.TableName, idx.IndexName, task.op,
			)
			mChan <- prometheus.MustNewConstMetric(
				hotTblIdxRegionCountDesc, prometheus.CounterValue, float64(m.Count),
				idx.DbName, idx.TableName, idx.IndexName, task.op,
			)
		}
	}
}
