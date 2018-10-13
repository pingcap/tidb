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

type hotTableIndexExporter struct {
	*tikvHandlerTool
	totalScrapes prometheus.Counter
	errorScrapes prometheus.Counter
}

func newHotTableIndexExport(handler *tikvHandlerTool) *hotTableIndexExporter {
	return &hotTableIndexExporter{
		totalScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "hot_region_scrapes_total",
			Help:      "Total number of times TiDB hot region was scraped for metrics.",
		}),
		errorScrapes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "hot_region_scrapes_error",
			Help:      "Total number of times TiDB hot region was scraped failed for metrics.",
		}),
		tikvHandlerTool: handler,
	}
}

// Describe implements prometheus.Collector.
func (e *hotTableIndexExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- e.totalScrapes.Desc()
	ch <- e.errorScrapes.Desc()
}

// Collect implements prometheus.Collector.
func (e *hotTableIndexExporter) Collect(ch chan<- prometheus.Metric) {
	e.scrape(ch)
	ch <- e.totalScrapes
	ch <- e.errorScrapes
}

func (e *hotTableIndexExporter) scrape(mChan chan<- prometheus.Metric) {
	e.scrapeOne(mChan, hotRead, "r")
	e.scrapeOne(mChan, hotWrite, "w")
}

func (e *hotTableIndexExporter) scrapeOne(mChan chan<- prometheus.Metric, rw, op string) {
	hotReadInfo, err := e.scrapeHotInfo(rw)
	if err != nil {
		log.Error(err)
		e.errorScrapes.Inc()
		return
	}
	for idx, m := range hotReadInfo {
		mChan <- prometheus.MustNewConstMetric(
			hotTblIdxFlowBytesDesc, prometheus.CounterValue, float64(m.FlowBytes),
			idx.DbName, idx.TableName, idx.IndexName, op,
		)
		mChan <- prometheus.MustNewConstMetric(
			hotTblIdxRegionCountDesc, prometheus.CounterValue, float64(m.Count),
			idx.DbName, idx.TableName, idx.IndexName, op,
		)
	}
}
