// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// TestStatusHandlerReportsProgress pins that the endpoint answers with the
// same numbers the progress log carries, in a shape a caller can compute with
// rather than parse out of a log line.
func TestStatusHandlerReportsProgress(t *testing.T) {
	conf := defaultConfigForTest(t)
	d := &Dumper{conf: conf, speedRecorder: NewSpeedRecorder()}
	d.metrics = newMetrics(conf.PromFactory, nil)

	AddCounter(d.metrics.finishedTablesCounter, 3)
	AddGauge(d.metrics.finishedSizeGauge, 4096)
	AddGauge(d.metrics.finishedRowsGauge, 250)
	AddCounter(d.metrics.estimateTotalRowsCounter, 1000)
	d.metrics.totalChunks.Store(8)
	d.metrics.completedChunks.Store(2)
	d.metrics.progressReady.Store(true)
	d.RefreshStatus()

	rec := httptest.NewRecorder()
	statusHandler(tcontext.Background(), d)(rec, httptest.NewRequest(http.MethodGet, "/status", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var got DumpStatus
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.EqualValues(t, 3, got.CompletedTables)
	require.EqualValues(t, 4096, got.FinishedBytes)
	require.EqualValues(t, 250, got.FinishedRows)
	require.EqualValues(t, 1000, got.EstimateTotalRows)
	require.NotNil(t, got.ProgressPercent)
	require.InDelta(t, 25, *got.ProgressPercent, 1e-9)
}

// TestStatusHandlerOmitsProgressBeforeChunksAreCounted covers the window
// before the chunk count is known. Reporting zero there would say no work had
// been done, which is a different claim from "the answer is not available
// yet", so the field is absent instead.
func TestStatusHandlerOmitsProgressBeforeChunksAreCounted(t *testing.T) {
	conf := defaultConfigForTest(t)
	d := &Dumper{conf: conf, speedRecorder: NewSpeedRecorder()}
	d.metrics = newMetrics(conf.PromFactory, nil)

	rec := httptest.NewRecorder()
	statusHandler(tcontext.Background(), d)(rec, httptest.NewRequest(http.MethodGet, "/status", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))
	require.NotContains(t, raw, "progressPercent")
	require.NotContains(t, raw, "progress")
}

func TestStatusHandlerDoesNotUpdateSpeed(t *testing.T) {
	conf := defaultConfigForTest(t)
	d := &Dumper{conf: conf, speedRecorder: NewSpeedRecorder()}
	d.metrics = newMetrics(conf.PromFactory, nil)
	lastUpdateTime := d.speedRecorder.lastUpdateTime

	for range 2 {
		AddGauge(d.metrics.finishedSizeGauge, 4096)
		rec := httptest.NewRecorder()
		statusHandler(tcontext.Background(), d)(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, lastUpdateTime, d.speedRecorder.lastUpdateTime)
		require.Zero(t, d.speedRecorder.lastFinished)
		require.Zero(t, d.speedRecorder.speedBPS)
	}

	d.RefreshStatus()
	snapshot := d.GetStatus()
	lastUpdateTime = d.speedRecorder.lastUpdateTime
	AddGauge(d.metrics.finishedSizeGauge, 4096)
	responses := make(chan *httptest.ResponseRecorder, 8)
	for range cap(responses) {
		go func() {
			rec := httptest.NewRecorder()
			statusHandler(tcontext.Background(), d)(rec, httptest.NewRequest(http.MethodGet, "/status", nil))
			responses <- rec
		}()
	}
	for range cap(responses) {
		rec := <-responses
		require.Equal(t, http.StatusOK, rec.Code)
		var got DumpStatus
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
		require.Equal(t, *snapshot, got)
	}
	require.Equal(t, lastUpdateTime, d.speedRecorder.lastUpdateTime)
	require.Equal(t, snapshot.FinishedBytes, d.speedRecorder.lastFinished)
	require.Equal(t, snapshot.CurrentSpeedBPS, d.speedRecorder.speedBPS)
}

// TestMetricsHandlerServesTheDumperRegistry pins the fix for an endpoint that
// used to answer every scrape without a single dump metric: the counters are
// registered with the registry the config names, and the handler was serving
// the process-wide default one, which nothing registers them with.
func TestMetricsHandlerServesTheDumperRegistry(t *testing.T) {
	conf := defaultConfigForTest(t)
	d := &Dumper{conf: conf, speedRecorder: NewSpeedRecorder()}
	d.metrics = newMetrics(conf.PromFactory, nil)
	d.metrics.registerTo(conf.PromRegistry)
	defer d.metrics.unregisterFrom(conf.PromRegistry)

	AddGauge(d.metrics.finishedRowsGauge, 42)

	rec := httptest.NewRecorder()
	metricsHandler(d).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	require.Equal(t, http.StatusOK, rec.Code)
	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), "dumpling_dump_finished_rows 42")
	require.Contains(t, string(body), "go_goroutines ")
	require.Contains(t, string(body), "process_cpu_seconds_total ")
	require.Contains(t, string(body), "promhttp_metric_handler_requests_total")
}

func TestMetricsHandlerPreservesConfiguredMetricFamilies(t *testing.T) {
	conf := defaultConfigForTest(t)
	d := &Dumper{conf: conf}
	d.metrics = newMetrics(conf.PromFactory, nil)
	d.metrics.registerTo(conf.PromRegistry)
	defer d.metrics.unregisterFrom(conf.PromRegistry)
	// Both registries expose this name. The configured family must win
	// without producing duplicate-metric errors on the scrape.
	configured := prometheus.NewGauge(prometheus.GaugeOpts{Name: "go_goroutines", Help: "Configured test value."})
	configured.Set(123)
	conf.PromRegistry.MustRegister(configured)

	rec := httptest.NewRecorder()
	metricsHandler(d).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "go_goroutines 123\n")
	require.Contains(t, rec.Body.String(), "process_cpu_seconds_total ")
}

func TestMetricsHandlerWithSharedDefaultGatherer(t *testing.T) {
	conf := defaultConfigForTest(t)
	d := &Dumper{conf: conf}
	d.metrics = newMetrics(conf.PromFactory, nil)
	d.metrics.registerTo(conf.PromRegistry)
	defer d.metrics.unregisterFrom(conf.PromRegistry)
	previous := prometheus.DefaultGatherer
	prometheus.DefaultGatherer = conf.PromRegistry.(prometheus.Gatherer)
	t.Cleanup(func() { prometheus.DefaultGatherer = previous })
	AddGauge(d.metrics.finishedRowsGauge, 42)

	rec := httptest.NewRecorder()
	metricsHandler(d).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "dumpling_dump_finished_rows 42")
}
