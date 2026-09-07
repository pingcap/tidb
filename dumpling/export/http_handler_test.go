// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	tcontext "github.com/pingcap/tidb/dumpling/context"
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
}
