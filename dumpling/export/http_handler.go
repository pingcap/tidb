// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/pingcap/errors"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/dumpling/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/soheilhy/cmux"
)

var cmuxReadTimeout = 10 * time.Second

func startHTTPServer(tctx *tcontext.Context, lis net.Listener, d *Dumper) {
	router := http.NewServeMux()
	router.Handle("/metrics", metricsHandler(d))
	router.HandleFunc("/status", statusHandler(tctx, d))

	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)

	httpServer := &http.Server{
		Handler: router,
	}
	err := httpServer.Serve(lis)
	err = errors.Cause(err)
	if err != nil && !isErrNetClosing(err) && err != http.ErrServerClosed {
		tctx.L().Info("dumpling http handler return with error", log.ShortError(err))
	}
}

func startDumplingService(tctx *tcontext.Context, addr string, d *Dumper) error {
	rootLis, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Annotate(err, "start listening")
	}

	// create a cmux
	m := cmux.New(rootLis)
	m.SetReadTimeout(cmuxReadTimeout) // set a timeout, ref: https://github.com/pingcap/tidb-binlog/pull/352

	httpL := m.Match(cmux.HTTP1Fast())
	go startHTTPServer(tctx, httpL, d)

	err = m.Serve() // start serving, block
	if err != nil && isErrNetClosing(err) {
		err = nil
	}
	return err
}

// metricsHandler serves the configured dump metrics while retaining the metric
// families exposed by the default handler. Registerers that do not implement
// Gatherer retain the default handler's behavior.
func metricsHandler(d *Dumper) http.Handler {
	gatherer := prometheus.DefaultGatherer
	if d != nil && d.conf != nil {
		if configured, ok := d.conf.PromRegistry.(prometheus.Gatherer); ok {
			gatherer = metricsGatherer{configured: configured, previous: gatherer}
		}
	}
	return promhttp.InstrumentMetricHandler(prometheus.DefaultRegisterer,
		promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
}

type metricsGatherer struct {
	configured prometheus.Gatherer
	previous   prometheus.Gatherer
}

func (g metricsGatherer) Gather() ([]*dto.MetricFamily, error) {
	configured, err := g.configured.Gather()
	if err != nil {
		return configured, err
	}
	// The CLI already installs its registry as DefaultGatherer.
	if registry, ok := g.configured.(*prometheus.Registry); ok && registry == g.previous {
		return configured, nil
	}
	previous, err := g.previous.Gather()
	if err != nil {
		return configured, err
	}
	names := make(map[string]struct{}, len(configured))
	result := make([]*dto.MetricFamily, 0, len(configured)+len(previous))
	result = append(result, configured...)
	for _, family := range configured {
		names[family.GetName()] = struct{}{}
	}
	// Prefer the configured family when both sources expose the same name.
	for _, family := range previous {
		if _, exists := names[family.GetName()]; !exists {
			result = append(result, family)
		}
	}
	return result, nil
}

// statusHandler serves the latest dump status snapshot as JSON.
// The progress loop refreshes it every statusRefreshTick independently of HTTP polling.
func statusHandler(tctx *tcontext.Context, d *Dumper) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if d == nil {
			http.Error(w, "dumper is not running", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(d.GetStatus()); err != nil {
			// The status line is already written by now, so this can only be
			// logged, not turned into an error response.
			tctx.L().Warn("failed to write dumpling status response", log.ShortError(err))
		}
	}
}

var useOfClosedErrMsg = "use of closed network connection"

// isErrNetClosing checks whether is an ErrNetClosing error
func isErrNetClosing(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), useOfClosedErrMsg)
}
