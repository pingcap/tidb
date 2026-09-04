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

// metricsHandler serves the metrics this Dumper records, which live in the
// registry its config names rather than the process-wide default one. Serving
// the default registry instead - as this endpoint used to - answered every
// scrape with the Go and process collectors and none of the dump counters,
// because nothing ever registers those with it.
//
// A registry that cannot be gathered from is not an error worth failing over:
// the endpoint falls back to the default handler so /metrics keeps answering.
func metricsHandler(d *Dumper) http.Handler {
	if d != nil && d.conf != nil {
		if gatherer, ok := d.conf.PromRegistry.(prometheus.Gatherer); ok {
			return promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{})
		}
	}
	return promhttp.Handler()
}

// statusHandler reports how far the dump has got, as JSON. The same numbers
// reach the log every logProgressTick, but a caller driving a progress bar
// should not have to scrape log lines to find them.
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
