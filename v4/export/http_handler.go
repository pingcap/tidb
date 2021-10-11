// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/pingcap/dumpling/v4/log"

	tcontext "github.com/pingcap/dumpling/v4/context"

	"github.com/pingcap/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
)

var cmuxReadTimeout = 10 * time.Second

func startHTTPServer(tctx *tcontext.Context, lis net.Listener) {
	router := http.NewServeMux()
	router.Handle("/metrics", promhttp.Handler())

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
		tctx.L().Warn("dumpling http handler return with error", log.ShortError(err))
	}
}

func startDumplingService(tctx *tcontext.Context, addr string) error {
	rootLis, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Annotate(err, "start listening")
	}

	// create a cmux
	m := cmux.New(rootLis)
	m.SetReadTimeout(cmuxReadTimeout) // set a timeout, ref: https://github.com/pingcap/tidb-binlog/pull/352

	httpL := m.Match(cmux.HTTP1Fast())
	go startHTTPServer(tctx, httpL)

	err = m.Serve() // start serving, block
	if err != nil && isErrNetClosing(err) {
		err = nil
	}
	return err
}

var useOfClosedErrMsg = "use of closed network connection"

// isErrNetClosing checks whether is an ErrNetClosing error
func isErrNetClosing(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), useOfClosedErrMsg)
}
