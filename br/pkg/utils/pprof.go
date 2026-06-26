// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"fmt"
	"net" //nolint:goimports
	// #nosec
	// register HTTP handler for /debug/pprof
	"net/http"
	"net/http/pprof"
	"os"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	tidbutils "github.com/pingcap/tidb/pkg/util"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var (
	startedPProf = ""
	mu           sync.Mutex
)

func listen(statusAddr string) (net.Listener, error) {
	mu.Lock()
	defer mu.Unlock()
	if startedPProf != "" {
		log.Warn("Try to start pprof when it has been started, nothing will happen", zap.String("address", startedPProf))
		return nil, errors.Annotate(berrors.ErrUnknown, "try to start pprof when it has been started at "+startedPProf)
	}
	failpoint.Inject("determined-pprof-port", func(v failpoint.Value) {
		port := v.(int)
		statusAddr = fmt.Sprintf(":%d", port)
		log.Info("injecting failpoint, pprof will start at determined port", zap.Int("port", port))
	})
	listener, err := net.Listen("tcp", statusAddr)
	if err != nil {
		log.Warn("failed to start pprof", zap.String("addr", statusAddr), zap.Error(err))
		return nil, errors.Trace(err)
	}
	startedPProf = listener.Addr().String()
	log.Info("bound pprof to addr", zap.String("addr", startedPProf))
	_, _ = fmt.Fprintf(os.Stderr, "bound pprof to addr %s\n", startedPProf)
	return listener, nil
}

// RegisterDefaultStatusHandlers registers the default metrics and pprof endpoints on the provided mux.
func RegisterDefaultStatusHandlers(mux *http.ServeMux) {
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
}

// StartStatusListenerWithHandler forks a new goroutine listening on specified port and serves the provided handler.
func StartStatusListenerWithHandler(statusAddr string, wrapper *tidbutils.TLS, handler http.Handler) error {
	listener, err := listen(statusAddr)
	if err != nil {
		return err
	}

	go func() {
		if e := http.Serve(wrapper.WrapListener(listener), handler); e != nil {
			log.Warn("failed to serve pprof", zap.String("addr", startedPProf), zap.Error(e))
			mu.Lock()
			startedPProf = ""
			mu.Unlock()
			return
		}
	}()
	return nil
}

// StartStatusListener forks a new goroutine listening on specified port and provide metrics and pprof info.
func StartStatusListener(statusAddr string, wrapper *tidbutils.TLS) error {
	mux := http.NewServeMux()
	RegisterDefaultStatusHandlers(mux)
	return StartStatusListenerWithHandler(statusAddr, wrapper, mux)
}
