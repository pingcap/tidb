package export

import (
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"time"

	"github.com/pingcap/dumpling/v4/log"

	"github.com/pingcap/errors"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
)

var (
	cmuxReadTimeout = 10 * time.Second
)

func startHTTPServer(lis net.Listener) {
	router := http.NewServeMux()

	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	router.HandleFunc("/debug/pprof/trace", pprof.Trace)

	httpServer := &http.Server{
		Handler: router,
	}
	err := httpServer.Serve(lis)
	if err != nil && !isErrNetClosing(err) && err != http.ErrServerClosed {
		log.Error("http server return with error", zap.Error(err))
	}
}

func startDumplingService(addr string) error {
	rootLis, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.Annotate(err, "start listening")
	}

	// create a cmux
	m := cmux.New(rootLis)
	m.SetReadTimeout(cmuxReadTimeout) // set a timeout, ref: https://github.com/pingcap/tidb-binlog/pull/352

	httpL := m.Match(cmux.HTTP1Fast())
	go startHTTPServer(httpL)

	err = m.Serve() // start serving, block
	if err != nil && isErrNetClosing(err) {
		err = nil
	}
	return err
}

var (
	useOfClosedErrMsg = "use of closed network connection"
)

// isErrNetClosing checks whether is an ErrNetClosing error
func isErrNetClosing(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), useOfClosedErrMsg)
}
