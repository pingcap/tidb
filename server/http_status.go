// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"runtime"
	rpprof "runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/fn"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/cpuprofile"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tidb/util/versioninfo"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/soheilhy/cmux"
	"github.com/tiancaiamao/appdash/traceapp"
	"go.uber.org/zap"
	"google.golang.org/grpc/channelz/service"
	static "sourcegraph.com/sourcegraph/appdash-data"
)

const defaultStatusPort = 10080

func (s *Server) startStatusHTTP() {
	go s.startHTTPServer()
}

func serveError(w http.ResponseWriter, status int, txt string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Go-Pprof", "1")
	w.Header().Del("Content-Disposition")
	w.WriteHeader(status)
	_, err := fmt.Fprintln(w, txt)
	terror.Log(err)
}

func sleepWithCtx(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func (s *Server) listenStatusHTTPServer() error {
	s.statusAddr = fmt.Sprintf("%s:%d", s.cfg.Status.StatusHost, s.cfg.Status.StatusPort)
	if s.cfg.Status.StatusPort == 0 && !RunInGoTest {
		s.statusAddr = fmt.Sprintf("%s:%d", s.cfg.Status.StatusHost, defaultStatusPort)
	}

	logutil.BgLogger().Info("for status and metrics report", zap.String("listening on addr", s.statusAddr))
	clusterSecurity := s.cfg.Security.ClusterSecurity()
	tlsConfig, err := clusterSecurity.ToTLSConfig()
	if err != nil {
		logutil.BgLogger().Error("invalid TLS config", zap.Error(err))
		return errors.Trace(err)
	}
	tlsConfig = s.setCNChecker(tlsConfig)

	if tlsConfig != nil {
		// we need to manage TLS here for cmux to distinguish between HTTP and gRPC.
		s.statusListener, err = tls.Listen("tcp", s.statusAddr, tlsConfig)
	} else {
		s.statusListener, err = net.Listen("tcp", s.statusAddr)
	}
	if err != nil {
		logutil.BgLogger().Info("listen failed", zap.Error(err))
		return errors.Trace(err)
	} else if RunInGoTest && s.cfg.Status.StatusPort == 0 {
		s.statusAddr = s.statusListener.Addr().String()
		s.cfg.Status.StatusPort = uint(s.statusListener.Addr().(*net.TCPAddr).Port)
	}
	return nil
}

// Ballast try to reduce the GC frequency by using Ballast Object
type Ballast struct {
	ballast     []byte
	ballastLock sync.Mutex

	maxSize int
}

func newBallast(maxSize int) *Ballast {
	var b Ballast
	b.maxSize = 1024 * 1024 * 1024 * 2
	if maxSize > 0 {
		b.maxSize = maxSize
	} else {
		// we try to use the total amount of ram as a reference to set the default ballastMaxSz
		// since the fatal throw "runtime: out of memory" would never yield to `recover`
		totalRAMSz, err := memory.MemTotal()
		if err != nil {
			logutil.BgLogger().Error("failed to get the total amount of RAM on this system", zap.Error(err))
		} else {
			maxSzAdvice := totalRAMSz >> 2
			if uint64(b.maxSize) > maxSzAdvice {
				b.maxSize = int(maxSzAdvice)
			}
		}
	}
	return &b
}

// GetSize get the size of ballast object
func (b *Ballast) GetSize() int {
	var sz int
	b.ballastLock.Lock()
	sz = len(b.ballast)
	b.ballastLock.Unlock()
	return sz
}

// SetSize set the size of ballast object
func (b *Ballast) SetSize(newSz int) error {
	if newSz < 0 {
		return fmt.Errorf("newSz cannot be negative: %d", newSz)
	}
	if newSz > b.maxSize {
		return fmt.Errorf("newSz cannot be bigger than %d but it has value %d", b.maxSize, newSz)
	}
	b.ballastLock.Lock()
	b.ballast = make([]byte, newSz)
	b.ballastLock.Unlock()
	return nil
}

// GenHTTPHandler generate a HTTP handler to get/set the size of this ballast object
func (b *Ballast) GenHTTPHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_, err := w.Write([]byte(strconv.Itoa(b.GetSize())))
			terror.Log(err)
		case http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				terror.Log(err)
				return
			}
			newSz, err := strconv.Atoi(string(body))
			if err == nil {
				err = b.SetSize(newSz)
			}
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				errStr := err.Error()
				if _, err := w.Write([]byte(errStr)); err != nil {
					terror.Log(err)
				}
				return
			}
		}
	}
}

func (s *Server) startHTTPServer() {
	router := mux.NewRouter()

	router.HandleFunc("/status", s.handleStatus).Name("Status")
	// HTTP path for prometheus.
	router.Handle("/metrics", promhttp.Handler()).Name("Metrics")

	// HTTP path for dump statistics.
	router.Handle("/stats/dump/{db}/{table}", s.newStatsHandler()).Name("StatsDump")
	router.Handle("/stats/dump/{db}/{table}/{snapshot}", s.newStatsHistoryHandler()).Name("StatsHistoryDump")

	router.Handle("/plan_replayer/dump/{filename}", s.newPlanReplayerHandler()).Name("PlanReplayerDump")

	router.Handle("/optimize_trace/dump/{filename}", s.newOptimizeTraceHandler()).Name("OptimizeTraceDump")

	tikvHandlerTool := s.newTikvHandlerTool()
	router.Handle("/settings", settingsHandler{tikvHandlerTool}).Name("Settings")
	router.Handle("/binlog/recover", binlogRecover{}).Name("BinlogRecover")

	router.Handle("/schema", schemaHandler{tikvHandlerTool}).Name("Schema")
	router.Handle("/schema/{db}", schemaHandler{tikvHandlerTool})
	router.Handle("/schema/{db}/{table}", schemaHandler{tikvHandlerTool})
	router.Handle("/tables/{colID}/{colTp}/{colFlag}/{colLen}", valueHandler{})

	router.Handle("/schema_storage", schemaStorageHandler{tikvHandlerTool}).Name("Schema Storage")
	router.Handle("/schema_storage/{db}", schemaStorageHandler{tikvHandlerTool})
	router.Handle("/schema_storage/{db}/{table}", schemaStorageHandler{tikvHandlerTool})

	router.Handle("/ddl/history", ddlHistoryJobHandler{tikvHandlerTool}).Name("DDL_History")
	router.Handle("/ddl/owner/resign", ddlResignOwnerHandler{tikvHandlerTool.Store.(kv.Storage)}).Name("DDL_Owner_Resign")

	// HTTP path for get the TiDB config
	router.Handle("/config", fn.Wrap(func() (*config.Config, error) {
		return config.GetGlobalConfig(), nil
	}))

	// HTTP path for get server info.
	router.Handle("/info", serverInfoHandler{tikvHandlerTool}).Name("Info")
	router.Handle("/info/all", allServerInfoHandler{tikvHandlerTool}).Name("InfoALL")
	// HTTP path for get db and table info that is related to the tableID.
	router.Handle("/db-table/{tableID}", dbTableHandler{tikvHandlerTool})
	// HTTP path for get table tiflash replica info.
	router.Handle("/tiflash/replica-deprecated", flashReplicaHandler{tikvHandlerTool})

	if s.cfg.Store == "tikv" {
		// HTTP path for tikv.
		router.Handle("/tables/{db}/{table}/regions", tableHandler{tikvHandlerTool, opTableRegions})
		router.Handle("/tables/{db}/{table}/ranges", tableHandler{tikvHandlerTool, opTableRanges})
		router.Handle("/tables/{db}/{table}/scatter", tableHandler{tikvHandlerTool, opTableScatter})
		router.Handle("/tables/{db}/{table}/stop-scatter", tableHandler{tikvHandlerTool, opStopTableScatter})
		router.Handle("/tables/{db}/{table}/disk-usage", tableHandler{tikvHandlerTool, opTableDiskUsage})
		router.Handle("/regions/meta", regionHandler{tikvHandlerTool}).Name("RegionsMeta")
		router.Handle("/regions/hot", regionHandler{tikvHandlerTool}).Name("RegionHot")
		router.Handle("/regions/{regionID}", regionHandler{tikvHandlerTool})
	}

	// HTTP path for get MVCC info
	router.Handle("/mvcc/key/{db}/{table}", mvccTxnHandler{tikvHandlerTool, opMvccGetByKey})
	router.Handle("/mvcc/key/{db}/{table}/{handle}", mvccTxnHandler{tikvHandlerTool, opMvccGetByKey})
	router.Handle("/mvcc/txn/{startTS}/{db}/{table}", mvccTxnHandler{tikvHandlerTool, opMvccGetByTxn})
	router.Handle("/mvcc/hex/{hexKey}", mvccTxnHandler{tikvHandlerTool, opMvccGetByHex})
	router.Handle("/mvcc/index/{db}/{table}/{index}", mvccTxnHandler{tikvHandlerTool, opMvccGetByIdx})
	router.Handle("/mvcc/index/{db}/{table}/{index}/{handle}", mvccTxnHandler{tikvHandlerTool, opMvccGetByIdx})

	// HTTP path for generate metric profile.
	router.Handle("/metrics/profile", profileHandler{tikvHandlerTool})
	// HTTP path for web UI.
	if host, port, err := net.SplitHostPort(s.statusAddr); err == nil {
		if host == "" {
			host = "localhost"
		}
		baseURL := &url.URL{
			Scheme: util.InternalHTTPSchema(),
			Host:   fmt.Sprintf("%s:%s", host, port),
		}
		router.HandleFunc("/web/trace", traceapp.HandleTiDB).Name("Trace Viewer")
		sr := router.PathPrefix("/web/trace/").Subrouter()
		if _, err := traceapp.New(traceapp.NewRouter(sr), baseURL); err != nil {
			logutil.BgLogger().Error("new failed", zap.Error(err))
		}
		router.PathPrefix("/static/").Handler(http.StripPrefix("/static", http.FileServer(static.Data)))
	}

	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)

	serverMux.HandleFunc("/debug/pprof/", pprof.Index)
	serverMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	serverMux.HandleFunc("/debug/pprof/profile", cpuprofile.ProfileHTTPHandler)
	serverMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	serverMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	ballast := newBallast(s.cfg.MaxBallastObjectSize)
	{
		err := ballast.SetSize(s.cfg.BallastObjectSize)
		if err != nil {
			logutil.BgLogger().Error("set initial ballast object size failed", zap.Error(err))
		}
	}
	serverMux.HandleFunc("/debug/ballast-object-sz", ballast.GenHTTPHandler())

	serverMux.HandleFunc("/debug/gogc", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			_, err := w.Write([]byte(strconv.Itoa(util.GetGOGC())))
			terror.Log(err)
		case http.MethodPost:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				terror.Log(err)
				return
			}

			val, err := strconv.Atoi(string(body))
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				if _, err := w.Write([]byte(err.Error())); err != nil {
					terror.Log(err)
				}
				return
			}

			util.SetGOGC(val)
		}
	})

	serverMux.HandleFunc("/debug/zip", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="tidb_debug"`+time.Now().Format("20060102150405")+".zip"))

		// dump goroutine/heap/mutex
		items := []struct {
			name   string
			gc     int
			debug  int
			second int
		}{
			{name: "goroutine", debug: 2},
			{name: "heap", gc: 1},
			{name: "mutex"},
		}
		zw := zip.NewWriter(w)
		for _, item := range items {
			p := rpprof.Lookup(item.name)
			if p == nil {
				serveError(w, http.StatusNotFound, "Unknown profile")
				return
			}
			if item.gc > 0 {
				runtime.GC()
			}
			fw, err := zw.Create(item.name)
			if err != nil {
				serveError(w, http.StatusInternalServerError, fmt.Sprintf("Create zipped %s fail: %v", item.name, err))
				return
			}
			err = p.WriteTo(fw, item.debug)
			terror.Log(err)
		}

		// dump profile
		fw, err := zw.Create("profile")
		if err != nil {
			serveError(w, http.StatusInternalServerError, fmt.Sprintf("Create zipped %s fail: %v", "profile", err))
			return
		}
		pc := cpuprofile.NewCollector()
		if err := pc.StartCPUProfile(fw); err != nil {
			serveError(w, http.StatusInternalServerError,
				fmt.Sprintf("Could not enable CPU profiling: %s", err))
			return
		}
		sec, err := strconv.ParseInt(r.FormValue("seconds"), 10, 64)
		if sec <= 0 || err != nil {
			sec = 10
		}
		sleepWithCtx(r.Context(), time.Duration(sec)*time.Second)
		err = pc.StopCPUProfile()
		if err != nil {
			serveError(w, http.StatusInternalServerError,
				fmt.Sprintf("Could not enable CPU profiling: %s", err))
			return
		}

		// dump config
		fw, err = zw.Create("config")
		if err != nil {
			serveError(w, http.StatusInternalServerError, fmt.Sprintf("Create zipped %s fail: %v", "config", err))
			return
		}
		js, err := json.MarshalIndent(config.GetGlobalConfig(), "", " ")
		if err != nil {
			serveError(w, http.StatusInternalServerError, fmt.Sprintf("get config info fail%v", err))
			return
		}
		_, err = fw.Write(js)
		terror.Log(err)

		// dump version
		fw, err = zw.Create("version")
		if err != nil {
			serveError(w, http.StatusInternalServerError, fmt.Sprintf("Create zipped %s fail: %v", "version", err))
			return
		}
		_, err = fw.Write([]byte(printer.GetTiDBInfo()))
		terror.Log(err)

		err = zw.Close()
		terror.Log(err)
	})

	// failpoint is enabled only for tests so we can add some http APIs here for tests.
	failpoint.Inject("enableTestAPI", func() {
		serverMux.HandleFunc("/fail/", func(w http.ResponseWriter, r *http.Request) {
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/fail")
			new(failpoint.HttpHandler).ServeHTTP(w, r)
		})

		router.Handle("/test/{mod}/{op}", &testHandler{tikvHandlerTool, 0})
	})

	// ddlHook is enabled only for tests so we can substitute the callback in the DDL.
	router.Handle("/test/ddl/hook", &ddlHookHandler{tikvHandlerTool.Store.(kv.Storage)})

	var (
		httpRouterPage bytes.Buffer
		pathTemplate   string
		err            error
	)
	httpRouterPage.WriteString("<html><head><title>TiDB Status and Metrics Report</title></head><body><h1>TiDB Status and Metrics Report</h1><table>")
	err = router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		pathTemplate, err = route.GetPathTemplate()
		if err != nil {
			logutil.BgLogger().Error("get HTTP router path failed", zap.Error(err))
		}
		name := route.GetName()
		// If the name attribute is not set, GetName returns "".
		// "traceapp.xxx" are introduced by the traceapp package and are also ignored.
		if name != "" && !strings.HasPrefix(name, "traceapp") && err == nil {
			httpRouterPage.WriteString("<tr><td><a href='" + pathTemplate + "'>" + name + "</a><td></tr>")
		}
		return nil
	})
	if err != nil {
		logutil.BgLogger().Error("generate root failed", zap.Error(err))
	}
	httpRouterPage.WriteString("<tr><td><a href='/debug/pprof/'>Debug</a><td></tr>")
	httpRouterPage.WriteString("</table></body></html>")
	router.HandleFunc("/", func(responseWriter http.ResponseWriter, request *http.Request) {
		_, err = responseWriter.Write(httpRouterPage.Bytes())
		if err != nil {
			logutil.BgLogger().Error("write HTTP index page failed", zap.Error(err))
		}
	})
	s.startStatusServerAndRPCServer(serverMux)
}

func (s *Server) startStatusServerAndRPCServer(serverMux *http.ServeMux) {
	m := cmux.New(s.statusListener)
	// Match connections in order:
	// First HTTP, and otherwise grpc.
	httpL := m.Match(cmux.HTTP1Fast())
	grpcL := m.Match(cmux.Any())

	s.statusServer = &http.Server{Addr: s.statusAddr, Handler: CorsHandler{handler: serverMux, cfg: s.cfg}}
	s.grpcServer = NewRPCServer(s.cfg, s.dom, s)
	service.RegisterChannelzServiceToServer(s.grpcServer)

	go util.WithRecovery(func() {
		err := s.grpcServer.Serve(grpcL)
		logutil.BgLogger().Error("grpc server error", zap.Error(err))
	}, nil)

	go util.WithRecovery(func() {
		err := s.statusServer.Serve(httpL)
		logutil.BgLogger().Error("http server error", zap.Error(err))
	}, nil)

	err := m.Serve()
	if err != nil {
		logutil.BgLogger().Error("start status/rpc server error", zap.Error(err))
	}
}

func (s *Server) setCNChecker(tlsConfig *tls.Config) *tls.Config {
	if tlsConfig != nil && len(s.cfg.Security.ClusterVerifyCN) != 0 {
		checkCN := make(map[string]struct{})
		for _, cn := range s.cfg.Security.ClusterVerifyCN {
			cn = strings.TrimSpace(cn)
			checkCN[cn] = struct{}{}
		}
		tlsConfig.VerifyPeerCertificate = func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			for _, chain := range verifiedChains {
				if len(chain) != 0 {
					if _, match := checkCN[chain[0].Subject.CommonName]; match {
						return nil
					}
				}
			}
			return errors.Errorf("client certificate authentication failed. The Common Name from the client certificate was not found in the configuration cluster-verify-cn with value: %s", s.cfg.Security.ClusterVerifyCN)
		}
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return tlsConfig
}

// status of TiDB.
type status struct {
	Connections int    `json:"connections"`
	Version     string `json:"version"`
	GitHash     string `json:"git_hash"`
}

func (s *Server) handleStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// If the server is in the process of shutting down, return a non-200 status.
	// It is important not to return status{} as acquiring the s.ConnectionCount()
	// acquires a lock that may already be held by the shutdown process.
	if s.inShutdownMode {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	st := status{
		Connections: s.ConnectionCount(),
		Version:     mysql.ServerVersion,
		GitHash:     versioninfo.TiDBGitHash,
	}
	js, err := json.Marshal(st)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		logutil.BgLogger().Error("encode json failed", zap.Error(err))
		return
	}
	_, err = w.Write(js)
	terror.Log(errors.Trace(err))
}
