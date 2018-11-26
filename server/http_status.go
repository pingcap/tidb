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
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/printer"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/tiancaiamao/appdash/traceapp"
	static "sourcegraph.com/sourcegraph/appdash-data"
)

const defaultStatusAddr = ":10080"

func (s *Server) startStatusHTTP() {
	go s.startHTTPServer()
}

func (s *Server) startHTTPServer() {
	router := mux.NewRouter()

	router.HandleFunc("/status", s.handleStatus).Name("Status")
	// HTTP path for prometheus.
	router.Handle("/metrics", prometheus.Handler()).Name("Metrics")

	// HTTP path for dump statistics.
	router.Handle("/stats/dump/{db}/{table}", s.newStatsHandler()).Name("StatsDump")

	router.Handle("/settings", settingsHandler{}).Name("Settings")
	router.Handle("/binlog/recover", binlogRecover{}).Name("BinlogRecover")

	tikvHandlerTool := s.newTikvHandlerTool()
	router.Handle("/schema", schemaHandler{tikvHandlerTool}).Name("Schema")
	router.Handle("/schema/{db}", schemaHandler{tikvHandlerTool})
	router.Handle("/schema/{db}/{table}", schemaHandler{tikvHandlerTool})
	router.Handle("/tables/{colID}/{colTp}/{colFlag}/{colLen}", valueHandler{})
	router.Handle("/ddl/history", ddlHistoryJobHandler{tikvHandlerTool}).Name("DDL_History")
	router.Handle("/ddl/owner/resign", ddlResignOwnerHandler{tikvHandlerTool.store.(kv.Storage)}).Name("DDL_Owner_Resign")

	// HTTP path for get server info.
	router.Handle("/info", serverInfoHandler{tikvHandlerTool}).Name("Info")
	router.Handle("/info/all", allServerInfoHandler{tikvHandlerTool}).Name("InfoALL")
	// HTTP path for get db and table info that is related to the tableID.
	router.Handle("/db-table/{tableID}", dbTableHandler{tikvHandlerTool})

	if s.cfg.Store == "tikv" {
		// HTTP path for tikv.
		router.Handle("/tables/{db}/{table}/regions", tableHandler{tikvHandlerTool, opTableRegions})
		router.Handle("/tables/{db}/{table}/scatter", tableHandler{tikvHandlerTool, opTableScatter})
		router.Handle("/tables/{db}/{table}/stop-scatter", tableHandler{tikvHandlerTool, opStopTableScatter})
		router.Handle("/tables/{db}/{table}/disk-usage", tableHandler{tikvHandlerTool, opTableDiskUsage})
		router.Handle("/regions/meta", regionHandler{tikvHandlerTool}).Name("RegionsMeta")
		router.Handle("/regions/{regionID}", regionHandler{tikvHandlerTool})
		router.Handle("/mvcc/key/{db}/{table}/{handle}", mvccTxnHandler{tikvHandlerTool, opMvccGetByKey})
		router.Handle("/mvcc/txn/{startTS}/{db}/{table}", mvccTxnHandler{tikvHandlerTool, opMvccGetByTxn})
		router.Handle("/mvcc/hex/{hexKey}", mvccTxnHandler{tikvHandlerTool, opMvccGetByHex})
		router.Handle("/mvcc/index/{db}/{table}/{index}/{handle}", mvccTxnHandler{tikvHandlerTool, opMvccGetByIdx})
	}
	addr := fmt.Sprintf(":%d", s.cfg.Status.StatusPort)
	if s.cfg.Status.StatusPort == 0 {
		addr = defaultStatusAddr
	}

	// HTTP path for web UI.
	if host, port, err := net.SplitHostPort(addr); err == nil {
		if host == "" {
			host = "localhost"
		}
		baseURL := &url.URL{
			Scheme: "http",
			Host:   fmt.Sprintf("%s:%s", host, port),
		}
		router.HandleFunc("/web/trace", traceapp.HandleTiDB).Name("Trace Viewer")
		sr := router.PathPrefix("/web/trace/").Subrouter()
		if _, err := traceapp.New(traceapp.NewRouter(sr), baseURL); err != nil {
			log.Error(err)
		}
		router.PathPrefix("/static/").Handler(http.StripPrefix("/static", http.FileServer(static.Data)))
	}

	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)

	serverMux.HandleFunc("/debug/pprof/", pprof.Index)
	serverMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	serverMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	serverMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	serverMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	var (
		err            error
		httpRouterPage bytes.Buffer
		pathTemplate   string
	)
	httpRouterPage.WriteString("<html><head><title>TiDB Status and Metrics Report</title></head><body><h1>TiDB Status and Metrics Report</h1><table>")
	err = router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		pathTemplate, err = route.GetPathTemplate()
		if err != nil {
			log.Error("Get http router path error ", err)
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
		log.Error("Generate root error ", err)
	}
	httpRouterPage.WriteString("<tr><td><a href='/debug/pprof/'>Debug</a><td></tr>")
	httpRouterPage.WriteString("</table></body></html>")
	router.HandleFunc("/", func(responseWriter http.ResponseWriter, request *http.Request) {
		_, err = responseWriter.Write([]byte(httpRouterPage.String()))
		if err != nil {
			log.Error("Http index page error ", err)
		}
	})

	log.Infof("Listening on %v for status and metrics report.", addr)
	s.statusServer = &http.Server{Addr: addr, Handler: CorsHandler{handler: serverMux, cfg: s.cfg}}

	if len(s.cfg.Security.ClusterSSLCA) != 0 {
		err = s.statusServer.ListenAndServeTLS(s.cfg.Security.ClusterSSLCert, s.cfg.Security.ClusterSSLKey)
	} else {
		err = s.statusServer.ListenAndServe()
	}

	if err != nil {
		log.Info(err)
	}
}

// status of TiDB.
type status struct {
	Connections int    `json:"connections"`
	Version     string `json:"version"`
	GitHash     string `json:"git_hash"`
}

func (s *Server) handleStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	st := status{
		Connections: s.ConnectionCount(),
		Version:     mysql.ServerVersion,
		GitHash:     printer.TiDBGitHash,
	}
	js, err := json.Marshal(st)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Error("Encode json error", err)
	} else {
		_, err = w.Write(js)
		terror.Log(errors.Trace(err))
	}
}
