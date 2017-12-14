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
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/printer"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

var once sync.Once

const defaultStatusAddr = ":10080"

func (s *Server) startStatusHTTP() {
	once.Do(func() {
		go s.startHTTPServer()
	})
}

func (s *Server) startHTTPServer() {
	router := mux.NewRouter()
	router.HandleFunc("/status", s.handleStatus)
	// HTTP path for prometheus.
	router.Handle("/metrics", prometheus.Handler())

	if s.cfg.Store == "tikv" {
		tikvHandler := s.newRegionHandler()
		// HTTP path for regions
		router.Handle("/tables/{db}/{table}/regions", tableHandler{tikvHandler, opTableRegions})
		router.Handle("/tables/{db}/{table}/disk-usage", tableHandler{tikvHandler, opTableDiskUsage})
		router.Handle("/regions/meta", tikvHandler)
		router.Handle("/regions/{regionID}", tikvHandler)
		router.Handle("/mvcc/key/{db}/{table}/{handle}", mvccTxnHandler{tikvHandler, opMvccGetByKey})
		router.Handle("/mvcc/txn/{startTS}/{db}/{table}", mvccTxnHandler{tikvHandler, opMvccGetByTxn})
		router.Handle("/mvcc/txn/{startTS}", mvccTxnHandler{tikvHandler, opMvccGetByTxn})
		router.Handle("/mvcc/hex/{hexKey}", mvccTxnHandler{tikvHandler, opMvccGetByHex})
		router.Handle("/mvcc/index/{db}/{table}/{index}/{handle}", mvccTxnHandler{tikvHandler, opMvccGetByIdx})
		router.Handle("/schema", schemaHandler{tikvHandler})
		router.Handle("/schema/{db}", schemaHandler{tikvHandler})
		router.Handle("/schema/{db}/{table}", schemaHandler{tikvHandler})
	}
	addr := fmt.Sprintf(":%d", s.cfg.Status.StatusPort)
	if s.cfg.Status.StatusPort == 0 {
		addr = defaultStatusAddr
	}
	log.Infof("Listening on %v for status and metrics report.", addr)
	http.Handle("/", router)

	var err error
	if len(s.cfg.Security.ClusterSSLCA) != 0 {
		err = http.ListenAndServeTLS(addr, s.cfg.Security.ClusterSSLCert, s.cfg.Security.ClusterSSLKey, nil)
	} else {
		err = http.ListenAndServe(addr, nil)
	}

	if err != nil {
		log.Fatal(err)
	}
}

// TiDB status
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
