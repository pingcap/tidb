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
	"github.com/ngaut/log"
	"github.com/pingcap/pd/pd-client"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/printer"
	"github.com/prometheus/client_golang/prometheus"
)

var once sync.Once

const defaultStatusAddr = ":10080"

func (s *Server) startStatusHTTP() {
	// prepare region cache for region http server
	pdClient, err := s.getPdClient()
	if err != nil {
		log.Fatal(err)
	}

	once.Do(func() {
		go s.startHTTPServer(pdClient)
	})
}

func (s *Server) getPdClient() (pd.Client, error) {
	if s.cfg.Store != "tikv" {
		return nil, nil
	}
	path := fmt.Sprintf("%s://%s", s.cfg.Store, s.cfg.StorePath)
	etcdAddrs, err := tikv.ParseEtcdAddr(path)
	if err != nil {
		return nil, err
	}
	client, err := pd.NewClient(etcdAddrs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return client, nil
}

func (s *Server) startHTTPServer(pdClient pd.Client) {
	router := mux.NewRouter()
	router.HandleFunc("/status", s.handleStatus)
	// HTTP path for prometheus.
	router.Handle("/metrics", prometheus.Handler())

	// HTTP path for regions
	router.Handle("/tables/{db}/{table}/regions", s.newTableRegionsHandler(pdClient))
	router.Handle("/regions/{regionID}", s.newRegionHandler(pdClient))

	addr := s.cfg.StatusAddr
	if len(addr) == 0 {
		addr = defaultStatusAddr
	}
	log.Infof("Listening on %v for status and metrics report.", addr)
	http.Handle("/", router)
	err := http.ListenAndServe(addr, nil)
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
		w.Write(js)
	}
}
