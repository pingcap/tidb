package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/printer"
	"github.com/prometheus/client_golang/prometheus"
)

var once sync.Once

const defaultStatusAddr = ":10080"

func (s *Server) startStatusHTTP() {
	// prepare region cache for region http server
	var regionCache *tikv.RegionCache
	if s.cfg.Store == "tikv" {
		var err error
		regionCache, err = tikv.NewRegionCacheFromStorePath(fmt.Sprintf("%s://%s", s.cfg.Store, s.cfg.StorePath))
		if err != nil {
			log.Fatal(err)
		}
	}

	once.Do(func() {
		go s.startHTTPServer(regionCache)
	})
}

func (s *Server) startHTTPServer(regionCache *tikv.RegionCache) {
	router := mux.NewRouter()
	router.HandleFunc("/status", s.handleStatus)
	// HTTP path for prometheus.
	router.Handle("/metrics", prometheus.Handler())

	router.HandleFunc("/tables/{db}/{table}/regions", func(w http.ResponseWriter, req *http.Request) {
		request := NewRegionsHTTPRequest(s, w, req, regionCache)
		request.HandleListRegions()
	})

	router.HandleFunc("/regions/{regionID}", func(w http.ResponseWriter, req *http.Request) {
		request := NewRegionsHTTPRequest(s, w, req, regionCache)
		request.HandleGetRegionByID()
	})
	addr := s.cfg.StatusAddr
	if len(addr) == 0 {
		addr = defaultStatusAddr
	}
	log.Infof("Listening on %v for status and metrics report.", addr)
	err := http.ListenAndServe(addr, router)
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
