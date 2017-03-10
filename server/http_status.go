package server

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/printer"
	"github.com/prometheus/client_golang/prometheus"
	"net/http"
	"sync"
)

var once sync.Once

const defaultStatusAddr = ":10080"

func (s *Server) startStatusHTTP() {
	once.Do(func() {
		go func() {
			router := mux.NewRouter()
			router.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				s := status{
					Connections: s.ConnectionCount(),
					Version:     mysql.ServerVersion,
					GitHash:     printer.TiDBGitHash,
				}
				js, err := json.Marshal(s)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					log.Error("Encode json error", err)
				} else {
					w.Write(js)
				}

			})
			// HTTP path for prometheus.
			router.Handle("/metrics", prometheus.Handler())

			router.HandleFunc("/tables/{db}/{table}/regions", func(w http.ResponseWriter, req *http.Request) {
				request := NewRegionsHTTPRequest(s, w, req)
				request.HandleListRegions()
			})

			router.HandleFunc("/regions/{regionID}", func(w http.ResponseWriter, req *http.Request) {
				request := NewRegionsHTTPRequest(s, w, req)
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
		}()
	})
}

// TiDB status
type status struct {
	Connections int    `json:"connections"`
	Version     string `json:"version"`
	GitHash     string `json:"git_hash"`
}
