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
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/autoid"
	autoid "github.com/pingcap/tidb/pkg/autoid_service"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/terror"
	"github.com/pingcap/tidb/pkg/server/handler/optimizor"
	util2 "github.com/pingcap/tidb/pkg/server/internal/util"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/statistics/handle/initstats"
	"github.com/pingcap/tidb/pkg/store"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/traceevent"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/channelz/service"
)

func (s *Server) setupAutoIDService(grpcServer *grpc.Server) {
	keyspaceName := config.GetGlobalKeyspaceName()

	var fullPath string
	if keyspaceName == "" {
		fullPath = fmt.Sprintf("%s://%s", s.cfg.Store, s.cfg.Path)
	} else {
		fullPath = fmt.Sprintf("%s://%s?keyspaceName=%s", s.cfg.Store, s.cfg.Path, keyspaceName)
	}

	store, err := store.New(fullPath)
	if err != nil {
		logutil.BgLogger().Error("new tikv store fail", zap.Error(err))
		return
	}

	ebd, ok := store.(kv.EtcdBackend)
	if !ok {
		return
	}

	etcdAddr, err := ebd.EtcdAddrs()
	if err != nil {
		logutil.BgLogger().Error("tikv store not etcd background", zap.Error(err))
		return
	}

	selfAddr := net.JoinHostPort(s.cfg.AdvertiseAddress, strconv.Itoa(int(s.cfg.Status.StatusPort)))
	service := autoid.New(selfAddr, etcdAddr, store, ebd.TLSConfig())
	logutil.BgLogger().Info("register auto service at", zap.String("addr", selfAddr))
	pb.RegisterAutoIDAllocServer(grpcServer, service)
	s.autoIDService = service
}

func (s *Server) startStatusServerAndRPCServer(serverMux *http.ServeMux) {
	m := cmux.New(s.statusListener)
	// Match connections in order:
	// First HTTP, and otherwise grpc.
	httpL := m.Match(cmux.HTTP1Fast())
	grpcL := m.Match(cmux.Any())

	statusServer := &http.Server{Addr: s.statusAddr, Handler: util2.NewCorsHandler(serverMux, s.cfg)}
	grpcServer := NewRPCServer(s.cfg, s.dom, s)
	service.RegisterChannelzServiceToServer(grpcServer)
	if s.cfg.Store == config.StoreTypeTiKV {
		s.setupAutoIDService(grpcServer)
	}

	s.statusServer.Store(statusServer)
	s.grpcServer = grpcServer

	go util.WithRecovery(func() {
		err := grpcServer.Serve(grpcL)
		logutil.BgLogger().Warn("grpc server error", zap.Error(err))
	}, nil)

	go util.WithRecovery(func() {
		err := statusServer.Serve(httpL)
		logutil.BgLogger().Warn("http server error", zap.Error(err))
	}, nil)

	err := m.Serve()
	if err != nil {
		logutil.BgLogger().Warn("start status/rpc server error", zap.Error(err))
	}
}

// SetCNChecker set the CN checker for server.
func (s *Server) SetCNChecker(tlsConfig *tls.Config) *tls.Config {
	if tlsConfig != nil && len(s.cfg.Security.ClusterVerifyCN) != 0 {
		checkCN := make(map[string]struct{})
		for _, cn := range s.cfg.Security.ClusterVerifyCN {
			cn = strings.TrimSpace(cn)
			checkCN[cn] = struct{}{}
		}
		tlsConfig.VerifyPeerCertificate = func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
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

// Status of TiDB.
type Status struct {
	Connections int          `json:"connections"`
	Version     string       `json:"version"`
	GitHash     string       `json:"git_hash"`
	Status      DetailStatus `json:"status"`
}

// DetailStatus is to show the detail status of TiDB. for example the init stats percentage.
type DetailStatus struct {
	InitStatsPercentage float64 `json:"init_stats_percentage"`
}

func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	// If the server is in the process of shutting down, return a non-200 status.
	// It is important not to return Status{} as acquiring the s.ConnectionCount()
	// acquires a lock that may already be held by the shutdown process.
	if !s.health.Load() {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	initStatsPercentage := min(100, initstats.InitStatsPercentage.Load())
	st := Status{
		Connections: s.ConnectionCount(),
		Version:     mysql.ServerVersion,
		GitHash:     versioninfo.TiDBGitHash,
		Status: DetailStatus{
			InitStatsPercentage: initStatsPercentage,
		},
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

func (s *Server) newStatsHandler() *optimizor.StatsHandler {
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		panic("Illegal driver")
	}

	do, err := session.GetDomain(store.store)
	if err != nil {
		panic("Failed to get domain")
	}
	return optimizor.NewStatsHandler(do)
}

func (s *Server) newStatsHistoryHandler() *optimizor.StatsHistoryHandler {
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		panic("Illegal driver")
	}

	do, err := session.GetDomain(store.store)
	if err != nil {
		panic("Failed to get domain")
	}
	return optimizor.NewStatsHistoryHandler(do)
}

func (s *Server) newStatsPriorityQueueHandler() *optimizor.StatsPriorityQueueHandler {
	store, ok := s.driver.(*TiDBDriver)
	if !ok {
		panic("Illegal driver")
	}

	do, err := session.GetDomain(store.store)
	if err != nil {
		panic("Failed to get domain")
	}

	return optimizor.NewStatsPriorityQueueHandler(do)
}

var traceeventCounter = make(chan struct{}, 1)

func traceeventHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	select {
	case traceeventCounter <- struct{}{}:
	default:
		http.Error(w, "http api /debug/traceevent only allow one request at a time", http.StatusTooManyRequests)
		return
	}
	defer func() {
		<-traceeventCounter
	}()

	var cfg traceevent.FlightRecorderConfig
	cfg.Initialize()
	body, _ := io.ReadAll(r.Body)
	if len(body) > 0 {
		if err := json.Unmarshal(body, &cfg); err != nil {
			http.Error(w, fmt.Sprintf("decode json failed: %s", err.Error()), http.StatusBadRequest)
			return
		}
	}

	logutil.BgLogger().Info("http api /debug/traceevent called", zap.Any("config", cfg))
	ch := make(chan []traceevent.Event, 256)
	recorder, err := traceevent.StartHTTPFlightRecorder(ch, &cfg)
	if err != nil {
		http.Error(w, fmt.Sprintf("validate config failed: %s", err.Error()), http.StatusBadRequest)
		return
	}
	defer recorder.Close()

	for {
		select {
		case events := <-ch:
			res := traceevent.ConvertEventsForRendering(events)
			fmt.Fprintf(w, "data: ")
			enc := json.NewEncoder(w)
			err := enc.Encode(res)
			if err != nil {
				logutil.BgLogger().Info("http api /debug/traceevent encode fail", zap.Error(err))
			}
			fmt.Fprintf(w, "\n\n")
			flusher.Flush()

		case <-r.Context().Done():
			logutil.BgLogger().Info("http api /debug/traceevent client disconnected")
			return
		}
	}
}
