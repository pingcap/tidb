// Copyright 2025 PingCAP, Inc.
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

//go:build flightsql

package server

import (
	"net"
	"strconv"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// flightSQLState holds FlightSQL-specific server state.
type flightSQLState struct {
	listener net.Listener
	server   *FlightSQLServer
}

func initFlightSQLListener(s *Server) error {
	if s.cfg.Host == "" {
		return nil
	}
	if s.cfg.FlightSQLPort == 0 && !RunInGoTest {
		return nil
	}

	addr := net.JoinHostPort(s.cfg.Host, strconv.Itoa(int(s.cfg.FlightSQLPort)))
	tcpProto := "tcp"
	if s.cfg.EnableTCP4Only {
		tcpProto = "tcp4"
	}

	listener, err := net.Listen(tcpProto, addr)
	if err != nil {
		return errors.Trace(err)
	}

	logutil.BgLogger().Info("server is running Arrow Flight SQL protocol", zap.String("addr", addr))

	if RunInGoTest && s.cfg.FlightSQLPort == 0 {
		s.cfg.FlightSQLPort = uint(listener.Addr().(*net.TCPAddr).Port)
	}

	// Store state in the server's flightSQL field
	s.flightSQL = &flightSQLState{listener: listener}
	return nil
}

func startFlightSQLServer(s *Server, errChan chan error) {
	errChan <- func() error {
		if s.flightSQL == nil {
			return nil
		}
		state := s.flightSQL.(*flightSQLState)
		if state.listener == nil {
			return nil
		}

		server, err := NewFlightSQLServer(s)
		if err != nil {
			return err
		}

		state.server = server
		return server.Serve(state.listener)
	}()
}

func closeFlightSQLServer(s *Server) {
	if s.flightSQL == nil {
		return
	}
	state := s.flightSQL.(*flightSQLState)
	if state.server != nil {
		state.server.Shutdown()
		state.server = nil
	}
	if state.listener != nil {
		err := state.listener.Close()
		if err != nil {
			logutil.BgLogger().Error("close FlightSQL listener error", zap.Error(err))
		}
		state.listener = nil
	}
}

func getFlightSQLListenerAddr(s *Server) net.Addr {
	if s.flightSQL == nil {
		return nil
	}
	state := s.flightSQL.(*flightSQLState)
	if state.listener == nil {
		return nil
	}
	return state.listener.Addr()
}

func getFlightSQLPort(s *Server) uint {
	if s.flightSQL == nil {
		return 0
	}
	state := s.flightSQL.(*flightSQLState)
	if state.listener == nil {
		return 0
	}
	return uint(state.listener.Addr().(*net.TCPAddr).Port)
}
