// Copyright 2015 PingCAP, Inc.
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
	"net"

	log "github.com/Sirupsen/logrus"
	"github.com/pingcap/tidb/util"
)

// clineConn used to communicate with user's client, It is usually dependence goroutine.
type clientConn interface {
	// start the main routine, receive message from client and reply the result set.
	Run()

	// cancel the connection.
	Cancel(query bool)

	// check the legacy of the client.
	handshake() error

	// check whether the client is killed
	isKilled() bool

	// return the connection id of the client
	id() uint32

	// return the process in fo the connection
	showProcess() util.ProcessInfo
}

// create client connection according to the server type, mysql or x-protocol
func createClientConn(conn net.Conn, s *Server) clientConn {
	switch s.tp {
	case MysqlProtocol:
		return s.newConn(conn)
	case MysqlXProtocol:
		return s.newXConn(conn)
	default:
		log.Errorf("can't create client connection, unknown server type [%d].", s.tp)
		return nil
	}
}
