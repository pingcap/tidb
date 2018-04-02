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
	"fmt"
	"net"

	"github.com/pingcap/tidb/util"
	"golang.org/x/net/context"
)

// baseClientConn is used to communicate with user's client, it is usually dependence goroutine.
type baseClientConn interface {
	// start the main routine, receive message from client and reply the result set.
	Run()

	// check the legacy of the client.
	handshake() error

	// return the connection id of the client
	id() uint32

	// return the process in fo the connection
	showProcess() util.ProcessInfo
	lockConn()
	unlockConn()
	getCancelFunc() context.CancelFunc
}

// create client connection according to the server type, mysql or x-protocol
func createClientConn(conn net.Conn, s *Server) baseClientConn {
	switch s.tp {
	case MySQLProtocol:
		return s.newConn(conn)
	case MySQLXProtocol:
		return s.newXConn(conn)
	default:
		panic(fmt.Sprintf("can't create client connection, unknown server type [%d].", s.tp))
	}
}
