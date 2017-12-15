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

package xserver

import (
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/arena"
	log "github.com/sirupsen/logrus"
	// For MySQL X Protocol
	_ "github.com/pingcap/tipb/go-mysqlx"
	_ "github.com/pingcap/tipb/go-mysqlx/Connection"
	_ "github.com/pingcap/tipb/go-mysqlx/Crud"
	_ "github.com/pingcap/tipb/go-mysqlx/Datatypes"
	_ "github.com/pingcap/tipb/go-mysqlx/Expect"
	_ "github.com/pingcap/tipb/go-mysqlx/Expr"
	_ "github.com/pingcap/tipb/go-mysqlx/Notice"
	_ "github.com/pingcap/tipb/go-mysqlx/Resultset"
	_ "github.com/pingcap/tipb/go-mysqlx/Session"
	_ "github.com/pingcap/tipb/go-mysqlx/Sql"
)

var (
	baseConnID uint32
)

// Server is the MySQL X protocol server
type Server struct {
	cfg               *Config
	listener          net.Listener
	rwlock            *sync.RWMutex
	concurrentLimiter *server.TokenLimiter

	stopListenerCh chan struct{}
}

// NewServer creates a new Server.
func NewServer(cfg *Config) (s *Server, err error) {
	s = &Server{
		cfg:               cfg,
		concurrentLimiter: server.NewTokenLimiter(cfg.TokenLimit),
		rwlock:            &sync.RWMutex{},
		stopListenerCh:    make(chan struct{}, 1),
	}
	if cfg.Socket != "" {
		cfg.SkipAuth = true
		s.listener, err = net.Listen("unix", cfg.Socket)
	} else {
		s.listener, err = net.Listen("tcp", s.cfg.Addr)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	rand.Seed(time.Now().UTC().UnixNano())
	log.Infof("Server run MySQL Protocol Listen at [%s]", s.cfg.Addr)
	return s, nil
}

// Close closes the server.
func (s *Server) Close() {
	if s.listener != nil {
		err := s.listener.Close()
		terror.Log(errors.Trace(err))
		s.listener = nil
	}
}

// Run runs the server.
func (s *Server) Run() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok {
				if opErr.Err.Error() == "use of closed network connection" {
					return nil
				}
			}
			log.Errorf("accept error %s", err.Error())
			return errors.Trace(err)
		}
		if s.shouldStopListener() {
			err = conn.Close()
			terror.Log(errors.Trace(err))
			break
		}
		go s.onConn(conn)
	}
	return nil
}

func (s *Server) shouldStopListener() bool {
	select {
	case <-s.stopListenerCh:
		return true
	default:
		return false
	}
}

// onConn runs in its own goroutine, handles queries from this connection.
func (s *Server) onConn(c net.Conn) {
	conn := s.newConn(c)
	defer func() {
		log.Infof("[%d] close x protocol connection", conn.connectionID)
	}()
	if err := conn.handshake(); err != nil {
		// Some keep alive services will send request to TiDB and disconnect immediately.
		// So we use info log level.
		log.Infof("handshake error %s", errors.ErrorStack(err))
		err := c.Close()
		terror.Log(errors.Trace(err))
		return
	}
	conn.Run()
}

// newConn creates a new *clientConn from a net.Conn.
// It allocates a connection ID and random salt data for authentication.
func (s *Server) newConn(conn net.Conn) *clientConn {
	cc := &clientConn{
		conn:         conn,
		server:       s,
		connectionID: atomic.AddUint32(&baseConnID, 1),
		collation:    mysql.DefaultCollationID,
		alloc:        arena.NewAllocator(32 * 1024),
	}
	log.Infof("[%d] new x protocol connection %s", cc.connectionID, conn.RemoteAddr().String())
	cc.salt = util.RandomBuf(20)
	return cc
}
