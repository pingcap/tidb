// The MIT License (MIT)
//
// Copyright (c) 2014 wandoulabs
// Copyright (c) 2014 siddontang
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of
// this software and associated documentation files (the "Software"), to deal in
// the Software without restriction, including without limitation the rights to
// use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software is furnished to do so,
// subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.

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
	"math/rand"
	"net"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/arena"
)

var (
	baseConnID uint32 = 10000
)

// Server is the MySQL protocol server
type Server struct {
	cfg               *Config
	driver            IDriver
	listener          net.Listener
	concurrentLimiter *TokenLimiter
	clients           map[uint32]*clientConn
}

func (s *Server) getToken() *Token {
	return s.concurrentLimiter.Get()
}

func (s *Server) releaseToken(token *Token) {
	s.concurrentLimiter.Put(token)
}

// Generate a random string using ASCII characters but avoid seperator character.
// See: https://github.com/mysql/mysql-server/blob/5.7/mysys_ssl/crypt_genhash_impl.cc#L435
func randomBuf(size int) []byte {
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = byte(rand.Intn(127))
		if buf[i] == 0 || buf[i] == byte('$') {
			buf[i]++
		}
	}
	return buf
}

func (s *Server) newConn(conn net.Conn) (cc *clientConn, err error) {
	log.Info("newConn", conn.RemoteAddr().String())
	cc = &clientConn{
		conn:         conn,
		pkg:          newPacketIO(conn),
		server:       s,
		connectionID: atomic.AddUint32(&baseConnID, 1),
		collation:    mysql.DefaultCollationID,
		charset:      mysql.DefaultCharset,
		alloc:        arena.NewAllocator(32 * 1024),
	}
	cc.salt = randomBuf(20)
	return
}

func (s *Server) skipAuth() bool {
	return s.cfg.SkipAuth
}

// NewServer creates a new Server.
func NewServer(cfg *Config, driver IDriver) (*Server, error) {
	s := &Server{
		cfg:               cfg,
		driver:            driver,
		concurrentLimiter: NewTokenLimiter(100),
		clients:           make(map[uint32]*clientConn),
	}

	var err error
	s.listener, err = net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Init rand seed for randomBuf()
	rand.Seed(time.Now().UTC().UnixNano())
	log.Infof("Server run MySql Protocol Listen at [%s]", s.cfg.Addr)
	return s, nil
}

// Run runs the server.
func (s *Server) Run() error {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			log.Errorf("accept error %s", err.Error())
			return errors.Trace(err)
		}

		go s.onConn(conn)
	}
}

// Close closes the server.
func (s *Server) Close() {
	if s.listener != nil {
		s.listener.Close()
		s.listener = nil
	}
}

func (s *Server) onConn(c net.Conn) {
	conn, err := s.newConn(c)
	if err != nil {
		log.Errorf("newConn error %s", errors.ErrorStack(err))
		return
	}
	if err := conn.handshake(); err != nil {
		log.Errorf("handshake error %s", errors.ErrorStack(err))
		c.Close()
		return
	}
	defer func() {
		log.Infof("close %s", conn)
	}()

	s.clients[conn.connectionID] = conn

	conn.Run()
}
