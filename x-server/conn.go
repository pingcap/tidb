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
	"io"
	"net"

	"github.com/juju/errors"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util/arena"
	log "github.com/sirupsen/logrus"
)

// clientConn represents a connection between server and client,
// it maintains connection specific state, handles client query.
type clientConn struct {
	conn         net.Conn
	server       *Server         // a reference of server instance.
	connectionID uint32          // atomically allocated by a global variable, unique in process scope.
	collation    uint8           // collation used by client, may be different from the collation used by database.
	user         string          // user of the client.
	dbname       string          // default database name.
	salt         []byte          // random bytes used for authentication.
	alloc        arena.Allocator // an memory allocator for reducing memory allocation.
	killed       bool
}

func (cc *clientConn) Run() {
	defer func() {
		x := recover()
		if x != nil {
			log.Error(x)
		}
		err := cc.Close()
		terror.Log(errors.Trace(err))
	}()

	for !cc.killed {
		tp, payload, err := cc.readPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				log.Errorf("[%d] read packet error, close this connection %s",
					cc.connectionID, errors.ErrorStack(err))
			}
			return
		}
		if err = cc.dispatch(tp, payload); err != nil {
			if terror.ErrorEqual(err, terror.ErrResultUndetermined) {
				log.Errorf("[%d] result undetermined error, close this connection %s",
					cc.connectionID, errors.ErrorStack(err))
			} else if terror.ErrorEqual(err, terror.ErrCritical) {
				log.Errorf("[%d] critical error, stop the server listener %s",
					cc.connectionID, errors.ErrorStack(err))
				select {
				case cc.server.stopListenerCh <- struct{}{}:
				default:
				}
			}
			log.Warnf("[%d] dispatch error: %s, %s", cc.connectionID, cc, err)
			cc.writeError(err)
			return
		}
	}
}

func (cc *clientConn) Close() error {
	err := cc.conn.Close()
	return errors.Trace(err)
}

func (cc *clientConn) handshake() error {
	// TODO: implement it.
	return nil
}

// readPacket reads a full size request encoded in x protocol.
// The message struct is like:
// ______________________________________________________
// | 4 bytes length | 1 byte type | payload[0:length-1] |
// ------------------------------------------------------
// See: https://dev.mysql.com/doc/internals/en/x-protocol-messages-messages.html
func (cc *clientConn) readPacket() (byte, []byte, error) {
	return 0x00, nil, nil
}

func (cc *clientConn) dispatch(tp byte, payload []byte) error {
	return nil
}

func (cc *clientConn) writeError(e error) {
}
