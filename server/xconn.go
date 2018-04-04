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
	"io"
	"net"
	"runtime"
	"sync"

	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/terror"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/arena"
	"github.com/pingcap/tidb/xprotocol/capability"
	xutil "github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tidb/xprotocol/xpacketio"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Connection"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

// xClientConn represents a connection between server and client,
// it maintains connection specific state, handles client query.
type xClientConn struct {
	pkt          *xpacketio.XPacketIO           // a helper to read and write data in packet format.
	conn         net.Conn                       // MySQL conn, used by authentication
	xsession     *xSession                      // client session
	server       *Server                        // a reference of server instance.
	capability   uint32                         // client capability affects the way server handles client request.
	capabilities Mysqlx_Connection.Capabilities // mysql shell client capabilities affects the way server handles client request.
	connectionID uint32                         // atomically allocated by a global variable, unique in process scope.
	collation    uint8                          // collation used by client, may be different from the collation used by database.
	user         string                         // user of the client.
	dbname       string                         // default database name.
	salt         []byte                         // random bytes used for authentication.
	alloc        arena.Allocator                // an memory allocator for reducing memory allocation.
	lastCmd      string                         // latest sql query string, currently used for logging error.
	ctx          QueryCtx                       // an interface to execute sql statements.
	attrs        map[string]string              // attributes parsed from client handshake response, not used for now.
	state        clientState                    // client state

	// mu is used for cancelling the execution of current transaction.
	mu struct {
		sync.RWMutex
		cancelFunc context.CancelFunc
	}
}

type clientState int32

var baseSessionID uint32

const (
	clientInvalid clientState = iota
	clientAccepted
	clientAcceptedWithSession
	clientAuthenticatingFirst
	clientRunning
	clientClosing
	clientClosed
)

func (xcc *xClientConn) Run() {
	const size = 4096
	defer func() {
		r := recover()
		if r != nil {
			buf := make([]byte, size)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			log.Errorf("lastCmd %s, %v, %s", xcc.lastCmd, r, buf)
		}
		err := xcc.Close()
		terror.Log(errors.Trace(err))
		log.Infof("[%d] connection exit.", xcc.connectionID)
	}()

	log.Infof("[%d] establish connection successfully.", xcc.connectionID)
	for xcc.state != clientClosed && xcc.xsession != nil {
		tp, payload, err := xcc.pkt.ReadPacket()
		if err != nil {
			if terror.ErrorNotEqual(err, io.EOF) {
				log.Errorf("[%d] read packet error, close this connection %s",
					xcc.connectionID, errors.ErrorStack(err))
			}
			return
		}
		log.Debugf("[%d] receive msg type[%d]", xcc.connectionID, tp)

		span := opentracing.StartSpan("server.dispatch")
		goCtx := opentracing.ContextWithSpan(context.Background(), span)
		_, cancelFunc := context.WithCancel(goCtx)
		xcc.mu.Lock()
		xcc.mu.cancelFunc = cancelFunc
		xcc.mu.Unlock()

		if xcc.state != clientAccepted && xcc.xsession != nil {
			if err = xcc.xsession.handleMessage(tp, payload); err != nil {
				if terror.ErrorEqual(err, terror.ErrResultUndetermined) {
					log.Errorf("[%d] result undetermined error, close this connection %s",
						xcc.connectionID, errors.ErrorStack(err))
					return
				} else if terror.ErrorEqual(err, terror.ErrCritical) {
					log.Errorf("[%d] critical error, stop the server listener %s",
						xcc.connectionID, errors.ErrorStack(err))
					select {
					case xcc.server.stopListenerCh <- struct{}{}:
					default:
					}
					return
				}
				log.Warnf("[%d] handle session message error: %s", xcc.connectionID, err)
				err1 := xcc.writeError(err)
				terror.Log(errors.Trace(err1))
			}
		} else {
			// Do something we could do before normal sql or crud.
			if err = xcc.handleMessage(tp, payload); err != nil {
				err1 := xcc.writeError(err)
				terror.Log(errors.Trace(err1))
			}
		}
	}
}

func (xcc *xClientConn) Close() error {
	xcc.state = clientClosing
	xcc.server.rwlock.Lock()
	delete(xcc.server.clients, xcc.connectionID)
	connections := len(xcc.server.clients)
	xcc.server.rwlock.Unlock()
	metrics.ConnGauge.Set(float64(connections))
	if err := xcc.conn.Close(); err != nil {
		return errors.Trace(err)
	}
	if xcc.ctx != nil {
		return xcc.ctx.Close()
	}
	return nil
}

func (xcc *xClientConn) handleMessage(tp Mysqlx.ClientMessages_Type, msg []byte) error {
	switch tp {
	case Mysqlx.ClientMessages_CON_CLOSE:
		if err := xutil.SendOK(xcc.pkt, "bye!"); err != nil {
			return errors.Trace(err)
		}
		return xcc.Close()
	case Mysqlx.ClientMessages_SESS_RESET:
		return nil
	case Mysqlx.ClientMessages_CON_CAPABILITIES_GET:
		return xcc.getCapabilities()
	case Mysqlx.ClientMessages_CON_CAPABILITIES_SET:
		return xcc.setCapabilities(msg)
	case Mysqlx.ClientMessages_SESS_AUTHENTICATE_START:
		xcc.state = clientAuthenticatingFirst
		if xcc.xsession != nil {
			return xcc.xsession.handleMessage(tp, msg)
		}
		fallthrough
	default:
		log.Infof("%d: Invalid message %s received during client initialization", xcc.connectionID, tp.String())
		return xutil.ErrXBadMessage
	}
}

func (xcc *xClientConn) getCapabilities() error {
	resp, err := xcc.capabilities.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	if err = xcc.pkt.WritePacket(Mysqlx.ServerMessages_CONN_CAPABILITIES, resp); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xcc *xClientConn) setCapabilities(msg []byte) error {
	vals, err := capability.DecodeCapabilitiesSetMsg(msg)
	if err != nil {
		return err
	}

	if expire, ok := vals["client.pwd_expire_ok"]; ok {
		if expire {
			xcc.addCapability(&capability.HandlerExpiredPasswords{
				Name:    "client.pwd_expire_ok",
				Expired: true})
			return xcc.pkt.WritePacket(Mysqlx.ServerMessages_OK, []byte{})
		}
	}

	if useTLS, ok := vals["tls"]; ok {
		if useTLS {
			return xcc.writeError(xutil.ErrXCapabilitiesPrepareFailed.GenByArgs("tls"))
		}
	}
	return nil
}

func (xcc *xClientConn) handshake() error {
	// Open session
	ctx, err := xcc.server.driver.OpenCtx(uint64(xcc.connectionID), xcc.capability, uint8(xcc.collation), xcc.dbname, nil)
	if err != nil {
		return errors.Trace(err)
	}
	xcc.ctx = ctx
	xcc.xsession = xcc.createXSession()
	xcc.configCapabilities()
	xcc.state = clientAccepted

	return nil
}

func (xcc *xClientConn) flush() error {
	return xcc.pkt.Flush()
}

func (xcc *xClientConn) writeError(e error) error {
	var (
		m  *mysql.SQLError
		te *terror.Error
		ok bool
	)
	originErr := errors.Cause(e)
	if te, ok = originErr.(*terror.Error); ok {
		m = te.ToSQLError()
	} else {
		m = mysql.NewErrf(mysql.ErrUnknown, "%s", e.Error())
	}
	errMsg, err := xutil.XErrorMessage(m.Code, m.Message, m.State).Marshal()
	if err != nil {
		return err
	}
	return xcc.pkt.WritePacket(Mysqlx.ServerMessages_ERROR, errMsg)
}

func (xcc *xClientConn) isKilled() bool {
	return xcc.state == clientClosed
}

func (xcc *xClientConn) Cancel(query bool) {
	if !query {
		xcc.state = clientClosed
	}
}

func (xcc *xClientConn) id() uint32 {
	return xcc.connectionID
}

func (xcc *xClientConn) showProcess() util.ProcessInfo {
	return xcc.ctx.ShowProcess()
}

func (xcc *xClientConn) useDB(db string) (err error) {
	// if input is "use `SELECT`", mysql client just send "SELECT"
	// so we add `` around db.
	_, err = xcc.ctx.Execute(context.Background(), "use `"+db+"`")
	if err != nil {
		return errors.Trace(err)
	}
	xcc.dbname = db
	return
}

func (xcc *xClientConn) addCapability(h capability.Handler) {
	xcc.capabilities.Capabilities = append(xcc.capabilities.Capabilities, h.Get())
}

func (xcc *xClientConn) configCapabilities() {
	xcc.addCapability(&capability.HandlerAuthMechanisms{Values: []string{"MYSQL41"}})
	xcc.addCapability(&capability.HandlerReadOnlyValue{Name: "doc.formats", Value: "text"})
	xcc.addCapability(&capability.HandlerReadOnlyValue{Name: "node_type", Value: "mysql"})
}

func (xcc *xClientConn) lockConn() {
	xcc.mu.RLock()
}

func (xcc *xClientConn) unlockConn() {
	xcc.mu.RUnlock()
}

func (xcc *xClientConn) getCancelFunc() context.CancelFunc {
	return xcc.mu.cancelFunc
}
