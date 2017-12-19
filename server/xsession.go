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
	"sync/atomic"

	"github.com/juju/errors"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/xprotocol/notice"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Session"
	log "github.com/sirupsen/logrus"
	goctx "golang.org/x/net/context"
)

type sessionState int32

const (
	// start as Authenticating
	authenticating sessionState = iota
	// once authenticated, we can handle work
	ready
	// connection is closing, but wait for data to flush out first
	closing
)

type xSession struct {
	xcc                    *xClientConn
	xsql                   *xSQL
	crud                   *xCrud
	sessionID              uint32
	sendWarnings           bool
	sendXPluginDeprecation bool
	authHandler            authHandler
	state                  sessionState
	stateBeforeClose       sessionState
}

func (xcc *xClientConn) createXSession() *xSession {
	return &xSession{
		xcc:                    xcc,
		xsql:                   createXSQL(xcc),
		crud:                   createCrud(xcc),
		sessionID:              atomic.AddUint32(&baseSessionID, 1),
		sendWarnings:           true,
		sendXPluginDeprecation: true,
		state:            authenticating,
		stateBeforeClose: authenticating,
	}
}

func (xs *xSession) setSendWarnings(flag bool) {
	xs.sendWarnings = flag
}

func (xs *xSession) getSendWarnings() bool {
	return xs.sendWarnings
}

func (xs *xSession) setXPluginDeprecation(flag bool) {
	xs.sendXPluginDeprecation = flag
}

func (xs *xSession) getXPluginDeprecation() bool {
	return xs.sendXPluginDeprecation
}

func (xs *xSession) handleMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	if xs.state == authenticating {
		return xs.handleAuthMessage(msgType, payload)
	} else if xs.state == ready {
		return xs.handleReadyMessage(msgType, payload)
	}
	return util.ErrXBadMessage
}

func (xs *xSession) handleReadyMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	if finish, err := xs.handleSessionMessage(msgType, payload); err != nil {
		return errors.Trace(err)
	} else if finish {
		return nil
	}
	if err := xs.dispatchCommand(msgType, payload); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xs *xSession) handleSessionMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) (bool, error) {
	switch msgType {
	case Mysqlx.ClientMessages_SESS_CLOSE:
		if err := util.SendOK(xs.xcc.pkt, "bye!"); err != nil {
			return true, errors.Trace(err)
		}
		xs.onClose(true)
		return true, nil
	case Mysqlx.ClientMessages_CON_CLOSE:
		if err := util.SendOK(xs.xcc.pkt, "bye!"); err != nil {
			return true, errors.Trace(err)
		}
		xs.onClose(true)
		return true, nil
	case Mysqlx.ClientMessages_SESS_RESET:
		xs.state = closing
		xs.onSessionReset()
		return true, nil
	}
	return false, nil
}

func (xs *xSession) dispatchCommand(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	span := opentracing.StartSpan("xserver.dispatch")
	goCtx := opentracing.ContextWithSpan(goctx.Background(), span)
	switch msgType {
	case Mysqlx.ClientMessages_SQL_STMT_EXECUTE:
		return xs.xsql.dealSQLStmtExecute(goCtx, payload)
	// @TODO will support in next pr
	case Mysqlx.ClientMessages_CRUD_FIND, Mysqlx.ClientMessages_CRUD_INSERT, Mysqlx.ClientMessages_CRUD_UPDATE, Mysqlx.ClientMessages_CRUD_DELETE,
		Mysqlx.ClientMessages_CRUD_CREATE_VIEW, Mysqlx.ClientMessages_CRUD_MODIFY_VIEW, Mysqlx.ClientMessages_CRUD_DROP_VIEW:
		return xs.crud.dealCrudStmtExecute(goCtx, msgType, payload)
	// @TODO will support in next pr
	case Mysqlx.ClientMessages_EXPECT_OPEN, Mysqlx.ClientMessages_EXPECT_CLOSE:
		return util.ErrXBadMessage
	default:
		return util.ErrXBadMessage
	}
}

func (xs *xSession) handleAuthMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	var r *response
	switch msgType {
	case Mysqlx.ClientMessages_SESS_AUTHENTICATE_START:
		var data Mysqlx_Session.AuthenticateStart
		if err := data.Unmarshal(payload); err != nil {
			log.Errorf("[%d] Can't Unmarshal message %s, err %s", xs.xcc.connectionID, msgType.String(), err.Error())
			return util.ErrXBadMessage
		}

		xs.authHandler = xs.createAuthHandler(*data.MechName)
		if xs.authHandler == nil {
			log.Errorf("[%d] Can't create xAuth handler with mech name %s", xs.xcc.connectionID, *data.MechName)
			xs.stopAuth()
			return util.ErrNotSupportedAuthMode
		}

		r = xs.authHandler.handleStart(data.MechName, data.AuthData, data.InitialResponse)
	case Mysqlx.ClientMessages_SESS_AUTHENTICATE_CONTINUE:
		var data Mysqlx_Session.AuthenticateContinue
		if err := data.Unmarshal(payload); err != nil {
			log.Errorf("[%d] Can't Unmarshal message %s, err %s", xs.xcc.connectionID, msgType.String(), err.Error())
			return util.ErrXBadMessage
		}

		r = xs.authHandler.handleContinue(data.GetAuthData())
	default:
		xs.stopAuth()
		return util.ErrXBadMessage
	}

	switch r.status {
	case authSucceed:
		if err := xs.onAuthSuccess(r); err != nil {
			return errors.Trace(err)
		}
		if xs.ready() {
			xs.xcc.state = clientRunning
			if xs.xcc.dbname != "" {
				if err := xs.xcc.useDB(xs.xcc.dbname); err != nil {
					if err = xs.xcc.writeError(err); err != nil {
						return errors.Trace(err)
					}
				}
			}
			xs.xcc.ctx.SetSessionManager(xs.xcc.server)
		}
	case authFailed:
		xs.onAuthFailure(r)
		return util.ErrorMessage(mysql.ErrAccessDenied, r.data)
	default:
		if err := xs.sendAuthContinue(&r.data); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (xs *xSession) onAuthSuccess(r *response) error {
	if err := notice.SendClientID(xs.xcc.pkt, xs.xcc.connectionID); err != nil {
		return errors.Trace(err)
	}
	xs.stopAuth()
	xs.state = ready
	if err := xs.sendAuthOk(&r.data); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xs *xSession) onAuthFailure(r *response) {
	xs.stopAuth()
}

//@TODO need to implement
func (xs *xSession) onSessionReset() {
}

func (xs *xSession) onClose(updateOldState bool) {
	if xs.state != closing {
		if updateOldState {
			xs.stateBeforeClose = xs.state
		}
		xs.state = closing
		if err := xs.xcc.Close(); err != nil {
			log.Errorf("error occurs when closing X Protocol connection")
		}
	}
}

func (xs *xSession) stopAuth() {
	xs.authHandler = nil
}

func (xs *xSession) ready() bool {
	return xs.state == ready
}

func (xs *xSession) sendAuthOk(value *string) error {
	msg := Mysqlx_Session.AuthenticateOk{
		AuthData: []byte(*value),
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	return xs.xcc.pkt.WritePacket(Mysqlx.ServerMessages_SESS_AUTHENTICATE_OK, data)
}

func (xs *xSession) sendAuthContinue(value *string) error {
	msg := Mysqlx_Session.AuthenticateContinue{
		AuthData: []byte(*value),
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	return xs.xcc.pkt.WritePacket(Mysqlx.ServerMessages_SESS_AUTHENTICATE_CONTINUE, data)
}
