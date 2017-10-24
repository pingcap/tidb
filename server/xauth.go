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
	log "github.com/Sirupsen/logrus"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/xprotocol/notice"
	"github.com/pingcap/tidb/xprotocol/util"
	"github.com/pingcap/tipb/go-mysqlx"
	"github.com/pingcap/tipb/go-mysqlx/Session"
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

type xAuth struct {
	xcc               *mysqlXClientConn
	authHandler       authHandler
	mState            sessionState
	mStateBeforeClose sessionState
}

func (xa *xAuth) handleMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	if xa.mState == authenticating {
		return xa.handleAuthMessage(msgType, payload)
	} else if xa.mState == ready {
		return xa.handleReadyMessage(msgType, payload)
	}

	// This is not the same as it is in mysql-x-plugin, which returns nothing, and you should never get here.
	return util.ErrorMessage(mysql.ErrUnknown, "unknown ssession state.")
}

func (xa *xAuth) handleReadyMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	switch msgType {
	case Mysqlx.ClientMessages_SESS_CLOSE:
		if err := notice.SendNoticeOK(xa.xcc.pkt, "bye!"); err != nil {
			return errors.Trace(err)
		}
		xa.onClose(false)
		return nil
	case Mysqlx.ClientMessages_CON_CLOSE:
		if err := notice.SendNoticeOK(xa.xcc.pkt, "bye!"); err != nil {
			return errors.Trace(err)
		}
		xa.onClose(false)
		return nil
	case Mysqlx.ClientMessages_SESS_RESET:
		xa.mState = closing
		xa.onSessionReset()
		return nil
	}
	return util.ErrXBadMessage
}

func (xa *xAuth) handleAuthMessage(msgType Mysqlx.ClientMessages_Type, payload []byte) error {
	var r *response
	switch msgType {
	case Mysqlx.ClientMessages_SESS_AUTHENTICATE_START:
		var data Mysqlx_Session.AuthenticateStart
		if err := data.Unmarshal(payload); err != nil {
			log.Errorf("[%d] Can't Unmarshal message %s, err %s", xa.xcc.connectionID, msgType.String(), err.Error())
			return util.ErrXBadMessage
		}

		xa.authHandler = xa.createAuthHandler(*data.MechName)
		if xa.authHandler == nil {
			log.Errorf("[%d] Can't create xAuth handler with mech name %s", xa.xcc.connectionID, *data.MechName)
			xa.stopAuth()
			return util.ErrNotSupportedAuthMode
		}

		r = xa.authHandler.handleStart(data.MechName, data.AuthData, data.InitialResponse)
	case Mysqlx.ClientMessages_SESS_AUTHENTICATE_CONTINUE:
		var data Mysqlx_Session.AuthenticateContinue
		if err := data.Unmarshal(payload); err != nil {
			log.Errorf("[%d] Can't Unmarshal message %s, err %s", xa.xcc.connectionID, msgType.String(), err.Error())
			return util.ErrXBadMessage
		}

		r = xa.authHandler.handleContinue(data.GetAuthData())
	default:
		xa.stopAuth()
		return util.ErrXBadMessage
	}

	switch r.status {
	case authSucceed:
		if err := xa.onAuthSuccess(r); err != nil {
			return errors.Trace(err)
		}
	case authFailed:
		xa.onAuthFailure(r)
		return util.ErrorMessage(mysql.ErrAccessDenied, r.data)
	default:
		if err := xa.sendAuthContinue(&r.data); err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (xa *xAuth) onAuthSuccess(r *response) error {
	if err := notice.SendClientID(xa.xcc.pkt, xa.xcc.connectionID); err != nil {
		return errors.Trace(err)
	}
	xa.stopAuth()
	xa.mState = ready
	if err := xa.sendAuthOk(&r.data); err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (xa *xAuth) onAuthFailure(r *response) {
	xa.stopAuth()
}

//@TODO need to implement
func (xa *xAuth) onSessionReset() {
}

func (xa *xAuth) onClose(updateOldState bool) {
	if xa.mState != closing {
		if updateOldState {
			xa.mStateBeforeClose = xa.mState
		}
		xa.mState = closing
	}
}

func (xa *xAuth) stopAuth() {
	xa.authHandler = nil
}

func (xa *xAuth) ready() bool {
	return xa.mState == ready
}

func (xa *xAuth) sendAuthOk(value *string) error {
	msg := Mysqlx_Session.AuthenticateOk{
		AuthData: []byte(*value),
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	return xa.xcc.pkt.WritePacket(Mysqlx.ServerMessages_SESS_AUTHENTICATE_OK, data)
}

func (xa *xAuth) sendAuthContinue(value *string) error {
	msg := Mysqlx_Session.AuthenticateContinue{
		AuthData: []byte(*value),
	}

	data, err := msg.Marshal()
	if err != nil {
		return err
	}

	return xa.xcc.pkt.WritePacket(Mysqlx.ServerMessages_SESS_AUTHENTICATE_CONTINUE, data)
}
