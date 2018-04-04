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
	"bytes"
	"net"

	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/auth"
	xutil "github.com/pingcap/tidb/xprotocol/util"
)

type authMysql41State int32

const (
	sStarting authMysql41State = iota
	sWaitingResponse
	sDone
	sError
)

type saslMysql41Auth struct {
	mState authMysql41State
	mSalts []byte
	xs     *xSession
}

func (spa *saslMysql41Auth) handleStart(mechanism *string, data []byte, initialResponses []byte) *response {
	r := response{}

	if spa.mState == sStarting {
		spa.mSalts = util.RandomBuf(mysql.ScrambleLength)
		r.data = string(spa.mSalts)
		r.status = authOngoing
		r.errCode = 0
		spa.mState = sWaitingResponse
	} else {
		r.status = authError
		r.errCode = mysql.ErrNetPacketsOutOfOrder

		spa.mState = sError
	}

	return &r
}

func (spa *saslMysql41Auth) handleContinue(data []byte) *response {
	if spa.mState == sWaitingResponse {
		dbname, user, passwd := spa.extractNullTerminatedElement(data)
		if user == "" {
			return &response{
				status:  authFailed,
				data:    xutil.ErrXBadMessage.ToSQLError().Message,
				errCode: xutil.ErrXBadMessage.ToSQLError().Code,
			}
		}

		xcc := spa.xs.xcc
		xcc.dbname = dbname
		xcc.user = user

		spa.mState = sDone
		if !spa.xs.xcc.server.skipAuth() {
			// Do Auth
			addr := spa.xs.xcc.conn.RemoteAddr().String()
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return &response{
					status:  authFailed,
					data:    xutil.ErrAccessDenied.GenByArgs(xcc.user, host, "YES").ToSQLError().Message,
					errCode: xutil.ErrAccessDenied.ToSQLError().Code,
				}
			}
			var hpwd []byte
			if len(passwd) != 0 {
				if hpwd, err = auth.DecodePassword(string(passwd)); err != nil {
					return &response{
						status:  authFailed,
						data:    xutil.ErrAccessDenied.GenByArgs(xcc.user, host, "YES").ToSQLError().Message,
						errCode: xutil.ErrAccessDenied.ToSQLError().Code,
					}
				}
			}
			if !spa.xs.xcc.ctx.Auth(&auth.UserIdentity{Username: string(user), Hostname: host},
				hpwd, spa.mSalts) {
				return &response{
					status:  authFailed,
					data:    xutil.ErrAccessDenied.GenByArgs(xcc.user, host, "YES").ToSQLError().Message,
					errCode: xutil.ErrAccessDenied.ToSQLError().Code,
				}
			}
		}

		return &response{
			status:  authSucceed,
			errCode: 0,
		}
	}
	spa.mState = sError

	return &response{
		status:  authError,
		errCode: mysql.ErrNetPacketsOutOfOrder,
	}
}

func (spa *saslMysql41Auth) extractNullTerminatedElement(data []byte) (dbname string, user string, passwd string) {
	slices := bytes.Split(data, []byte{0})
	if len(slices) != 3 {
		return
	}
	dbname = string(slices[0])
	user = string(slices[1])
	passwd = string(slices[2])
	return
}
