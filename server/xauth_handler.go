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
	log "github.com/sirupsen/logrus"
)

type authStatus int32

const (
	authOngoing authStatus = iota
	authSucceed
	authFailed
	authError
)

type response struct {
	data    string
	status  authStatus
	errCode uint16
}

type authHandler interface {
	handleStart(mechanism *string, data []byte, initialResponses []byte) *response
	handleContinue(data []byte) *response
}

func (xs *xSession) createAuthHandler(method string) authHandler {
	switch method {
	case "MYSQL41":
		return &saslMysql41Auth{
			mState: sStarting,
			xs:     xs,
		}
	//@TODO support in next pr
	case "PLAIN":
		//return &saslPlainAuth{}
		return nil
	default:
		log.Errorf("unknown x-protocol auth handler type [%s].", method)
		return nil
	}
}
