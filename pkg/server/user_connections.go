// Copyright 2025 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"

	"github.com/pingcap/tidb/pkg/privilege"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// incrementUserConnectionsCounter increases the count of connections when user login the database.
func (cc *clientConn) incrementUserConnectionsCounter() {
	user := cc.ctx.GetSessionVars().User
	targetUser := user.AuthUsername + user.AuthHostname

	cc.server.userResLock.Lock()
	_, ok := cc.server.userResource[targetUser]
	if !ok {
		userHost := &userResourceLimits{
			connections: 0,
		}
		cc.server.userResource[targetUser] = userHost
	}

	cc.server.userResource[targetUser].connections++
	cc.server.userResLock.Unlock()
}

// decrementUserConnectionsCounter decreases the count of connections when user logout the database.
func (cc *clientConn) decrementUserConnectionsCounter() {
	tidbContext := cc.getCtx()
	if tidbContext == nil {
		return
	}

	user := tidbContext.GetSessionVars().User
	if user != nil {
		targetUser := user.AuthUsername + user.AuthHostname

		cc.server.userResLock.Lock()
		_, ok := cc.server.userResource[targetUser]
		if ok && cc.server.userResource[targetUser].connections > 0 {
			cc.server.userResource[targetUser].connections--
		}

		cc.server.userResLock.Unlock()
	}
}

// getUserConnectionsCounter gets the count of connections.
func (cc *clientConn) getUserConnectionsCounter(targetUser string) int {
	cc.server.userResLock.Lock()
	defer cc.server.userResLock.Unlock()

	ur, ok := cc.server.userResource[targetUser]
	if ok {
		return ur.connections
	}

	return 0
}

// checkUserConnectionCount checks whether the count of connections exceeds the limits.
func (cc *clientConn) checkUserConnectionCount(host string) error {
	authUser, err := cc.ctx.MatchIdentity(context.Background(), cc.user, host)
	if err != nil {
		return err
	}

	pm := privilege.GetPrivilegeManager(cc.ctx.Session)
	connections, err := pm.GetUserResources(authUser.Username, authUser.Hostname)
	if err != nil {
		return err
	}

	if connections == 0 && vardef.MaxUserConnectionsCount.Load() == 0 {
		return nil
	}

	targetUser := authUser.Username + authUser.Hostname
	conns := int64(cc.getUserConnectionsCounter(targetUser))

	if (connections > 0 && conns >= connections) || (connections == 0 && conns >= int64(vardef.MaxUserConnectionsCount.Load())) {
		var count uint32
		if connections > 0 {
			count = uint32(connections)
		} else {
			count = vardef.MaxUserConnectionsCount.Load()
		}
		logutil.BgLogger().Error("The current user has too many connections",
			zap.Uint32("max user connections", count), zap.Error(servererr.ErrConCount))
		return servererr.ErrTooManyUserConnections.GenWithStackByArgs(authUser.Username)
	}

	return nil
}
