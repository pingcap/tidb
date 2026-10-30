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

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/privilege"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/sessionctx/vardef"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.uber.org/zap"
)

// calculateConnectionLimit gets the limit of connection.
// The user's attribute has a higher priority than global limit.
// 0 represents no limit.
func calculateConnectionLimit(globalLimit uint32, userLimit uint32) uint32 {
	// if user limit is valid, return user limit directly. Or return global limit.
	if userLimit > 0 {
		return userLimit
	}
	return globalLimit
}

// increaseUserConnectionsCount increases the count of connections when user login the database
// and returns a release function bound to the account that acquired the quota.
// Note: increaseUserConnectionsCount() is called when create connections only, and not in 'changeUser'(COM_CHANGE_USER).
// In mysql, 'COM_CHANGE_USER' will only reset the session state (such as permissions, default database, etc.),
// but will not change the ownership of the number of connections.
func (cc *clientConn) increaseUserConnectionsCount() (func(), error) {
	var (
		user       = cc.ctx.GetSessionVars().User
		targetUser = user.String()
		globaLimit = vardef.MaxUserConnectionsValue.Load()
		server     = cc.server
	)

	// check the number of connections again.
	pm := privilege.GetPrivilegeManager(cc.ctx.Session)
	userLimit, err := pm.GetUserResources(user.AuthUsername, user.AuthHostname)
	if err != nil {
		return nil, err
	}
	limit := calculateConnectionLimit(globaLimit, uint32(userLimit))

	server.userResLock.Lock()
	defer server.userResLock.Unlock()
	ur, ok := server.userResource[targetUser]
	if !ok {
		ur = &userResourceLimits{}
		server.userResource[targetUser] = ur
	} else if limit > 0 && ur.connections >= int(limit) {
		// Check the global MAX_USER_CONNECTIONS and the user's limit against the active count.
		return nil, servererr.ErrTooManyUserConnections.GenWithStackByArgs(targetUser)
	}

	ur.connections++
	return func() { server.decreaseUserConnectionCount(targetUser) }, nil
}

// decreaseUserConnectionCount decreases the count of connections when user logout the database.
func (s *Server) decreaseUserConnectionCount(targetUser string) {
	s.userResLock.Lock()
	defer s.userResLock.Unlock()
	ur, ok := s.userResource[targetUser]
	if ok && ur.connections > 0 {
		ur.connections--
		if ur.connections <= 0 {
			delete(s.userResource, targetUser)
		}
	}
}

// getUserConnectionCount gets the count of connections.
func (cc *clientConn) getUserConnectionCount(user *auth.UserIdentity) int {
	targetUser := user.String()
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
	userLimit, err := pm.GetUserResources(authUser.Username, authUser.Hostname)
	if err != nil {
		return err
	}

	limit := calculateConnectionLimit(vardef.MaxUserConnectionsValue.Load(), uint32(userLimit))
	if limit == 0 {
		return nil
	}

	conns := cc.getUserConnectionCount(authUser)
	if conns >= int(limit) {
		logutil.BgLogger().Error(("the maximum allowed connections exceeded"),
			zap.String("user", authUser.LoginString()),
			zap.Uint32("max-user-connections", limit),
			zap.Error(servererr.ErrConCount))
		return servererr.ErrTooManyUserConnections.GenWithStackByArgs(authUser.LoginString())
	}
	return nil
}
