// Copyright 2023-2023 PingCAP Xingchen (Beijing) Technology Co., Ltd.

package server

import (
	"context"

	"github.com/pingcap/tidb/pkg/privilege"
	servererr "github.com/pingcap/tidb/pkg/server/err"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
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
func (s *Server) checkUserConnectionCount(cc *clientConn, host string) error {
	authUser, err := cc.ctx.MatchIdentity(context.Background(), cc.user, host)
	if err != nil {
		return err
	}

	pm := privilege.GetPrivilegeManager(cc.ctx.Session)
	connections, err := pm.GetUserResources(authUser.Username, authUser.Hostname)
	if err != nil {
		return err
	}

	if connections == 0 && variable.MaxUserConnectionsCount.Load() == 0 {
		return nil
	}

	targetUser := authUser.Username + authUser.Hostname
	conns := int64(cc.getUserConnectionsCounter(targetUser))

	if (connections > 0 && conns >= connections) || (connections == 0 && conns >= int64(variable.MaxUserConnectionsCount.Load())) {
		var count uint32
		if connections > 0 {
			count = uint32(connections)
		} else {
			count = variable.MaxUserConnectionsCount.Load()
		}
		logutil.BgLogger().Error("The current user has too many connections",
			zap.Uint32("max user connections", count), zap.Error(servererr.ErrConCount))
		return servererr.ErrTooManyUserConnections.GenWithStackByArgs(authUser.Username)
	}

	return nil
}
