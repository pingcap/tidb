// Copyright 2023-2023 PingCAP Xingchen (Beijing) Technology Co., Ltd.

package server

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
