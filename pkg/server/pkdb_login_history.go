// Copyright 2023-2023 PingCAP Xingchen (Beijing) Technology Co., Ltd.

package server

import (
	"context"
	"strings"
	"time"

	"github.com/ngaut/pools"
	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

// insertLoginRecords inserts a record into mysql.login_history.
func insertLoginRecords(ctx context.Context, conn *clientConn, user *auth.UserIdentity, result error) error {
	r := "success"
	detail := ""
	if result != nil {
		r = "fail"
		detail = result.Error()
	}

	dom := domain.GetDomain(conn.ctx.Session.(sessionctx.Context))
	sysSessionPool := dom.SysSessionPool()
	currentCtx, err := sysSessionPool.Get()
	if err != nil {
		return err
	}
	restrictedCtx := currentCtx.(sessionctx.Context)
	restrictedCtx.GetSessionVars().InRestrictedSQL = true

	cur := time.Now().Format("2006-01-02 15:04:05.999999")
	sql := new(strings.Builder)

	userName, userHost := user.Username, user.Hostname
	if nil == result {
		userName, userHost = user.AuthUsername, user.AuthHostname
	}
	sqlescape.MustFormatSQL(sql,
		`insert into mysql.LOGIN_HISTORY(Time, Server_host, User, User_host, DB, Connection_id, Result, Client_host, Detail) values (%?, %?, %?, %?, %?, %?, %?, %?, %?)`,
		cur, conn.serverHost, userName, userHost, conn.dbname, conn.connectionID, r, conn.peerHost, detail)

	kvCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	_, err = restrictedCtx.(sqlexec.SQLExecutor).ExecuteInternal(kvCtx, sql.String())
	if err != nil {
		logutil.BgLogger().Warn("insert into login_history fail", zap.Error(err))
		return err
	}
	sysSessionPool.Put(restrictedCtx.(sqlexec.SQLExecutor).(pools.Resource))
	return nil
}

func enableLogHistory() bool {
	return variable.EnableLoginHistory.Load()
}

func insertLoginHistoryTable(ctx context.Context, conn *clientConn, user *auth.UserIdentity, result error) error {
	if !enableLogHistory() {
		return nil
	}

	return insertLoginRecords(ctx, conn, user, result)
}
