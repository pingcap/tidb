// Copyright 2023-2023 PingCAP Xingchen (Beijing) Technology Co., Ltd.

package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/tidb/pkg/domain"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/sqlescape"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	sqlInsertLoginHistory = `insert into mysql.LOGIN_HISTORY(Time, Server_host, User, User_host, DB, Connection_id, Result, Client_host, Detail) values (%?, %?, %?, %?, %?, %?, %?, %?, %?)`
	sqlQueryHistoryQuery  = `(select TIME,SERVER_HOST,USER,DB,CONNECTION_ID,RESULT,
            CLIENT_HOST from mysql.login_history
            where result ='success' and user = %? order by time desc limit 1)
            union all
            (select TIME,SERVER_HOST,USER,DB,CONNECTION_ID,RESULT,
            CLIENT_HOST from mysql.login_history
            where result ='fail' and user = %? order by time desc limit 1);`
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
	defer sysSessionPool.Put(currentCtx)

	userName, userHost := user.Username, user.Hostname
	if result == nil {
		userName, userHost = user.AuthUsername, user.AuthHostname
	}
	cur := time.Now().Format("2006-01-02 15:04:05.999999")
	sql := new(strings.Builder)
	sqlescape.MustFormatSQL(sql, sqlInsertLoginHistory,
		cur, conn.serverHost, userName, userHost, conn.dbname, conn.connectionID, r, conn.peerHost, detail)

	kvCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	exec := currentCtx.(sessionctx.Context).GetSQLExecutor()
	_, err = exec.(sqlexec.SQLExecutor).ExecuteInternal(kvCtx, sql.String())
	return err
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

type userLoginHistory struct {
	time       string
	host       string
	user       string
	db         string
	result     string
	clientHost string
}

func (u *userLoginHistory) String() string {
	return fmt.Sprintf("\n [Last %s Login] %s %s@%s %s %s",
		cases.Title(language.English).String(u.result), u.time, u.user, u.host, u.clientHost, u.result)
}

// queryLoginRecords query records from mysql.login_history.
func queryLoginRecords(ctx context.Context, cc *clientConn) error {
	dom := domain.GetDomain(cc.ctx.Session.(sessionctx.Context))
	sysSessionPool := dom.SysSessionPool()
	currentCtx, err := sysSessionPool.Get()
	if err != nil {
		return err
	}
	defer sysSessionPool.Put(currentCtx)

	query := new(strings.Builder)
	sqlescape.MustFormatSQL(query, sqlQueryHistoryQuery, cc.user, cc.user)

	internalCtx := kv.WithInternalSourceType(ctx, kv.InternalTxnPrivilege)
	exec := currentCtx.(sessionctx.Context).GetSQLExecutor()
	rs, err := exec.ExecuteInternal(internalCtx, query.String())
	if err != nil {
		return err
	}
	if rs == nil {
		return nil
	}
	sb := new(strings.Builder)
	sb.WriteByte('\n')
	defer rs.Close()
	rows, err := sqlexec.DrainRecordSet(internalCtx, rs, 8)
	if err != nil {
		return err
	}
	for _, row := range rows {
		data := &userLoginHistory{
			time:       row.GetTime(0).String(),
			host:       row.GetString(1),
			user:       row.GetString(2),
			db:         row.GetString(3),
			result:     row.GetString(5),
			clientHost: row.GetString(6),
		}
		if data.result == "success" {
			sb.WriteString(data.String())
		} else {
			sb.WriteString(data.String())
		}
	}
	cc.ctx.GetSessionVars().LogHistory = sb.String()
	return nil
}

func queryLoginHistoryTable(ctx context.Context, conn *clientConn) error {
	if !enableLogHistory() {
		return nil
	}
	return queryLoginRecords(ctx, conn)
}
