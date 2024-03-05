// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/utils"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"go.uber.org/zap"
)

// BaseConn wraps connection instance.
type BaseConn struct {
	DBConn *sql.Conn

	backOffer     backOfferResettable
	rebuildConnFn func(*sql.Conn, bool) (*sql.Conn, error)
}

func newBaseConn(conn *sql.Conn, shouldRetry bool, rebuildConnFn func(*sql.Conn, bool) (*sql.Conn, error)) *BaseConn {
	baseConn := &BaseConn{DBConn: conn}
	baseConn.backOffer = newRebuildConnBackOffer(shouldRetry)
	if shouldRetry {
		baseConn.rebuildConnFn = rebuildConnFn
	}
	return baseConn
}

// QuerySQL defines query statement, and connect to real DB.
func (conn *BaseConn) QuerySQL(tctx *tcontext.Context, handleOneRow func(*sql.Rows) error, reset func(), query string, args ...any) error {
	retryTime := 0
	err := utils.WithRetry(tctx, func() (err error) {
		retryTime++
		if retryTime > 1 && conn.rebuildConnFn != nil {
			conn.DBConn, err = conn.rebuildConnFn(conn.DBConn, false)
			if err != nil {
				return
			}
		}
		err = simpleQueryWithArgs(tctx, conn.DBConn, handleOneRow, query, args...)
		if err != nil {
			tctx.L().Info("cannot execute query", zap.Int("retryTime", retryTime), zap.String("sql", query),
				zap.Any("args", args), zap.Error(err))
			reset()
			return err
		}
		return nil
	}, conn.backOffer)
	conn.backOffer.Reset()
	return err
}

// QuerySQLWithColumns defines query statement, and connect to real DB and get results for special column names
func (conn *BaseConn) QuerySQLWithColumns(tctx *tcontext.Context, columns []string, query string, args ...any) ([][]string, error) {
	retryTime := 0
	var results [][]string
	err := utils.WithRetry(tctx, func() (err error) {
		retryTime++
		if retryTime > 1 && conn.rebuildConnFn != nil {
			conn.DBConn, err = conn.rebuildConnFn(conn.DBConn, false)
			if err != nil {
				tctx.L().Warn("rebuild connection failed", zap.Error(err))
				return
			}
		}
		rows, err := conn.DBConn.QueryContext(tctx, query, args...)
		if err != nil {
			tctx.L().Info("cannot execute query", zap.Int("retryTime", retryTime), zap.String("sql", query),
				zap.Any("args", args), zap.Error(err))
			return errors.Annotatef(err, "sql: %s", query)
		}
		results, err = GetSpecifiedColumnValuesAndClose(rows, columns...)
		if err != nil {
			tctx.L().Info("cannot execute query", zap.Int("retryTime", retryTime), zap.String("sql", query),
				zap.Any("args", args), zap.Error(err))
			results = nil
			return errors.Annotatef(err, "sql: %s", query)
		}
		return err
	}, conn.backOffer)
	conn.backOffer.Reset()
	return results, err
}

// ExecSQL defines exec statement, and connect to real DB.
func (conn *BaseConn) ExecSQL(tctx *tcontext.Context, canRetryFunc func(sql.Result, error) error, query string, args ...any) error {
	retryTime := 0
	err := utils.WithRetry(tctx, func() (err error) {
		retryTime++
		if retryTime > 1 && conn.rebuildConnFn != nil {
			conn.DBConn, err = conn.rebuildConnFn(conn.DBConn, false)
			if err != nil {
				return
			}
		}
		res, err := conn.DBConn.ExecContext(tctx, query, args...)
		if err = canRetryFunc(res, err); err != nil {
			tctx.L().Info("cannot execute query", zap.Int("retryTime", retryTime), zap.String("sql", query),
				zap.Any("args", args), zap.Error(err))
			return err
		}
		return nil
	}, conn.backOffer)
	conn.backOffer.Reset()
	return err
}
