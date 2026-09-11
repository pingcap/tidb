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
	return conn.queryRows(tctx, func(rows *sql.Rows) error {
		for rows.Next() {
			if err := handleOneRow(rows); err != nil {
				return err
			}
		}
		return nil
	}, reset, query, args...)
}

func (conn *BaseConn) queryRows(tctx *tcontext.Context, handleRows func(*sql.Rows) error, reset func(), query string, args ...any) error {
	retryTime := 0
	err := utils.WithRetry(tctx, func() (err error) {
		retryTime++
		if retryTime > 1 && conn.rebuildConnFn != nil {
			conn.DBConn, err = conn.rebuildConnFn(conn.DBConn, false)
			if err != nil {
				return
			}
		}
		rows, err := conn.DBConn.QueryContext(tctx, query, args...)
		if err == nil {
			defer rows.Close()
			err = handleRows(rows)
			if err == nil {
				err = rows.Err()
			}
		}
		if err != nil {
			tctx.L().Info("cannot execute query", zap.Int("retryTime", retryTime), zap.String("sql", query),
				zap.Any("args", args), zap.Error(err))
			reset()
			return errors.Annotatef(err, "sql: %s, args: %v", query, args)
		}
		return nil
	}, conn.backOffer)
	conn.backOffer.Reset()
	return err
}

// QuerySQLWithColumns defines query statement, and connect to real DB and get results for special column names
func (conn *BaseConn) QuerySQLWithColumns(tctx *tcontext.Context, columns []string, query string, args ...any) ([][]string, error) {
	var results [][]string
	err := conn.queryRows(tctx, func(rows *sql.Rows) error {
		var err error
		results, err = GetSpecifiedColumnValuesAndClose(rows, columns...)
		return err
	}, func() {
		results = nil
	}, query, args...)
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
