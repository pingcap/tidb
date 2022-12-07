package session

import (
	"context"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
)

// Session wraps sessionctx.Context for transaction usage.
type Session struct {
	sessionctx.Context
}

func NewSession(s sessionctx.Context) *Session {
	return &Session{s}
}

func (s *Session) Begin() error {
	err := sessiontxn.NewTxn(context.Background(), s.Context)
	if err != nil {
		return err
	}
	s.GetSessionVars().SetInTxn(true)
	return nil
}

func (s *Session) Commit() error {
	s.StmtRollback(context.Background(), false)
	return s.CommitTxn(context.Background())
}

func (s *Session) Txn() (kv.Transaction, error) {
	return s.Context.Txn(true)
}

func (s *Session) Rollback() {
	s.StmtRollback(context.Background(), false)
	s.RollbackTxn(context.Background())
}

func (s *Session) Reset() {
	s.StmtRollback(context.Background(), false)
}

func (s *Session) Execute(ctx context.Context, query string, label string) ([]chunk.Row, error) {
	startTime := time.Now()
	var err error
	defer func() {
		metrics.DDLJobTableDuration.WithLabelValues(label + "-" + metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
	}()
	rs, err := s.Context.(sqlexec.SQLExecutor).ExecuteInternal(kv.WithInternalSourceType(ctx, kv.InternalTxnDDL), query)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	defer terror.Call(rs.Close)
	if rows, err = sqlexec.DrainRecordSet(ctx, rs, 8); err != nil {
		return nil, errors.Trace(err)
	}
	return rows, nil
}

func (s *Session) Session() sessionctx.Context {
	return s.Context
}

func (s *Session) RunInTxn(f func(*Session) error) (err error) {
	err = s.Begin()
	if err != nil {
		return err
	}
	err = f(s)
	if err != nil {
		s.Rollback()
		return
	}
	return errors.Trace(s.Commit())
}
