package tmpsession

import (
	"context"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/metrics"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/sqlexec"
	"time"
)

// session wraps sessionctx.Context for transaction usage.
type session struct {
	sessionctx.Context
}

func NewSession(s sessionctx.Context) *session {
	return &session{s}
}

func newSession(s sessionctx.Context) *session {
	return &session{s}
}

func (s *session) Begin() error {
	err := sessiontxn.NewTxn(context.Background(), s)
	if err != nil {
		return err
	}
	s.GetSessionVars().SetInTxn(true)
	return nil
}

func (s *session) begin() error {
	err := sessiontxn.NewTxn(context.Background(), s)
	if err != nil {
		return err
	}
	s.GetSessionVars().SetInTxn(true)
	return nil
}

func (s *session) Commit() error {
	s.StmtCommit()
	return s.CommitTxn(context.Background())
}

func (s *session) commit() error {
	s.StmtCommit()
	return s.CommitTxn(context.Background())
}

func (s *session) STxn() (kv.Transaction, error) {
	return s.Txn(true)
}

func (s *session) txn() (kv.Transaction, error) {
	return s.Txn(true)
}

func (s *session) Rollback() {
	s.StmtRollback()
	s.RollbackTxn(context.Background())
}

func (s *session) rollback() {
	s.StmtRollback()
	s.RollbackTxn(context.Background())
}

func (s *session) reset() {
	s.StmtRollback()
}

func (s *session) execute(ctx context.Context, query string, label string) ([]chunk.Row, error) {
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

func (s *session) session() sessionctx.Context {
	return s.Context
}
