package session

import (
	"github.com/pingcap/tipb/go-binlog"
)

// StmtCommit implements the sessionctx.Context interface.
func (s *session) StmtCommit() error {
	return s.txn.StmtCommit(s)
}

// StmtRollback implements the sessionctx.Context interface.
func (s *session) StmtRollback() {
	s.txn.StmtRollback()
}

// StmtGetMutation implements the sessionctx.Context interface.
func (s *session) StmtGetMutation(tableID int64) *binlog.TableMutation {
	return s.txn.StmtGetMutation(tableID)
}

// StmtAddDirtyTableOP implements the sessionctx.Context interface.
func (s *session) StmtAddDirtyTableOP(op int, tid int64, handle int64) {
	s.txn.AppendDirtyTableOP(op, tid, handle)
}
