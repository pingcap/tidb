package stmtstats

import (
	"fmt"

	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

// ExecState is the state of statement execution fsm.
// The state transition diagram is shown in here: https://drive.google.com/file/d/1yBemhOeuuEOPewF4q08n33D38SgtrQKy/view?usp=sharing
type ExecState int

const (
	execStateInitial ExecState = iota
	execStateDispatchBegin
	execStateDispatchFinish
	execStateHandleQueryBegin
	execStateHandleQueryFinish
	execStateHandleStmtBegin
	execStateHandleStmtFinish
	execStateStmtReadyToExecute
	execStateHandleStmtExecuteBegin
	execStateHandleStmtExecuteFinish
	execStateHandleStmtFetchBegin
	execStateHandleStmtFetchFinish
	execStateUseDBBegin
	execStateUseDBFinish
	// Add new state here.
)

func (s *ExecState) tryGoTo(target ExecState, acceptLastStates []ExecState) (valid bool) {
	found := false
	for _, state := range acceptLastStates {
		if *s == state {
			found = true
			break
		}
	}
	if found {
		*s = target
		return true
	}
	logutil.BgLogger().Warn("[stmt-stats] unexpected state", zap.String("state", s.String()), zap.String("target", target.String()))
	return false
}

// String implements Stringer interface.
func (s ExecState) String() string {
	switch s {
	case execStateInitial:
		return "initial"
	case execStateDispatchBegin:
		return "dispatch_begin"
	case execStateDispatchFinish:
		return "dispatch_finish"
	case execStateHandleQueryBegin:
		return "handle_query_begin"
	case execStateHandleQueryFinish:
		return "handle_query_finish"
	case execStateHandleStmtBegin:
		return "handle_stmt_begin"
	case execStateHandleStmtFinish:
		return "handle_stmt_finish"
	case execStateStmtReadyToExecute:
		return "stmt_ready_to_execute"
	case execStateHandleStmtExecuteBegin:
		return "handle_stmt_execute_begin"
	case execStateHandleStmtExecuteFinish:
		return "handle_stmt_execute_finish"
	case execStateHandleStmtFetchBegin:
		return "handle_stmt_fetch_begin"
	case execStateHandleStmtFetchFinish:
		return "handle_stmt_fetch_finish"
	case execStateUseDBBegin:
		return "use_db_begin"
	case execStateUseDBFinish:
		return "use_db_finish"
	default:
		return fmt.Sprintf("unknown_%d", int(s))
	}
}
