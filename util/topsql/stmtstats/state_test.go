package stmtstats

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExecState(t *testing.T) {
	state := execStateInitial
	valid := state.tryGoTo(execStateDispatchBegin, []ExecState{execStateDispatchFinish, execStateInitial})
	require.True(t, valid)
	require.Equal(t, execStateDispatchBegin, state)
	valid = state.tryGoTo(execStateHandleStmtFinish, []ExecState{execStateHandleStmtBegin, execStateStmtReadyToExecute})
	require.False(t, valid)
	require.Equal(t, execStateDispatchBegin, state)
	require.Equal(t, "dispatch_begin", state.String())
	require.Equal(t, 13, int(execStateUseDBFinish))
}

func TestExecState_String(t *testing.T) {
	m := map[string]struct{}{}
	state := 0
	for {
		str := ExecState(state).String()
		if strings.HasPrefix(str, "unknown") {
			break
		}
		state++
		m[str] = struct{}{}
	}
	require.Equal(t, state, len(m))
}
