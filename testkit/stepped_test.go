// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package testkit

import (
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/sessiontxn"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestCancelBreakPoints(t *testing.T) {
	store, _, clean := CreateMockStoreAndDomain(t)
	defer clean()
	tk := NewSteppedTestKit(t, store)

	containsBreakpoint := func(tk *SteppedTestKit, bp string) bool {
		for _, b := range tk.breakPoints {
			if b == bp {
				return true
			}
		}
		return false
	}

	tk.SetBreakPoints(
		sessiontxn.BreakPointBeforeExecutorFirstRun,
		sessiontxn.BreakPointOnStmtRetryAfterLockError,
	)
	path := "github.com/pingcap/tidb/util/breakpoint/" + sessiontxn.BreakPointBeforeExecutorFirstRun
	require.NoError(tk.t, failpoint.Enable(path, "return"))
	path = "github.com/pingcap/tidb/util/breakpoint/" + sessiontxn.BreakPointOnStmtRetryAfterLockError
	require.NoError(tk.t, failpoint.Enable(path, "return"))

	require.True(t, containsBreakpoint(tk, sessiontxn.BreakPointBeforeExecutorFirstRun))
	require.True(t, containsBreakpoint(tk, sessiontxn.BreakPointOnStmtRetryAfterLockError))

	tk.CancelBreakPoints(sessiontxn.BreakPointBeforeExecutorFirstRun)
	require.False(t, containsBreakpoint(tk, sessiontxn.BreakPointBeforeExecutorFirstRun))
	require.True(t, containsBreakpoint(tk, sessiontxn.BreakPointOnStmtRetryAfterLockError))

	// It's idempotent.
	tk.CancelBreakPoints(sessiontxn.BreakPointBeforeExecutorFirstRun)
	require.False(t, containsBreakpoint(tk, sessiontxn.BreakPointBeforeExecutorFirstRun))
	require.True(t, containsBreakpoint(tk, sessiontxn.BreakPointOnStmtRetryAfterLockError))

	tk.CancelBreakPoints(sessiontxn.BreakPointOnStmtRetryAfterLockError)
	require.False(t, containsBreakpoint(tk, sessiontxn.BreakPointBeforeExecutorFirstRun))
	require.False(t, containsBreakpoint(tk, sessiontxn.BreakPointOnStmtRetryAfterLockError))
}
