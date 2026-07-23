// Copyright 2026 PingCAP, Inc.
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

package memory

import (
	"testing"

	"github.com/pingcap/tidb/pkg/util/sqlkiller"
	"github.com/stretchr/testify/require"
)

type memQuotaLogHookRecorder struct {
	connIDs []uint64
}

func (h *memQuotaLogHookRecorder) LogOnQueryExceedMemQuota(connID uint64) {
	h.connIDs = append(h.connIDs, connID)
}

type panickingMemQuotaLogHook struct {
	calls int
}

func (h *panickingMemQuotaLogHook) LogOnQueryExceedMemQuota(uint64) {
	h.calls++
	panic("log hook")
}

func TestMemQuotaLogHookHandler(t *testing.T) {
	tracker := NewTracker(1, 1)
	hook := &memQuotaLogHookRecorder{}

	logAction := &LogOnExceed{ConnID: 42}
	logAction.SetLogHookHandler(hook)
	logAction.Action(tracker)
	logAction.Action(tracker)
	require.Equal(t, []uint64{42}, hook.connIDs)
	logAction.SetLogHookHandler(hook)
	logAction.Action(tracker)
	require.Equal(t, []uint64{42}, hook.connIDs)

	hook.connIDs = nil
	panicAction := &PanicOnExceed{
		Killer: &sqlkiller.SQLKiller{},
		ConnID: 43,
	}
	panicAction.SetLogHookHandler(hook)
	require.Panics(t, func() {
		panicAction.Action(tracker)
	})
	require.Panics(t, func() {
		panicAction.Action(tracker)
	})
	require.Equal(t, []uint64{43}, hook.connIDs)
	panicAction.SetLogHookHandler(hook)
	require.Panics(t, func() {
		panicAction.Action(tracker)
	})
	require.Equal(t, []uint64{43}, hook.connIDs)
}

func TestNilMemQuotaLogHook(t *testing.T) {
	logAction := &LogOnExceed{}
	logAction.SetLogHook(nil)
	require.Nil(t, logAction.logHook)

	panicAction := &PanicOnExceed{}
	panicAction.SetLogHook(nil)
	require.Nil(t, panicAction.logHook)
}

func TestMemQuotaLogHookFunc(t *testing.T) {
	tracker := NewTracker(1, 1)
	var connIDs []uint64
	action := &LogOnExceed{ConnID: 44}
	action.SetLogHook(func(connID uint64) {
		connIDs = append(connIDs, connID)
	})

	action.Action(tracker)
	action.SetLogHook(func(connID uint64) {
		connIDs = append(connIDs, connID)
	})
	action.Action(tracker)
	require.Equal(t, []uint64{44}, connIDs)
}

func TestPanickingMemQuotaLogHook(t *testing.T) {
	tracker := NewTracker(1, 1)

	logHook := &panickingMemQuotaLogHook{}
	logAction := &LogOnExceed{}
	logAction.SetLogHookHandler(logHook)
	require.Panics(t, func() {
		logAction.Action(tracker)
	})
	require.NotPanics(t, func() {
		logAction.Action(tracker)
	})
	require.Equal(t, 1, logHook.calls)

	panicHook := &panickingMemQuotaLogHook{}
	panicAction := &PanicOnExceed{}
	panicAction.SetLogHookHandler(panicHook)
	require.Panics(t, func() {
		panicAction.Action(tracker)
	})
	require.Panics(t, func() {
		panicAction.Action(tracker)
	})
	require.Equal(t, 2, panicHook.calls)
}
