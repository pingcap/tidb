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
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/util/breakpoint"
	"github.com/stretchr/testify/require"
)

var errCommandRunFailed = errors.New("command run failed")

var defaultChanTimeout = time.Second * 10

type steppedTestkitMsgType int

const (
	msgTpCmdContinue steppedTestkitMsgType = iota
	msgTpCmdStopOnBreakPoint
	msgTpCmdDone
)

type steppedTestKitMsg struct {
	tp  steppedTestkitMsgType
	val any
}

type steppedTestKitMsgChan chan *steppedTestKitMsg

func (ch steppedTestKitMsgChan) sendMsg(tp steppedTestkitMsgType, val any) error {
	select {
	case ch <- &steppedTestKitMsg{tp: tp, val: val}:
		return nil
	case <-time.After(defaultChanTimeout):
		return errors.New("send msg timeout")
	}
}

func (ch steppedTestKitMsgChan) sendMsgWithType(tp steppedTestkitMsgType) error {
	return ch.sendMsg(tp, nil)
}

func (ch steppedTestKitMsgChan) recvMsg() (*steppedTestKitMsg, error) {
	select {
	case msg := <-ch:
		return msg, nil
	case <-time.After(defaultChanTimeout):
		return nil, errors.New("send msg timeout")
	}
}

func (ch steppedTestKitMsgChan) recvMsgWithCheck(tp steppedTestkitMsgType) (*steppedTestKitMsg, error) {
	msg, err := ch.recvMsg()
	if err != nil {
		return nil, err
	}

	if msg.tp != tp {
		return nil, errors.Errorf("unexpected msg type: %v, expect: %v", msg.tp, tp)
	}

	return msg, nil
}

type steppedTestKitCommandContext struct {
	t                       testing.TB
	tk                      *TestKit
	notifyBreakPointAndWait func(string)
}

type steppedTestKitCommand func(ctx *steppedTestKitCommandContext) any

// SteppedTestKit is the testkit that can run stepped command
type SteppedTestKit struct {
	t  testing.TB
	tk *TestKit

	// ch1 is used to send msg from foreground to background
	ch1 steppedTestKitMsgChan
	// ch2 is used to send msg from background to foreground
	ch2 steppedTestKitMsgChan
	// breakPoints is the break points we want to stop at
	breakPoints []string
	// cmdStopAt is the current break point it stopped at
	cmdStopAt string
	// the result of the current command
	cmdResult any
}

// NewSteppedTestKit creates a new SteppedTestKit
func NewSteppedTestKit(t testing.TB, store kv.Storage) *SteppedTestKit {
	tk := &SteppedTestKit{
		t:   t,
		tk:  NewTestKit(t, store),
		ch1: make(steppedTestKitMsgChan),
		ch2: make(steppedTestKitMsgChan),
	}
	return tk
}

// ExpectIdle checks no command is running
func (tk *SteppedTestKit) ExpectIdle() {
	require.Equal(tk.t, "", tk.cmdStopAt)
}

// ExpectStopOnBreakPoint checks stopped on the specified break point
func (tk *SteppedTestKit) ExpectStopOnBreakPoint(breakPoint string) {
	require.Equal(tk.t, breakPoint, tk.cmdStopAt)
}

// ExpectStopOnAnyBreakPoint checks stopped on any break point
func (tk *SteppedTestKit) ExpectStopOnAnyBreakPoint() {
	require.NotEqual(tk.t, "", tk.cmdStopAt)
}

// SetBreakPoints sets the break points we want to stop at
func (tk *SteppedTestKit) SetBreakPoints(breakPoints ...string) {
	tk.breakPoints = breakPoints
}

func (tk *SteppedTestKit) handleCommandMsg() {
	msg, err := tk.ch2.recvMsg()
	require.NoError(tk.t, err)
	switch msg.tp {
	case msgTpCmdDone:
		tk.cmdStopAt = ""
		if msg.val == errCommandRunFailed {
			require.FailNow(tk.t, "internal command failed")
		} else {
			tk.cmdResult = msg.val
		}
	case msgTpCmdStopOnBreakPoint:
		require.IsType(tk.t, "", msg.val)
		require.NotEqual(tk.t, "", msg.val)
		tk.cmdStopAt = msg.val.(string)
	default:
		require.FailNow(tk.t, "invalid msg type", "tp %v", msg.tp)
	}
}

func (tk *SteppedTestKit) beforeCommand() {
	tk.ExpectIdle()
	tk.cmdResult = nil
}

func (tk *SteppedTestKit) steppedCommand(cmd steppedTestKitCommand) *SteppedTestKit {
	tk.beforeCommand()
	go func() {
		var success bool
		var result any
		var breakPointPaths []string
		defer func() {
			if !success {
				result = errCommandRunFailed
			}

			tk.tk.Session().SetValue(breakpoint.NotifyBreakPointFuncKey, nil)
			for _, path := range breakPointPaths {
				require.NoError(tk.t, failpoint.Disable(path))
			}

			require.NoError(tk.t, tk.ch2.sendMsg(msgTpCmdDone, result))
		}()

		ctx := &steppedTestKitCommandContext{
			t:  tk.t,
			tk: tk.tk,
			notifyBreakPointAndWait: func(breakPoint string) {
				require.NoError(tk.t, tk.ch2.sendMsg(msgTpCmdStopOnBreakPoint, breakPoint))
				_, err := tk.ch1.recvMsgWithCheck(msgTpCmdContinue)
				require.NoError(tk.t, err)
			},
		}

		tk.tk.Session().SetValue(breakpoint.NotifyBreakPointFuncKey, ctx.notifyBreakPointAndWait)
		for _, breakPoint := range tk.breakPoints {
			path := "github.com/pingcap/tidb/pkg/util/breakpoint/" + breakPoint
			require.NoError(tk.t, failpoint.Enable(path, "return"))
			breakPointPaths = append(breakPointPaths, path)
		}

		result = cmd(ctx)
		success = true
	}()

	tk.handleCommandMsg()
	return tk
}

// Continue continues current command
func (tk *SteppedTestKit) Continue() *SteppedTestKit {
	tk.ExpectStopOnAnyBreakPoint()
	require.NoError(tk.t, tk.ch1.sendMsgWithType(msgTpCmdContinue))
	tk.handleCommandMsg()
	return tk
}

// SteppedMustExec creates a new stepped task for MustExec
func (tk *SteppedTestKit) SteppedMustExec(sql string, args ...any) *SteppedTestKit {
	return tk.steppedCommand(func(_ *steppedTestKitCommandContext) any {
		tk.MustExec(sql, args...)
		return nil
	})
}

// SteppedMustQuery creates a new stepped task for MustQuery
func (tk *SteppedTestKit) SteppedMustQuery(sql string, args ...any) *SteppedTestKit {
	return tk.steppedCommand(func(_ *steppedTestKitCommandContext) any {
		return tk.MustQuery(sql, args...)
	})
}

// MustExec executes a sql statement and asserts nil error.
func (tk *SteppedTestKit) MustExec(sql string, args ...any) {
	tk.beforeCommand()
	tk.tk.MustExec(sql, args...)
}

// MustQuery query the statements and returns result rows.
// If expected result is set it asserts the query result equals expected result.
func (tk *SteppedTestKit) MustQuery(sql string, args ...any) *Result {
	tk.beforeCommand()
	result := tk.tk.MustQuery(sql, args...)
	tk.cmdResult = result
	return result
}

// GetResult returns the result of the latest command
func (tk *SteppedTestKit) GetResult() any {
	tk.ExpectIdle()
	return tk.cmdResult
}

// GetQueryResult returns the query result of the latest command
func (tk *SteppedTestKit) GetQueryResult() *Result {
	tk.ExpectIdle()
	require.IsType(tk.t, &Result{}, tk.cmdResult)
	return tk.cmdResult.(*Result)
}
