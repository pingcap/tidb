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

package executor

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/sqlexec"
	"go.uber.org/zap"
)

const (
	mvTaskHistStatusRunning  = "running"
	mvTaskHistStatusSuccess  = "success"
	mvTaskHistStatusFailed   = "failed"
	mvTaskHistStatusOrphaned = "orphaned"

	mvTaskMonitorPollInterval   = 5 * time.Second
	mvTaskHistHeartbeatInterval = 10 * time.Minute
	mvTaskMonitorSQLTimeout     = 5 * time.Second
)

var errMVTaskCanceledManually = errors.NewNoStackError("materialized view task canceled manually")

type mvTaskCancelReason uint8

const (
	mvTaskCancelReasonNone mvTaskCancelReason = iota
	mvTaskCancelReasonManual
)

type mvTaskCancelController struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu        sync.Mutex
	reason    mvTaskCancelReason
	requester string
}

func newMVTaskCancelController(parent context.Context) *mvTaskCancelController {
	ctx, cancel := context.WithCancel(parent)
	return &mvTaskCancelController{ctx: ctx, cancel: cancel}
}

func (c *mvTaskCancelController) context() context.Context {
	if c == nil {
		return nil
	}
	return c.ctx
}

func (c *mvTaskCancelController) requestManualCancelByRequester(requester string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	if c.reason == mvTaskCancelReasonNone {
		c.reason = mvTaskCancelReasonManual
	}
	if c.requester == "" && requester != "" {
		c.requester = requester
	}
	cancel := c.cancel
	c.mu.Unlock()
	cancel()
}

func (c *mvTaskCancelController) normalizeTaskFailure(taskErr error) (*string, error) {
	if c == nil {
		return nil, taskErr
	}
	c.mu.Lock()
	reason := c.reason
	requester := c.requester
	c.mu.Unlock()
	if reason != mvTaskCancelReasonManual {
		return nil, taskErr
	}
	failedReason := formatMVManualCancelFailureReason(requester)
	return &failedReason, errMVTaskCanceledManually
}

func (c *mvTaskCancelController) isManualCancelRequested() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason == mvTaskCancelReasonManual
}

func formatMVManualCancelFailureReason(requester string) string {
	if requester == "" {
		return "cancelled manually"
	}
	return "cancelled manually by " + requester
}

func formatMVManualCancelRequester(user *auth.UserIdentity) string {
	if user == nil {
		return ""
	}
	username := user.AuthUsername
	if username == "" {
		username = user.Username
	}
	hostname := user.AuthHostname
	if hostname == "" {
		hostname = user.Hostname
	}
	if username == "" && hostname == "" {
		return ""
	}
	return "'" + strings.ReplaceAll(username, "'", "''") + "'@'" + strings.ReplaceAll(hostname, "'", "''") + "'"
}

type mvTaskCancelPoller func(context.Context, sqlexec.SQLExecutor) (requested bool, requester string, err error)
type mvTaskHeartbeatWriter func(context.Context, sqlexec.SQLExecutor) error

func startMVTaskMonitor(
	taskCtx context.Context,
	getSysSession func() (sessionctx.Context, error),
	releaseWatchSession func(sessionctx.Context),
	taskCancelController *mvTaskCancelController,
	monitorName string,
	poller mvTaskCancelPoller,
	heartbeatWriter mvTaskHeartbeatWriter,
) (func(), error) {
	if taskCancelController == nil {
		return func() {}, errors.New("mv task monitor: task cancel controller is nil")
	}
	monitorSctx, err := getSysSession()
	if err != nil {
		return func() {}, err
	}
	monitorCtx, stopMonitor := context.WithCancel(taskCtx)
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		defer releaseWatchSession(monitorSctx)

		sqlExec := monitorSctx.GetSQLExecutor()
		ticker := time.NewTicker(getMVTaskMonitorPollInterval())
		defer ticker.Stop()
		nextHeartbeatAt := time.Now().Add(getMVTaskHistHeartbeatInterval())
		for {
			if heartbeatWriter != nil && !time.Now().Before(nextHeartbeatAt) {
				heartbeatCtx, cancelHeartbeat := context.WithTimeout(monitorCtx, getMVTaskMonitorSQLTimeout())
				err := heartbeatWriter(heartbeatCtx, sqlExec)
				cancelHeartbeat()
				nextHeartbeatAt = time.Now().Add(getMVTaskHistHeartbeatInterval())
				if err != nil {
					if monitorCtx.Err() != nil {
						return
					}
					logutil.BgLogger().Warn("materialized view task heartbeat failed", zap.String("monitor", monitorName), zap.Error(err))
				}
			}

			pollCtx, cancelPoll := context.WithTimeout(monitorCtx, getMVTaskMonitorSQLTimeout())
			requested, requester, err := poller(pollCtx, sqlExec)
			cancelPoll()
			failpoint.InjectCall("mvTaskMonitorPolled", monitorName)
			if err != nil {
				if monitorCtx.Err() != nil {
					return
				}
				logutil.BgLogger().Warn("materialized view task monitor cancel poll failed", zap.String("monitor", monitorName), zap.Error(err))
			} else if requested {
				taskCancelController.requestManualCancelByRequester(requester)
				failpoint.InjectCall("mvTaskCancelWatcherRequested", monitorName)
				return
			}

			select {
			case <-monitorCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		stopMonitor()
		<-monitorDone
	}, nil
}

func getMVTaskMonitorPollInterval() time.Duration {
	interval := mvTaskMonitorPollInterval
	failpoint.Inject("mockMVTaskMonitorPollInterval", func(val failpoint.Value) {
		switch v := val.(type) {
		case int:
			interval = time.Duration(v) * time.Millisecond
		case int64:
			interval = time.Duration(v) * time.Millisecond
		}
	})
	return interval
}

func getMVTaskHistHeartbeatInterval() time.Duration {
	interval := mvTaskHistHeartbeatInterval
	failpoint.Inject("mockMVTaskHistHeartbeatInterval", func(val failpoint.Value) {
		switch v := val.(type) {
		case int:
			interval = time.Duration(v) * time.Millisecond
		case int64:
			interval = time.Duration(v) * time.Millisecond
		}
	})
	return interval
}

func getMVTaskMonitorSQLTimeout() time.Duration {
	timeout := mvTaskMonitorSQLTimeout
	failpoint.Inject("mockMVTaskMonitorSQLTimeout", func(val failpoint.Value) {
		switch v := val.(type) {
		case int:
			timeout = time.Duration(v) * time.Millisecond
		case int64:
			timeout = time.Duration(v) * time.Millisecond
		}
	})
	return timeout
}
