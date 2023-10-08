// Copyright 2023 PingCAP, Inc.
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

package config

import (
	"time"
)

const (
	// DefaultHistorySubtaskTableGcInterval is the interval of gc history subtask table.
	DefaultHistorySubtaskTableGcInterval = 24 * time.Hour
	// DefaultCleanUpInterval is the interval of cleanUp routine.
	DefaultCleanUpInterval = 10 * time.Minute
	// DefaultSubtaskConcurrency is the default concurrency for handling subtask.
	DefaultSubtaskConcurrency = 16
	// MaxSubtaskConcurrency is the maximum concurrency for handling subtask.
	MaxSubtaskConcurrency = 256
	// DefaultLiveNodesCheckInterval is the tick interval of fetching all server infos from etcd.
	DefaultLiveNodesCheckInterval = 2
	// DefaultCheckSubtaskCanceledInterval is the default check interval for cancel cancelled subtasks.
	DefaultCheckSubtaskCanceledInterval = 2 * time.Second
	// RetrySQLTimes is the max retry times when executing SQL.
	RetrySQLTimes = 30
	// RetrySQLMaxInterval is the max interval between two SQL retries.
	RetrySQLMaxInterval = 30 * time.Second
	// SchedulerPoolSize is the size of scheduler pool.
	SchedulerPoolSize int32 = 4
)

var (
	// DefaultDispatchConcurrency is the default concurrency for dispatching task.
	DefaultDispatchConcurrency = 4
	// CheckTaskRunningInterval is the interval for loading running/unfinished tasks.
	CheckTaskRunningInterval = 3 * time.Second
	// CheckTaskFinishedInterval
	CheckTaskFinishedInterval = 500 * time.Millisecond
	// RetrySQLInterval is the initial interval between two SQL retries.
	RetrySQLInterval = 3 * time.Second
	// CheckTaskInterval is the time used for check task state in scheduler.Manager.
	CheckTaskInterval = 300 * time.Millisecond
)
