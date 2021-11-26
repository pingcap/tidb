// Copyright 2015 PingCAP, Inc.
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

package main

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/trace"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"sourcegraph.com/sourcegraph/appdash"
)

// NewStreamCommand specifies adding several commands for backup log
func NewStreamCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "stream",
		Short:        "backup stream log from TiDB/TiKV cluster",
		SilenceUsage: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return errors.Trace(err)
			}
			build.LogInfo(build.BR)
			utils.LogEnvVariables()
			task.LogArguments(c)

			// Do not run ddl worker in BR.
			//ddl.RunWorker = false

			//summary.SetUnit(summary.BackupUnit)
			return nil
		},
	}
	command.AddCommand(
		newStreamStartCommand(),
		newStreamStopCommand(),
		newStreamPauseCommand(),
		newSteramResumeComamnd(),
	)
	return command
}

func newStreamStartCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "start",
		Short: "start a stream task",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return streamCommand(command, task.StreamStart)
		},
	}

	task.DefineStreamTaskNameFlags(command.Flags())
	task.DefineFilterFlags(command, acceptAllTables)
	task.DefineStreamTSFlags(command.PersistentFlags())
	return command
}

func newStreamStopCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "stop",
		Short: "stop a stream task",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return streamCommand(command, task.StreamStop)
		},
	}

	task.DefineStreamTaskNameFlags(command.Flags())
	return command
}

func newStreamPauseCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "pause",
		Short: "pause a stream task",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return streamCommand(command, task.StreamPause)
		},
	}

	task.DefineStreamTaskNameFlags(command.Flags())
	return command
}

func newSteramResumeComamnd() *cobra.Command {
	command := &cobra.Command{
		Use:   "resume",
		Short: "resume a stream task",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			// empty db/table means full backup.
			return streamCommand(command, task.StreamResume)
		},
	}

	task.DefineStreamTaskNameFlags(command.Flags())
	return command
}

func streamCommand(command *cobra.Command, cmdName string) error {
	var cfg task.StreamConfig
	if err := cfg.ParseCommonFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}

	if cmdName == task.StreamStart {
		if err := cfg.ParseTSFromFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}
	}

	ctx := GetDefaultContext()
	if cfg.EnableOpenTracing {
		var store *appdash.MemoryStore
		ctx, store = trace.TracerStartSpan(ctx)
		defer trace.TracerFinishSpan(ctx, store)
	}

	commandFn, exist := task.StreamCommandMap[cmdName]
	if !exist {
		return errors.Annotatef(berrors.ErrInvalidArgument, "invalid command %s\n", cmdName)
	}

	if err := commandFn(ctx, tidbGlue, cmdName, &cfg); err != nil {
		log.Error("failed to stream", zap.String("command", cmdName), zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}
