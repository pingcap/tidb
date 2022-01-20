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
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/trace"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/spf13/cobra"
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
			return nil
		},
	}
	command.AddCommand(
		newStreamStartCommand(),
		newStreamStopCommand(),
		newStreamPauseCommand(),
		newStreamResumeCommand(),
		newStreamStatusCommand(),
		newStreamRestoreCommand(),
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

	task.DefineStreamCommonFlags(command.Flags())
	task.DefineFilterFlags(command, acceptAllTables)
	task.DefineStreamStartFlags(command.PersistentFlags())
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

	task.DefineStreamCommonFlags(command.Flags())
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

	task.DefineStreamCommonFlags(command.Flags())
	return command
}

func newStreamResumeCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "resume",
		Short: "resume a stream task",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return streamCommand(command, task.StreamResume)
		},
	}

	task.DefineStreamCommonFlags(command.Flags())
	return command
}

func newStreamStatusCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "status",
		Short: "get status of a stream task",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return streamCommand(command, task.StreamStatus)
		},
	}

	task.DefineStreamStatusCommonFlags(command.Flags())
	return command
}

func newStreamRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "restore",
		Short: "restore a stream backups",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return streamCommand(command, task.StreamRestore)
		},
	}
	task.DefineFilterFlags(command, acceptAllTables)
	task.DefineStreamRestoreFlags(command.Flags())
	return command
}

func streamCommand(command *cobra.Command, cmdName string) error {
	var cfg task.StreamConfig
	var err error
	defer func() {
		if err != nil {
			command.SilenceUsage = false
		}
	}()
	if err = cfg.Config.ParseFromFlags(command.Flags()); err != nil {
		return errors.Trace(err)
	}

	switch cmdName {
	case task.StreamRestore:
		if err = cfg.ParseStreamRestoreFromFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}
	case task.StreamStart:
		if err = cfg.ParseStreamStartFromFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}
		fallthrough
	default:
		if err = cfg.ParseStreamCommonFromFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}
	}
	ctx := GetDefaultContext()
	if cfg.EnableOpenTracing {
		var store *appdash.MemoryStore
		ctx, store = trace.TracerStartSpan(ctx)
		defer trace.TracerFinishSpan(ctx, store)
	}

	return task.RunStreamCommand(ctx, tidbGlue, cmdName, &cfg)
}
