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
		//newStreamPauseCommand(),
		//newStreamResumeCommand(),
		newStreamStatusCommand(),
		newStreamTruncateCommand(),
	)
	command.SetHelpFunc(func(command *cobra.Command, strings []string) {
		task.HiddenFlagsForStream(command.Root().PersistentFlags())
		command.Root().HelpFunc()(command, strings)
	})

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
	task.DefineFilterFlags(command, acceptAllTables, true)
	task.DefineStreamStartFlags(command.Flags())
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

//nolint:unused,deadcode
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

//nolint:unused,deadcode
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

func newStreamTruncateCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "truncate",
		Short: "truncate the incremental data until sometime.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			return streamCommand(cmd, task.StreamTruncate)
		},
	}
	task.DefineStreamTruncateLogFlags(command.Flags())
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

	cfg.Config = task.Config{LogProgress: HasLogFile()}
	if err = cfg.Config.ParseFromFlags(command.Flags()); err != nil {
		return errors.Trace(err)
	}

	switch cmdName {
	case task.StreamTruncate:
		if err = cfg.ParseStreamTruncateFromFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}
	case task.StreamStatus:
		if err = cfg.ParseStreamStatusFromFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}
		if err = cfg.ParseStreamCommonFromFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}
	case task.StreamStart:
		if err = cfg.ParseStreamStartFromFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}
		// TODO use `br restore stream` rather than `br stream restore`
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
