// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/trace"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"sourcegraph.com/sourcegraph/appdash"
)

// NewAbortCommand returns an abort subcommand
func NewAbortCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "abort",
		Short:        "abort restore tasks",
		SilenceUsage: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return errors.Trace(err)
			}
			build.LogInfo(build.BR)
			logutil.LogEnvVariables()
			task.LogArguments(c)
			return nil
		},
	}

	command.AddCommand(
		newAbortRestoreCommand(),
		// future: newAbortBackupCommand(),
	)
	task.DefineRestoreFlags(command.PersistentFlags())

	return command
}

// newAbortRestoreCommand returns an abort restore subcommand
func newAbortRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "restore",
		Short:        "abort restore tasks",
		SilenceUsage: true,
	}

	command.AddCommand(
		newAbortRestoreFullCommand(),
		newAbortRestoreDBCommand(),
		newAbortRestoreTableCommand(),
		newAbortRestorePointCommand(),
	)

	return command
}

func newAbortRestoreFullCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "abort a full restore task",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAbortRestoreCommand(cmd, task.FullRestoreCmd)
		},
	}
	// define flags specific to full restore
	task.DefineFilterFlags(command, filterOutSysAndMemKeepAuthAndBind, false)
	task.DefineRestoreSnapshotFlags(command)
	return command
}

func newAbortRestoreDBCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "db",
		Short: "abort a database restore task",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAbortRestoreCommand(cmd, task.DBRestoreCmd)
		},
	}
	task.DefineDatabaseFlags(command)
	return command
}

func newAbortRestoreTableCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "table",
		Short: "abort a table restore task",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAbortRestoreCommand(cmd, task.TableRestoreCmd)
		},
	}
	task.DefineTableFlags(command)
	return command
}

func newAbortRestorePointCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "point",
		Short: "abort a point-in-time restore task",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAbortRestoreCommand(cmd, task.PointRestoreCmd)
		},
	}
	task.DefineFilterFlags(command, filterOutSysAndMemKeepAuthAndBind, true)
	task.DefineStreamRestoreFlags(command)
	return command
}

func runAbortRestoreCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RestoreConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags(), false); err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}

	if task.IsStreamRestore(cmdName) {
		if err := cfg.ParseStreamRestoreFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}
	}

	ctx := GetDefaultContext()
	if cfg.EnableOpenTracing {
		var store *appdash.MemoryStore
		ctx, store = trace.TracerStartSpan(ctx)
		defer trace.TracerFinishSpan(ctx, store)
	}

	if err := task.RunRestoreAbort(ctx, tidbGlue, cmdName, &cfg); err != nil {
		log.Error("failed to abort restore task", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}
