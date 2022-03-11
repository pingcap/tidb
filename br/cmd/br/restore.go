// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/gluetikv"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/trace"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/session"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"sourcegraph.com/sourcegraph/appdash"
)

func runRestoreCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RestoreConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}

	ctx := GetDefaultContext()
	if cfg.EnableOpenTracing {
		var store *appdash.MemoryStore
		ctx, store = trace.TracerStartSpan(ctx)
		defer trace.TracerFinishSpan(ctx, store)
	}
	if err := task.RunRestore(GetDefaultContext(), tidbGlue, cmdName, &cfg); err != nil {
		log.Error("failed to restore", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func runRestoreRawCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RestoreRawConfig{
		RawKvConfig: task.RawKvConfig{Config: task.Config{LogProgress: HasLogFile()}},
	}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}

	ctx := GetDefaultContext()
	if cfg.EnableOpenTracing {
		var store *appdash.MemoryStore
		ctx, store = trace.TracerStartSpan(ctx)
		defer trace.TracerFinishSpan(ctx, store)
	}
	if err := task.RunRestoreRaw(GetDefaultContext(), gluetikv.Glue{}, cmdName, &cfg); err != nil {
		log.Error("failed to restore raw kv", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// NewRestoreCommand returns a restore subcommand.
func NewRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "restore",
		Short:        "restore a TiDB/TiKV cluster",
		SilenceUsage: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return errors.Trace(err)
			}
			build.LogInfo(build.BR)
			utils.LogEnvVariables()
			task.LogArguments(c)
			session.DisableStats4Test()

			summary.SetUnit(summary.RestoreUnit)
			return nil
		},
	}
	command.AddCommand(
		newFullRestoreCommand(),
		newDBRestoreCommand(),
		newTableRestoreCommand(),
		newRawRestoreCommand(),
	)
	task.DefineRestoreFlags(command.PersistentFlags())

	return command
}

func newFullRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "restore all tables",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreCommand(cmd, "Full restore")
		},
	}
	task.DefineFilterFlags(command, filterOutSysAndMemTables)
	return command
}

func newDBRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "db",
		Short: "restore tables in a database from the backup data",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreCommand(cmd, "Database restore")
		},
	}
	task.DefineDatabaseFlags(command)
	return command
}

func newTableRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "table",
		Short: "restore a table from the backup data",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreCommand(cmd, "Table restore")
		},
	}
	task.DefineTableFlags(command)
	return command
}

func newRawRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "raw",
		Short: "(experimental) restore a raw kv range to TiKV cluster",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreRawCommand(cmd, "Raw restore")
		},
	}

	task.DefineRawRestoreFlags(command)
	return command
}
