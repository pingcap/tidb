// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/gluetikv"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/trace"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/pingcap/tidb/pkg/session"
	"github.com/pingcap/tidb/pkg/util/gctuner"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/pingcap/tidb/pkg/util/metricsutil"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"sourcegraph.com/sourcegraph/appdash"
)

func runBackupCommand(command *cobra.Command, cmdName string) error {
	cfg := task.BackupConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}

	if err := metricsutil.RegisterMetricsForBR(cfg.PD, cfg.KeyspaceName); err != nil {
		return errors.Trace(err)
	}

	ctx := GetDefaultContext()
	if cfg.EnableOpenTracing {
		var store *appdash.MemoryStore
		ctx, store = trace.TracerStartSpan(ctx)
		defer trace.TracerFinishSpan(ctx, store)
	}

	if cfg.FullBackupType == task.FullBackupTypeEBS {
		if err := task.RunBackupEBS(ctx, tidbGlue, &cfg); err != nil {
			log.Error("failed to backup", zap.Error(err))
			return errors.Trace(err)
		}
		return nil
	}

	// No need to cache the coproceesor result
	config.GetGlobalConfig().TiKVClient.CoprCache.CapacityMB = 0

	// Disable the memory limit tuner. That's because the server memory is get from TiDB node instead of BR node.
	gctuner.GlobalMemoryLimitTuner.DisableAdjustMemoryLimit()
	defer gctuner.GlobalMemoryLimitTuner.EnableAdjustMemoryLimit()

	if err := task.RunBackup(ctx, tidbGlue, cmdName, &cfg); err != nil {
		log.Error("failed to backup", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func runBackupRawCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RawKvConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseBackupConfigFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}

	ctx := GetDefaultContext()
	if cfg.EnableOpenTracing {
		var store *appdash.MemoryStore
		ctx, store = trace.TracerStartSpan(ctx)
		defer trace.TracerFinishSpan(ctx, store)
	}
	if err := task.RunBackupRaw(ctx, gluetikv.Glue{}, cmdName, &cfg); err != nil {
		log.Error("failed to backup raw kv", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

func runBackupTxnCommand(command *cobra.Command, cmdName string) error {
	cfg := task.TxnKvConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseBackupConfigFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}

	ctx := GetDefaultContext()
	if cfg.EnableOpenTracing {
		var store *appdash.MemoryStore
		ctx, store = trace.TracerStartSpan(ctx)
		defer trace.TracerFinishSpan(ctx, store)
	}
	if err := task.RunBackupTxn(ctx, gluetikv.Glue{}, cmdName, &cfg); err != nil {
		log.Error("failed to backup txn kv", zap.Error(err))
		return errors.Trace(err)
	}
	return nil
}

// NewBackupCommand return a full backup subcommand.
func NewBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "backup",
		Short:        "backup a TiDB/TiKV cluster",
		SilenceUsage: true,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return errors.Trace(err)
			}
			build.LogInfo(build.BR)
			logutil.LogEnvVariables()
			task.LogArguments(c)
			// Do not run stat worker in BR.
			session.DisableStats4Test()

			// Do not run ddl worker in BR.
			config.GetGlobalConfig().Instance.TiDBEnableDDL.Store(false)

			summary.SetUnit(summary.BackupUnit)
			return nil
		},
	}
	command.AddCommand(
		newFullBackupCommand(),
		newDBBackupCommand(),
		newTableBackupCommand(),
		newRawBackupCommand(),
		newTxnBackupCommand(),
	)

	task.DefineBackupFlags(command.PersistentFlags())
	return command
}

// newFullBackupCommand return a full backup subcommand.
func newFullBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "full",
		Short: "backup all database",
		// prevents incorrect usage like `--checksum false` instead of `--checksum=false`.
		// the former, according to pflag parsing rules, means `--checksum=true false`.
		Args: cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			// empty db/table means full backup.
			return runBackupCommand(command, task.FullBackupCmd)
		},
	}
	task.DefineFilterFlags(command, acceptAllTables, false)
	task.DefineBackupEBSFlags(command.PersistentFlags())
	return command
}

// newDBBackupCommand return a db backup subcommand.
func newDBBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "db",
		Short: "backup a database",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runBackupCommand(command, task.DBBackupCmd)
		},
	}
	task.DefineDatabaseFlags(command)
	return command
}

// newTableBackupCommand return a table backup subcommand.
func newTableBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "table",
		Short: "backup a table",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runBackupCommand(command, task.TableBackupCmd)
		},
	}
	task.DefineTableFlags(command)
	return command
}

// newRawBackupCommand return a raw kv range backup subcommand.
func newRawBackupCommand() *cobra.Command {
	// TODO: remove experimental tag if it's stable
	command := &cobra.Command{
		Use:   "raw",
		Short: "(experimental) backup a raw kv range from TiKV cluster",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runBackupRawCommand(command, task.RawBackupCmd)
		},
	}

	task.DefineRawBackupFlags(command)
	return command
}

// newTxnBackupCommand return a txn kv range backup subcommand.
func newTxnBackupCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "txn",
		Short: "(experimental) backup a txn kv range from TiKV cluster",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runBackupTxnCommand(command, task.TxnBackupCmd)
		},
	}

	task.DefineTxnBackupFlags(command)
	return command
}
