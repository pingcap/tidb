// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
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

func runRestoreCommand(command *cobra.Command, cmdName string) error {
	cfg := task.RestoreConfig{Config: task.Config{LogProgress: HasLogFile()}}
	if err := cfg.ParseFromFlags(command.Flags()); err != nil {
		command.SilenceUsage = false
		return errors.Trace(err)
	}

	if err := metricsutil.RegisterMetricsForBR(cfg.PD, cfg.KeyspaceName); err != nil {
		return errors.Trace(err)
	}

	if task.IsStreamRestore(cmdName) {
		if err := cfg.ParseStreamRestoreFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}
	}

	// have to skip grant table, in order to NotifyUpdatePrivilege in binary mode
	config.GetGlobalConfig().Security.SkipGrantTable = true

	ctx := GetDefaultContext()
	if cfg.EnableOpenTracing {
		var store *appdash.MemoryStore
		ctx, store = trace.TracerStartSpan(ctx)
		defer trace.TracerFinishSpan(ctx, store)
	}

	if cfg.FullBackupType == task.FullBackupTypeEBS {
		if cfg.Prepare {
			if err := task.RunRestoreEBSMeta(GetDefaultContext(), gluetikv.Glue{}, cmdName, &cfg); err != nil {
				log.Error("failed to restore EBS meta", zap.Error(err))
				return errors.Trace(err)
			}
		} else {
			if err := task.RunResolveKvData(GetDefaultContext(), tidbGlue, cmdName, &cfg); err != nil {
				log.Error("failed to restore data", zap.Error(err))
				return errors.Trace(err)
			}
		}
		return nil
	}

	// No need to cache the coproceesor result
	config.GetGlobalConfig().TiKVClient.CoprCache.CapacityMB = 0

	// Disable the memory limit tuner. That's because the server memory is get from TiDB node instead of BR node.
	gctuner.GlobalMemoryLimitTuner.DisableAdjustMemoryLimit()
	defer gctuner.GlobalMemoryLimitTuner.EnableAdjustMemoryLimit()

	if err := task.RunRestore(GetDefaultContext(), tidbGlue, cmdName, &cfg); err != nil {
		log.Error("failed to restore", zap.Error(err))
		printWorkaroundOnFullRestoreError(command, err)
		return errors.Trace(err)
	}
	return nil
}

// print workaround when we met not fresh or incompatible cluster error on full cluster restore
func printWorkaroundOnFullRestoreError(command *cobra.Command, err error) {
	if !errors.ErrorEqual(err, berrors.ErrRestoreNotFreshCluster) &&
		!errors.ErrorEqual(err, berrors.ErrRestoreIncompatibleSys) {
		return
	}
	fmt.Println("#######################################################################")
	switch {
	case errors.ErrorEqual(err, berrors.ErrRestoreNotFreshCluster):
		fmt.Println("# the target cluster is not fresh, cannot restore.")
		fmt.Println("# you can drop existing databases and tables and start restore again")
	case errors.ErrorEqual(err, berrors.ErrRestoreIncompatibleSys):
		fmt.Println("# the target cluster is not compatible with the backup data,")
		fmt.Println("# you can use '--with-sys-table=false' to skip restoring system tables")
	}
	fmt.Println("#######################################################################")
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

func runRestoreTxnCommand(command *cobra.Command, cmdName string) error {
	cfg := task.Config{LogProgress: HasLogFile()}
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
	if err := task.RunRestoreTxn(GetDefaultContext(), gluetikv.Glue{}, cmdName, &cfg); err != nil {
		log.Error("failed to restore txn kv", zap.Error(err))
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
			logutil.LogEnvVariables()
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
		newTxnRestoreCommand(),
		newStreamRestoreCommand(),
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
			return runRestoreCommand(cmd, task.FullRestoreCmd)
		},
	}
	task.DefineFilterFlags(command, filterOutSysAndMemTables, false)
	task.DefineRestoreSnapshotFlags(command)
	return command
}

func newDBRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "db",
		Short: "restore tables in a database from the backup data",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreCommand(cmd, task.DBRestoreCmd)
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
			return runRestoreCommand(cmd, task.TableRestoreCmd)
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
			return runRestoreRawCommand(cmd, task.RawRestoreCmd)
		},
	}

	task.DefineRawRestoreFlags(command)
	return command
}

func newTxnRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "txn",
		Short: "(experimental) restore txn kv to TiKV cluster",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runRestoreTxnCommand(cmd, task.TxnRestoreCmd)
		},
	}

	task.DefineRawRestoreFlags(command)
	return command
}

func newStreamRestoreCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "point",
		Short: "restore data from log until specify commit timestamp",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return runRestoreCommand(command, task.PointRestoreCmd)
		},
	}
	task.DefineFilterFlags(command, filterOutSysAndMemTables, true)
	task.DefineStreamRestoreFlags(command)
	return command
}
