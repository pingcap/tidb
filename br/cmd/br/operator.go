// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/task/operator"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/spf13/cobra"
)

func newOperatorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "operator <subcommand>",
		Short: "utilities for operators like tidb-operator.",
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return errors.Trace(err)
			}
			build.LogInfo(build.BR)
			logutil.LogEnvVariables()
			task.LogArguments(c)
			return nil
		},
		Hidden: true,
	}
	cmd.AddCommand(newPrepareForSnapshotBackupCommand(
		"pause-gc-and-schedulers",
		"(Will be replaced with `prepare-for-snapshot-backup`) pause gc, schedulers and importing until the program exits."))
	cmd.AddCommand(newPrepareForSnapshotBackupCommand(
		"prepare-for-snapshot-backup",
		"pause gc, schedulers and importing until the program exits, for snapshot backup."))
	cmd.AddCommand(newBase64ifyCommand())
	cmd.AddCommand(newListMigrationsCommand())
	cmd.AddCommand(newMigrateToCommand())
	cmd.AddCommand(newForceFlushCommand())
	cmd.AddCommand(newChecksumCommand())
	return cmd
}

func newPrepareForSnapshotBackupCommand(use string, short string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := operator.PauseGcConfig{}
			if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
				return err
			}
			ctx := GetDefaultContext()
			return operator.AdaptEnvForSnapshotBackup(ctx, &cfg)
		},
	}
	operator.DefineFlagsForPrepareSnapBackup(cmd.Flags())
	return cmd
}

func newBase64ifyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "base64ify [-r] -s <storage>",
		Short: "generate base64 for a storage. this may be passed to `tikv-ctl compact-log-backup`.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := operator.Base64ifyConfig{}
			if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
				return err
			}
			ctx := GetDefaultContext()
			return operator.Base64ify(ctx, cfg)
		},
	}
	operator.DefineFlagsForBase64ifyConfig(cmd.Flags())
	return cmd
}

func newListMigrationsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-migrations",
		Short: "list all migrations",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := operator.ListMigrationConfig{}
			if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
				return err
			}
			ctx := GetDefaultContext()
			return operator.RunListMigrations(ctx, cfg)
		},
	}
	operator.DefineFlagsForListMigrationConfig(cmd.Flags())
	return cmd
}

func newMigrateToCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate-to",
		Short: "migrate to a specific version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := operator.MigrateToConfig{}
			if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
				return err
			}
			ctx := GetDefaultContext()
			return operator.RunMigrateTo(ctx, cfg)
		},
	}
	operator.DefineFlagsForMigrateToConfig(cmd.Flags())
	return cmd
}

func newChecksumCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "checksum-as",
		Short: "calculate the checksum with rewrite rules",
		Long: "Calculate the checksum of the current cluster (specified by `-u`) " +
			"with applying the rewrite rules generated from a backup (specified by `-s`). " +
			"This can be used when you have the checksum of upstream elsewhere.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := operator.ChecksumWithRewriteRulesConfig{}
			if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
				return err
			}
			ctx := GetDefaultContext()
			return operator.RunChecksumTable(ctx, tidbGlue, cfg)
		},
	}
	task.DefineFilterFlags(cmd, []string{"!*.*"}, false)
	operator.DefineFlagsForChecksumTableConfig(cmd.Flags())
	return cmd
}

func newForceFlushCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "force-flush",
		Short: "force a log backup task to flush",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := operator.ForceFlushConfig{}
			if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
				return err
			}
			ctx := GetDefaultContext()
			return operator.RunForceFlush(ctx, &cfg)
		},
	}
	operator.DefineFlagsForForceFlushConfig(cmd.Flags())
	return cmd
}
