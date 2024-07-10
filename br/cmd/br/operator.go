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
