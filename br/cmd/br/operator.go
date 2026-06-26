// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/stream/crr/service"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/task/operator"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/spf13/cobra"
)

type crrCheckpointServiceStateKey struct{}

type crrCheckpointServiceState struct {
	service *service.Service
	cleanup func()
}

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
	cmd.AddCommand(newCRRCheckpointCommand())
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
		Use: "unsafe-migrate-to",
		Short: "migrate to a specific version, use truncate will auto migrate to correct version, " +
			"you should never use this command unless you know what you are doing",
		Args: cobra.NoArgs,
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
	cmd.Hidden = true
	return cmd
}

func newCRRCheckpointCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "crr-checkpoint",
		Short: "run the CRR checkpoint service",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			state, err := getCRRCheckpointServiceState(cmd)
			if err != nil {
				return err
			}
			defer state.cleanup()
			return state.service.Run(GetDefaultContext())
		},
	}
	operator.DefineFlagsForCRRCheckpointConfig(cmd.Flags())
	registerStatusServerPreparer(cmd, prepareCRRCheckpointStatusServer)
	return cmd
}

func prepareCRRCheckpointStatusServer(cmd *cobra.Command) (statusServerRegistrar, error) {
	cfg := operator.CRRCheckpointConfig{}
	if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
		return nil, err
	}
	svc, cleanup, err := operator.NewCRRCheckpointService(GetDefaultContext(), tidbGlue, cfg)
	if err != nil {
		return nil, err
	}

	baseCtx := cmd.Context()
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	cmd.SetContext(context.WithValue(baseCtx, crrCheckpointServiceStateKey{}, &crrCheckpointServiceState{
		service: svc,
		cleanup: cleanup,
	}))
	return svc.Register, nil
}

func getCRRCheckpointServiceState(cmd *cobra.Command) (*crrCheckpointServiceState, error) {
	if cmd.Context() == nil {
		return nil, errors.New("crr checkpoint service context is missing")
	}
	state, ok := cmd.Context().Value(crrCheckpointServiceStateKey{}).(*crrCheckpointServiceState)
	if !ok || state == nil {
		return nil, errors.New("crr checkpoint service is not prepared")
	}
	return state, nil
}
