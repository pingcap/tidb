// Copyright 2023 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/task/operator"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/spf13/cobra"
)

func newOpeartorCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "operator <subcommand>",
		Short: "utilities for operators like tidb-operator.",
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			if err := Init(c); err != nil {
				return errors.Trace(err)
			}
			build.LogInfo(build.BR)
			utils.LogEnvVariables()
			task.LogArguments(c)
			return nil
		},
		Hidden: true,
	}
	cmd.AddCommand(newPauseGcCommand())
	return cmd
}

func newPauseGcCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause-gc",
		Short: "pause gc to the ts until the program exits.",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := operator.PauseGcConfig{}
			if err := cfg.ParseFromFlags(cmd.Flags()); err != nil {
				return err
			}
			ctx := GetDefaultContext()
			return operator.PauseGC(ctx, &cfg)
		},
	}
	operator.DefineFlagsForPauseGcConfig(cmd.Flags())
	return cmd
}
