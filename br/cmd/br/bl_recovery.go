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

// NewBlRecoveryCommand used to start a recovery service for restore a txnTiKV from block-level storage.
func NewBlRecoveryCommand() *cobra.Command {
	command := &cobra.Command{
		Use:          "blr",
		Short:        "recovery an offline txnTiKV cluster by specific TSO",
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
		newRecoveryStartCommand(),
	)
	return command
}

func newRecoveryStartCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "start",
		Short: "start block-level recovery",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return recoveryCommand(command, task.RecoveryStart)
		},
	}
	task.DefineRecoveryStartFlags(command.Flags())

	return command
}

func recoveryCommand(command *cobra.Command, cmdName string) error {
	var cfg task.RecoveryConfig
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
	// case task.RecoveryStatus:
	// 	if err = cfg.ParseRecoveryStatusFromFlags(command.Flags()); err != nil {
	// 		return errors.Trace(err)
	// 	}
	case task.RecoveryStart:
		if err = cfg.ParseRecoveryStartFromFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}

	default:
		if err = cfg.ParseRecoveryCommonFromFlags(command.Flags()); err != nil {
			return errors.Trace(err)
		}
	}
	ctx := GetDefaultContext()
	if cfg.EnableOpenTracing {
		var store *appdash.MemoryStore
		ctx, store = trace.TracerStartSpan(ctx)
		defer trace.TracerFinishSpan(ctx, store)
	}

	return task.RunRecoveryCommand(ctx, cmdName, &cfg)
}
