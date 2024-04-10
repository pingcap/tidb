package main

import (
	"context"
	"os"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func main() {
	gCtx := context.Background()
	ctx, cancel := utils.StartExitSingleListener(gCtx)

	rootCmd := &cobra.Command{
		Use:              "br",
		Short:            "br is a TiDB/TiKV cluster backup restore tool.",
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	AddFlags(rootCmd)
	SetDefaultContext(ctx)
	rootCmd.AddCommand(
		NewDebugCommand(),
		NewBackupCommand(),
		NewRestoreCommand(),
		NewStreamCommand(),
		newOperatorCommand(),
	)
	// Outputs cmd.Print to stdout.
	rootCmd.SetOut(os.Stdout)

	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		cancel()
		log.Error("br failed", zap.Error(err))
		os.Exit(1) // nolint:gocritic
	}
}
