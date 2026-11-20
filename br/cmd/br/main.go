package main

import (
	"context"
	"os"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/config"
	"github.com/spf13/cobra"
)

func main() {
	gCtx := context.Background()
	ctx, cancel := utils.StartExitSingleListener(gCtx)
	defer cancel()

	rootCmd := &cobra.Command{
		Use:              "br",
		Short:            "br is a TiDB/TiKV cluster backup restore tool.",
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	DefineCommonFlags(rootCmd)
	SetDefaultContext(ctx)

	config.GetGlobalConfig().Instance.TiDBEnableDDL.Store(false)

	rootCmd.AddCommand(
		NewDebugCommand(),
		NewRepoCommand(),
		NewBackupCommand(),
		NewRestoreCommand(),
		NewStreamCommand(),
		newOperatorCommand(),
		NewAbortCommand(),
	)
	// Outputs cmd.Print to stdout.
	rootCmd.SetOut(os.Stdout)

	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		fields := summary.FailureFields(err, redactDiagnosticErrorText)
		summary.Log("br failure summary", fields...)
		log.Error("br failed", fields...)
		os.Exit(1) // nolint:gocritic
	}
}
