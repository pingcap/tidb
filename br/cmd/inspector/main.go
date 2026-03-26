// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package main

import (
	"context"
	"os"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/br/pkg/version/build"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	flagLogLevel  = "log-level"
	flagLogFile   = "log-file"
	flagLogFormat = "log-format"
)

var defaultContext context.Context

func setDefaultContext(ctx context.Context) {
	defaultContext = ctx
}

func getDefaultContext() context.Context {
	return defaultContext
}

func main() {
	gCtx := context.Background()
	ctx, cancel := utils.StartExitSingleListener(gCtx)
	defer cancel()

	rootCmd := &cobra.Command{
		Use:              "inspector",
		Short:            "inspector is a TiDB/TiKV offline backup inspection tool.",
		TraverseChildren: true,
		SilenceUsage:     true,
	}
	defineInspectorFlags(rootCmd)
	setDefaultContext(ctx)
	rootCmd.AddCommand(newBackupMetaCommand())
	rootCmd.AddCommand(newMetaKVCommand())
	rootCmd.SetOut(os.Stdout)

	rootCmd.SetArgs(os.Args[1:])
	if err := rootCmd.Execute(); err != nil {
		log.Error("inspector failed", zap.Error(err))
		os.Exit(1) // nolint:gocritic
	}
}

func defineInspectorFlags(cmd *cobra.Command) {
	cmd.Version = build.Info()
	cmd.Flags().BoolP("version", "V", false, "Display version information about inspector")
	cmd.SetVersionTemplate("{{printf \"%s\" .Version}}\n")

	cmd.PersistentFlags().StringP(flagLogLevel, "L", "warn", "Set the log level")
	cmd.PersistentFlags().String(flagLogFile, "", "Set the log file path. If not set, logs are written to stderr")
	cmd.PersistentFlags().String(flagLogFormat, "text", "Set the log format")

	// Register storage / S3 / GCS / azblob flags so subcommands can use -s / --s3.* etc.
	task.DefineCommonFlags(cmd.PersistentFlags())

	// Hide flags that are irrelevant for an offline inspection tool.
	for _, name := range []string{
		"pd",
		"ca", "cert", "key",
		"send-credentials-to-tikv",
		"checksum", "checksum-concurrency",
		"ratelimit", "ratelimit-unit",
		"remove-tiflash",
		"check-requirements",
		"switch-mode-interval",
		"grpc-keepalive-time",
		"grpc-keepalive-timeout",
		"enable-opentracing",
		"no-credentials",
		"skip-check-path",
		"crypter.method", "crypter.key", "crypter.key-file",
		"log.crypter.method", "log.crypter.key", "log.crypter.key-file",
		"master-key-crypter-method", "master-key",
		"metadata-download-batch-size",
	} {
		_ = cmd.PersistentFlags().MarkHidden(name)
	}
}

// initInspector initialises logging. Called at the start of each subcommand's RunE.
func initInspector(cmd *cobra.Command) error {
	conf := new(log.Config)
	var err error
	conf.Level, err = cmd.Flags().GetString(flagLogLevel)
	if err != nil {
		return err
	}
	conf.File.Filename, err = cmd.Flags().GetString(flagLogFile)
	if err != nil {
		return err
	}
	conf.Format, err = cmd.Flags().GetString(flagLogFormat)
	if err != nil {
		return err
	}
	lg, p, err := log.InitLogger(conf)
	if err != nil {
		return err
	}
	log.ReplaceGlobals(lg, p)
	return nil
}
