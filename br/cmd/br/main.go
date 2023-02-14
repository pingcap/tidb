package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/log"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func main() {
	gCtx := context.Background()
	ctx, cancel := context.WithCancel(gCtx)
	defer cancel()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		log.Warn("received signal to exit", zap.Stringer("signal", sig))
		cancel()
		fmt.Fprintln(os.Stderr, "gracefully shuting down, press ^C again to force exit")
		<-sc
		// Even user use SIGTERM to exit, there isn't any checkpoint for resuming,
		// hence returning fail exit code.
		os.Exit(1)
	}()

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
