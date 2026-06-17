// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pingcap/tidb/dumpling/cli"
	"github.com/pingcap/tidb/dumpling/export"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
)

func writeTerminationLog(dumper *export.Dumper) {
	exportSizePath := "/dev/termination-log"

	s := dumper.GetStatus()
	value := fmt.Sprintf("[FinishedSize]: %f\n[CompressedSize]: %f\n", s.FinishedBytes, s.CompressedBytes)

	file, err := os.OpenFile(exportSizePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		dumper.L().Error("failed to open termination log file", zap.Error(err), zap.String("path", exportSizePath))
		return
	}

	_, writeErr := file.WriteString(value)
	syncErr := file.Sync() // Force flush to disk
	closeErr := file.Close()

	if writeErr != nil {
		dumper.L().Error("failed to write to termination log", zap.Error(writeErr))
		return
	}
	if syncErr != nil {
		dumper.L().Error("failed to sync termination log", zap.Error(syncErr))
		return
	}
	if closeErr != nil {
		dumper.L().Error("failed to close termination log", zap.Error(closeErr))
		return
	}

	dumper.L().Info("successfully wrote export size to termination log",
		zap.Float64("finishedBytes", s.FinishedBytes),
		zap.Float64("compressedBytes", s.CompressedBytes))
}

func main() {
	pflag.Usage = func() {
		fmt.Fprint(os.Stderr,
			"Dumpling is a CLI tool that helps you dump MySQL/TiDB data\n\nUsage:\n  dumpling [flags]\n\nFlags:\n")
		pflag.PrintDefaults()
	}
	printVersion := pflag.BoolP("version", "V", false, "Print Dumpling version")

	conf := export.DefaultConfig()
	conf.DefineFlags(pflag.CommandLine)

	pflag.Parse()
	if printHelp, err := pflag.CommandLine.GetBool(export.FlagHelp); printHelp || err != nil {
		if err != nil {
			fmt.Printf("\nGet help flag error: %s\n", err)
		}
		pflag.Usage()
		return
	}
	println(cli.LongVersion())
	if *printVersion {
		return
	}

	err := conf.ParseFromFlags(pflag.CommandLine)
	if err != nil {
		fmt.Printf("\nparse arguments failed: %+v\n", err)
		os.Exit(1)
	}
	if pflag.NArg() > 0 {
		fmt.Printf("\nmeet some unparsed arguments, please check again: %+v\n", pflag.Args())
		os.Exit(1)
	}

	registry := conf.PromRegistry
	registry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registry.MustRegister(collectors.NewGoCollector())
	if gatherer, ok := registry.(prometheus.Gatherer); ok {
		prometheus.DefaultGatherer = gatherer
	}

	dumper, err := export.NewDumper(context.Background(), conf)
	if err != nil {
		fmt.Printf("\ncreate dumper failed: %s\n", err.Error())
		os.Exit(1)
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-stop
		dumper.L().Info(fmt.Sprintf("received signal %v, preparing to terminate", sig))
		writeTerminationLog(dumper)
		if err := dumper.Close(); err != nil {
			dumper.L().Error("error closing dumper", zap.Error(err))
		}
	}()
	err = dumper.Dump()
	_ = dumper.Close()
	if err != nil {
		dumper.L().Error("dump failed error stack info", zap.Error(err))
		fmt.Printf("\ndump failed: %s\n", err.Error())
		os.Exit(1)
	}

	// Write final status to termination log on successful completion
	writeTerminationLog(dumper)
	dumper.L().Info("dump data successfully, dumpling will exit now")
}
