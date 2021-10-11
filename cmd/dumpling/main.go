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
	_ "net/http/pprof"
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/cli"
	"github.com/pingcap/dumpling/v4/export"
)

func main() {
	pflag.Usage = func() {
		fmt.Fprint(os.Stderr, "Dumpling is a CLI tool that helps you dump MySQL/TiDB data\n\nUsage:\n  dumpling [flags]\n\nFlags:\n")
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

	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())
	export.InitMetricsVector(conf.Labels)
	export.RegisterMetrics(registry)
	prometheus.DefaultGatherer = registry
	dumper, err := export.NewDumper(context.Background(), conf)
	if err != nil {
		fmt.Printf("\ncreate dumper failed: %s\n", err.Error())
		os.Exit(1)
	}
	err = dumper.Dump()
	dumper.Close()
	if err != nil {
		dumper.L().Error("dump failed error stack info", zap.Error(err))
		fmt.Printf("\ndump failed: %s\n", err.Error())
		os.Exit(1)
	}
	dumper.L().Info("dump data successfully, dumpling will exit now")
}
