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
	"errors"
	"fmt"
	_ "net/http/pprof"
	"os"

	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/cli"
	"github.com/pingcap/dumpling/v4/export"
	"github.com/pingcap/dumpling/v4/log"
)

func main() {
	pflag.Usage = func() {
		fmt.Fprint(os.Stderr, "Dumpling is a CLI tool that helps you dump MySQL/TiDB data\n\nUsage:\n  dumpling [flags]\n\nFlags:\n")
		pflag.PrintDefaults()
	}
	pflag.ErrHelp = errors.New("")

	printVersion := pflag.BoolP("version", "V", false, "Print Dumpling version")

	conf := export.DefaultConfig()
	conf.DefineFlags(pflag.CommandLine)

	pflag.Parse()
	println(cli.LongVersion())
	if *printVersion {
		return
	}

	err := conf.ParseFromFlags(pflag.CommandLine)
	if err != nil {
		fmt.Printf("\nparse arguments failed: %+v\n", err)
		os.Exit(1)
	}

	err = export.Dump(context.Background(), conf)
	if err != nil {
		log.Error("dump failed error stack info", zap.Error(err))
		fmt.Printf("\ndump failed: %s\n", err.Error())
		os.Exit(1)
	} else {
		log.Info("dump data successfully, dumpling will exit now")
	}
}
