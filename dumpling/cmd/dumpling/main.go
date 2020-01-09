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
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/pingcap/dumpling/v4/cli"
	"github.com/pingcap/dumpling/v4/export"
	"github.com/pingcap/dumpling/v4/log"
	"go.uber.org/zap"
)

var (
	database    string
	host        string
	user        string
	port        int
	password    string
	threads     int
	outputDir   string
	fileSize    uint64
	logLevel    string
	consistency string
	snapshot    string
)

func init() {
	flag.StringVar(&database, "database", "", "Database to dump")
	flag.StringVar(&database, "B", "", "Database to dump")

	flag.StringVar(&host, "h", "127.0.0.1", "The host to connect to")
	flag.StringVar(&host, "host", "127.0.0.1", "The host to connect to")

	flag.StringVar(&user, "user", "root", "Username with privileges to run the dump")
	flag.StringVar(&user, "u", "root", "Username with privileges to run the dump")

	flag.IntVar(&port, "port", 4000, "TCP/IP port to connect to")
	flag.IntVar(&port, "P", 4000, "TCP/IP port to connect to")

	flag.StringVar(&password, "password", "", "User password")
	flag.StringVar(&password, "p", "", "User password")

	flag.IntVar(&threads, "threads", 4, "Number of goroutines to use, default 4")
	flag.IntVar(&threads, "t", 4, "Number of goroutines to use, default 4")

	flag.Uint64Var(&fileSize, "F", export.UnspecifiedSize, "The approximate size of output file")
	flag.Uint64Var(&fileSize, "filesize", export.UnspecifiedSize, "The approximate size of output file")

	flag.StringVar(&outputDir, "output", defaultOutputDir, "Output directory")
	flag.StringVar(&outputDir, "o", defaultOutputDir, "Output directory")

	flag.StringVar(&logLevel, "loglevel", "info", "Log level: {debug|info|warn|error|dpanic|panic|fatal}")

	flag.StringVar(&consistency, "consistency", "auto", "Consistency level during dumping: {auto|none|flush|lock|snapshot}")

	flag.StringVar(&snapshot, "snapshot", "", "Snapshot position. Valid only when consistency=snapshot")
}

var defaultOutputDir = timestampDirName()

func timestampDirName() string {
	return fmt.Sprintf("./export-%s", time.Now().Format(time.RFC3339))
}

func main() {
	flag.Parse()
	println(cli.LongVersion())

	err := log.InitAppLogger(&log.Config{Level: logLevel})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "initialize logger failed: %s", err.Error())
		os.Exit(1)
	}

	conf := export.DefaultConfig()
	conf.Database = database
	conf.Host = host
	conf.User = user
	conf.Port = port
	conf.Password = password
	conf.Threads = threads
	conf.FileSize = fileSize
	conf.OutputDirPath = outputDir

	err = export.Dump(conf)
	if err != nil {
		log.Zap().Error("dump failed", zap.String("error", err.Error()))
		os.Exit(1)
	}
	return
}
