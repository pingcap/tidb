// Copyright 2015 PingCAP, Inc.
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
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/metric"
	"github.com/pingcap/tidb/store/localstore/boltdb"
	"github.com/pingcap/tidb/tidb-server/server"
	"github.com/pingcap/tidb/util/printer"
)

var (
	storePath = flag.String("path", "/tmp/tidb", "tidb storage path")
	logLevel  = flag.String("L", "debug", "log level: info, debug, warn, error, fatal")
	port      = flag.String("P", "4000", "mp server port")
	lease     = flag.Int("lease", 1, "schema lease seconds, very dangerous to change only if you know what you do")
)

func main() {
	tidb.RegisterLocalStore("boltdb", boltdb.Driver{})

	metric.RunMetric(3 * time.Second)
	printer.PrintTiDBInfo()
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	if *lease < 0 {
		log.Fatalf("invalid lease seconds %d", *lease)
	}

	tidb.SetSchemaLease(time.Duration(*lease) * time.Second)

	cfg := &server.Config{
		Addr:     fmt.Sprintf(":%s", *port),
		LogLevel: *logLevel,
	}

	log.SetLevelByString(cfg.LogLevel)
	store, err := tidb.NewStore(fmt.Sprintf("%s://%s", "boltdb", *storePath))
	if err != nil {
		log.Fatal(err)
	}

	var driver server.IDriver
	driver = server.NewTiDBDriver(store)
	var svr *server.Server
	svr, err = server.NewServer(cfg, driver)
	if err != nil {
		log.Fatal(err)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		log.Infof("Got signal [%d] to exit.", sig)
		svr.Close()
		os.Exit(0)
	}()

	log.Error(svr.Run())
}
