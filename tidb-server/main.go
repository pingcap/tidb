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
	"net"
	"net/url"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/systimemon"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/perfschema"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/server"
	"github.com/pingcap/tidb/sessionctx/binloginfo"
	"github.com/pingcap/tidb/store/localstore/boltdb"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/util/printer"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/push"
	"google.golang.org/grpc"
)

var (
	store           = flag.String("store", "goleveldb", "registered store name, [memory, goleveldb, boltdb, tikv]")
	storePath       = flag.String("path", "/tmp/tidb", "tidb storage path")
	logLevel        = flag.String("L", "info", "log level: info, debug, warn, error, fatal")
	host            = flag.String("host", "0.0.0.0", "tidb server host")
	port            = flag.String("P", "4000", "tidb server port")
	statusPort      = flag.String("status", "10080", "tidb server status port")
	lease           = flag.String("lease", "1s", "schema lease duration, very dangerous to change only if you know what you do")
	socket          = flag.String("socket", "", "The socket file to use for connection.")
	enablePS        = flag.Bool("perfschema", false, "If enable performance schema.")
	reportStatus    = flag.Bool("report-status", true, "If enable status report HTTP service.")
	logFile         = flag.String("log-file", "", "log file path")
	joinCon         = flag.Int("join-concurrency", 5, "the number of goroutines that participate joining.")
	metricsAddr     = flag.String("metrics-addr", "", "prometheus pushgateway address, leaves it empty will disable prometheus push.")
	metricsInterval = flag.Int("metrics-interval", 15, "prometheus client push interval in second, set \"0\" to disable prometheus push.")
	binlogSocket    = flag.String("binlog-socket", "", "socket file to write binlog")
)

func main() {
	tidb.RegisterLocalStore("boltdb", boltdb.Driver{})
	tidb.RegisterStore("tikv", tikv.Driver{})

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	leaseDuration := parseLease()
	tidb.SetSchemaLease(leaseDuration)

	cfg := &server.Config{
		Addr:         fmt.Sprintf("%s:%s", *host, *port),
		LogLevel:     *logLevel,
		StatusAddr:   fmt.Sprintf(":%s", *statusPort),
		Socket:       *socket,
		ReportStatus: *reportStatus,
	}

	// set log options
	if len(*logFile) > 0 {
		err := log.SetOutputByName(*logFile)
		if err != nil {
			log.Fatal(errors.ErrorStack(err))
		}
		log.SetRotateByDay()
	}

	if joinCon != nil && *joinCon > 0 {
		plan.JoinConcurrency = *joinCon
	}
	// Call this before setting log level to make sure that TiDB info could be printed.
	printer.PrintTiDBInfo()
	log.SetLevelByString(cfg.LogLevel)

	store := createStore()

	if *enablePS {
		perfschema.EnablePerfSchema()
	}
	if *binlogSocket != "" {
		createBinlogClient()
	}

	// Create a session to load information schema.
	se, err := tidb.CreateSession(store)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	se.Close()

	var driver server.IDriver
	driver = server.NewTiDBDriver(store)
	var svr *server.Server
	svr, err = server.NewServer(cfg, driver)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
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

	go systimemon.StartMonitor(time.Now, func() {
		log.Error("error: system time jump backward")
	})

	pushMetric(*metricsAddr, time.Duration(*metricsInterval)*time.Second)

	log.Error(svr.Run())
}

func createStore() kv.Storage {
	fullPath := fmt.Sprintf("%s://%s", *store, *storePath)
	u, err := url.Parse(fullPath)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	if cluster := u.Query().Get("cluster"); cluster != "" {
		id, err1 := strconv.ParseUint(cluster, 10, 64)
		if err1 != nil {
			log.Fatal(errors.ErrorStack(err1))
		}
		binloginfo.ClusterID = id
	}
	store, err := tidb.NewStore(fullPath)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	return store
}

func createBinlogClient() {
	dialerOpt := grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("unix", addr, timeout)
	})
	clientCon, err := grpc.Dial(*binlogSocket, dialerOpt, grpc.WithInsecure())
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	binloginfo.PumpClient = binlog.NewPumpClient(clientCon)
}

// Prometheus push.
const zeroDuration = time.Duration(0)

// PushMetric pushs metircs in background.
func pushMetric(addr string, interval time.Duration) {
	if interval == zeroDuration || len(addr) == 0 {
		log.Info("disable Prometheus push client")
		return
	}
	log.Infof("start Prometheus push client with server addr %s and interval %d", addr, interval)
	go prometheusPushClient(addr, interval)
}

// PrometheusPushClient pushs metrics to Prometheus Pushgateway.
func prometheusPushClient(addr string, interval time.Duration) {
	// TODO: TiDB do not have uniq name, so we use host+port to compose a name.
	job := "tidb"
	for {
		err := push.FromGatherer(
			job, push.HostnameGroupingKey(),
			addr,
			prometheus.DefaultGatherer,
		)
		if err != nil {
			log.Errorf("could not push metrics to Prometheus Pushgateway: %v", err)
		}
		time.Sleep(interval)
	}
}

// parseLease parses lease argument string.
func parseLease() time.Duration {
	dur, err := time.ParseDuration(*lease)
	if err != nil {
		dur, err = time.ParseDuration(*lease + "s")
	}
	if err != nil || dur < 0 {
		log.Fatalf("invalid lease duration %s", *lease)
	}
	return dur
}
