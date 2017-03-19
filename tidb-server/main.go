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
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/ngaut/systimemon"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/perfschema"
	"github.com/pingcap/tidb/plan"
	"github.com/pingcap/tidb/privilege/privileges"
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
	version         = flag.Bool("V", false, "print version information and exit")
	store           = flag.String("store", "goleveldb", "registered store name, [memory, goleveldb, boltdb, tikv]")
	storePath       = flag.String("path", "/tmp/tidb", "tidb storage path")
	logLevel        = flag.String("L", "info", "log level: info, debug, warn, error, fatal")
	host            = flag.String("host", "0.0.0.0", "tidb server host")
	port            = flag.String("P", "4000", "tidb server port")
	statusPort      = flag.String("status", "10080", "tidb server status port")
	lease           = flag.String("lease", "1s", "schema lease duration, very dangerous to change only if you know what you do")
	socket          = flag.String("socket", "", "The socket file to use for connection.")
	enablePS        = flag.Bool("perfschema", false, "If enable performance schema.")
	enablePrivilege = flag.Bool("privilege", false, "If enable privilege check feature.")
	reportStatus    = flag.Bool("report-status", true, "If enable status report HTTP service.")
	logFile         = flag.String("log-file", "", "log file path")
	joinCon         = flag.Int("join-concurrency", 5, "the number of goroutines that participate joining.")
	crossJoin       = flag.Bool("cross-join", true, "whether support cartesian product or not.")
	metricsAddr     = flag.String("metrics-addr", "", "prometheus pushgateway address, leaves it empty will disable prometheus push.")
	metricsInterval = flag.Int("metrics-interval", 15, "prometheus client push interval in second, set \"0\" to disable prometheus push.")
	binlogSocket    = flag.String("binlog-socket", "", "socket file to write binlog")
	runDDL          = flag.Bool("run-ddl", true, "run ddl worker on this tidb-server")
	retryLimit      = flag.Int("retry-limit", 10, "the maximum number of retries when commit a transaction")

	timeJumpBackCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "tidb",
			Subsystem: "monitor",
			Name:      "time_jump_back_total",
			Help:      "Counter of system time jumps backward.",
		})
)

func main() {
	tidb.RegisterLocalStore("boltdb", boltdb.Driver{})
	tidb.RegisterStore("tikv", tikv.Driver{})

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	if *version {
		printer.PrintRawTiDBInfo()
		os.Exit(0)
	}

	leaseDuration := parseLease()
	tidb.SetSchemaLease(leaseDuration)
	ddl.RunWorker = *runDDL
	tidb.SetCommitRetryLimit(*retryLimit)

	cfg := &server.Config{
		Addr:         fmt.Sprintf("%s:%s", *host, *port),
		LogLevel:     *logLevel,
		StatusAddr:   fmt.Sprintf(":%s", *statusPort),
		Socket:       *socket,
		ReportStatus: *reportStatus,
		Store:        *store,
		StorePath:    *storePath,
	}

	// set log options
	if len(*logFile) > 0 {
		err := log.SetOutputByName(*logFile)
		if err != nil {
			log.Fatal(errors.ErrorStack(err))
		}
		log.SetRotateByDay()
		log.SetHighlighting(false)
	}

	if joinCon != nil && *joinCon > 0 {
		plan.JoinConcurrency = *joinCon
	}
	plan.AllowCartesianProduct = *crossJoin
	// Call this before setting log level to make sure that TiDB info could be printed.
	printer.PrintTiDBInfo()
	log.SetLevelByString(cfg.LogLevel)

	store := createStore()

	if *enablePS {
		perfschema.EnablePerfSchema()
	}
	privileges.Enable = *enablePrivilege
	if *binlogSocket != "" {
		createBinlogClient()
	}

	// Bootstrap a session to load information schema.
	_, err := tidb.BootstrapSession(store)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}

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

	prometheus.MustRegister(timeJumpBackCounter)
	go systimemon.StartMonitor(time.Now, func() {
		timeJumpBackCounter.Inc()
	})

	pushMetric(*metricsAddr, time.Duration(*metricsInterval)*time.Second)

	log.Error(svr.Run())
}

func createStore() kv.Storage {
	fullPath := fmt.Sprintf("%s://%s", *store, *storePath)
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
	log.Infof("created binlog client at %s", *binlogSocket)
}

// Prometheus push.
const zeroDuration = time.Duration(0)

// PushMetric pushs metircs in background.
func pushMetric(addr string, interval time.Duration) {
	if interval == zeroDuration || len(addr) == 0 {
		log.Info("disable Prometheus push client")
		return
	}
	log.Infof("start Prometheus push client with server addr %s and interval %s", addr, interval)
	go prometheusPushClient(addr, interval)
}

// PrometheusPushClient pushs metrics to Prometheus Pushgateway.
func prometheusPushClient(addr string, interval time.Duration) {
	// TODO: TiDB do not have uniq name, so we use host+port to compose a name.
	job := "tidb"
	for {
		err := push.AddFromGatherer(
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
