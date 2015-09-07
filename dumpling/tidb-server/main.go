package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/tidb-server/server"
)

var (
	store     = flag.String("store", "goleveldb", "registered store name, [memory, goleveldb, boltdb]")
	storePath = flag.String("store_path", "/tmp/tidb", "tidb storage path")
	logLevel  = flag.String("L", "debug", "log level: info, debug, warn, error, fatal")
	port      = flag.String("P", "4000", "mp server port")
)

//version infomation
var (
	buildstamp = "No Build Stamp Provided"
	githash    = "No Git Hash Provided"
)

func main() {
	fmt.Printf("Git Commit Hash:%s\nUTC Build Time :%s\n", githash, buildstamp)
	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()

	cfg := &server.Config{
		Addr:     fmt.Sprintf(":%s", *port),
		User:     "root",
		Password: "",
		LogLevel: *logLevel,
	}

	log.SetLevelByString(cfg.LogLevel)
	store, err := tidb.NewStore(fmt.Sprintf("%s://%s", *store, *storePath))
	if err != nil {
		log.Error(err.Error())
		return
	}
	server.CreateTiDBTestDatabase(store)
	var svr *server.Server
	var driver server.IDriver
	driver = server.NewTiDBDriver(store)
	svr, err = server.NewServer(cfg, driver)
	if err != nil {
		log.Error(err.Error())
		return
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
