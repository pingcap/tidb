package main

import (
	"flag"
	"os"
	"time"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv"
)

var testConfig tikv.BatchClientTestConfig

func init() {
	flag.Uint64Var(&testConfig.Concurrent, "concurrent", 128, "The number of test RPC loops per tikv instance")
	flag.DurationVar(&testConfig.Timeout, "timeout", 10*time.Second, "The fail timeout")
	flag.DurationVar(&testConfig.TestLength, "time", 20*time.Minute, "The full running time of the test")
	flag.Uint64Var(&testConfig.MinDelay, "minDelay", 10, "The minimum delay in the test RPCs, in milliseconds")
	flag.Uint64Var(&testConfig.MaxDelay, "maxDelay", 1000, "The maximum delay in the test RPCs, in milliseconds")
}

func main() {
	flag.Parse()
	result := tikv.BatchTest(config.Security{}, flag.Args(), testConfig)
	var exit int
	if result {
		exit = 255
	}
	os.Exit(exit)
}
