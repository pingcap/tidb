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
	flag.DurationVar(&testConfig.TestLength, "testLength", 20*time.Minute, "The full length of the test")
	flag.Uint64Var(&testConfig.MinDelay, "minDelay", 1000, "The minimum delay in the test RPCs")
	flag.Uint64Var(&testConfig.MaxDelay, "maxDelay", 10000, "The maximum delay in the test RPCs")
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
