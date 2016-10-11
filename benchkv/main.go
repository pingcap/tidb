// Copyright 2016 PingCAP, Inc.
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
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	store     kv.Storage
	dataCnt   = flag.Int("N", 1000000, "data num")
	workerCnt = flag.Int("C", 400, "concurrent num")
	pdAddr    = flag.String("pd", "localhost:2379", "pd address:localhost:2379")
	valueSize = flag.Int("V", 5, "value size in byte")

	txnCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tikv",
			Subsystem: "txn",
			Name:      "total",
			Help:      "Counter of txns.",
		}, []string{"type"})

	txnRolledbackCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "tikv",
			Subsystem: "txn",
			Name:      "failed_total",
			Help:      "Counter of rolled back txns.",
		}, []string{"type"})

	txnDurations = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "tikv",
			Subsystem: "txn",
			Name:      "durations_histogram_seconds",
			Help:      "Txn latency distributions.",
			Buckets:   prometheus.ExponentialBuckets(0.0005, 2, 13),
		}, []string{"type"})
)

// Init initializes informations.
func Init() {
	driver := tikv.Driver{}
	var err error
	store, err = driver.Open(fmt.Sprintf("tikv://%s?cluster=1", *pdAddr))
	if err != nil {
		log.Fatal(err)
	}

	prometheus.MustRegister(txnCounter)
	prometheus.MustRegister(txnRolledbackCounter)
	prometheus.MustRegister(txnDurations)
	http.Handle("/metrics", prometheus.Handler())

	go http.ListenAndServe(":9191", nil)
}

// without conflict
func batchRW(value []byte) {
	wg := sync.WaitGroup{}
	base := *dataCnt / *workerCnt
	wg.Add(*workerCnt)
	for i := 0; i < *workerCnt; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < base; j++ {
				txnCounter.WithLabelValues("txn").Inc()
				start := time.Now()
				k := base*i + j
				txn, err := store.Begin()
				if err != nil {
					log.Fatal(err)
				}
				key := fmt.Sprintf("key_%d", k)
				txn.Set([]byte(key), value)
				err = txn.Commit()
				if err != nil {
					txnRolledbackCounter.WithLabelValues("txn").Inc()
					txn.Rollback()
				}

				txnDurations.WithLabelValues("txn").Observe(time.Since(start).Seconds())
			}
		}(i)
	}
	wg.Wait()
}

func main() {
	flag.Parse()
	log.SetLevelByString("error")
	Init()

	value := make([]byte, *valueSize)
	t := time.Now()
	batchRW(value)
	resp, err := http.Get("http://localhost:9191/metrics")
	if err != nil {
		log.Fatal(err)
	}

	defer resp.Body.Close()
	text, err1 := ioutil.ReadAll(resp.Body)
	if err1 != nil {
		log.Fatal(err)
	}

	fmt.Println(string(text))

	fmt.Printf("\nelapse:%v, total %v\n", time.Since(t), *dataCnt)
}
