package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

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

	go http.ListenAndServe(":9191", prometheus.Handler())
}

// without conflict
func batchRW() {
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
				txn.Set([]byte(key), []byte("value"))
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

	t := time.Now()
	batchRW()
	resp, err := http.Get("http://localhost:9191/")
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
