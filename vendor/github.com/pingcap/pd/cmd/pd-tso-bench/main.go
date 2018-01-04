// Copyright 2017 PingCAP, Inc.
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
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/pd/pd-client"
)

var (
	pdAddrs     = flag.String("pd", "127.0.0.1:2379", "pd address")
	concurrency = flag.Int("C", 1000, "concurrency")
	sleep       = flag.Duration("sleep", time.Millisecond, "sleep time after a request, used to adjust pressure")
	interval    = flag.Duration("interval", time.Second, "interval to output the statistics")
	caPath      = flag.String("cacert", "", "path of file that contains list of trusted SSL CAs.")
	certPath    = flag.String("cert", "", "path of file that contains X509 certificate in PEM format..")
	keyPath     = flag.String("key", "", "path of file that contains X509 key in PEM format.")
	wg          sync.WaitGroup
)

func main() {
	flag.Parse()

	pdCli, err := pd.NewClient([]string{*pdAddrs}, pd.SecurityOption{
		CAPath:   *caPath,
		CertPath: *certPath,
		KeyPath:  *keyPath,
	})
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// To avoid the first time high latency.
	for i := 0; i < *concurrency; i++ {
		_, _, err = pdCli.GetTS(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}

	durCh := make(chan time.Duration, *concurrency*2)

	wg.Add(*concurrency)
	for i := 0; i < *concurrency; i++ {
		go reqWorker(ctx, pdCli, durCh)
	}

	wg.Add(1)
	go showStats(ctx, durCh)

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		<-sc
		cancel()
	}()

	wg.Wait()

	pdCli.Close()
}

func showStats(ctx context.Context, durCh chan time.Duration) {
	defer wg.Done()

	statCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	ticker := time.NewTicker(*interval)

	s := newStats()
	total := newStats()

	for {
		select {
		case <-ticker.C:
			println(s.String())
			total.merge(s)
			s = newStats()
		case d := <-durCh:
			s.update(d)
		case <-statCtx.Done():
			println("\nTotal:")
			println(total.String())
			return
		}
	}
}

const (
	twoDur    = time.Millisecond * 2
	fiveDur   = time.Millisecond * 5
	tenDur    = time.Millisecond * 10
	thirtyDur = time.Millisecond * 30
)

type stats struct {
	maxDur       time.Duration
	minDur       time.Duration
	count        int
	milliCnt     int
	twoMilliCnt  int
	fiveMilliCnt int
	tenMSCnt     int
	thirtyCnt    int
}

func newStats() *stats {
	return &stats{
		minDur: time.Hour,
		maxDur: 0,
	}
}

func (s *stats) update(dur time.Duration) {
	s.count++

	if dur > s.maxDur {
		s.maxDur = dur
	}
	if dur < s.minDur {
		s.minDur = dur
	}

	if dur > thirtyDur {
		s.thirtyCnt++
		return
	}

	if dur > tenDur {
		s.tenMSCnt++
		return
	}

	if dur > fiveDur {
		s.fiveMilliCnt++
		return
	}

	if dur > twoDur {
		s.twoMilliCnt++
		return
	}

	if dur > time.Millisecond {
		s.milliCnt++
		return
	}
}

func (s *stats) merge(other *stats) {
	if s.maxDur < other.maxDur {
		s.maxDur = other.maxDur
	}
	if s.minDur > other.minDur {
		s.minDur = other.minDur
	}

	s.count += other.count
	s.milliCnt += other.milliCnt
	s.twoMilliCnt += other.twoMilliCnt
	s.fiveMilliCnt += other.fiveMilliCnt
	s.tenMSCnt += other.tenMSCnt
	s.thirtyCnt += other.thirtyCnt
}

func (s *stats) String() string {
	return fmt.Sprintf("count:%d, max:%d, min:%d, >1ms:%d, >2ms:%d, >5ms:%d, >10ms:%d, >30ms:%d",
		s.count, s.maxDur.Nanoseconds()/int64(time.Millisecond), s.minDur.Nanoseconds()/int64(time.Millisecond),
		s.milliCnt, s.twoMilliCnt, s.fiveMilliCnt, s.tenMSCnt, s.thirtyCnt)
}

func reqWorker(ctx context.Context, pdCli pd.Client, durCh chan time.Duration) {
	defer wg.Done()

	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		start := time.Now()
		_, _, err := pdCli.GetTS(reqCtx)
		if errors.Cause(err) == context.Canceled {
			return
		}

		if err != nil {
			log.Fatal(err)
		}
		dur := time.Since(start)

		select {
		case <-reqCtx.Done():
			return
		case durCh <- dur:
		}
	}
}
