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

// +build !race

package tikv

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/kv"
)

// testIsolationSuite represents test isolation suite.
// The test suite takes too long under the race detector.
type testIsolationSuite struct {
	OneByOneSuite
	store *tikvStore
}

var _ = Suite(&testIsolationSuite{})

func (s *testIsolationSuite) SetUpSuite(c *C) {
	s.OneByOneSuite.SetUpSuite(c)
	s.store = NewTestStore(c).(*tikvStore)
}

func (s *testIsolationSuite) TearDownSuite(c *C) {
	s.store.Close()
	s.OneByOneSuite.TearDownSuite(c)
}

type writeRecord struct {
	startTS  uint64
	commitTS uint64
}

type writeRecords []writeRecord

func (r writeRecords) Len() int           { return len(r) }
func (r writeRecords) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r writeRecords) Less(i, j int) bool { return r[i].startTS <= r[j].startTS }

func (s *testIsolationSuite) SetWithRetry(c *C, k, v []byte) writeRecord {
	for {
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)

		err = txn.Set(k, v)
		c.Assert(err, IsNil)

		err = txn.Commit(context.Background())
		if err == nil {
			return writeRecord{
				startTS:  txn.StartTS(),
				commitTS: txn.(*tikvTxn).commitTS,
			}
		}
	}
}

type readRecord struct {
	startTS uint64
	value   []byte
}

type readRecords []readRecord

func (r readRecords) Len() int           { return len(r) }
func (r readRecords) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r readRecords) Less(i, j int) bool { return r[i].startTS <= r[j].startTS }

func (s *testIsolationSuite) GetWithRetry(c *C, k []byte) readRecord {
	for {
		txn, err := s.store.Begin()
		c.Assert(err, IsNil)

		val, err := txn.Get(context.TODO(), k)
		if err == nil {
			return readRecord{
				startTS: txn.StartTS(),
				value:   val,
			}
		}
		c.Assert(kv.IsTxnRetryableError(err), IsTrue)
	}
}

func (s *testIsolationSuite) TestWriteWriteConflict(c *C) {
	const (
		threadCount  = 10
		setPerThread = 50
	)
	var (
		mu     sync.Mutex
		writes []writeRecord
		wg     sync.WaitGroup
	)
	wg.Add(threadCount)
	for i := 0; i < threadCount; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < setPerThread; j++ {
				w := s.SetWithRetry(c, []byte("k"), []byte("v"))
				mu.Lock()
				writes = append(writes, w)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// Check all transactions' [startTS, commitTS] are not overlapped.
	sort.Sort(writeRecords(writes))
	for i := 0; i < len(writes)-1; i++ {
		c.Assert(writes[i].commitTS, Less, writes[i+1].startTS)
	}
}

func (s *testIsolationSuite) TestReadWriteConflict(c *C) {
	const (
		readThreadCount = 10
		writeCount      = 10
	)

	var (
		writes []writeRecord
		mu     sync.Mutex
		reads  []readRecord
		wg     sync.WaitGroup
	)

	s.SetWithRetry(c, []byte("k"), []byte("0"))

	writeDone := make(chan struct{})
	go func() {
		for i := 1; i <= writeCount; i++ {
			w := s.SetWithRetry(c, []byte("k"), []byte(fmt.Sprintf("%d", i)))
			writes = append(writes, w)
			time.Sleep(time.Microsecond * 10)
		}
		close(writeDone)
	}()

	wg.Add(readThreadCount)
	for i := 0; i < readThreadCount; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-writeDone:
					return
				default:
				}
				r := s.GetWithRetry(c, []byte("k"))
				mu.Lock()
				reads = append(reads, r)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	sort.Sort(readRecords(reads))

	// Check all reads got the value committed before it's startTS.
	var i, j int
	for ; i < len(writes); i++ {
		for ; j < len(reads); j++ {
			w, r := writes[i], reads[j]
			if r.startTS >= w.commitTS {
				break
			}
			c.Assert(string(r.value), Equals, fmt.Sprintf("%d", i))
		}
	}
	for ; j < len(reads); j++ {
		c.Assert(string(reads[j].value), Equals, fmt.Sprintf("%d", len(writes)))
	}
}
