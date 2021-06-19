// Copyright 2021 PingCAP, Inc.
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

package deadlockhistory

import (
	"testing"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/parser"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
	tikverr "github.com/tikv/client-go/v2/error"
)

type testDeadlockHistorySuite struct{}

var _ = Suite(&testDeadlockHistorySuite{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (s *testDeadlockHistorySuite) TestDeadlockHistoryCollection(c *C) {
	h := NewDeadlockHistory(1)
	c.Assert(len(h.GetAll()), Equals, 0)
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 0)

	rec1 := &DeadlockRecord{
		OccurTime: time.Now(),
	}
	h.Push(rec1)
	res := h.GetAll()
	c.Assert(len(res), Equals, 1)
	c.Assert(res[0], Equals, rec1) // Checking pointer equals is ok.
	c.Assert(res[0].ID, Equals, uint64(1))
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 1)

	rec2 := &DeadlockRecord{
		OccurTime: time.Now(),
	}
	h.Push(rec2)
	res = h.GetAll()
	c.Assert(len(res), Equals, 1)
	c.Assert(res[0], Equals, rec2)
	c.Assert(res[0].ID, Equals, uint64(2))
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 1)

	h.Clear()
	c.Assert(len(h.GetAll()), Equals, 0)

	h = NewDeadlockHistory(3)
	rec1 = &DeadlockRecord{
		OccurTime: time.Now(),
	}
	h.Push(rec1)
	res = h.GetAll()
	c.Assert(len(res), Equals, 1)
	c.Assert(res[0], Equals, rec1) // Checking pointer equals is ok.
	c.Assert(res[0].ID, Equals, uint64(1))
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 1)

	rec2 = &DeadlockRecord{
		OccurTime: time.Now(),
	}
	h.Push(rec2)
	res = h.GetAll()
	c.Assert(len(res), Equals, 2)
	c.Assert(res[0], Equals, rec1)
	c.Assert(res[0].ID, Equals, uint64(1))
	c.Assert(res[1], Equals, rec2)
	c.Assert(res[1].ID, Equals, uint64(2))
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 2)

	rec3 := &DeadlockRecord{
		OccurTime: time.Now(),
	}
	h.Push(rec3)
	res = h.GetAll()
	c.Assert(len(res), Equals, 3)
	c.Assert(res[0], Equals, rec1)
	c.Assert(res[0].ID, Equals, uint64(1))
	c.Assert(res[1], Equals, rec2)
	c.Assert(res[1].ID, Equals, uint64(2))
	c.Assert(res[2], Equals, rec3)
	c.Assert(res[2].ID, Equals, uint64(3))
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 3)

	// Continuously pushing items to check the correctness of the deque
	expectedItems := []*DeadlockRecord{rec1, rec2, rec3}
	expectedIDs := []uint64{1, 2, 3}
	expectedDequeHead := 0
	for i := 0; i < 6; i++ {
		newRec := &DeadlockRecord{
			OccurTime: time.Now(),
		}
		h.Push(newRec)

		expectedItems = append(expectedItems[1:], newRec)
		for idx := range expectedIDs {
			expectedIDs[idx]++
		}
		expectedDequeHead = (expectedDequeHead + 1) % 3

		res = h.GetAll()
		c.Assert(len(res), Equals, 3)
		for idx, item := range res {
			c.Assert(item, Equals, expectedItems[idx])
			c.Assert(item.ID, Equals, expectedIDs[idx])
		}
		c.Assert(h.head, Equals, expectedDequeHead)
		c.Assert(h.size, Equals, 3)
	}

	h.Clear()
	c.Assert(len(h.GetAll()), Equals, 0)
}

func (s *testDeadlockHistorySuite) TestGetDatum(c *C) {
	time1 := time.Date(2021, 05, 14, 15, 28, 30, 123456000, time.UTC)
	time2 := time.Date(2022, 06, 15, 16, 29, 31, 123457000, time.UTC)

	h := NewDeadlockHistory(10)
	h.Push(&DeadlockRecord{
		OccurTime:   time1,
		IsRetryable: false,
		WaitChain: []WaitChainItem{
			{
				TryLockTxn:     101,
				SQLDigest:      "sql1",
				Key:            []byte("k1"),
				AllSQLDigests:  []string{"sql1", "sql2"},
				TxnHoldingLock: 102,
			},
			// It should work even some information are missing.
			{
				TryLockTxn:     102,
				TxnHoldingLock: 101,
			},
		},
	})
	h.Push(&DeadlockRecord{
		OccurTime:   time2,
		IsRetryable: true,
		WaitChain: []WaitChainItem{
			{
				TryLockTxn:     201,
				AllSQLDigests:  []string{},
				TxnHoldingLock: 202,
			},
			{
				TryLockTxn:     202,
				AllSQLDigests:  []string{"sql1"},
				TxnHoldingLock: 201,
			},
		},
	})
	// A deadlock error without wait chain shows nothing in the query result.
	h.Push(&DeadlockRecord{
		OccurTime:   time.Now(),
		IsRetryable: false,
		WaitChain:   nil,
	})

	res := h.GetAllDatum()
	c.Assert(len(res), Equals, 4)
	for _, row := range res {
		c.Assert(len(row), Equals, 7)
	}

	toGoTime := func(d types.Datum) time.Time {
		v, ok := d.GetValue().(types.Time)
		c.Assert(ok, IsTrue)
		t, err := v.GoTime(time.UTC)
		c.Assert(err, IsNil)
		return t
	}

	c.Assert(res[0][0].GetValue(), Equals, uint64(1))   // ID
	c.Assert(toGoTime(res[0][1]), Equals, time1)        // OCCUR_TIME
	c.Assert(res[0][2].GetValue(), Equals, int64(0))    // RETRYABLE
	c.Assert(res[0][3].GetValue(), Equals, uint64(101)) // TRY_LOCK_TRX_ID
	c.Assert(res[0][4].GetValue(), Equals, "sql1")      // SQL_DIGEST
	c.Assert(res[0][5].GetValue(), Equals, "6B31")      // KEY
	c.Assert(res[0][6].GetValue(), Equals, uint64(102)) // TRX_HOLDING_LOCK

	c.Assert(res[1][0].GetValue(), Equals, uint64(1))   // ID
	c.Assert(toGoTime(res[1][1]), Equals, time1)        // OCCUR_TIME
	c.Assert(res[1][2].GetValue(), Equals, int64(0))    // RETRYABLE
	c.Assert(res[1][3].GetValue(), Equals, uint64(102)) // TRY_LOCK_TRX_ID
	c.Assert(res[1][4].GetValue(), Equals, nil)         // SQL_DIGEST
	c.Assert(res[1][5].GetValue(), Equals, nil)         // KEY
	c.Assert(res[1][6].GetValue(), Equals, uint64(101)) // TRX_HOLDING_LOCK

	c.Assert(res[2][0].GetValue(), Equals, uint64(2))   // ID
	c.Assert(toGoTime(res[2][1]), Equals, time2)        // OCCUR_TIME
	c.Assert(res[2][2].GetValue(), Equals, int64(1))    // RETRYABLE
	c.Assert(res[2][3].GetValue(), Equals, uint64(201)) // TRY_LOCK_TRX_ID
	c.Assert(res[2][6].GetValue(), Equals, uint64(202)) // TRX_HOLDING_LOCK

	c.Assert(res[3][0].GetValue(), Equals, uint64(2))   // ID
	c.Assert(toGoTime(res[3][1]), Equals, time2)        // OCCUR_TIME
	c.Assert(res[3][2].GetValue(), Equals, int64(1))    // RETRYABLE
	c.Assert(res[3][3].GetValue(), Equals, uint64(202)) // TRY_LOCK_TRX_ID
	c.Assert(res[3][6].GetValue(), Equals, uint64(201)) // TRX_HOLDING_LOCK
}

func (s *testDeadlockHistorySuite) TestErrDeadlockToDeadlockRecord(c *C) {
	digest1, digest2 := parser.NewDigest([]byte("aabbccdd")), parser.NewDigest([]byte("ddccbbaa"))
	tag1 := tipb.ResourceGroupTag{SqlDigest: digest1.Bytes()}
	tag2 := tipb.ResourceGroupTag{SqlDigest: digest2.Bytes()}
	tag1Data, _ := tag1.Marshal()
	tag2Data, _ := tag2.Marshal()
	err := &tikverr.ErrDeadlock{
		Deadlock: &kvrpcpb.Deadlock{
			LockTs:          101,
			LockKey:         []byte("k1"),
			DeadlockKeyHash: 1234567,
			WaitChain: []*deadlock.WaitForEntry{
				{
					Txn:              100,
					WaitForTxn:       101,
					Key:              []byte("k2"),
					ResourceGroupTag: tag1Data,
				},
				{
					Txn:              101,
					WaitForTxn:       100,
					Key:              []byte("k1"),
					ResourceGroupTag: tag2Data,
				},
			},
		},
		IsRetryable: true,
	}

	expectedRecord := &DeadlockRecord{
		IsRetryable: true,
		WaitChain: []WaitChainItem{
			{
				TryLockTxn:     100,
				SQLDigest:      digest1.String(),
				Key:            []byte("k2"),
				TxnHoldingLock: 101,
			},
			{
				TryLockTxn:     101,
				SQLDigest:      digest2.String(),
				Key:            []byte("k1"),
				TxnHoldingLock: 100,
			},
		},
	}

	record := ErrDeadlockToDeadlockRecord(err)
	// The OccurTime is set to time.Now
	c.Assert(time.Since(record.OccurTime), Less, time.Millisecond*5)
	expectedRecord.OccurTime = record.OccurTime
	c.Assert(record, DeepEquals, expectedRecord)
}

func dummyRecord() *DeadlockRecord {
	return &DeadlockRecord{}
}

func (s *testDeadlockHistorySuite) TestResize(c *C) {
	h := NewDeadlockHistory(2)
	h.Push(dummyRecord()) // id=1 inserted
	h.Push(dummyRecord()) // id=2 inserted,
	h.Push(dummyRecord()) // id=3 inserted, id=1 is removed
	c.Assert(h.head, Equals, 1)
	c.Assert(h.size, Equals, 2)
	c.Assert(len(h.GetAll()), Equals, 2)
	c.Assert(h.GetAll()[0].ID, Equals, uint64(2))
	c.Assert(h.GetAll()[1].ID, Equals, uint64(3))

	h.Resize(3)
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 2)
	h.Push(dummyRecord()) // id=4 inserted
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 3)
	c.Assert(len(h.GetAll()), Equals, 3)
	c.Assert(h.GetAll()[0].ID, Equals, uint64(2))
	c.Assert(h.GetAll()[1].ID, Equals, uint64(3))
	c.Assert(h.GetAll()[2].ID, Equals, uint64(4))

	h.Resize(2) // id=2 removed
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 2)
	c.Assert(len(h.GetAll()), Equals, 2)
	c.Assert(h.GetAll()[0].ID, Equals, uint64(3))
	c.Assert(h.GetAll()[1].ID, Equals, uint64(4))

	h.Resize(0) // all removed
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 0)
	c.Assert(len(h.GetAll()), Equals, 0)

	h.Resize(2)
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 0)
	h.Push(dummyRecord()) // id=5 inserted
	c.Assert(h.head, Equals, 0)
	c.Assert(h.size, Equals, 1)
}
