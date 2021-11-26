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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package deadlockhistory

import (
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/deadlock"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tipb/go-tipb"
	"github.com/stretchr/testify/assert"
	tikverr "github.com/tikv/client-go/v2/error"
)

func getAllDatum(d *DeadlockHistory, columns []*model.ColumnInfo) [][]types.Datum {
	records := d.GetAll()
	rowsCount := 0
	for _, rec := range records {
		rowsCount += len(rec.WaitChain)
	}
	rows := make([][]types.Datum, 0, rowsCount)
	for _, rec := range records {
		for waitChainIdx := range rec.WaitChain {
			row := make([]types.Datum, len(columns))
			for colIdx, column := range columns {
				row[colIdx] = rec.ToDatum(waitChainIdx, column.Name.O)
			}
			rows = append(rows, row)
		}
	}

	return rows

}

func TestDeadlockHistoryCollection(t *testing.T) {
	h := NewDeadlockHistory(1)
	assert.Equal(t, 0, len(h.GetAll()))
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 0, h.size)

	rec1 := &DeadlockRecord{
		OccurTime: time.Now(),
	}
	h.Push(rec1)
	res := h.GetAll()
	assert.Equal(t, 1, len(res))
	assert.Equal(t, rec1, res[0]) // Checking pointer equals is ok.
	assert.Equal(t, uint64(1), res[0].ID)
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 1, h.size)

	rec2 := &DeadlockRecord{
		OccurTime: time.Now(),
	}
	h.Push(rec2)
	res = h.GetAll()
	assert.Equal(t, 1, len(res))
	assert.Equal(t, rec2, res[0])
	assert.Equal(t, uint64(2), res[0].ID)
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 1, h.size)

	h.Clear()
	assert.Equal(t, 0, len(h.GetAll()))

	h = NewDeadlockHistory(3)
	rec1 = &DeadlockRecord{
		OccurTime: time.Now(),
	}
	h.Push(rec1)
	res = h.GetAll()
	assert.Equal(t, 1, len(res))
	assert.Equal(t, rec1, res[0]) // Checking pointer equals is ok.
	assert.Equal(t, uint64(1), res[0].ID)
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 1, h.size)

	rec2 = &DeadlockRecord{
		OccurTime: time.Now(),
	}
	h.Push(rec2)
	res = h.GetAll()
	assert.Equal(t, 2, len(res))
	assert.Equal(t, rec1, res[0])
	assert.Equal(t, uint64(1), res[0].ID)
	assert.Equal(t, rec2, res[1])
	assert.Equal(t, uint64(2), res[1].ID)
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 2, h.size)

	rec3 := &DeadlockRecord{
		OccurTime: time.Now(),
	}
	h.Push(rec3)
	res = h.GetAll()
	assert.Equal(t, 3, len(res))
	assert.Equal(t, rec1, res[0])
	assert.Equal(t, uint64(1), res[0].ID)
	assert.Equal(t, rec2, res[1])
	assert.Equal(t, uint64(2), res[1].ID)
	assert.Equal(t, rec3, res[2])
	assert.Equal(t, uint64(3), res[2].ID)
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 3, h.size)

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
		assert.Equal(t, 3, len(res))
		for idx, item := range res {
			assert.Equal(t, expectedItems[idx], item)
			assert.Equal(t, expectedIDs[idx], item.ID)
		}
		assert.Equal(t, expectedDequeHead, h.head)
		assert.Equal(t, 3, h.size)
	}

	h.Clear()
	assert.Equal(t, 0, len(h.GetAll()))
}

func TestGetDatum(t *testing.T) {
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

	dummyColumnInfo := []*model.ColumnInfo{
		{Name: model.NewCIStr(ColDeadlockIDStr)},
		{Name: model.NewCIStr(ColOccurTimeStr)},
		{Name: model.NewCIStr(ColRetryableStr)},
		{Name: model.NewCIStr(ColTryLockTrxIDStr)},
		{Name: model.NewCIStr(ColCurrentSQLDigestStr)},
		{Name: model.NewCIStr(ColCurrentSQLDigestTextStr)},
		{Name: model.NewCIStr(ColKeyStr)},
		{Name: model.NewCIStr(ColKeyInfoStr)},
		{Name: model.NewCIStr(ColTrxHoldingLockStr)},
	}
	res := getAllDatum(h, dummyColumnInfo)

	assert.Equal(t, 4, len(res))
	for _, row := range res {
		assert.Equal(t, 9, len(row))
	}

	toGoTime := func(d types.Datum) time.Time {
		v, ok := d.GetValue().(types.Time)
		assert.True(t, ok)
		tm, err := v.GoTime(time.UTC)
		assert.Nil(t, err)
		return tm
	}

	assert.Equal(t, uint64(1), res[0][0].GetValue())   // ID
	assert.Equal(t, time1, toGoTime(res[0][1]))        // OCCUR_TIME
	assert.Equal(t, int64(0), res[0][2].GetValue())    // RETRYABLE
	assert.Equal(t, uint64(101), res[0][3].GetValue()) // TRY_LOCK_TRX_ID
	assert.Equal(t, "sql1", res[0][4].GetValue())      // SQL_DIGEST
	assert.Equal(t, nil, res[0][5].GetValue())         // SQL_DIGEST_TEXT
	assert.Equal(t, "6B31", res[0][6].GetValue())      // KEY
	assert.Equal(t, uint64(102), res[0][8].GetValue()) // TRX_HOLDING_LOCK

	assert.Equal(t, uint64(1), res[1][0].GetValue())   // ID
	assert.Equal(t, time1, toGoTime(res[1][1]))        // OCCUR_TIME
	assert.Equal(t, int64(0), res[1][2].GetValue())    // RETRYABLE
	assert.Equal(t, uint64(102), res[1][3].GetValue()) // TRY_LOCK_TRX_ID
	assert.Equal(t, nil, res[1][4].GetValue())         // SQL_DIGEST
	assert.Equal(t, nil, res[1][5].GetValue())         // SQL_DIGEST_TEXT
	assert.Equal(t, nil, res[1][6].GetValue())         // KEY
	assert.Equal(t, uint64(101), res[1][8].GetValue()) // TRX_HOLDING_LOCK

	assert.Equal(t, uint64(2), res[2][0].GetValue())   // ID
	assert.Equal(t, time2, toGoTime(res[2][1]))        // OCCUR_TIME
	assert.Equal(t, int64(1), res[2][2].GetValue())    // RETRYABLE
	assert.Equal(t, uint64(201), res[2][3].GetValue()) // TRY_LOCK_TRX_ID
	assert.Equal(t, uint64(202), res[2][8].GetValue()) // TRX_HOLDING_LOCK

	assert.Equal(t, uint64(2), res[3][0].GetValue())   // ID
	assert.Equal(t, time2, toGoTime(res[3][1]))        // OCCUR_TIME
	assert.Equal(t, int64(1), res[3][2].GetValue())    // RETRYABLE
	assert.Equal(t, uint64(202), res[3][3].GetValue()) // TRY_LOCK_TRX_ID
	assert.Equal(t, uint64(201), res[3][8].GetValue()) // TRX_HOLDING_LOCK
}

func TestErrDeadlockToDeadlockRecord(t *testing.T) {
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
	assert.Less(t, time.Since(record.OccurTime), time.Millisecond*5)
	expectedRecord.OccurTime = record.OccurTime
	assert.Equal(t, expectedRecord, record)
}

func dummyRecord() *DeadlockRecord {
	return &DeadlockRecord{}
}

func TestResize(t *testing.T) {
	h := NewDeadlockHistory(2)
	h.Push(dummyRecord()) // id=1 inserted
	h.Push(dummyRecord()) // id=2 inserted,
	h.Push(dummyRecord()) // id=3 inserted, id=1 is removed
	assert.Equal(t, 1, h.head)
	assert.Equal(t, 2, h.size)
	assert.Equal(t, 2, len(h.GetAll()))
	assert.Equal(t, uint64(2), h.GetAll()[0].ID)
	assert.Equal(t, uint64(3), h.GetAll()[1].ID)

	h.Resize(3)
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 2, h.size)
	h.Push(dummyRecord()) // id=4 inserted
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 3, h.size)
	assert.Equal(t, 3, len(h.GetAll()))
	assert.Equal(t, uint64(2), h.GetAll()[0].ID)
	assert.Equal(t, uint64(3), h.GetAll()[1].ID)
	assert.Equal(t, uint64(4), h.GetAll()[2].ID)

	h.Resize(2) // id=2 removed
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 2, h.size)
	assert.Equal(t, 2, len(h.GetAll()))
	assert.Equal(t, uint64(3), h.GetAll()[0].ID)
	assert.Equal(t, uint64(4), h.GetAll()[1].ID)

	h.Resize(0) // all removed
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 0, h.size)
	assert.Equal(t, 0, len(h.GetAll()))

	h.Resize(2)
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 0, h.size)
	h.Push(dummyRecord()) // id=5 inserted
	assert.Equal(t, 0, h.head)
	assert.Equal(t, 1, h.size)
}
