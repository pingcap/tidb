// Copyright 2026 PingCAP, Inc.
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

package reorder

import (
	"fmt"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/chunk"
	"github.com/stretchr/testify/require"
)

func newIntChunk(vals ...int64) *chunk.Chunk {
	chk := chunk.New([]*types.FieldType{types.NewFieldType(mysql.TypeLonglong)}, len(vals), len(vals))
	for _, v := range vals {
		chk.AppendInt64(0, v)
	}
	return chk
}

func collectOutput(resultCh <-chan resultMsg) (rows []int64, err error) {
	for r := range resultCh {
		if r.err != nil {
			return nil, r.err
		}
		if r.chk == nil {
			return rows, nil
		}
		for i := range r.chk.NumRows() {
			rows = append(rows, r.chk.GetRow(i).GetInt64(0))
		}
	}
	return rows, nil
}

type resultMsg struct {
	chk *chunk.Chunk
	err error
}

// TestRunInOrder verifies that Run emits results in strict sequence order
// even when workers deliver them out of order.
func TestRunInOrder(t *testing.T) {
	inputCh := make(chan SeqResult[*chunk.Chunk], 10)
	resultCh := make(chan resultMsg, 10)
	freeChkCh := make(chan *chunk.Chunk, 2)
	paceCh := make(chan struct{}, 20)
	exit := make(chan struct{})

	ft := types.NewFieldType(mysql.TypeLonglong)
	for range 2 {
		chk := chunk.New([]*types.FieldType{ft}, 0, 8)
		freeChkCh <- chk
	}

	// Simulate producer acquiring pace tokens.
	for range 5 {
		paceCh <- struct{}{}
	}

	// Send results out of order: 2, 0, 4, 1, 3.
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 2, Val: newIntChunk(20)}
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 0, Val: newIntChunk(0)}
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 4, Val: newIntChunk(40)}
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 1, Val: newIntChunk(10)}
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 3, Val: newIntChunk(30)}
	close(inputCh)

	emitRows := func(appendRow AppendRow, val *chunk.Chunk) bool {
		if val != nil {
			for i := range val.NumRows() {
				if appendRow(val.GetRow(i)) {
					return true
				}
			}
		}
		return false
	}

	sendResult := func(chk *chunk.Chunk, err error) bool {
		resultCh <- resultMsg{chk: chk, err: err}
		return false
	}

	go func() {
		Run(inputCh, emitRows, sendResult, freeChkCh, paceCh, exit)
		close(resultCh)
	}()

	rows, err := collectOutput(resultCh)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 10, 20, 30, 40}, rows)
}

// TestRunError verifies that an error result is propagated to sendResult.
func TestRunError(t *testing.T) {
	inputCh := make(chan SeqResult[*chunk.Chunk], 5)
	resultCh := make(chan resultMsg, 5)
	freeChkCh := make(chan *chunk.Chunk, 2)
	paceCh := make(chan struct{}, 10)
	exit := make(chan struct{})

	ft := types.NewFieldType(mysql.TypeLonglong)
	for range 2 {
		freeChkCh <- chunk.New([]*types.FieldType{ft}, 0, 8)
	}
	for range 2 {
		paceCh <- struct{}{}
	}

	testErr := fmt.Errorf("test error")
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 0, Val: newIntChunk(0)}
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 1, Err: testErr}
	close(inputCh)

	emitRows := func(appendRow AppendRow, val *chunk.Chunk) bool {
		if val != nil {
			for i := range val.NumRows() {
				if appendRow(val.GetRow(i)) {
					return true
				}
			}
		}
		return false
	}

	sendResult := func(chk *chunk.Chunk, err error) bool {
		resultCh <- resultMsg{chk: chk, err: err}
		return false
	}

	go func() {
		Run(inputCh, emitRows, sendResult, freeChkCh, paceCh, exit)
		close(resultCh)
	}()

	// Should get the first row, then the error.
	var gotErr error
	for r := range resultCh {
		if r.err != nil {
			gotErr = r.err
			break
		}
	}
	require.ErrorIs(t, gotErr, testErr)
}

// TestRunExit verifies that closing the exit channel causes Run to return
// promptly without blocking.
func TestRunExit(t *testing.T) {
	inputCh := make(chan SeqResult[*chunk.Chunk], 5)
	resultCh := make(chan resultMsg, 5)
	freeChkCh := make(chan *chunk.Chunk, 2)
	paceCh := make(chan struct{}, 10)
	exit := make(chan struct{})

	ft := types.NewFieldType(mysql.TypeLonglong)
	for range 2 {
		freeChkCh <- chunk.New([]*types.FieldType{ft}, 0, 8)
	}

	done := make(chan struct{})
	emitRows := func(appendRow AppendRow, val *chunk.Chunk) bool {
		return false
	}
	sendResult := func(chk *chunk.Chunk, err error) bool {
		select {
		case resultCh <- resultMsg{chk: chk, err: err}:
			return false
		case <-exit:
			return true
		}
	}

	go func() {
		Run(inputCh, emitRows, sendResult, freeChkCh, paceCh, exit)
		close(done)
	}()

	// Close exit before sending any data — Run should return.
	close(exit)
	<-done
}

// TestRunMultiRowResult verifies that results with multiple rows (like
// parallel apply's multi-chunk results) are emitted correctly.
func TestRunMultiRowResult(t *testing.T) {
	inputCh := make(chan SeqResult[[]*chunk.Chunk], 5)
	resultCh := make(chan resultMsg, 10)
	freeChkCh := make(chan *chunk.Chunk, 3)
	paceCh := make(chan struct{}, 10)
	exit := make(chan struct{})

	ft := types.NewFieldType(mysql.TypeLonglong)
	for range 3 {
		freeChkCh <- chunk.New([]*types.FieldType{ft}, 0, 8)
	}
	for range 3 {
		paceCh <- struct{}{}
	}

	// Seq 0: 2 rows, Seq 1: 0 rows, Seq 2: 3 rows.
	inputCh <- SeqResult[[]*chunk.Chunk]{Seq: 1, Val: nil}
	inputCh <- SeqResult[[]*chunk.Chunk]{Seq: 0, Val: []*chunk.Chunk{newIntChunk(1, 2)}}
	inputCh <- SeqResult[[]*chunk.Chunk]{Seq: 2, Val: []*chunk.Chunk{newIntChunk(3, 4, 5)}}
	close(inputCh)

	emitRows := func(appendRow AppendRow, chks []*chunk.Chunk) bool {
		for _, chk := range chks {
			for i := range chk.NumRows() {
				if appendRow(chk.GetRow(i)) {
					return true
				}
			}
		}
		return false
	}

	sendResult := func(chk *chunk.Chunk, err error) bool {
		resultCh <- resultMsg{chk: chk, err: err}
		return false
	}

	go func() {
		Run(inputCh, emitRows, sendResult, freeChkCh, paceCh, exit)
		close(resultCh)
	}()

	rows, err := collectOutput(resultCh)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4, 5}, rows)
}

// TestRunEmptyInput verifies that Run handles an immediately-closed input
// channel (no results at all) and sends EOF.
func TestRunEmptyInput(t *testing.T) {
	inputCh := make(chan SeqResult[*chunk.Chunk])
	resultCh := make(chan resultMsg, 5)
	freeChkCh := make(chan *chunk.Chunk, 2)
	paceCh := make(chan struct{}, 10)
	exit := make(chan struct{})

	ft := types.NewFieldType(mysql.TypeLonglong)
	for range 2 {
		freeChkCh <- chunk.New([]*types.FieldType{ft}, 0, 8)
	}

	close(inputCh)

	emitRows := func(appendRow AppendRow, val *chunk.Chunk) bool {
		return false
	}

	sendResult := func(chk *chunk.Chunk, err error) bool {
		resultCh <- resultMsg{chk: chk, err: err}
		return false
	}

	go func() {
		Run(inputCh, emitRows, sendResult, freeChkCh, paceCh, exit)
		close(resultCh)
	}()

	rows, err := collectOutput(resultCh)
	require.NoError(t, err)
	require.Empty(t, rows)
}

// TestRunDuplicateSeq verifies that a duplicate sequence number is reported
// as an error.
func TestRunDuplicateSeq(t *testing.T) {
	inputCh := make(chan SeqResult[*chunk.Chunk], 5)
	resultCh := make(chan resultMsg, 5)
	freeChkCh := make(chan *chunk.Chunk, 2)
	paceCh := make(chan struct{}, 10)
	exit := make(chan struct{})

	ft := types.NewFieldType(mysql.TypeLonglong)
	for range 2 {
		freeChkCh <- chunk.New([]*types.FieldType{ft}, 0, 8)
	}
	for range 3 {
		paceCh <- struct{}{}
	}

	// Send seq 1 twice (duplicate) — seq 0 hasn't arrived yet so both
	// are buffered in pending.
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 1, Val: newIntChunk(10)}
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 1, Val: newIntChunk(10)} // dup
	close(inputCh)

	emitRows := func(appendRow AppendRow, val *chunk.Chunk) bool {
		return false
	}
	sendResult := func(chk *chunk.Chunk, err error) bool {
		resultCh <- resultMsg{chk: chk, err: err}
		return false
	}

	go func() {
		Run(inputCh, emitRows, sendResult, freeChkCh, paceCh, exit)
		close(resultCh)
	}()

	_, err := collectOutput(resultCh)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicate seq")
}

// TestRunSeqBelowNextSeq verifies that a sequence number already emitted
// is reported as an error.
func TestRunSeqBelowNextSeq(t *testing.T) {
	inputCh := make(chan SeqResult[*chunk.Chunk], 5)
	resultCh := make(chan resultMsg, 5)
	freeChkCh := make(chan *chunk.Chunk, 2)
	paceCh := make(chan struct{}, 10)
	exit := make(chan struct{})

	ft := types.NewFieldType(mysql.TypeLonglong)
	for range 2 {
		freeChkCh <- chunk.New([]*types.FieldType{ft}, 0, 8)
	}
	for range 3 {
		paceCh <- struct{}{}
	}

	// Send seq 0, then seq 0 again after it's been consumed.
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 0, Val: newIntChunk(0)}
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 0, Val: newIntChunk(0)} // below nextSeq
	close(inputCh)

	emitRows := func(appendRow AppendRow, val *chunk.Chunk) bool {
		if val != nil {
			for i := range val.NumRows() {
				if appendRow(val.GetRow(i)) {
					return true
				}
			}
		}
		return false
	}
	sendResult := func(chk *chunk.Chunk, err error) bool {
		resultCh <- resultMsg{chk: chk, err: err}
		return false
	}

	go func() {
		Run(inputCh, emitRows, sendResult, freeChkCh, paceCh, exit)
		close(resultCh)
	}()

	_, err := collectOutput(resultCh)
	require.Error(t, err)
	require.Contains(t, err.Error(), "below nextSeq")
}

// TestRunSequenceGapOnClose verifies that if inputCh is closed while
// there are pending results (a gap in the sequence), Run reports an error.
func TestRunSequenceGapOnClose(t *testing.T) {
	inputCh := make(chan SeqResult[*chunk.Chunk], 5)
	resultCh := make(chan resultMsg, 5)
	freeChkCh := make(chan *chunk.Chunk, 2)
	paceCh := make(chan struct{}, 10)
	exit := make(chan struct{})

	ft := types.NewFieldType(mysql.TypeLonglong)
	for range 2 {
		freeChkCh <- chunk.New([]*types.FieldType{ft}, 0, 8)
	}
	for range 2 {
		paceCh <- struct{}{}
	}

	// Send seq 0 and seq 2 but never seq 1 — creates a gap.
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 0, Val: newIntChunk(0)}
	inputCh <- SeqResult[*chunk.Chunk]{Seq: 2, Val: newIntChunk(20)}
	close(inputCh)

	emitRows := func(appendRow AppendRow, val *chunk.Chunk) bool {
		if val != nil {
			for i := range val.NumRows() {
				if appendRow(val.GetRow(i)) {
					return true
				}
			}
		}
		return false
	}
	sendResult := func(chk *chunk.Chunk, err error) bool {
		resultCh <- resultMsg{chk: chk, err: err}
		return false
	}

	go func() {
		Run(inputCh, emitRows, sendResult, freeChkCh, paceCh, exit)
		close(resultCh)
	}()

	_, err := collectOutput(resultCh)
	require.Error(t, err)
	require.Contains(t, err.Error(), "sequence gap")
}
