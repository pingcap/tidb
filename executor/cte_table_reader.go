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

package executor

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/cteutil"
)

// CTETableReaderExec scans data in iterInTbl, which is filled by corresponding CTEExec.
type CTETableReaderExec struct {
	baseExecutor

	iterInTbl cteutil.Storage
	chkIdx    int
	curIter   int
}

// Open implements the Executor interface.
func (e *CTETableReaderExec) Open(ctx context.Context) error {
	e.reset()
	return e.baseExecutor.Open(ctx)
}

// Next implements the Executor interface.
func (e *CTETableReaderExec) Next(ctx context.Context, req *chunk.Chunk) (err error) {
	req.Reset()

	// We should read `iterInTbl` from the beginning when the next iteration starts.
	// Can not directly judge whether to start the next iteration based on e.chkIdx,
	// because some operators(Selection) may use for loop to read all data in `iterInTbl`.
	if e.curIter != e.iterInTbl.GetIter() {
		if e.curIter > e.iterInTbl.GetIter() {
			return errors.Errorf("invalid iteration for CTETableReaderExec (e.curIter: %d, e.iterInTbl.GetIter(): %d)",
				e.curIter, e.iterInTbl.GetIter())
		}
		e.chkIdx = 0
		e.curIter = e.iterInTbl.GetIter()
	}
	if e.chkIdx < e.iterInTbl.NumChunks() {
		res, err := e.iterInTbl.GetChunk(e.chkIdx)
		if err != nil {
			return err
		}
		// Need to copy chunk to make sure upper operators will not change chunk in iterInTbl.
		req.SwapColumns(res.CopyConstructSel())
		e.chkIdx++
	}
	return nil
}

// Close implements the Executor interface.
func (e *CTETableReaderExec) Close() (err error) {
	e.reset()
	return e.baseExecutor.Close()
}

func (e *CTETableReaderExec) reset() {
	e.chkIdx = 0
	e.curIter = 0
}
