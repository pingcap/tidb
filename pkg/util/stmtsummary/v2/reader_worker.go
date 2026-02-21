// Copyright 2023 PingCAP, Inc.
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

package stmtsummary

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"math"
	"os"
	"time"

	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util"
)

type stmtScanWorker struct {
	ctx       context.Context
	batchSize int
	checker   *stmtChecker
}

func (w *stmtScanWorker) run(
	fileCh <-chan *os.File,
	linesCh chan<- [][]byte,
	errCh chan<- error,
) {
	for {
		select {
		case file, ok := <-fileCh:
			if !ok {
				return
			}
			w.handleFile(file, linesCh, errCh)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *stmtScanWorker) handleFile(
	file *os.File,
	linesCh chan<- [][]byte,
	errCh chan<- error,
) {
	if file == nil {
		return
	}

	reader := bufio.NewReader(file)
	for {
		if isCtxDone(w.ctx) {
			return
		}

		lines, err := w.readlines(reader)
		if err == io.EOF {
			return
		}
		if err != nil {
			w.putErr(err, errCh)
			return
		}

		w.putLines(lines, linesCh)
	}
}

func (w *stmtScanWorker) putErr(
	err error,
	errCh chan<- error,
) {
	select {
	case errCh <- err:
	case <-w.ctx.Done():
	}
}

func (w *stmtScanWorker) putLines(
	lines [][]byte,
	linesCh chan<- [][]byte,
) {
	select {
	case linesCh <- lines:
	case <-w.ctx.Done():
	}
}

func (w *stmtScanWorker) readlines(reader *bufio.Reader) ([][]byte, error) {
	var firstLine []byte
	var record *stmtTinyRecord
	for { // ingore invalid lines
		var err error
		firstLine, err = readLine(reader)
		if err != nil {
			return nil, err
		}

		record, err = w.parse(firstLine)
		if err == nil {
			break
		}
	}

	if w.needStop(record) {
		// done because remaining lines in file
		// are not in the time range
		return nil, io.EOF
	}

	lines := make([][]byte, 0, w.batchSize)
	lines = append(lines, firstLine)

	newLines, err := readLines(reader, w.batchSize-1)
	if err == io.EOF {
		return lines, nil
	}
	if err != nil {
		return nil, err
	}

	lines = append(lines, newLines...)
	return lines, nil
}

func (*stmtScanWorker) parse(raw []byte) (*stmtTinyRecord, error) {
	var record stmtTinyRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (w *stmtScanWorker) needStop(record *stmtTinyRecord) bool {
	return w.checker.needStop(record.Begin)
}

type stmtParseWorker struct {
	ctx             context.Context
	instanceAddr    string
	timeLocation    *time.Location
	checker         *stmtChecker
	columnFactories []columnFactory
}

func (w *stmtParseWorker) run(
	linesCh <-chan [][]byte,
	rowsCh chan<- [][]types.Datum,
	errCh chan<- error,
) {
	for {
		select {
		case lines, ok := <-linesCh:
			if !ok {
				return
			}
			w.handleLines(lines, rowsCh, errCh)
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *stmtParseWorker) handleLines(
	lines [][]byte,
	rowsCh chan<- [][]types.Datum,
	_ chan<- error,
) {
	if len(lines) == 0 {
		return
	}

	rows := make([][]types.Datum, 0, len(lines))
	for _, line := range lines {
		record, err := w.parse(line)
		if err != nil {
			// ignore invalid lines
			continue
		}

		if w.needStop(record) {
			break
		}

		if !w.matchConds(record) {
			continue
		}

		row := w.buildRow(record)
		rows = append(rows, row)
	}

	if len(rows) > 0 {
		w.putRows(rows, rowsCh)
	}
}

func (w *stmtParseWorker) putRows(
	rows [][]types.Datum,
	rowsCh chan<- [][]types.Datum,
) {
	select {
	case rowsCh <- rows:
	case <-w.ctx.Done():
	}
}

func (*stmtParseWorker) parse(raw []byte) (*StmtRecord, error) {
	var record StmtRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return nil, err
	}
	return &record, nil
}

func (w *stmtParseWorker) needStop(record *StmtRecord) bool {
	return w.checker.needStop(record.Begin)
}

func (w *stmtParseWorker) matchConds(record *StmtRecord) bool {
	if !w.checker.isTimeValid(record.Begin, record.End) {
		return false
	}
	if !w.checker.isDigestValid(record.Digest) {
		return false
	}
	if !w.checker.hasPrivilege(record.AuthUsers) {
		return false
	}
	return true
}

func (w *stmtParseWorker) buildRow(record *StmtRecord) []types.Datum {
	row := make([]types.Datum, len(w.columnFactories))
	for n, factory := range w.columnFactories {
		row[n] = types.NewDatum(factory(w, record))
	}
	return row
}

// getInstanceAddr implements columnInfo.
func (w *stmtParseWorker) getInstanceAddr() string {
	return w.instanceAddr
}

// getInstanceAddr implements columnInfo.
func (w *stmtParseWorker) getTimeLocation() *time.Location {
	return w.timeLocation
}

func isCtxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}

func readLine(reader *bufio.Reader) ([]byte, error) {
	return util.ReadLine(reader, maxLineSize)
}

func readLines(reader *bufio.Reader, count int) ([][]byte, error) {
	return util.ReadLines(reader, count, maxLineSize)
}

func timeRangeOverlap(aBegin, aEnd, bBegin, bEnd int64) bool {
	if aEnd == 0 || aEnd < aBegin {
		aEnd = math.MaxInt64
	}
	if bEnd == 0 || bEnd < bBegin {
		bEnd = math.MaxInt64
	}
	// https://stackoverflow.com/questions/3269434/whats-the-most-efficient-way-to-test-if-two-ranges-overlap
	return aBegin <= bEnd && aEnd >= bBegin
}
