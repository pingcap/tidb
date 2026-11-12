// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql"

	"github.com/pingcap/errors"
	tcontext "github.com/pingcap/tidb/dumpling/context"
)

const (
	readBatchCount = 3
	readBatchRows  = 256
	readBatchBytes = 1 << 20
)

type readBatchValue struct {
	offset int
	length int // -1 denotes SQL NULL; zero denotes a non-NULL empty value.
}

type readBatch struct {
	data   []byte
	values []readBatchValue
	rows   int
}

func (b *readBatch) wouldOverflow(rowBytes int) bool {
	// An individual row may exceed the byte target, but gets its own batch.
	return b.rows > 0 && len(b.data)+rowBytes > readBatchBytes
}

// appendRow copies borrowed values before the source advances. A nil values
// slice counts a generated-only row without storing any cells.
func (b *readBatch) appendRow(values []sql.RawBytes, rowBytes int) {
	if rowBytes > 0 && b.data == nil {
		b.data = make([]byte, 0, max(readBatchBytes, rowBytes))
	}
	for _, value := range values {
		cell := readBatchValue{offset: len(b.data), length: len(value)}
		if value == nil {
			cell.length = -1
		} else {
			b.data = append(b.data, value...)
		}
		b.values = append(b.values, cell)
	}
	b.rows++
}

func (b *readBatch) full() bool {
	return b.rows == readBatchRows || len(b.data) >= readBatchBytes
}

func (b *readBatch) decodeRow(rowIndex int, args []any) error {
	for i, arg := range args {
		destination, ok := arg.(*sql.RawBytes)
		if !ok {
			return errors.Errorf("unsupported Dumpling row receiver %T", arg)
		}
		value := b.values[rowIndex*len(args)+i]
		switch value.length {
		case -1:
			*destination = nil
		case 0:
			*destination = sql.RawBytes{}
		default:
			*destination = b.data[value.offset : value.offset+value.length]
		}
	}
	return nil
}

func (b *readBatch) reset() {
	b.rows = 0
	b.values = b.values[:0]
	// Do not retain an oversized row's allocation for the remainder of a chunk.
	if cap(b.data) > readBatchBytes {
		b.data = nil
	} else {
		b.data = b.data[:0]
	}
}

// readResult carries a batch, a decode failure on its own logical row, or a
// terminal result. Only one payload is set; all nil denotes a clean end of stream.
type readResult struct {
	batch       *readBatch
	decodeErr   error
	terminalErr error
}

// tableDataReader separates source iteration from formatting and storage. Only
// its producer touches the source iterator. A batch remains immutable until the
// consumer advances past its last row and returns it to the pool.
type tableDataReader struct {
	TableDataIR

	// Shared lifecycle and immutable source configuration.
	tctx        *tcontext.Context
	cancel      func()
	pool        chan *readBatch
	results     chan readResult
	done        chan struct{}
	columnCount int

	// Consumer-owned cursor and row binding scratch space.
	args      []any
	current   *readBatch
	rowIndex  int
	decodeErr error
	err       error
	ended     bool
}

func newTableDataReader(tctx *tcontext.Context, cancel func(), meta TableMeta, source TableDataIR) *tableDataReader {
	columnCount := len(meta.ColumnTypes())
	r := &tableDataReader{
		TableDataIR: source,
		tctx:        tctx,
		cancel:      cancel,
		pool:        make(chan *readBatch, readBatchCount),
		results:     make(chan readResult, readBatchCount),
		done:        make(chan struct{}),
		columnCount: columnCount,
		args:        make([]any, columnCount),
	}
	for range readBatchCount {
		r.pool <- &readBatch{}
	}
	go r.read(meta.SelectedField() != "")
	return r
}

func (r *tableDataReader) read(decode bool) {
	defer close(r.done)
	defer close(r.results)
	if r.tctx.Err() != nil {
		return
	}
	iter := r.TableDataIR.Rows()
	row := &RowReceiverArr{data: make([]sql.RawBytes, r.columnCount)}
	var batch *readBatch
	for {
		if r.tctx.Err() != nil {
			return
		}
		if !iter.HasNext() {
			r.finish(batch, iter.Error())
			return
		}
		if batch == nil {
			batch = r.takeBatch()
			if batch == nil {
				return
			}
		}
		var values []sql.RawBytes
		var rowBytes int
		if decode {
			if err := iter.Decode(row); err != nil {
				if !r.publishBatch(batch) {
					return
				}
				// Keep a decode failure on its own logical row. In particular,
				// the preceding file's Error check must not observe it.
				r.send(readResult{decodeErr: err})
				return
			}
			values = row.data
			for _, value := range values {
				rowBytes += len(value)
			}
		}
		if batch.wouldOverflow(rowBytes) {
			if !r.publishBatch(batch) {
				return
			}
			batch = r.takeBatch()
			if batch == nil {
				return
			}
		}
		batch.appendRow(values, rowBytes)
		iter.Next()
		if batch.full() {
			if !r.publishBatch(batch) {
				return
			}
			batch = nil
		}
	}
}

func (r *tableDataReader) takeBatch() *readBatch {
	if r.tctx.Err() != nil {
		return nil
	}
	select {
	case <-r.tctx.Done():
		return nil
	case batch := <-r.pool:
		return batch
	}
}

func (r *tableDataReader) send(result readResult) bool {
	if r.tctx.Err() != nil {
		return false
	}
	select {
	case <-r.tctx.Done():
		return false
	case r.results <- result:
		return true
	}
}

func (r *tableDataReader) publishBatch(batch *readBatch) bool {
	if batch == nil || batch.rows == 0 {
		return true
	}
	return r.send(readResult{batch: batch})
}

func (r *tableDataReader) finish(batch *readBatch, terminalErr error) {
	if !r.publishBatch(batch) {
		return
	}
	// Errors are ordered after all copied rows, including at file boundaries.
	r.send(readResult{terminalErr: terminalErr})
}

func (r *tableDataReader) Rows() SQLRowIter { return r }

func (r *tableDataReader) HasNext() bool {
	if err := r.tctx.Err(); err != nil {
		r.err = err
		return false
	}
	if r.current != nil || r.decodeErr != nil {
		return true
	}
	if r.ended {
		return false
	}
	select {
	case <-r.tctx.Done():
		r.err = r.tctx.Err()
		return false
	case result, ok := <-r.results:
		if result.decodeErr != nil {
			r.decodeErr = result.decodeErr
			return true
		}
		if !ok || result.batch == nil {
			r.err = result.terminalErr
			r.ended = true
			return false
		}
		r.current = result.batch
		r.rowIndex = 0
		return true
	}
}

func (r *tableDataReader) Decode(row RowReceiver) error {
	if !r.HasNext() {
		if err := r.Error(); err != nil {
			return err
		}
		return errors.New("buffered row iterator has no current row")
	}
	if r.decodeErr != nil {
		return r.decodeErr
	}
	row.BindAddress(r.args)
	return r.current.decodeRow(r.rowIndex, r.args)
}

func (r *tableDataReader) Next() {
	if !r.HasNext() {
		return
	}
	if r.decodeErr != nil {
		r.err = r.decodeErr
		r.decodeErr = nil
		r.ended = true
		return
	}
	r.rowIndex++
	if r.rowIndex < r.current.rows {
		return
	}
	batch := r.current
	r.current = nil
	batch.reset()
	select {
	case <-r.tctx.Done():
		r.err = r.tctx.Err()
		return
	case r.pool <- batch:
	}
	// A writer may check Error at a file boundary without calling HasNext again.
	// Observe terminal errors now, but leave decode failures on the next row.
	r.HasNext()
}

func (r *tableDataReader) Error() error {
	if err := r.tctx.Err(); err != nil {
		return err
	}
	return r.err
}

// Close stops and joins the producer before the worker closes the source IR.
// The storage context is independent and remains usable for output cleanup.
func (r *tableDataReader) Close() error {
	r.cancel()
	<-r.done
	return nil
}
