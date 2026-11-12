// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql"

	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/util"
)

const (
	readBatchCount = 3
	readBatchRows  = 256
	readBatchBytes = units.MiB
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
	wg          util.WaitGroupWrapper
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
		columnCount: columnCount,
		args:        make([]any, columnCount),
	}
	for range readBatchCount {
		r.pool <- &readBatch{}
	}
	decode := meta.SelectedField() != ""
	r.wg.Run(func() {
		r.read(decode)
	})
	return r
}

func (r *tableDataReader) read(decode bool) {
	defer close(r.results)
	if r.tctx.Err() != nil {
		return
	}
	iter := r.TableDataIR.Rows()
	row := &RowReceiverArr{data: make([]sql.RawBytes, r.columnCount)}
	batches := readBatchBuilder{reader: r}
	for {
		if r.tctx.Err() != nil {
			return
		}
		if !iter.HasNext() {
			batches.finish(readResult{terminalErr: iter.Error()})
			return
		}
		if !batches.ensureBatch() {
			return
		}
		var values []sql.RawBytes
		if decode {
			if err := iter.Decode(row); err != nil {
				// Keep a decode failure on its own logical row. In particular,
				// the preceding file's Error check must not observe it.
				batches.finish(readResult{decodeErr: err})
				return
			}
			values = row.data
		}
		if !batches.appendRow(values) {
			return
		}
		iter.Next()
		if !batches.flushIfFull() {
			return
		}
	}
}

func (r *tableDataReader) takeBatch() *readBatch {
	select {
	case <-r.tctx.Done():
		return nil
	case batch := <-r.pool:
		return batch
	}
}

func (r *tableDataReader) send(result readResult) bool {
	select {
	case <-r.tctx.Done():
		return false
	case r.results <- result:
		return true
	}
}

// readBatchBuilder owns the producer's current batch. Flushing transfers that
// ownership to the consumer, which alone may reset and recycle the batch.
type readBatchBuilder struct {
	reader *tableDataReader
	batch  *readBatch
}

func (b *readBatchBuilder) ensureBatch() bool {
	if b.batch == nil {
		b.batch = b.reader.takeBatch()
	}
	return b.batch != nil
}

// appendRow requires an acquired batch and copies values before source advance.
func (b *readBatchBuilder) appendRow(values []sql.RawBytes) bool {
	var rowBytes int
	for _, value := range values {
		rowBytes += len(value)
	}
	if b.batch.wouldOverflow(rowBytes) {
		if !b.flush() {
			return false
		}
		if !b.ensureBatch() {
			return false
		}
	}
	b.batch.appendRow(values, rowBytes)
	return true
}

// flushIfFull is called after advancing the source, preserving publication timing.
func (b *readBatchBuilder) flushIfFull() bool {
	if !b.batch.full() {
		return true
	}
	return b.flush()
}

func (b *readBatchBuilder) flush() bool {
	if b.batch != nil && b.batch.rows > 0 {
		if !b.reader.send(readResult{batch: b.batch}) {
			return false
		}
	}
	b.batch = nil
	return true
}

func (b *readBatchBuilder) finish(result readResult) {
	if !b.flush() {
		return
	}
	// Errors are ordered after all copied rows, including at file boundaries.
	b.reader.send(result)
}

func (r *tableDataReader) Rows() SQLRowIter { return r }

func (r *tableDataReader) HasNext() bool {
	if r.Error() != nil {
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
	// The consumer owns one of readBatchCount batches, so the pool has room.
	r.pool <- batch
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
	r.wg.Wait()
	return nil
}
