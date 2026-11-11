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

type readResult struct {
	batch     *readBatch
	decodeErr error
	err       error
}

// tableDataReader separates source iteration from formatting and storage. Only
// its producer touches the source iterator. A batch remains immutable until the
// consumer advances past its last row and returns it to the pool.
type tableDataReader struct {
	TableDataIR
	tctx      *tcontext.Context
	cancel    func()
	pool      chan *readBatch
	results   chan readResult
	done      chan struct{}
	args      []any
	current   *readBatch
	index     int
	decodeErr error
	err       error
	ended     bool
}

func newTableDataReader(tctx *tcontext.Context, cancel func(), meta TableMeta, source TableDataIR) *tableDataReader {
	r := &tableDataReader{
		TableDataIR: source,
		tctx:        tctx,
		cancel:      cancel,
		pool:        make(chan *readBatch, readBatchCount),
		results:     make(chan readResult, readBatchCount),
		done:        make(chan struct{}),
		args:        make([]any, len(meta.ColumnTypes())),
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
	row := &RowReceiverArr{data: make([]sql.RawBytes, len(r.args))}
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
		var rowBytes int
		if decode {
			if err := iter.Decode(row); err != nil {
				if batch.rows != 0 && !r.send(readResult{batch: batch}) {
					return
				}
				// Keep a decode failure on its own logical row. In particular,
				// the preceding file's Error check must not observe it.
				r.send(readResult{decodeErr: err})
				return
			}
			for _, value := range row.data {
				rowBytes += len(value)
			}
		}
		// An individual row may exceed the byte target, but never shares that
		// batch with another row. Copy before Next invalidates borrowed bytes.
		if batch.rows > 0 && len(batch.data)+rowBytes > readBatchBytes {
			if !r.send(readResult{batch: batch}) {
				return
			}
			batch = r.takeBatch()
			if batch == nil {
				return
			}
		}
		if decode {
			if rowBytes > 0 && batch.data == nil {
				batch.data = make([]byte, 0, max(readBatchBytes, rowBytes))
			}
			for _, value := range row.data {
				cell := readBatchValue{offset: len(batch.data), length: len(value)}
				if value == nil {
					cell.length = -1
				} else {
					batch.data = append(batch.data, value...)
				}
				batch.values = append(batch.values, cell)
			}
		}
		batch.rows++
		iter.Next()
		if batch.rows == readBatchRows || len(batch.data) >= readBatchBytes {
			if !r.send(readResult{batch: batch}) {
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

func (r *tableDataReader) finish(batch *readBatch, err error) {
	if batch != nil && batch.rows != 0 && !r.send(readResult{batch: batch}) {
		return
	}
	// Errors are ordered after all copied rows, including at file boundaries.
	r.send(readResult{err: err})
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
			r.err = result.err
			r.ended = true
			return false
		}
		r.current = result.batch
		r.index = 0
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
	for i, arg := range r.args {
		destination, ok := arg.(*sql.RawBytes)
		if !ok {
			return errors.Errorf("unsupported Dumpling row receiver %T", arg)
		}
		value := r.current.values[r.index*len(r.args)+i]
		switch value.length {
		case -1:
			*destination = nil
		case 0:
			*destination = sql.RawBytes{}
		default:
			*destination = r.current.data[value.offset : value.offset+value.length]
		}
	}
	return nil
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
	r.index++
	if r.index < r.current.rows {
		return
	}
	batch := r.current
	r.current = nil
	batch.rows = 0
	batch.values = batch.values[:0]
	// Do not retain an oversized row's allocation for the remainder of a chunk.
	if cap(batch.data) > readBatchBytes {
		batch.data = nil
	} else {
		batch.data = batch.data[:0]
	}
	select {
	case <-r.tctx.Done():
		r.err = r.tctx.Err()
		return
	case r.pool <- batch:
	}
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
