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

package filesort

import (
	"container/heap"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/tidb/sessionctx/stmtctx"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
)

type comparableRow struct {
	key    []types.Datum
	val    []types.Datum
	handle int64
}

type item struct {
	index int // source file index
	value *comparableRow
}

// rowHeap maintains a min-heap property of comparableRows.
type rowHeap struct {
	sc     *stmtctx.StatementContext
	ims    []*item
	byDesc []bool
	err    error
}

var headSize = 8

func lessThan(sc *stmtctx.StatementContext, i []types.Datum, j []types.Datum, byDesc []bool) (bool, error) {
	for k := range byDesc {
		v1 := i[k]
		v2 := j[k]

		ret, err := v1.CompareDatum(sc, &v2)
		if err != nil {
			return false, errors.Trace(err)
		}

		if byDesc[k] {
			ret = -ret
		}

		if ret < 0 {
			return true, nil
		} else if ret > 0 {
			return false, nil
		}
	}
	return false, nil
}

// Len implements heap.Interface Len interface.
func (rh *rowHeap) Len() int { return len(rh.ims) }

// Swap implements heap.Interface Swap interface.
func (rh *rowHeap) Swap(i, j int) { rh.ims[i], rh.ims[j] = rh.ims[j], rh.ims[i] }

// Less implements heap.Interface Less interface.
func (rh *rowHeap) Less(i, j int) bool {
	l := rh.ims[i].value.key
	r := rh.ims[j].value.key
	ret, err := lessThan(rh.sc, l, r, rh.byDesc)
	if rh.err == nil {
		rh.err = err
	}
	return ret
}

// Push pushes an element into rowHeap.
func (rh *rowHeap) Push(x interface{}) {
	rh.ims = append(rh.ims, x.(*item))
}

// Pop pops the last element from rowHeap.
func (rh *rowHeap) Pop() interface{} {
	old := rh.ims
	n := len(old)
	x := old[n-1]
	rh.ims = old[0 : n-1]
	return x
}

// FileSorter sorts the given rows according to the byDesc order.
// FileSorter can sort rows that exceed predefined memory capacity.
type FileSorter struct {
	sc     *stmtctx.StatementContext
	byDesc []bool

	workers  []*Worker
	nWorkers int // number of workers used in async sorting
	cWorker  int // the next worker to which the sorting job is sent

	mu     sync.Mutex
	tmpDir string
	files  []string
	nFiles int
	cursor int // required when performing full in-memory sort

	rowHeap    *rowHeap
	fds        []*os.File
	rowBytes   []byte
	head       []byte
	dcod       []types.Datum
	keySize    int
	valSize    int
	maxRowSize int

	wg       sync.WaitGroup
	closed   bool
	fetched  bool
	external bool // mark the necessity of performing external file sort
}

// Worker sorts file asynchronously.
type Worker struct {
	ctx     *FileSorter
	busy    int32
	keySize int
	valSize int
	rowSize int
	bufSize int
	buf     []*comparableRow
	head    []byte
	err     error
}

// Builder builds a new FileSorter.
type Builder struct {
	sc       *stmtctx.StatementContext
	keySize  int
	valSize  int
	bufSize  int
	nWorkers int
	byDesc   []bool
	tmpDir   string
}

// SetSC sets StatementContext instance which is required in row comparison.
func (b *Builder) SetSC(sc *stmtctx.StatementContext) *Builder {
	b.sc = sc
	return b
}

// SetSchema sets the schema of row, including key size and value size.
func (b *Builder) SetSchema(keySize, valSize int) *Builder {
	b.keySize = keySize
	b.valSize = valSize
	return b
}

// SetBuf sets the number of rows FileSorter can hold in memory at a time.
func (b *Builder) SetBuf(bufSize int) *Builder {
	b.bufSize = bufSize
	return b
}

// SetWorkers sets the number of workers used in async sorting.
func (b *Builder) SetWorkers(nWorkers int) *Builder {
	b.nWorkers = nWorkers
	return b
}

// SetDesc sets the ordering rule of row comparison.
func (b *Builder) SetDesc(byDesc []bool) *Builder {
	b.byDesc = byDesc
	return b
}

// SetDir sets the working directory for FileSorter.
func (b *Builder) SetDir(tmpDir string) *Builder {
	b.tmpDir = tmpDir
	return b
}

// Build creates a FileSorter instance using given data.
func (b *Builder) Build() (*FileSorter, error) {
	// Sanity checks
	if b.sc == nil {
		return nil, errors.New("StatementContext is nil")
	}
	if b.keySize != len(b.byDesc) {
		return nil, errors.New("mismatch in key size and byDesc slice")
	}
	if b.keySize <= 0 {
		return nil, errors.New("key size is not positive")
	}
	if b.valSize <= 0 {
		return nil, errors.New("value size is not positive")
	}
	if b.bufSize <= 0 {
		return nil, errors.New("buffer size is not positive")
	}
	_, err := os.Stat(b.tmpDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.New("tmpDir does not exist")
		}
		return nil, errors.Trace(err)
	}

	ws := make([]*Worker, b.nWorkers)
	for i := range ws {
		ws[i] = &Worker{
			keySize: b.keySize,
			valSize: b.valSize,
			rowSize: b.keySize + b.valSize + 1,
			bufSize: b.bufSize / b.nWorkers,
			buf:     make([]*comparableRow, 0, b.bufSize/b.nWorkers),
			head:    make([]byte, headSize),
		}
	}

	rh := &rowHeap{sc: b.sc,
		ims:    make([]*item, 0),
		byDesc: b.byDesc,
	}

	fs := &FileSorter{sc: b.sc,
		workers:  ws,
		nWorkers: b.nWorkers,
		cWorker:  0,

		head:    make([]byte, headSize),
		dcod:    make([]types.Datum, 0, b.keySize+b.valSize+1),
		keySize: b.keySize,
		valSize: b.valSize,

		tmpDir:  b.tmpDir,
		files:   make([]string, 0),
		byDesc:  b.byDesc,
		rowHeap: rh,
	}

	for i := 0; i < b.nWorkers; i++ {
		fs.workers[i].ctx = fs
	}

	return fs, nil
}

func (fs *FileSorter) getUniqueFileName() string {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	ret := filepath.Join(fs.tmpDir, strconv.Itoa(fs.nFiles))
	fs.nFiles++
	return ret
}

func (fs *FileSorter) appendFileName(fn string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.files = append(fs.files, fn)
}

func (fs *FileSorter) closeAllFiles() error {
	var reportErr error
	for _, fd := range fs.fds {
		err := fd.Close()
		if reportErr == nil {
			reportErr = err
		}
	}
	err := os.RemoveAll(fs.tmpDir)
	if reportErr == nil {
		reportErr = err
	}
	if reportErr != nil {
		return errors.Trace(reportErr)
	}
	return nil
}

// internalSort performs full in-memory sort.
func (fs *FileSorter) internalSort() (*comparableRow, error) {
	w := fs.workers[fs.cWorker]

	if !fs.fetched {
		sort.Sort(w)
		if w.err != nil {
			return nil, errors.Trace(w.err)
		}
		fs.fetched = true
	}
	if fs.cursor < len(w.buf) {
		r := w.buf[fs.cursor]
		fs.cursor++
		return r, nil
	}
	return nil, nil
}

// externalSort performs external file sort.
func (fs *FileSorter) externalSort() (*comparableRow, error) {
	if !fs.fetched {
		// flush all remaining content to file (if any)
		for _, w := range fs.workers {
			if atomic.LoadInt32(&(w.busy)) == 0 && len(w.buf) > 0 {
				fs.wg.Add(1)
				go w.flushToFile()
			}
		}

		// wait for all workers to finish
		fs.wg.Wait()

		// check errors from workers
		for _, w := range fs.workers {
			if w.err != nil {
				return nil, errors.Trace(w.err)
			}
			if w.rowSize > fs.maxRowSize {
				fs.maxRowSize = w.rowSize
			}
		}

		heap.Init(fs.rowHeap)
		if fs.rowHeap.err != nil {
			return nil, errors.Trace(fs.rowHeap.err)
		}

		fs.rowBytes = make([]byte, fs.maxRowSize)

		err := fs.openAllFiles()
		if err != nil {
			return nil, errors.Trace(err)
		}

		for id := range fs.fds {
			row, err := fs.fetchNextRow(id)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if row == nil {
				return nil, errors.New("file is empty")
			}

			im := &item{
				index: id,
				value: row,
			}

			heap.Push(fs.rowHeap, im)
			if fs.rowHeap.err != nil {
				return nil, errors.Trace(fs.rowHeap.err)
			}
		}

		fs.fetched = true
	}

	if fs.rowHeap.Len() > 0 {
		im := heap.Pop(fs.rowHeap).(*item)
		if fs.rowHeap.err != nil {
			return nil, errors.Trace(fs.rowHeap.err)
		}

		row, err := fs.fetchNextRow(im.index)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if row != nil {
			nextIm := &item{
				index: im.index,
				value: row,
			}

			heap.Push(fs.rowHeap, nextIm)
			if fs.rowHeap.err != nil {
				return nil, errors.Trace(fs.rowHeap.err)
			}
		}

		return im.value, nil
	}

	return nil, nil
}

func (fs *FileSorter) openAllFiles() error {
	for _, fname := range fs.files {
		fd, err := os.Open(fname)
		if err != nil {
			return errors.Trace(err)
		}
		fs.fds = append(fs.fds, fd)
	}
	return nil
}

// fetchNextRow fetches the next row given the source file index.
func (fs *FileSorter) fetchNextRow(index int) (*comparableRow, error) {
	n, err := fs.fds[index].Read(fs.head)
	if err == io.EOF {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n != headSize {
		return nil, errors.New("incorrect header")
	}
	rowSize := int(binary.BigEndian.Uint64(fs.head))

	n, err = fs.fds[index].Read(fs.rowBytes)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if n != rowSize {
		return nil, errors.New("incorrect row")
	}

	fs.dcod, err = codec.Decode(fs.rowBytes, fs.keySize+fs.valSize+1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &comparableRow{
		key:    fs.dcod[:fs.keySize],
		val:    fs.dcod[fs.keySize : fs.keySize+fs.valSize],
		handle: fs.dcod[fs.keySize+fs.valSize:][0].GetInt64(),
	}, nil
}

// Input adds one row into FileSorter.
// Caller should not call Input after calling Output.
func (fs *FileSorter) Input(key []types.Datum, val []types.Datum, handle int64) error {
	if fs.closed {
		return errors.New("FileSorter has been closed")
	}
	if fs.fetched {
		return errors.New("call input after output")
	}

	assigned := false
	abortTime := time.Duration(1) * time.Minute           // 1 minute
	cooldownTime := time.Duration(100) * time.Millisecond // 100 milliseconds
	row := &comparableRow{
		key:    key,
		val:    val,
		handle: handle,
	}

	origin := time.Now()
	// assign input row to some worker in a round-robin way
	for {
		for i := 0; i < fs.nWorkers; i++ {
			wid := (fs.cWorker + i) % fs.nWorkers
			if atomic.LoadInt32(&(fs.workers[wid].busy)) == 0 {
				fs.workers[wid].input(row)
				assigned = true
				fs.cWorker = wid
				break
			}
		}
		if assigned {
			break
		}

		// all workers are busy now, cooldown and retry
		time.Sleep(cooldownTime)

		if time.Since(origin) >= abortTime {
			// weird: all workers are busy for at least 1 min
			// choose to abort for safety
			return errors.New("can not make progress since all workers are busy")
		}
	}
	return nil
}

// Output gets the next sorted row.
func (fs *FileSorter) Output() ([]types.Datum, []types.Datum, int64, error) {
	var (
		r   *comparableRow
		err error
	)
	if fs.closed {
		return nil, nil, 0, errors.New("FileSorter has been closed")
	}

	if fs.external {
		r, err = fs.externalSort()
	} else {
		r, err = fs.internalSort()
	}

	if err != nil {
		return nil, nil, 0, errors.Trace(err)
	} else if r != nil {
		return r.key, r.val, r.handle, nil
	} else {
		return nil, nil, 0, nil
	}
}

// Close terminates the input or output process and discards all remaining data.
func (fs *FileSorter) Close() error {
	if fs.closed {
		return nil
	}
	fs.wg.Wait()
	for _, w := range fs.workers {
		w.buf = w.buf[:0]
	}
	fs.closed = true
	err := fs.closeAllFiles()
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (w *Worker) Len() int { return len(w.buf) }

func (w *Worker) Swap(i, j int) { w.buf[i], w.buf[j] = w.buf[j], w.buf[i] }

func (w *Worker) Less(i, j int) bool {
	l := w.buf[i].key
	r := w.buf[j].key
	ret, err := lessThan(w.ctx.sc, l, r, w.ctx.byDesc)
	if w.err == nil {
		w.err = errors.Trace(err)
	}
	return ret
}

func (w *Worker) input(row *comparableRow) {
	w.buf = append(w.buf, row)

	if len(w.buf) > w.bufSize {
		atomic.StoreInt32(&(w.busy), int32(1))
		w.ctx.wg.Add(1)
		w.ctx.external = true
		go w.flushToFile()
	}
}

// flushToFile flushes the buffer to file if it is full.
func (w *Worker) flushToFile() {
	defer w.ctx.wg.Done()
	var (
		outputByte []byte
		prevLen    int
	)

	sort.Sort(w)
	if w.err != nil {
		return
	}

	fileName := w.ctx.getUniqueFileName()

	outputFile, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		w.err = errors.Trace(err)
		return
	}
	defer terror.Call(outputFile.Close)
	sc := &stmtctx.StatementContext{TimeZone: time.Local}
	for _, row := range w.buf {
		prevLen = len(outputByte)
		outputByte = append(outputByte, w.head...)
		outputByte, err = codec.EncodeKey(sc, outputByte, row.key...)
		if err != nil {
			w.err = errors.Trace(err)
			return
		}
		outputByte, err = codec.EncodeKey(sc, outputByte, row.val...)
		if err != nil {
			w.err = errors.Trace(err)
			return
		}
		outputByte, err = codec.EncodeKey(sc, outputByte, types.NewIntDatum(row.handle))
		if err != nil {
			w.err = errors.Trace(err)
			return
		}

		if len(outputByte)-prevLen-headSize > w.rowSize {
			w.rowSize = len(outputByte) - prevLen - headSize
		}
		binary.BigEndian.PutUint64(w.head, uint64(len(outputByte)-prevLen-headSize))
		for i := 0; i < headSize; i++ {
			outputByte[prevLen+i] = w.head[i]
		}
	}

	_, err = outputFile.Write(outputByte)
	if err != nil {
		w.err = errors.Trace(err)
		return
	}

	w.ctx.appendFileName(fileName)
	w.buf = w.buf[:0]
	atomic.StoreInt32(&(w.busy), int32(0))
}
