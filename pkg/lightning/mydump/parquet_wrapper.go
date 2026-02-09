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

package mydump

import (
	"context"
	"io"
	"math"

	"github.com/apache/arrow-go/v18/parquet/metadata"
	"github.com/docker/go-units"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/lightning/backend/external"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"golang.org/x/sync/errgroup"
)

var (
	// rowGroupInMemoryThreshold is the max row-group size that enables in-memory
	// reader. For each row group in a Parquet file that is smaller than this
	// threshold, we load that row group into memory once, reducing the number
	// of GET requests for less throttling risk, with the acceptable memory
	// consumption.
	rowGroupInMemoryThreshold = 140 * units.MiB
)

type readerAtSeekerCloser interface {
	io.ReaderAt
	io.Seeker
	io.Closer
}

// parquetWrapper implements parquet.ReaderAtSeeker.
type parquetWrapper struct {
	storeapi.ReadSeekCloser
	lastOff int64
	skipBuf []byte
}

func (p *parquetWrapper) readNBytes(buf []byte) (int, error) {
	n, err := io.ReadFull(p, buf)
	if err != nil && err != io.EOF {
		return 0, errors.Trace(err)
	}
	if n != len(buf) {
		return n, errors.Errorf("error reading %d bytes, only read %d bytes", len(buf), n)
	}
	return n, nil
}

// ReadAt implement ReaderAt interface
func (p *parquetWrapper) ReadAt(buf []byte, off int64) (int, error) {
	// We want to minimize the number of Seek call as much as possible,
	// since the underlying reader may require reopening the file.
	gap := int(off - p.lastOff)
	if gap < 0 || gap > cap(p.skipBuf) {
		if _, err := p.Seek(off, io.SeekStart); err != nil {
			return 0, err
		}
	} else {
		p.skipBuf = p.skipBuf[:gap]
		if read, err := p.readNBytes(p.skipBuf); err != nil {
			return read, err
		}
	}

	read, err := p.readNBytes(buf)
	if err != nil {
		return read, err
	}
	p.lastOff = off + int64(read)

	return len(buf), nil
}

// Seek implement Seeker interface
func (p *parquetWrapper) Seek(offset int64, whence int) (int64, error) {
	newOffset, err := p.ReadSeekCloser.Seek(offset, whence)
	p.lastOff = newOffset
	return newOffset, err
}

func (*parquetWrapper) Write(_ []byte) (n int, err error) {
	return 0, errors.New("unsupported operation")
}

func newParquetWrapper(
	ctx context.Context,
	store storeapi.Storage,
	path string,
	opts *storeapi.ReaderOption,
) (*parquetWrapper, error) {
	reader, err := store.Open(ctx, path, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var lastOff int64
	if opts != nil && opts.StartOffset != nil {
		lastOff = *opts.StartOffset
	}

	return &parquetWrapper{
		ReadSeekCloser: reader,
		lastOff:        lastOff,
		skipBuf:        make([]byte, defaultBufSize),
	}, nil
}

type rowGroupRange struct {
	start int64
	end   int64

	columnStarts []int64
	columnEnds   []int64
}

func (r *rowGroupRange) add(start, end int64) {
	r.start = min(r.start, start)
	r.end = max(r.end, end)
	r.columnStarts = append(r.columnStarts, start)
	r.columnEnds = append(r.columnEnds, end)
}

// inMemoryReaderBase reads one row group into memory and serves ReaderAt.
type inMemoryReaderBase struct {
	buffer   []byte
	rowGroup rowGroupRange
}

func newInMemoryReaderBase(
	ctx context.Context,
	store storeapi.Storage,
	path string,
	rowGroup rowGroupRange,
) (*inMemoryReaderBase, error) {
	base := &inMemoryReaderBase{
		rowGroup: rowGroup,
		buffer:   make([]byte, rowGroup.end-rowGroup.start),
	}
	return base, base.loadRowGroup(ctx, store, path)
}

func (r *inMemoryReaderBase) ReadAt(p []byte, off int64) (int, error) {
	start := off - r.rowGroup.start
	groupSize := r.rowGroup.end - r.rowGroup.start

	// Sanity check, which shouldn't happen.
	if start < 0 {
		return 0, errors.Errorf("invalid offset %d before current row group start %d",
			off, r.rowGroup.start)
	}
	if start >= groupSize {
		return 0, io.EOF
	}

	n := copy(p, r.buffer[start:groupSize])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (r *inMemoryReaderBase) loadRowGroup(
	ctx context.Context, store storeapi.Storage, path string,
) error {
	rg := r.rowGroup

	var eg errgroup.Group
	readStart := rg.start
	for readStart < rg.end {
		batchSize := min(int64(external.ConcurrentReaderBufferSizePerConc), rg.end-readStart)
		start := readStart
		readStart += batchSize
		offset := start - rg.start
		eg.Go(func() error {
			_, err := objstore.ReadDataInRange(
				ctx,
				store,
				path,
				start,
				r.buffer[offset:offset+batchSize],
			)
			return err
		})
	}

	return eg.Wait()
}

type inMemoryParquetWrapper struct {
	base     *inMemoryReaderBase
	fileSize int64
	pos      int64
}

func (w *inMemoryParquetWrapper) ReadAt(p []byte, off int64) (int, error) {
	return w.base.ReadAt(p, off)
}

func (w *inMemoryParquetWrapper) Seek(offset int64, whence int) (int64, error) {
	var base int64
	switch whence {
	case io.SeekStart:
		base = 0
	case io.SeekCurrent:
		base = w.pos
	case io.SeekEnd:
		base = w.fileSize
	default:
		return 0, errors.Errorf("invalid whence %d", whence)
	}
	newPos := base + offset
	if newPos < 0 {
		return 0, errors.Errorf("invalid offset %d", newPos)
	}
	w.pos = newPos
	return newPos, nil
}

func (*inMemoryParquetWrapper) Close() error {
	return nil
}

func rowGroupRangeFromMeta(rg *metadata.RowGroupMetaData) rowGroupRange {
	ranges := rowGroupRange{start: math.MaxInt64}
	for i := range rg.NumColumns() {
		col, _ := rg.ColumnChunk(i)
		start := col.DataPageOffset()
		if col.HasDictionaryPage() && col.DictionaryPageOffset() > 0 {
			start = min(start, col.DictionaryPageOffset())
		}
		ranges.add(start, start+col.TotalCompressedSize())
	}

	return ranges
}
