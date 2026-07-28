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

package export

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/dumpformat/csvfile"
	"github.com/pingcap/tidb/pkg/format/textrow"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const (
	// uploadConcurrency and uploadPartSize configure each output file's
	// concurrent multipart upload. A cross-region object-store write is
	// per-connection RTT-bound, so a single file needs many parts in flight to
	// use the bandwidth.
	uploadConcurrency = 16
	uploadPartSize    = 8 * 1024 * 1024
)

// csvConfig returns the default CSV framing, byte-compatible with Dumpling's
// default CSV output.
func csvConfig() *csvfile.Config {
	return &csvfile.Config{
		FieldsTerminatedBy: ",",
		FieldsEnclosedBy:   `"`,
		FieldsEscapedBy:    `\`,
		LinesTerminatedBy:  "\n",
		NullValue:          []byte(`\N`),
		BinaryFormat:       csvfile.BinaryFormatUTF8,
	}
}

// fieldKinds classifies each column for CSV framing.
func fieldKinds(colInfos []*model.ColumnInfo) []csvfile.FieldKind {
	kinds := make([]csvfile.FieldKind, len(colInfos))
	for i, col := range colInfos {
		switch col.GetType() {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
			mysql.TypeYear, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			kinds[i] = csvfile.KindNumber
		case mysql.TypeTinyBlob, mysql.TypeMediumBlob, mysql.TypeLongBlob, mysql.TypeBlob:
			if mysql.HasBinaryFlag(col.GetFlag()) {
				kinds[i] = csvfile.KindBytes
			} else {
				kinds[i] = csvfile.KindString
			}
		default:
			kinds[i] = csvfile.KindString
		}
	}
	return kinds
}

// fileName mirrors Dumpling's naming: <db>.<table>.<ordinal><file>.csv with
// zero-padded fields so lexicographic order equals key order.
func fileName(db, table string, ordinal, file int) string {
	return fmt.Sprintf("%s.%s.%07d%04d.csv", db, table, ordinal, file)
}

// rowEncoder turns a chunk.Row into the per-column raw byte values csvfile
// expects. FormatValueText's result is backed by the encoder's shared buffer
// and overwritten on the next call, so values are copied into a stable row
// buffer before being sliced.
type rowEncoder struct {
	cols  []textrow.ColumnInfo
	enc   *textrow.ResultEncoder
	buf   []byte
	spans []span
	row   []sql.RawBytes
}

type span struct {
	off, ln int
	null    bool
}

func newRowEncoder(tblName string, colInfos []*model.ColumnInfo) *rowEncoder {
	cols := make([]textrow.ColumnInfo, len(colInfos))
	for i, col := range colInfos {
		cols[i] = textrow.ColumnInfo{
			Table:   tblName,
			Charset: uint16(mysql.CharsetNameToID(col.GetCharset())),
			Flag:    uint16(col.GetFlag()),
			Decimal: uint8(col.GetDecimal()),
			Type:    col.GetType(),
		}
	}
	return &rowEncoder{
		cols:  cols,
		enc:   textrow.NewResultEncoder(charset.CharsetUTF8MB4),
		spans: make([]span, len(cols)),
		row:   make([]sql.RawBytes, 0, len(cols)),
	}
}

func (e *rowEncoder) encode(r chunk.Row) ([]sql.RawBytes, error) {
	e.buf = e.buf[:0]
	for i := range e.cols {
		if r.IsNull(i) {
			e.spans[i] = span{null: true}
			continue
		}
		val, err := textrow.FormatValueText(r, i, e.cols[i], e.enc)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.spans[i] = span{off: len(e.buf), ln: len(val)}
		e.buf = append(e.buf, val...)
	}
	e.row = e.row[:0]
	for _, s := range e.spans {
		if s.null {
			e.row = append(e.row, nil)
			continue
		}
		e.row = append(e.row, e.buf[s.off:s.off+s.ln])
	}
	return e.row, nil
}

// chunkWriter uploads one chunk's rows as a group of CSV files sharing the
// chunk's name prefix, cutting a new file every FileSize on a row boundary.
type chunkWriter struct {
	ctx      context.Context
	objStore storeapi.Storage
	fileSize int64
	db       string
	table    string
	ordinal  int
	kinds    []csvfile.FieldKind

	fileIdx int
	obj     objectio.Writer
	cw      *csvfile.Writer
}

func (w *chunkWriter) writeRow(row []sql.RawBytes) error {
	if w.cw == nil {
		name := fileName(w.db, w.table, w.ordinal, w.fileIdx)
		obj, err := w.objStore.Create(w.ctx, name, &storeapi.WriterOption{
			Concurrency: uploadConcurrency,
			PartSize:    uploadPartSize,
		})
		if err != nil {
			return errors.Trace(err)
		}
		w.obj = obj
		w.cw = csvfile.NewWriter(&wrappedWriter{ctx: w.ctx, w: obj}, w.kinds, csvConfig())
	}
	if err := w.cw.Write(row); err != nil {
		return errors.Trace(err)
	}
	if w.fileSize > 0 && int64(w.cw.EstimateFileSize()) >= w.fileSize {
		return w.closeFile()
	}
	return nil
}

// closeFile flushes and finalizes the current file, advancing the file index.
func (w *chunkWriter) closeFile() error {
	if w.cw == nil {
		return nil
	}
	if err := w.cw.Close(); err != nil {
		return errors.Trace(err)
	}
	if err := w.obj.Close(w.ctx); err != nil {
		return errors.Trace(err)
	}
	w.cw, w.obj = nil, nil
	w.fileIdx++
	return nil
}

// close finalizes the last (possibly partial) file. A chunk with no rows writes
// no file.
func (w *chunkWriter) close() error {
	return w.closeFile()
}

// wrappedWriter adapts a context-carrying objectio.Writer to a plain io.Writer
// so csvfile (which is stdlib-only) can write to the object store.
type wrappedWriter struct {
	ctx context.Context
	w   objectio.Writer
}

func (w *wrappedWriter) Write(p []byte) (int, error) {
	return w.w.Write(w.ctx, p)
}

var _ io.Writer = (*wrappedWriter)(nil)
