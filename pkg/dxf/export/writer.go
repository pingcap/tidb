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
	"bytes"
	"context"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/format/textrow"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/chunk"
)

const (
	csvNullToken   = "\\N"
	writerPartSize = 8 * 1024 * 1024
	writerPartConc = 4
)

// fileName mirrors dumpling's naming: <db>.<table>.<ordinal><writer><file>.csv
// with zero-padded fields so lexicographic order equals key order.
func fileName(db, table string, ordinal, writer, file int) string {
	return fmt.Sprintf("%s.%s.%07d%03d%04d.csv", db, table, ordinal, writer, file)
}

// quotedByType reports whether values of the column type are quoted in CSV.
func quotedByType(tp byte) bool {
	switch tp {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong, mysql.TypeLonglong,
		mysql.TypeYear, mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
		return false
	default:
		return true
	}
}

// csvEncoder encodes chunks into CSV bytes. It is owned by one encoder
// goroutine and serves multiple writers.
type csvEncoder struct {
	cols   []textrow.ColumnInfo
	quoted []bool
	enc    *textrow.ResultEncoder
	valBuf []byte
}

func newCSVEncoder(tblName string, colInfos []*model.ColumnInfo) *csvEncoder {
	cols := make([]textrow.ColumnInfo, 0, len(colInfos))
	quoted := make([]bool, 0, len(colInfos))
	for _, col := range colInfos {
		cols = append(cols, textrow.ColumnInfo{
			Table:   tblName,
			Charset: uint16(mysql.CharsetNameToID(col.GetCharset())),
			Flag:    uint16(col.GetFlag()),
			Decimal: uint8(col.GetDecimal()),
			Type:    col.GetType(),
		})
		quoted = append(quoted, quotedByType(col.GetType()))
	}
	return &csvEncoder{
		cols:   cols,
		quoted: quoted,
		enc:    textrow.NewResultEncoder(charset.CharsetUTF8MB4),
	}
}

// appendEscaped quotes val, copying escape-free segments in bulk.
func appendEscaped(buf, val []byte) []byte {
	buf = append(buf, '"')
	for len(val) > 0 {
		i := bytes.IndexByte(val, '\\')
		if j := bytes.IndexByte(val, '"'); j >= 0 && (i < 0 || j < i) {
			i = j
		}
		if i < 0 {
			buf = append(buf, val...)
			break
		}
		buf = append(buf, val[:i]...)
		buf = append(buf, '\\', val[i])
		val = val[i+1:]
	}
	return append(buf, '"')
}

// encodeChunk appends the CSV encoding of all rows in chk to buf.
func (e *csvEncoder) encodeChunk(chk *chunk.Chunk, buf []byte) ([]byte, error) {
	for rowIdx := range chk.NumRows() {
		row := chk.GetRow(rowIdx)
		for i := range e.cols {
			if i > 0 {
				buf = append(buf, ',')
			}
			if row.IsNull(i) {
				buf = append(buf, csvNullToken...)
				continue
			}
			val, err := textrow.AppendValueText(e.valBuf[:0], row, i, e.cols[i], e.enc)
			if err != nil {
				return nil, errors.Trace(err)
			}
			e.valBuf = val
			if e.quoted[i] {
				buf = appendEscaped(buf, val)
			} else {
				buf = append(buf, val...)
			}
		}
		buf = append(buf, '\n')
	}
	return buf, nil
}

// fileWriter streams encoded buffers of one writer to the object store,
// cutting a new file when the current one reaches fileSize. Buffers arrive at
// chunk granularity, so files are always cut at a row boundary.
type fileWriter struct {
	ctx context.Context

	store    storeapi.Storage
	db       string
	table    string
	ordinal  int
	writerID int
	fileSize int64

	cur     objectio.Writer
	fileIdx int
	curSize int64
}

func newFileWriter(
	ctx context.Context,
	store storeapi.Storage,
	taskMeta *TaskMeta,
	ordinal, writerID int,
) *fileWriter {
	return &fileWriter{
		ctx:      ctx,
		store:    store,
		db:       taskMeta.DBName,
		table:    taskMeta.TableInfo.Name.O,
		ordinal:  ordinal,
		writerID: writerID,
		fileSize: taskMeta.FileSize,
	}
}

// Write writes one encoded buffer, cutting files as needed.
func (w *fileWriter) Write(buf []byte) error {
	if len(buf) == 0 {
		return nil
	}
	if err := w.switchWriter(); err != nil {
		return errors.Trace(err)
	}
	if _, err := w.cur.Write(w.ctx, buf); err != nil {
		return errors.Trace(err)
	}
	w.curSize += int64(len(buf))
	return nil
}

// switchWriter switches to a new file if the current one has reached the size limit.
func (w *fileWriter) switchWriter() error {
	if w.curSize <= w.fileSize && w.cur != nil {
		return nil
	}

	if w.cur != nil {
		if err := w.Close(); err != nil {
			return errors.Trace(err)
		}
	}

	name := fileName(w.db, w.table, w.ordinal, w.writerID, w.fileIdx)
	writer, err := w.store.Create(w.ctx, name, &storeapi.WriterOption{
		Concurrency: writerPartConc,
		PartSize:    writerPartSize,
	})

	if err != nil {
		return errors.Trace(err)
	}

	w.cur = writer
	w.curSize = 0
	w.fileIdx++
	return nil
}

// Close closes the current file.
func (w *fileWriter) Close() error {
	if w.cur != nil {
		if err := w.cur.Close(w.ctx); err != nil {
			return errors.Trace(err)
		}
		w.cur = nil
	}
	return nil
}
