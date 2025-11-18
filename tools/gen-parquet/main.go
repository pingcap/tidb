// Copyright 2020 PingCAP, Inc.
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

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
)

type writeWrapper struct {
	Writer *os.File
}

func (*writeWrapper) Seek(_ int64, _ int) (int64, error) {
	return 0, nil
}

func (*writeWrapper) Read(_ []byte) (int, error) {
	return 0, nil
}

func (w *writeWrapper) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func (w *writeWrapper) Close() error {
	return w.Writer.Close()
}

func getParquetWriter(w io.Writer, rowNames []string, rowTypes []parquet.Type) *file.Writer {
	fields := make([]schema.Node, len(rowNames))
	for i, name := range rowNames {
		fields[i], _ = schema.NewPrimitiveNode(
			name,
			parquet.Repetitions.Optional,
			rowTypes[i],
			-1, 8,
		)
	}

	node, _ := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	schema := schema.NewSchema(node)

	opts := []parquet.WriterProperty{}
	for i := range rowNames {
		opts = append(opts, parquet.WithDictionaryFor(schema.Column(i).Name(), true))
		opts = append(opts, parquet.WithCompressionFor(schema.Column(i).Name(), compress.Codecs.Snappy))
	}
	props := parquet.NewWriterProperties(opts...)

	return file.NewParquetWriter(w, schema.Root(), file.WithWriterProps(props))
}

func writeColumn(rgw file.SerialRowGroupWriter, rows int) error {
	cw, err := rgw.NextColumn()
	if err != nil {
		return err
	}
	//nolint: errcheck
	defer cw.Close()

	defLevel := make([]int16, rows)
	for i := range rows {
		defLevel[i] = 1
	}

	switch w := cw.(type) {
	case *file.Int64ColumnChunkWriter:
		buf := make([]int64, rows)
		for i := range rows {
			buf[i] = int64(i)
		}
		_, err = w.WriteBatch(buf, defLevel, nil)
	case *file.Float64ColumnChunkWriter:
		buf := make([]float64, rows)
		for i := range rows {
			buf[i] = float64(i)
		}
		_, err = w.WriteBatch(buf, defLevel, nil)
	case *file.ByteArrayColumnChunkWriter:
		buf := make([]parquet.ByteArray, rows)
		for i := range rows {
			s := strconv.Itoa(i)
			buf[i] = []byte(s)
		}
		_, err = w.WriteBatch(buf, defLevel, nil)
	default:
		return fmt.Errorf("unsupported column type: %T", w)
	}

	return err
}

func writeSimpleParquetFile(filePath string, rows int) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	w := &writeWrapper{Writer: file}

	rowNames := []string{"iVal", "s"}
	rowTypes := []parquet.Type{parquet.Types.Int64, parquet.Types.ByteArray}
	pw := getParquetWriter(w, rowNames, rowTypes)
	//nolint: errcheck
	defer pw.Close()

	// Only one row group for simplicity
	rgw := pw.AppendRowGroup()
	//nolint: errcheck
	defer rgw.Close()

	for range rowNames {
		if err := writeColumn(rgw, rows); err != nil {
			return err
		}
	}
	return nil
}

var (
	schemaName = flag.String("schema", "test", "Test schema name")
	tableName  = flag.String("table", "parquet", "Test table name")
	chunks     = flag.Int("chunk", 10, "Chunk files count")
	rowNumbers = flag.Int("rows", 1000, "Row number for each test file")
	sourceDir  = flag.String("dir", "", "test directory path")
)

func main() {
	flag.Parse()

	for i := range *chunks {
		name := fmt.Sprintf("%s.%s.%04d.parquet", *schemaName, *tableName, i)
		filePath := filepath.Join(*sourceDir, name)
		err := writeSimpleParquetFile(filePath, *rowNumbers)
		if err != nil {
			log.Fatalf("generate test source failed, name: %s, err: %+v", name, err)
		}
	}
}
