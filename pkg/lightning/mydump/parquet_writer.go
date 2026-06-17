// Copyright 2025 PingCAP, Inc.
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
	"fmt"

	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/schema"
	"github.com/pingcap/tidb/br/pkg/storage"
)

// ParquetColumn defines the properties of a column in a Parquet file.
// Used to generate parquet files in tests.
type ParquetColumn struct {
	Name      string
	Type      parquet.Type
	Converted schema.ConvertedType
	TypeLen   int
	Precision int
	Scale     int
	Gen       func(numRows int) (any, []int16)
}

type writeWrapper struct {
	Writer storage.ExternalFileWriter
}

func (*writeWrapper) Seek(_ int64, _ int) (int64, error) {
	return 0, nil
}

func (*writeWrapper) Read(_ []byte) (int, error) {
	return 0, nil
}

func (w *writeWrapper) Write(b []byte) (int, error) {
	return w.Writer.Write(context.Background(), b)
}

func (w *writeWrapper) Close() error {
	return w.Writer.Close(context.Background())
}

func getStore(path string) (storage.ExternalStorage, error) {
	s, err := storage.ParseBackend(path, nil)
	if err != nil {
		return nil, err
	}

	store, err := storage.NewWithDefaultOpt(context.Background(), s)
	if err != nil {
		return nil, err
	}
	return store, nil
}

// WriteParquetFile writes a simple Parquet file with the specified columns and number of rows.
// It's used for test and DON'T use this function to generate large Parquet files.
func WriteParquetFile(path, fileName string, pcolumns []ParquetColumn, rows int, addOpts ...parquet.WriterProperty) error {
	s, err := getStore(path)
	if err != nil {
		return err
	}
	writer, err := s.Create(context.Background(), fileName, nil)
	if err != nil {
		return err
	}
	wrapper := &writeWrapper{Writer: writer}

	fields := make([]schema.Node, len(pcolumns))
	opts := make([]parquet.WriterProperty, 0, len(pcolumns)*2)
	for i, pc := range pcolumns {
		typeLen := -1
		if pc.TypeLen > 0 {
			typeLen = pc.TypeLen
		}
		if fields[i], err = schema.NewPrimitiveNodeConverted(
			pc.Name,
			parquet.Repetitions.Optional,
			pc.Type, pc.Converted,
			typeLen, pc.Precision, pc.Scale,
			-1,
		); err != nil {
			return err
		}
		opts = append(opts, parquet.WithDictionaryFor(pc.Name, true))
		opts = append(opts, parquet.WithCompressionFor(pc.Name, compress.Codecs.Snappy))
	}

	node, _ := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, -1)
	opts = append(opts, addOpts...)
	props := parquet.NewWriterProperties(opts...)
	pw := file.NewParquetWriter(wrapper, node, file.WithWriterProps(props))
	//nolint: errcheck
	defer pw.Close()

	// Only one row group for simplicity
	rgw := pw.AppendRowGroup()
	//nolint: errcheck
	defer rgw.Close()

	for _, pc := range pcolumns {
		cw, err := rgw.NextColumn()
		if err != nil {
			return err
		}
		vals, defLevel := pc.Gen(rows)

		switch w := cw.(type) {
		case *file.Int96ColumnChunkWriter:
			buf, _ := vals.([]parquet.Int96)
			_, err = w.WriteBatch(buf, defLevel, nil)
		case *file.Int64ColumnChunkWriter:
			buf, _ := vals.([]int64)
			_, err = w.WriteBatch(buf, defLevel, nil)
		case *file.Float64ColumnChunkWriter:
			buf, _ := vals.([]float64)
			_, err = w.WriteBatch(buf, defLevel, nil)
		case *file.ByteArrayColumnChunkWriter:
			buf, _ := vals.([]parquet.ByteArray)
			_, err = w.WriteBatch(buf, defLevel, nil)
		case *file.Int32ColumnChunkWriter:
			buf, _ := vals.([]int32)
			_, err = w.WriteBatch(buf, defLevel, nil)
		case *file.BooleanColumnChunkWriter:
			buf, _ := vals.([]bool)
			_, err = w.WriteBatch(buf, defLevel, nil)
		default:
			return fmt.Errorf("unsupported column type %T", cw)
		}

		if err != nil {
			return err
		}
		if err := cw.Close(); err != nil {
			return err
		}
	}

	return nil
}
