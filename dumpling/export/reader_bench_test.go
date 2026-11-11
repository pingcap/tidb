// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
	"io"
	"strings"
	"testing"

	tcontext "github.com/pingcap/tidb/dumpling/context"
)

// BenchmarkTableDataReader measures the copying, batching and channel overhead
// of overlapping raw-row reads with SQL/CSV/Parquet formatting. The source copies
// and decodes a fixed text-protocol row into a reused packet buffer; the sink is
// io.Discard. It does not model database/network latency or storage throughput,
// so these results alone do not predict an end-to-end export speedup.
func BenchmarkTableDataReader(b *testing.B) {
	const rows = 4096
	infos := []*ColumnInfo{
		{Name: "id", DatabaseTypeName: "BIGINT"},
		{Name: "tenant", DatabaseTypeName: "INT"},
		{Name: "amount", DatabaseTypeName: "DECIMAL", Precision: 10, Scale: 2},
		{Name: "created", DatabaseTypeName: "DATETIME"},
		{Name: "state", DatabaseTypeName: "VARCHAR"},
		{Name: "email", DatabaseTypeName: "VARCHAR"},
		{Name: "label", DatabaseTypeName: "VARCHAR"},
		{Name: "pad", DatabaseTypeName: "TEXT"},
		{Name: "payload", DatabaseTypeName: "TEXT"},
		{Name: "checksum", DatabaseTypeName: "VARCHAR"},
		{Name: "optional", DatabaseTypeName: "TEXT", Nullable: true},
	}
	values := []sql.RawBytes{
		[]byte("1234567890"),
		[]byte("123456"),
		[]byte("1234.56"),
		[]byte("2026-11-12 12:34:56.123456"),
		[]byte("exported"),
		[]byte("benchmark@example.invalid"),
		[]byte(strings.Repeat("label-", 6)),
		[]byte(strings.Repeat("p", 640)),
		[]byte(strings.Repeat("a'\"\\,\n\tZ", 32)),
		[]byte(strings.Repeat("01234567", 4)),
		nil,
	}
	var packet []byte
	var rowBytes int
	for _, value := range values {
		rowBytes += len(value)
		switch {
		case value == nil:
			packet = append(packet, 0xfb)
		case len(value) < 251:
			packet = append(packet, byte(len(value)))
		default:
			packet = append(packet, 0xfc, byte(len(value)), byte(len(value)>>8))
		}
		packet = append(packet, value...)
	}
	meta := newMockTableIRWithColumnInfo("benchmark", "wide_row", nil, nil, infos)
	for _, info := range infos {
		meta.colNames = append(meta.colNames, info.Name)
	}

	for _, format := range []FileFormat{FileFormatSQLText, FileFormatCSV, FileFormatParquet} {
		b.Run(format.Extension(), func(b *testing.B) {
			for _, pipeline := range []bool{false, true} {
				name := "synchronous"
				if pipeline {
					name = "pipeline"
				}
				b.Run(name, func(b *testing.B) {
					conf := DefaultConfig()
					tctx := tcontext.Background()
					m := &metrics{}
					b.SetBytes(int64(rows * rowBytes))
					b.ReportAllocs()
					b.ResetTimer()
					for range b.N {
						source := &benchmarkTableData{
							mockTableIR: meta,
							iter: &benchmarkRawRowIter{
								packet: packet,
								buffer: make([]byte, len(packet)),
								args:   make([]any, len(infos)),
								rows:   rows,
							},
						}
						var data TableDataIR = source
						var reader *tableDataReader
						if pipeline {
							readCtx, cancel := tctx.WithCancel()
							reader = newTableDataReader(readCtx, cancel, meta, source)
							data = reader
						}
						n, err := format.WriteInsert(tctx, conf, meta, data, benchmarkDiscardWriter{}, m)
						if reader != nil {
							_ = reader.Close()
						}
						_ = source.Close()
						if err != nil {
							b.Fatal(err)
						}
						if n != rows {
							b.Fatalf("wrote %d rows, expected %d", n, rows)
						}
					}
				})
			}
		})
	}
}

type benchmarkTableData struct {
	*mockTableIR
	iter *benchmarkRawRowIter
}

func (d *benchmarkTableData) Rows() SQLRowIter { return d.iter }

type benchmarkRawRowIter struct {
	packet []byte
	buffer []byte
	args   []any
	rows   int
	index  int
}

func (i *benchmarkRawRowIter) Decode(row RowReceiver) error {
	// Every value borrows this buffer until the next Decode, like sql.RawBytes
	// returned by the database driver. The pipeline must copy before advancing.
	copy(i.buffer, i.packet)
	// Vary the final ID digit without allocating or changing packet lengths.
	i.buffer[10] = byte('0' + i.index%10)
	row.BindAddress(i.args)
	offset := 0
	for _, arg := range i.args {
		target := arg.(*sql.RawBytes)
		length := int(i.buffer[offset])
		offset++
		switch length {
		case 0xfb:
			*target = nil
			continue
		case 0xfc:
			length = int(i.buffer[offset]) | int(i.buffer[offset+1])<<8
			offset += 2
		}
		*target = i.buffer[offset : offset+length]
		offset += length
	}
	return nil
}

func (i *benchmarkRawRowIter) Next()         { i.index++ }
func (i *benchmarkRawRowIter) HasNext() bool { return i.index < i.rows }
func (*benchmarkRawRowIter) Error() error    { return nil }
func (*benchmarkRawRowIter) Close() error    { return nil }

type benchmarkDiscardWriter struct{}

func (benchmarkDiscardWriter) Write(_ context.Context, p []byte) (int, error) {
	return io.Discard.Write(p)
}

func (benchmarkDiscardWriter) Close(context.Context) error { return nil }
