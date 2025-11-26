// Copyright 2023-2023 PingCAP, Inc.

package mydump

import (
	"context"
	"io"
	"reflect"
	"testing"
	"time"

	"gitee.com/joccau/orc"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/types"
	orc_proto "github.com/scritchley/orc/proto"
	"github.com/stretchr/testify/require"
)

func createMockOrcParser(orcFilePath string) (*ORCParser, error) {
	ctx := context.Background()
	mockStore, err := storage.NewLocalStorage("./orc/")
	if err != nil {
		return nil, errors.Trace(err)
	}

	filemeta := SourceFileMeta{
		Path:        orcFilePath,
		Type:        SourceTypeORC,
		Compression: CompressionNone,
	}

	reader, err := OpenReader(ctx, &filemeta, mockStore, storage.DecompressConfig{})
	if err != nil {
		return nil, errors.Trace(err)
	}

	parser, err := NewORCParser(log.L(), reader, "")
	return parser, err
}

func TestReadOrcSchemas(t *testing.T) {
	orcFileName := "TestOrcFile.testDate1900.orc"
	parser, err := createMockOrcParser(orcFileName)
	require.NoError(t, err)
	defer parser.Close()

	require.Equal(t, []string{"time", "date"}, parser.columnName)
	require.Equal(t, 2, len(parser.columnType))
	require.Equal(t, orc_proto.Type_TIMESTAMP, parser.columnType[0].Type().GetKind())
	require.Equal(t, orc_proto.Type_DATE, parser.columnType[1].Type().GetKind())

	// test column
	columns := parser.Columns()
	require.Equal(t, columns, parser.columnName)
}

func TestReadOrcFileRowCount(t *testing.T) {
	ctx := context.Background()
	orcFileName := "TestOrcFile.testDate1900.orc"
	mockStore, err := storage.NewLocalStorage("./orc/")
	require.NoError(t, err)

	filemeta := SourceFileMeta{
		Path:        orcFileName,
		Type:        SourceTypeORC,
		Compression: CompressionNone,
	}

	reader, err := OpenReader(ctx, &filemeta, mockStore, storage.DecompressConfig{})
	require.NoError(t, err)
	parser, err := NewORCParser(log.L(), reader, "")
	require.NoError(t, err)
	defer parser.Close()

	count := parser.NumRows()
	require.Equal(t, count, int(70000))

	count2, err := ReadOrcFileRowCountByFile(ctx, mockStore, filemeta)
	require.NoError(t, err)
	require.Equal(t, count2, int64(70000))
}

func TestOrcReadRow(t *testing.T) {
	orcFileName := "TestOrcFile.testDate1900.orc"
	parser, err := createMockOrcParser(orcFileName)
	require.NoError(t, err)
	defer parser.Close()

	var count int64 = 0
	for {
		if err = parser.ReadRow(); err != io.EOF {
			count++
			row := parser.LastRow()
			require.Equal(t, row.RowID, count)
			pos, rowid := parser.Pos()
			require.Equal(t, pos, count)
			require.Equal(t, rowid, count)
		} else {
			break
		}
	}
	require.Equal(t, err, io.EOF)
	require.Equal(t, count, int64(70000))
}

func TestCalStripeIdx(t *testing.T) {
	orcFileName := "TestOrcFile.testDate1900.orc"
	parser, err := createMockOrcParser(orcFileName)
	require.NoError(t, err)
	defer parser.Close()

	cases := []struct {
		pos            int64
		stripeIdx      int64
		rowIdxInStripe int64
		result         bool
	}{
		{
			pos:            0,
			stripeIdx:      0,
			rowIdxInStripe: 0,
			result:         true,
		},
		{
			pos:            1,
			stripeIdx:      0,
			rowIdxInStripe: 1,
			result:         true,
		},
		{
			pos:            15000,
			stripeIdx:      0,
			rowIdxInStripe: 15000,
			result:         true,
		},
		{
			pos:            15001,
			stripeIdx:      1,
			rowIdxInStripe: 1,
			result:         true,
		},
		{
			pos:            25000,
			stripeIdx:      1,
			rowIdxInStripe: 10000,
			result:         true,
		},
		{
			pos:            70000,
			stripeIdx:      7,
			rowIdxInStripe: 10000,
			result:         true,
		},
		{
			pos:    70001,
			result: false,
		},
	}

	for _, c := range cases {
		stripeIdx, rowIdxInStripe, err := parser.calStripeIdx(c.pos)
		if c.result {
			require.NoError(t, err)
			require.Equal(t, c.stripeIdx, stripeIdx)
			require.Equal(t, c.rowIdxInStripe, rowIdxInStripe)
		} else {
			require.NotNil(t, err)
		}
	}
}

func TestOrcSetPos(t *testing.T) {
	orcFileName := "TestOrcFile.columnProjection.orc"
	parser, err := createMockOrcParser(orcFileName)
	require.NoError(t, err)
	defer parser.Close()
	require.Equal(t, parser.NumRows(), int(21000))

	// test SetPos
	err = parser.SetPos(10000, 10000)
	require.NoError(t, err)
	require.Equal(t, parser.pos, int64(10000))
	require.Equal(t, parser.stripeIdx, int64(1))
	require.Equal(t, parser.rowIdxInStripe, int64(5000))

	err = parser.ReadRow()
	require.NoError(t, err)
	row := parser.LastRow()
	require.Equal(t, row.Row[0], types.NewDatum(-578147096))
}

func TestOrcSetPosEnd(t *testing.T) {
	orcFileName := "TestOrcFile.columnProjection.orc"
	parser, err := createMockOrcParser(orcFileName)
	require.NoError(t, err)
	defer parser.Close()

	require.Equal(t, parser.NumRows(), int(21000))

	// test SetPos
	err = parser.SetPos(21000, 21000)
	require.NoError(t, err)
	require.Equal(t, parser.pos, int64(21000))
	require.Equal(t, parser.stripeIdx, int64(4))
	require.Equal(t, parser.rowIdxInStripe, int64(1000))

	err = parser.ReadRow()
	require.Equal(t, err, io.EOF)
}

func TestOrcKinds(t *testing.T) {
	orcFileName := "TestOrcFile.test1.orc"
	parser, err := createMockOrcParser(orcFileName)
	require.NoError(t, err)
	defer parser.Close()

	columns := parser.Columns()
	require.Equal(t, len(columns), 12)
	require.Equal(t, columns, []string{
		"boolean1",
		"byte1",
		"short1",
		"int1",
		"long1",
		"float1",
		"double1",
		"bytes1",
		"string1",
		"middle",
		"list",
		"map",
	})

	// check the first type: struct
	schema := parser.Reader.Schema()
	require.Equal(t, *schema.Type().Kind, orc_proto.Type_STRUCT)

	// check all of subtypes
	subTypes := make([]orc_proto.Type_Kind, 0, len(columns))
	for _, column := range columns {
		subSchema, err := schema.GetField(column)
		require.NoError(t, err)
		subTypes = append(subTypes, subSchema.Type().GetKind())
	}
	require.Equal(t, len(subTypes), 12)
	require.Equal(t, subTypes, []orc_proto.Type_Kind{
		orc_proto.Type_BOOLEAN,
		orc_proto.Type_BYTE,
		orc_proto.Type_SHORT,
		orc_proto.Type_INT,
		orc_proto.Type_LONG,
		orc_proto.Type_FLOAT,
		orc_proto.Type_DOUBLE,
		orc_proto.Type_BINARY,
		orc_proto.Type_STRING,
		orc_proto.Type_STRUCT,
		orc_proto.Type_LIST,
		orc_proto.Type_MAP,
	})
}

func TestOrcReflectKind(t *testing.T) {
	orcFileName := "TestOrcFile.test1.orc"
	parser, err := createMockOrcParser(orcFileName)
	require.NoError(t, err)
	defer parser.Close()

	hasNext := parser.cursor.Stripes()
	require.True(t, hasNext)
	hasNext = parser.cursor.Next()
	require.True(t, hasNext)

	row := parser.cursor.Row()
	require.Equal(t, len(row), 12)

	types := make([]reflect.Kind, 0, len(row))
	for _, value := range row {
		t := reflect.TypeOf(value).Kind()
		types = append(types, t)
	}
	require.Equal(t, []reflect.Kind{
		reflect.Bool,
		reflect.Int8,
		reflect.Int64,
		reflect.Int64,
		reflect.Int64,
		reflect.Float32,
		reflect.Float64,
		reflect.Slice,
		reflect.String,
		reflect.Map,
		reflect.Slice,
		reflect.Slice,
	}, types, types)
}

func TestOrcValues(t *testing.T) {
	orcFileName := "TestOrcFile.test1.orc"
	parser, err := createMockOrcParser(orcFileName)
	require.NoError(t, err)
	defer parser.Close()

	// test the 1th record.
	err = parser.ReadRow()
	require.NoError(t, err)
	row := parser.LastRow()
	require.Equal(t, 12, len(row.Row))

	/*
		the first row content is:
			{
				"boolean1": false,
				"byte1": 1,
				"short1": 1024,
				"int1": 65536,
				"long1": 9223372036854775807,
				"float1": 1,
				"double1": -15,
				"bytes1": [0, 1, 2, 3, 4],
				"string1": "hi",
				"middle": {"list": [{"int1": 1, "string1": "bye"}, {"int1": 2, "string1": "sigh"}]},
				"list": [{"int1": 3, "string1": "good"}, {"int1": 4, "string1": "bad"}],
				 "map": []
			}
	*/
	require.Equal(t, row.Row[0], types.NewDatum(false))
	require.Equal(t, row.Row[1], types.NewDatum(1))
	require.Equal(t, row.Row[2], types.NewDatum(1024))
	require.Equal(t, row.Row[3], types.NewDatum(65536))
	require.Equal(t, row.Row[4], types.NewDatum(9223372036854775807))
	require.Equal(t, row.Row[5], types.NewDatum(float32(1)))
	require.Equal(t, row.Row[6], types.NewDatum(float64(-15)))
	require.Equal(t, row.Row[7], types.NewDatum([]byte{0, 1, 2, 3, 4}))
	require.Equal(t, row.Row[8], types.NewDatum("hi"))
	// test struct kind
	require.Equal(
		t,
		row.Row[9].String(),
		"KindMysqlJSON {\"list\": [{\"int1\": 1, \"string1\": \"bye\"}, {\"int1\": 2, \"string1\": \"sigh\"}]}",
		row.Row[9].GetMysqlJSON().String(),
	)
	// test list kind
	require.Equal(
		t,
		row.Row[10].String(),
		"KindMysqlJSON [{\"int1\": 3, \"string1\": \"good\"}, {\"int1\": 4, \"string1\": \"bad\"}]",
		row.Row[10].GetMysqlJSON().String(),
	)
	// test map kind
	require.Equal(
		t,
		row.Row[11].String(),
		"KindMysqlJSON []",
		row.Row[11].GetMysqlJSON().String(),
	)

	// test the 2th records to check the struct/list/map kind.
	/*
		the second record content is:
		{
			"boolean1": true,
			"byte1": 100,
			"short1": 2048,
			"int1": 65536,
			"long1": 9223372036854775807,
			"float1": 2, "double1": -5,
			"bytes1": [], "string1": "bye",
			"middle": {
				"list": [
					{"int1": 1, "string1": "bye"},
					{"int1": 2, "string1": "sigh"}
				],
			},
			"list": [
				{"int1": 100000000, "string1": "cat"},
				{"int1": -100000, "string1": "in"},
				{"int1": 1234, "string1": "hat"},
			],
			"map": [
				{
					"key": "chani",
					"value": {
						"int1": 5,
						"string1": "chani",
					}
				},
				{
					"key": "mauddib",
					"value": {
						"int1": 1,
						"string1": "mauddib",
					}
				},
			]
		}
	*/
	err = parser.ReadRow()
	require.NoError(t, err)
	row = parser.LastRow()
	require.Equal(t, 12, len(row.Row))
	require.Equal(
		t,
		row.Row[9].String(),
		"KindMysqlJSON {\"list\": [{\"int1\": 1, \"string1\": \"bye\"}, {\"int1\": 2, \"string1\": \"sigh\"}]}",
		row.Row[9].GetMysqlJSON().String(),
	)
	// test list kind
	require.Equal(
		t,
		row.Row[10].String(),
		"KindMysqlJSON [{\"int1\": 100000000, \"string1\": \"cat\"}, {\"int1\": -100000, \"string1\": \"in\"}, {\"int1\": 1234, \"string1\": \"hat\"}]",
		row.Row[10].GetMysqlJSON().String(),
	)
	// test map kind
	require.Equal(
		t,
		row.Row[11].String(),
		"KindMysqlJSON [{\"key\": \"chani\", \"value\": {\"int1\": 5, \"string1\": \"chani\"}}, {\"key\": \"mauddib\", \"value\": {\"int1\": 1, \"string1\": \"mauddib\"}}]",
		row.Row[11].GetMysqlJSON().String(),
	)
}

// TestOrcDateAndTimestamp tests the orc format with data and timestamp.
func TestOrcTimestampValue(t *testing.T) {
	orcFileName := "TestOrcFile.testDate1900.orc"
	parser, err := createMockOrcParser(orcFileName)
	require.NoError(t, err)
	defer parser.Close()

	// read the first stripe
	hasNext := parser.cursor.Stripes()
	require.True(t, hasNext)
	hasNext = parser.cursor.Next()
	require.True(t, hasNext)

	// read the first row
	row := parser.cursor.Row()
	require.Equal(t, len(row), 2)

	ts, ok := row[0].(time.Time)
	require.True(t, ok)
	require.Equal(t, "1900-05-05 12:34:56.1 +0000 UTC", ts.String())
	date, ok := row[1].(orc.Date)
	require.True(t, ok)
	require.Equal(t, "1900-12-25 00:00:00 +0000 UTC", date.String())
}

// TestOrcUnionValue tests the kind of union and deciaml.
func TestOrcUnionValue(t *testing.T) {
	orcFileName := "TestOrcFile.testUnionAndTimestamp.orc"
	parser, err := createMockOrcParser(orcFileName)
	require.NoError(t, err)
	defer parser.Close()

	// read the 1th record.
	err = parser.ReadRow()
	require.NoError(t, err)
	row := parser.LastRow()
	require.Equal(t, 3, len(row.Row))

	// test timestamp
	name := parser.columnName[0]
	require.Equal(t, name, "time")
	kind := parser.columnType[0].Type().GetKind()
	require.Equal(t, kind, orc_proto.Type_TIMESTAMP)
	require.Equal(t, row.Row[0].String(), "KindMysqlTime 2000-03-12 15:00:00.000000", row.Row[0].String())

	// test union
	name = parser.columnName[1]
	require.Equal(t, name, "union")
	kind = parser.columnType[1].Type().GetKind()
	require.Equal(t, kind, orc_proto.Type_UNION)
	require.Equal(t,
		row.Row[1].String(),
		"KindMysqlJSON {\"tag\": 0, \"value\": 42}",
		row.Row[1].String(),
	)

	// test decimal
	name = parser.columnName[2]
	require.Equal(t, name, "decimal")
	kind = parser.columnType[2].Type().GetKind()
	require.Equal(t, kind, orc_proto.Type_DECIMAL)
	mydecimal := &types.MyDecimal{}
	mydecimal.FromFloat64(12345678.654745600000000000)
	require.Equal(t, row.Row[2], types.NewDatum(mydecimal))
}

func TestOrcNullValue(t *testing.T) {
	orcFileName := "TestOrcFile.testUnionAndTimestamp.orc"
	parser, err := createMockOrcParser(orcFileName)
	require.NoError(t, err)
	defer parser.Close()

	// read the 3th record.
	parser.SetPos(2, 2)
	err = parser.ReadRow()
	require.NoError(t, err)
	row := parser.LastRow()
	require.Equal(t, 3, len(row.Row))

	types.NewDatum(nil)
	require.Equal(t, row.Row[0], types.NewDatum(nil), row.Row[0])
	require.Equal(t, row.Row[1], types.NewDatum(nil), row.Row[0])
	require.Equal(t, row.Row[2], types.NewDatum(nil), row.Row[0])
}
