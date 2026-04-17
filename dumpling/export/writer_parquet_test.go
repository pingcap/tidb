package export

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"path"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/types"
)

func TestWriteTableDataWithParquet(t *testing.T) {
	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.FileType = "parquet"
	config.OutputDirPath = dir
	config.ParquetPageSize = 1024 * 1024
	config.ParquetCompressType = NoCompression
	// mock data
	row := make([]driver.Value, 0, len(allTypesColumnInfos))
	for _, c := range allTypesColumnInfos {
		switch c.Type {
		case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
			row = append(row, "byte_array_string")
		case "DATE":
			row = append(row, "1977-01-01")
		case "TIME":
			row = append(row, "23:59:59")
		case "JSON":
			row = append(row, "{\"a\": 1, \"b\": \"2\"}")
		case "VECTOR":
			row = append(row, "[1,2,3]")
		case "ENUM":
			row = append(row, "byte_array_enum")
		case "SET":
			row = append(row, "a,b")
		case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY":
			byteValue, _ := hex.DecodeString("1520c5")
			row = append(row, byteValue)
		case "BIT":
			byteValue := []byte{0x55}
			row = append(row, byteValue)
		case "TIMESTAMP":
			row = append(row, "1973-12-30 15:30:00")
		case "DATETIME":
			row = append(row, "9999-12-31 23:59:59")
		case "TINYINT", "SMALLINT", "MEDIUMINT", "UNSIGNED TINYINT", "UNSIGNED SMALLINT", "UNSIGNED MEDIUMINT", "YEAR":
			row = append(row, "1995")
		case "INT":
			row = append(row, "-2147483648")
		case "UNSIGNED INT":
			row = append(row, "4294967295")
		case "BIGINT":
			row = append(row, "-9223372036854775808")
		case "UNSIGNED BIGINT":
			row = append(row, "18446744073709551615")
		case "FLOAT", "DOUBLE":
			row = append(row, "123.123")
		case "DECIMAL":
			if c.Precision == 9 {
				row = append(row, "12345678.9")
			} else if c.Precision == 18 {
				row = append(row, "12345678912345678.9")
			} else if c.Precision == 38 {
				row = append(row, "1234567890123456789012345678901234567.8")
			} else if c.Precision == 40 {
				row = append(row, "123456789012345678901234567890123456780.9")
			} else {
				t.FailNow()
			}
		default:
			t.FailNow()
		}
	}
	data := [][]driver.Value{row}
	// parquet need more information to write, so we need to provide columnInfos
	tableIR := newMockTableIRWithColumnInfo("test", "employee", data, nil, allTypesColumnInfos)
	// write parquet
	writer := createTestWriter(config, t)
	err := writer.WriteTableData(tableIR, tableIR, 0)
	require.NoError(t, err)

	// read parquet and check answer
	fr, err := local.NewLocalFileReader(path.Join(config.OutputDirPath, "test.employee.000000000.parquet"))
	require.NoError(t, err)
	pr, err := reader.NewParquetReader(fr, new(ParquetRow), 1)
	require.NoError(t, err)
	readRows := make([]ParquetRow, 1)
	err = pr.Read(&readRows)
	require.NoError(t, err)
	readRow := readRows[0]

	for i, c := range allTypesColumnInfos {
		names := strings.Split(c.Name, "_")
		structName := ""
		for j := 1; j < len(names); j++ {
			structName += fmt.Sprintf("%s%s", strings.ToUpper(names[j][:1]), names[j][1:])
		}
		switch c.Type {
		case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT", "DATE", "TIME", "JSON", "VECTOR", "ENUM", "SET":
			value := reflect.ValueOf(readRow).FieldByName(structName).String()
			require.Equal(t, data[0][i], value)
		case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB", "BINARY", "VARBINARY", "BIT":
			value := reflect.ValueOf(readRow).FieldByName(structName).String()
			require.Equal(t, data[0][i], []byte(value))
		case "TIMESTAMP", "DATETIME":
			value := reflect.ValueOf(readRow).FieldByName(structName).Interface().(*int64)
			require.Equal(t, data[0][i], time.UnixMicro(*value).UTC().Format(time.DateTime))
		case "TINYINT", "UNSIGNED TINYINT", "SMALLINT", "MEDIUMINT", "UNSIGNED SMALLINT", "UNSIGNED MEDIUMINT", "YEAR", "INT":
			value := reflect.ValueOf(readRow).FieldByName(structName).Int()
			require.Equal(t, data[0][i], strconv.FormatInt(value, 10))
		case "UNSIGNED INT", "BIGINT":
			value := reflect.ValueOf(readRow).FieldByName(structName).Int()
			require.Equal(t, data[0][i], strconv.FormatInt(value, 10))
		case "UNSIGNED BIGINT":
			value := reflect.ValueOf(readRow).FieldByName(structName).String()
			bigint := types.DECIMAL_BYTE_ARRAY_ToString([]byte(value), 20, 0)
			require.Equal(t, data[0][i], bigint)
		case "FLOAT", "DOUBLE":
			value := reflect.ValueOf(readRow).FieldByName(structName).Float()
			require.Equal(t, data[0][i], fmt.Sprintf("%.3f", value))
		case "DECIMAL":
			if c.Precision == 9 || c.Precision == 18 {
				value := reflect.ValueOf(readRow).FieldByName(structName).Int()
				stringInt := strconv.FormatInt(value, 10)
				result := stringInt[:len(stringInt)-int(c.Scale)] + "." + stringInt[len(stringInt)-int(c.Scale):]
				require.Equal(t, data[0][i], result)
			} else if c.Precision == 38 || c.Precision == 40 {
				value := reflect.ValueOf(readRow).FieldByName(structName).String()
				require.Equal(t, data[0][i], value)
			}
		}
	}
}

type ParquetRow struct {
	Tinyint           int32   `parquet:"name=t_tinyint, type=INT32"`
	TinyintUnsigned   int32   `parquet:"name=t_tinyint_unsigned, type=INT32"`
	Smallint          int32   `parquet:"name=t_smallint, type=INT32"`
	SmallintUnsigned  int32   `parquet:"name=t_smallint_unsigned, type=INT32"`
	Mediumint         int32   `parquet:"name=t_mediumint, type=INT32"`
	MediumintUnsigned int32   `parquet:"name=t_mediumint_unsigned, type=INT32"`
	Int               int32   `parquet:"name=t_int, type=INT32"`
	IntUnsigned       int64   `parquet:"name=t_int_unsigned, type=INT64"`
	Bigint            int64   `parquet:"name=t_bigint, type=INT64"`
	BigintUnsigned    string  `parquet:"name=t_bigint_unsigned, type=FIXED_LEN_BYTE_ARRAY,length=9"`
	Float             float64 `parquet:"name=t_float, type=FLOAT"`
	Double            float64 `parquet:"name=t_double, type=DOUBLE"`
	Char              string  `parquet:"name=t_char, type=BYTE_ARRAY"`
	Varchar           string  `parquet:"name=t_varchar, type=BYTE_ARRAY"`
	Binary            string  `parquet:"name=t_binary, type=BYTE_ARRAY"`
	Varbinary         string  `parquet:"name=t_varbinary, type=BYTE_ARRAY"`
	Tinytext          string  `parquet:"name=t_tinytext, type=BYTE_ARRAY"`
	Text              string  `parquet:"name=t_text, type=BYTE_ARRAY"`
	Mediumtext        string  `parquet:"name=t_mediumtext, type=BYTE_ARRAY"`
	Longtext          string  `parquet:"name=t_longtext, type=BYTE_ARRAY"`
	Tinyblob          string  `parquet:"name=t_tinyblob, type=BYTE_ARRAY"`
	Blob              string  `parquet:"name=t_blob, type=BYTE_ARRAY"`
	Mediumblob        string  `parquet:"name=t_mediumblob, type=BYTE_ARRAY"`
	Longblob          string  `parquet:"name=t_longblob, type=BYTE_ARRAY"`
	Date              string  `parquet:"name=t_date, type=BYTE_ARRAY"`
	Datetime          *int64  `parquet:"name=t_datetime, type=INT64"`
	Timestamp         *int64  `parquet:"name=t_timestamp, type=INT64"`
	Time              string  `parquet:"name=t_time, type=BYTE_ARRAY"`
	Year              int32   `parquet:"name=t_year, type=INT32"`
	Enum              string  `parquet:"name=t_enum, type=BYTE_ARRAY"`
	Set               string  `parquet:"name=t_set, type=BYTE_ARRAY"`
	Bit               string  `parquet:"name=t_bit, type=BYTE_ARRAY"`
	Json              string  `parquet:"name=t_json, type=BYTE_ARRAY"`
	Decimal9          int32   `parquet:"name=t_decimal9, type=INT32"`
	Decimal18         int64   `parquet:"name=t_decimal18, type=INT64"`
	Decimal38         string  `parquet:"name=t_decimal38, type=BYTE_ARRAY"`
	Decimal40         string  `parquet:"name=t_decimal40, type=BYTE_ARRAY"`
	Vector            string  `parquet:"name=t_vector, type=BYTE_ARRAY"`
}

var allTypesColumnInfos = []*ColumnInfo{
	{
		Name: "t_tinyint",
		Type: "TINYINT",
	},
	{
		Name: "t_tinyint_unsigned",
		Type: "UNSIGNED TINYINT",
	},
	{
		Name: "t_smallint",
		Type: "SMALLINT",
	},
	{
		Name: "t_smallint_unsigned",
		Type: "UNSIGNED SMALLINT",
	},
	{
		Name: "t_mediumint",
		Type: "MEDIUMINT",
	},
	{
		Name: "t_mediumint_unsigned",
		Type: "UNSIGNED MEDIUMINT",
	},
	{
		Name: "t_int",
		Type: "INT",
	},
	{
		Name: "t_int_unsigned",
		Type: "UNSIGNED INT",
	},
	{
		Name: "t_bigint",
		Type: "BIGINT",
	},
	{
		Name: "t_bigint_unsigned",
		Type: "UNSIGNED BIGINT",
	},
	{
		Name: "t_float",
		Type: "FLOAT",
	},
	{
		Name: "t_double",
		Type: "DOUBLE",
	},
	{
		Name: "t_char",
		Type: "CHAR",
	},
	{
		Name: "t_varchar",
		Type: "VARCHAR",
	},
	{
		Name: "t_binary",
		Type: "BINARY",
	},
	{
		Name: "t_varbinary",
		Type: "VARBINARY",
	},
	{
		Name: "t_tinytext",
		Type: "TINYTEXT",
	},
	{
		Name: "t_text",
		Type: "TEXT",
	},
	{
		Name: "t_mediumtext",
		Type: "MEDIUMTEXT",
	},
	{
		Name: "t_longtext",
		Type: "LONGTEXT",
	},
	{
		Name: "t_tinyblob",
		Type: "TINYBLOB",
	},
	{
		Name: "t_blob",
		Type: "BLOB",
	},
	{
		Name: "t_mediumblob",
		Type: "MEDIUMBLOB",
	},
	{
		Name: "t_longblob",
		Type: "LONGBLOB",
	},
	{
		Name: "t_date",
		Type: "DATE",
	},
	{
		Name: "t_datetime",
		Type: "DATETIME",
	},
	{
		Name: "t_timestamp",
		Type: "TIMESTAMP",
	},
	{
		Name: "t_time",
		Type: "TIME",
	},
	{
		Name: "t_year",
		Type: "YEAR",
	},
	{
		Name: "t_enum",
		Type: "ENUM",
	},
	{
		Name: "t_set",
		Type: "SET",
	},
	{
		Name: "t_bit",
		Type: "BIT",
	},
	{
		Name: "t_json",
		Type: "JSON",
	},
	{
		Name:      "t_decimal9",
		Type:      "DECIMAL",
		Precision: 9,
		Scale:     1,
	},
	{
		Name:      "t_decimal18",
		Type:      "DECIMAL",
		Precision: 18,
		Scale:     1,
	},
	{
		Name:      "t_decimal38",
		Type:      "DECIMAL",
		Precision: 38,
		Scale:     1,
	},
	{
		Name:      "t_decimal40",
		Type:      "DECIMAL",
		Precision: 40,
		Scale:     1,
	},
	{
		Name: "t_vector",
		Type: "VECTOR",
	},
}

func TestWriteParquetWithInvalidDate(t *testing.T) {
	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.FileType = "parquet"
	config.OutputDirPath = dir
	config.ParquetPageSize = 1024 * 1024
	config.ParquetCompressType = NoCompression

	columnInfos := []*ColumnInfo{
		{
			Name: "t_datetime",
			Type: "DATETIME",
		},
		{
			Name: "t_timestamp",
			Type: "TIMESTAMP",
		},
	}
	// mock data
	mockDatas := [][]string{
		{"1971-00-00 00:00:00", "0000-00-00 00:00:00"}, // invalid date
		{"0000-00-00 00:00:00", "0000-00-00 00:00:00"}, // zero date
	}
	data := make([][]driver.Value, len(mockDatas))
	for i, mockData := range mockDatas {
		row := make([]driver.Value, 0, len(columnInfos))
		for _, mock := range mockData {
			row = append(row, mock)
		}
		data[i] = row
	}
	// parquet need more information to write, so we need to provide columnInfos
	tableIR := newMockTableIRWithColumnInfo("test", "date", data, nil, columnInfos)
	// write parquet
	writer := createTestWriter(config, t)
	err := writer.WriteTableData(tableIR, tableIR, 0)
	require.NoError(t, err)

	// read parquet and check answer
	type dateRow struct {
		Datetime  *int64 `parquet:"name=t_datetime, type=INT64"`
		Timestamp *int64 `parquet:"name=t_timestamp, type=INT64"`
	}

	fr, err := local.NewLocalFileReader(path.Join(config.OutputDirPath, "test.date.000000000.parquet"))
	require.NoError(t, err)
	pr, err := reader.NewParquetReader(fr, new(dateRow), 1)
	require.NoError(t, err)
	readRows := make([]dateRow, len(mockDatas))
	err = pr.Read(&readRows)
	require.NoError(t, err)

	exceptedResult := [][]*int64{
		{nil, nil},
		{nil, nil},
	}

	for i, row := range readRows {
		for j, column := range columnInfos {
			if column.Type == "DATETIME" {
				require.Equal(t, exceptedResult[i][j], row.Datetime)
			} else if column.Type == "TIMESTAMP" {
				require.Equal(t, exceptedResult[i][j], row.Timestamp)
			}
		}
	}
}
