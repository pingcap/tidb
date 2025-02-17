// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/storage"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/stretchr/testify/require"
)

func TestWriteMeta(t *testing.T) {
	createTableStmt := "CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	specCmts := []string{"/*!40103 SET TIME_ZONE='+00:00' */;"}
	meta := newMockMetaIR("t1", createTableStmt, specCmts)
	writer := storage.NewBufferWriter()

	err := WriteMeta(tcontext.Background(), meta, writer)
	require.NoError(t, err)

	expected := "/*!40103 SET TIME_ZONE='+00:00' */;\n" +
		"CREATE TABLE `t1` (\n" +
		"  `a` int(11) DEFAULT NULL\n" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;\n"
	require.Equal(t, expected, writer.String())
}

func TestWriteInsert(t *testing.T) {
	cfg := createMockConfig()

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	bf := storage.NewBufferWriter()

	conf := configForWriteSQL(cfg, UnspecifiedSize, UnspecifiedSize)
	m := newMetrics(conf.PromFactory, conf.Labels)
	n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf, m)
	require.NoError(t, err)
	require.Equal(t, uint64(4), n)

	expected := "/*!40101 SET NAMES binary*/;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `employee` VALUES\n" +
		"(1,'male','bob@mail.com','020-1234',NULL),\n" +
		"(2,'female','sarah@mail.com','020-1253','healthy'),\n" +
		"(3,'male','john@mail.com','020-1256','healthy'),\n" +
		"(4,'female','sarah@mail.com','020-1235','healthy');\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, ReadGauge(m.finishedRowsGauge), float64(len(data)))
	require.Equal(t, ReadGauge(m.finishedSizeGauge), float64(len(expected)))
}

func TestWriteInsertReturnsError(t *testing.T) {
	cfg := createMockConfig()

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	// row errors at last line
	rowErr := errors.New("mock row error")
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	tableIR.rowErr = rowErr
	bf := storage.NewBufferWriter()

	conf := configForWriteSQL(cfg, UnspecifiedSize, UnspecifiedSize)
	m := newMetrics(conf.PromFactory, conf.Labels)
	n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf, m)
	require.ErrorIs(t, err, rowErr)
	require.Equal(t, uint64(3), n)

	expected := "/*!40101 SET NAMES binary*/;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `employee` VALUES\n" +
		"(1,'male','bob@mail.com','020-1234',NULL),\n" +
		"(2,'female','sarah@mail.com','020-1253','healthy'),\n" +
		"(3,'male','john@mail.com','020-1256','healthy');\n"
	require.Equal(t, expected, bf.String())
	// error occurred, should revert pointer to zero
	require.Equal(t, ReadGauge(m.finishedRowsGauge), float64(0))
	require.Equal(t, ReadGauge(m.finishedSizeGauge), float64(0))
}

func TestWriteInsertInCsv(t *testing.T) {
	cfg := createMockConfig()

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	tableIR := newMockTableIR("test", "employee", data, nil, colTypes)
	bf := storage.NewBufferWriter()

	// test nullValue
	opt := &csvOption{separator: []byte(","), delimiter: []byte{'"'}, nullValue: "\\N", lineTerminator: []byte("\r\n")}
	conf := configForWriteCSV(cfg, true, opt)
	m := newMetrics(cfg.PromFactory, conf.Labels)
	n, err := WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, m)
	require.Equal(t, uint64(4), n)
	require.NoError(t, err)

	expected := "1,\"male\",\"bob@mail.com\",\"020-1234\",\\N\r\n" +
		"2,\"female\",\"sarah@mail.com\",\"020-1253\",\"healthy\"\r\n" +
		"3,\"male\",\"john@mail.com\",\"020-1256\",\"healthy\"\r\n" +
		"4,\"female\",\"sarah@mail.com\",\"020-1235\",\"healthy\"\r\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, float64(len(data)), ReadGauge(m.finishedRowsGauge))
	require.Equal(t, float64(len(expected)), ReadGauge(m.finishedSizeGauge))

	// test delimiter
	bf.Reset()
	opt.delimiter = quotationMark
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	conf = configForWriteCSV(cfg, true, opt)
	m = newMetrics(conf.PromFactory, conf.Labels)
	n, err = WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, m)
	require.Equal(t, uint64(4), n)
	require.NoError(t, err)

	expected = "1,'male','bob@mail.com','020-1234',\\N\r\n" +
		"2,'female','sarah@mail.com','020-1253','healthy'\r\n" +
		"3,'male','john@mail.com','020-1256','healthy'\r\n" +
		"4,'female','sarah@mail.com','020-1235','healthy'\r\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, float64(len(data)), ReadGauge(m.finishedRowsGauge))
	require.Equal(t, float64(len(expected)), ReadGauge(m.finishedSizeGauge))

	// test separator
	bf.Reset()
	opt.separator = []byte(";")
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	conf = configForWriteCSV(cfg, true, opt)
	m = newMetrics(conf.PromFactory, conf.Labels)
	n, err = WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, m)
	require.Equal(t, uint64(4), n)
	require.NoError(t, err)

	expected = "1;'male';'bob@mail.com';'020-1234';\\N\r\n" +
		"2;'female';'sarah@mail.com';'020-1253';'healthy'\r\n" +
		"3;'male';'john@mail.com';'020-1256';'healthy'\r\n" +
		"4;'female';'sarah@mail.com';'020-1235';'healthy'\r\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, float64(len(data)), ReadGauge(m.finishedRowsGauge))
	require.Equal(t, float64(len(expected)), ReadGauge(m.finishedSizeGauge))

	// test line terminator
	bf.Reset()
	opt.lineTerminator = []byte("\n")
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	conf = configForWriteCSV(cfg, true, opt)
	m = newMetrics(conf.PromFactory, conf.Labels)
	n, err = WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, m)
	require.Equal(t, uint64(4), n)
	require.NoError(t, err)

	expected = "1;'male';'bob@mail.com';'020-1234';\\N\n" +
		"2;'female';'sarah@mail.com';'020-1253';'healthy'\n" +
		"3;'male';'john@mail.com';'020-1256';'healthy'\n" +
		"4;'female';'sarah@mail.com';'020-1235';'healthy'\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, float64(len(data)), ReadGauge(m.finishedRowsGauge))
	require.Equal(t, float64(len(expected)), ReadGauge(m.finishedSizeGauge))

	// test delimiter that included in values
	bf.Reset()
	opt.separator = []byte("&;,?")
	opt.delimiter = []byte("ma")
	opt.lineTerminator = []byte("\r\n")
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	tableIR.colNames = []string{"id", "gender", "email", "phone_number", "status"}
	conf = configForWriteCSV(cfg, false, opt)
	m = newMetrics(conf.PromFactory, conf.Labels)
	n, err = WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, m)
	require.Equal(t, uint64(4), n)
	require.NoError(t, err)

	expected = "maidma&;,?magenderma&;,?maemamailma&;,?maphone_numberma&;,?mastatusma\r\n" +
		"1&;,?mamamalema&;,?mabob@mamail.comma&;,?ma020-1234ma&;,?\\N\r\n" +
		"2&;,?mafemamalema&;,?masarah@mamail.comma&;,?ma020-1253ma&;,?mahealthyma\r\n" +
		"3&;,?mamamalema&;,?majohn@mamail.comma&;,?ma020-1256ma&;,?mahealthyma\r\n" +
		"4&;,?mafemamalema&;,?masarah@mamail.comma&;,?ma020-1235ma&;,?mahealthyma\r\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, float64(len(data)), ReadGauge(m.finishedRowsGauge))
	require.Equal(t, float64(len(expected)), ReadGauge(m.finishedSizeGauge))
}

func TestWriteInsertInCsvReturnsError(t *testing.T) {
	cfg := createMockConfig()

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}

	// row errors at last line
	rowErr := errors.New("mock row error")
	tableIR := newMockTableIR("test", "employee", data, nil, colTypes)
	tableIR.rowErr = rowErr
	bf := storage.NewBufferWriter()

	// test nullValue
	opt := &csvOption{separator: []byte(","), delimiter: []byte{'"'}, nullValue: "\\N", lineTerminator: []byte("\r\n")}
	conf := configForWriteCSV(cfg, true, opt)
	m := newMetrics(conf.PromFactory, conf.Labels)
	n, err := WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, m)
	require.Equal(t, uint64(3), n)
	require.ErrorIs(t, err, rowErr)

	expected := "1,\"male\",\"bob@mail.com\",\"020-1234\",\\N\r\n" +
		"2,\"female\",\"sarah@mail.com\",\"020-1253\",\"healthy\"\r\n" +
		"3,\"male\",\"john@mail.com\",\"020-1256\",\"healthy\"\r\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, float64(0), ReadGauge(m.finishedRowsGauge))
	require.Equal(t, float64(0), ReadGauge(m.finishedSizeGauge))
}

func TestWriteInsertInCsvWithDialect(t *testing.T) {
	cfg := createMockConfig()

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", "blob1"},
		{"2", "female", "sarah@mail.com", "020-1253", "blob2"},
		{"3", "male", "john@mail.com", "020-1256", "blob3"},
		{"4", "female", "sarah@mail.com", "020-1235", "blob4"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "BLOB"}
	opt := &csvOption{separator: []byte(","), delimiter: []byte{'"'}, nullValue: "\\N", lineTerminator: []byte("\r\n")}
	conf := configForWriteCSV(cfg, true, opt)

	{
		// test UTF8
		conf.CsvOutputDialect = CSVDialectDefault
		tableIR := newMockTableIR("test", "employee", data, nil, colTypes)
		m := newMetrics(conf.PromFactory, conf.Labels)
		bf := storage.NewBufferWriter()
		n, err := WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, m)
		require.NoError(t, err)
		require.Equal(t, uint64(4), n)

		expected := "1,\"male\",\"bob@mail.com\",\"020-1234\",\"blob1\"\r\n" +
			"2,\"female\",\"sarah@mail.com\",\"020-1253\",\"blob2\"\r\n" +
			"3,\"male\",\"john@mail.com\",\"020-1256\",\"blob3\"\r\n" +
			"4,\"female\",\"sarah@mail.com\",\"020-1235\",\"blob4\"\r\n"
		require.Equal(t, expected, bf.String())
		require.Equal(t, float64(4), ReadGauge(m.finishedRowsGauge))
		require.Equal(t, float64(185), ReadGauge(m.finishedSizeGauge))
	}
	{
		// test HEX
		conf.CsvOutputDialect = CSVDialectRedshift
		tableIR := newMockTableIR("test", "employee", data, nil, colTypes)
		m := newMetrics(conf.PromFactory, conf.Labels)
		bf := storage.NewBufferWriter()
		n, err := WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, m)
		require.NoError(t, err)
		require.Equal(t, uint64(4), n)

		expected := "1,\"male\",\"bob@mail.com\",\"020-1234\",\"626c6f6231\"\r\n" +
			"2,\"female\",\"sarah@mail.com\",\"020-1253\",\"626c6f6232\"\r\n" +
			"3,\"male\",\"john@mail.com\",\"020-1256\",\"626c6f6233\"\r\n" +
			"4,\"female\",\"sarah@mail.com\",\"020-1235\",\"626c6f6234\"\r\n"
		require.Equal(t, expected, bf.String())
		require.Equal(t, float64(4), ReadGauge(m.finishedRowsGauge))
		require.Equal(t, float64(205), ReadGauge(m.finishedSizeGauge))
	}
	{
		// test Base64
		conf.CsvOutputDialect = CSVDialectBigQuery
		tableIR := newMockTableIR("test", "employee", data, nil, colTypes)
		m := newMetrics(conf.PromFactory, conf.Labels)
		bf := storage.NewBufferWriter()
		n, err := WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf, m)
		require.NoError(t, err)
		require.Equal(t, uint64(4), n)

		expected := "1,\"male\",\"bob@mail.com\",\"020-1234\",\"YmxvYjE=\"\r\n" +
			"2,\"female\",\"sarah@mail.com\",\"020-1253\",\"YmxvYjI=\"\r\n" +
			"3,\"male\",\"john@mail.com\",\"020-1256\",\"YmxvYjM=\"\r\n" +
			"4,\"female\",\"sarah@mail.com\",\"020-1235\",\"YmxvYjQ=\"\r\n"
		require.Equal(t, expected, bf.String())
		require.Equal(t, float64(4), ReadGauge(m.finishedRowsGauge))
		require.Equal(t, float64(197), ReadGauge(m.finishedSizeGauge))
	}
}

func TestSQLDataTypes(t *testing.T) {
	cfg := createMockConfig()

	data := [][]driver.Value{
		{"CHAR", "char1", `'char1'`},
		{"INT", 12345, `12345`},
		{"BINARY", 1234, "x'31323334'"},
	}

	for _, datum := range data {
		sqlType, origin, result := datum[0].(string), datum[1], datum[2].(string)

		tableData := [][]driver.Value{{origin}}
		colType := []string{sqlType}
		tableIR := newMockTableIR("test", "t", tableData, nil, colType)
		bf := storage.NewBufferWriter()

		conf := configForWriteSQL(cfg, UnspecifiedSize, UnspecifiedSize)
		m := newMetrics(conf.PromFactory, conf.Labels)
		n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf, m)
		require.NoError(t, err)
		require.Equal(t, uint64(1), n)

		lines := strings.Split(bf.String(), "\n")
		require.Len(t, lines, 3)
		require.Equal(t, fmt.Sprintf("(%s);", result), lines[1])
		require.Equal(t, float64(1), ReadGauge(m.finishedRowsGauge))
		require.Equal(t, float64(len(bf.String())), ReadGauge(m.finishedSizeGauge))
	}
}

func TestWrite(t *testing.T) {
	mocksw := &mockPoisonWriter{}
	src := []string{"test", "loooooooooooooooooooong", "poison"}
	exp := []string{"test", "loooooooooooooooooooong", "poison_error"}

	for i, s := range src {
		err := write(tcontext.Background(), mocksw, s)
		if err != nil {
			require.EqualError(t, err, exp[i])
		} else {
			require.Equal(t, s, mocksw.buf)
			require.Equal(t, exp[i], mocksw.buf)
		}
	}
	require.NoError(t, write(tcontext.Background(), mocksw, "test"))
}

// cloneConfigForTest clones a dumpling config.
func cloneConfigForTest(conf *Config) *Config {
	clone := &Config{}
	*clone = *conf
	return clone
}

func configForWriteSQL(config *Config, fileSize, statementSize uint64) *Config {
	cfg := cloneConfigForTest(config)
	cfg.FileSize = fileSize
	cfg.StatementSize = statementSize
	return cfg
}

func configForWriteCSV(config *Config, noHeader bool, opt *csvOption) *Config {
	cfg := cloneConfigForTest(config)
	cfg.NoHeader = noHeader
	cfg.CsvNullValue = opt.nullValue
	cfg.CsvDelimiter = string(opt.delimiter)
	cfg.CsvSeparator = string(opt.separator)
	cfg.CsvLineTerminator = string(opt.lineTerminator)
	cfg.FileSize = UnspecifiedSize
	return cfg
}

func createMockConfig() *Config {
	return &Config{
		FileSize: UnspecifiedSize,
		Labels: map[string]string{
			"test": "test",
		},
		PromFactory:  promutil.NewDefaultFactory(),
		PromRegistry: promutil.NewDefaultRegistry(),
	}
}
