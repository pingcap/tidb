// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	"github.com/pingcap/errors"
	"github.com/stretchr/testify/require"

	"github.com/pingcap/tidb/br/pkg/storage"
	tcontext "github.com/pingcap/tidb/dumpling/context"
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
	cfg, clean := createMockConfig(t)
	defer clean()

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
	n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf)
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
	require.Equal(t, ReadGauge(finishedRowsGauge, conf.Labels), float64(len(data)))
	require.Equal(t, ReadGauge(finishedSizeGauge, conf.Labels), float64(len(expected)))
}

func TestWriteInsertReturnsError(t *testing.T) {
	cfg, clean := createMockConfig(t)
	defer clean()

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
	n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf)
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
	require.Equal(t, ReadGauge(finishedRowsGauge, conf.Labels), float64(0))
	require.Equal(t, ReadGauge(finishedSizeGauge, conf.Labels), float64(0))
}

func TestWriteInsertInCsv(t *testing.T) {
	cfg, clean := createMockConfig(t)
	defer clean()

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
	opt := &csvOption{separator: []byte(","), delimiter: []byte{'"'}, nullValue: "\\N"}
	conf := configForWriteCSV(cfg, true, opt)
	n, err := WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf)
	require.Equal(t, uint64(4), n)
	require.NoError(t, err)

	expected := "1,\"male\",\"bob@mail.com\",\"020-1234\",\\N\n" +
		"2,\"female\",\"sarah@mail.com\",\"020-1253\",\"healthy\"\n" +
		"3,\"male\",\"john@mail.com\",\"020-1256\",\"healthy\"\n" +
		"4,\"female\",\"sarah@mail.com\",\"020-1235\",\"healthy\"\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, float64(len(data)), ReadGauge(finishedRowsGauge, conf.Labels))
	require.Equal(t, float64(len(expected)), ReadGauge(finishedSizeGauge, conf.Labels))

	RemoveLabelValuesWithTaskInMetrics(conf.Labels)

	// test delimiter
	bf.Reset()
	opt.delimiter = quotationMark
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	conf = configForWriteCSV(cfg, true, opt)
	n, err = WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf)
	require.Equal(t, uint64(4), n)
	require.NoError(t, err)

	expected = "1,'male','bob@mail.com','020-1234',\\N\n" +
		"2,'female','sarah@mail.com','020-1253','healthy'\n" +
		"3,'male','john@mail.com','020-1256','healthy'\n" +
		"4,'female','sarah@mail.com','020-1235','healthy'\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, float64(len(data)), ReadGauge(finishedRowsGauge, conf.Labels))
	require.Equal(t, float64(len(expected)), ReadGauge(finishedSizeGauge, conf.Labels))

	RemoveLabelValuesWithTaskInMetrics(conf.Labels)

	// test separator
	bf.Reset()
	opt.separator = []byte(";")
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	conf = configForWriteCSV(cfg, true, opt)
	n, err = WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf)
	require.Equal(t, uint64(4), n)
	require.NoError(t, err)

	expected = "1;'male';'bob@mail.com';'020-1234';\\N\n" +
		"2;'female';'sarah@mail.com';'020-1253';'healthy'\n" +
		"3;'male';'john@mail.com';'020-1256';'healthy'\n" +
		"4;'female';'sarah@mail.com';'020-1235';'healthy'\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, float64(len(data)), ReadGauge(finishedRowsGauge, conf.Labels))
	require.Equal(t, float64(len(expected)), ReadGauge(finishedSizeGauge, conf.Labels))

	RemoveLabelValuesWithTaskInMetrics(conf.Labels)

	// test delimiter that included in values
	bf.Reset()
	opt.separator = []byte("&;,?")
	opt.delimiter = []byte("ma")
	tableIR = newMockTableIR("test", "employee", data, nil, colTypes)
	tableIR.colNames = []string{"id", "gender", "email", "phone_number", "status"}
	conf = configForWriteCSV(cfg, false, opt)
	n, err = WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf)
	require.Equal(t, uint64(4), n)
	require.NoError(t, err)

	expected = "maidma&;,?magenderma&;,?maemamailma&;,?maphone_numberma&;,?mastatusma\n" +
		"1&;,?mamamalema&;,?mabob@mamail.comma&;,?ma020-1234ma&;,?\\N\n" +
		"2&;,?mafemamalema&;,?masarah@mamail.comma&;,?ma020-1253ma&;,?mahealthyma\n" +
		"3&;,?mamamalema&;,?majohn@mamail.comma&;,?ma020-1256ma&;,?mahealthyma\n" +
		"4&;,?mafemamalema&;,?masarah@mamail.comma&;,?ma020-1235ma&;,?mahealthyma\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, float64(len(data)), ReadGauge(finishedRowsGauge, conf.Labels))
	require.Equal(t, float64(len(expected)), ReadGauge(finishedSizeGauge, conf.Labels))

	RemoveLabelValuesWithTaskInMetrics(conf.Labels)
}

func TestWriteInsertInCsvReturnsError(t *testing.T) {
	cfg, clean := createMockConfig(t)
	defer clean()

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
	opt := &csvOption{separator: []byte(","), delimiter: []byte{'"'}, nullValue: "\\N"}
	conf := configForWriteCSV(cfg, true, opt)
	n, err := WriteInsertInCsv(tcontext.Background(), conf, tableIR, tableIR, bf)
	require.Equal(t, uint64(3), n)
	require.ErrorIs(t, err, rowErr)

	expected := "1,\"male\",\"bob@mail.com\",\"020-1234\",\\N\n" +
		"2,\"female\",\"sarah@mail.com\",\"020-1253\",\"healthy\"\n" +
		"3,\"male\",\"john@mail.com\",\"020-1256\",\"healthy\"\n"
	require.Equal(t, expected, bf.String())
	require.Equal(t, float64(0), ReadGauge(finishedRowsGauge, conf.Labels))
	require.Equal(t, float64(0), ReadGauge(finishedSizeGauge, conf.Labels))

	RemoveLabelValuesWithTaskInMetrics(conf.Labels)
}

func TestSQLDataTypes(t *testing.T) {
	cfg, clean := createMockConfig(t)
	defer clean()

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
		n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf)
		require.NoError(t, err)
		require.Equal(t, uint64(1), n)

		lines := strings.Split(bf.String(), "\n")
		require.Len(t, lines, 3)
		require.Equal(t, fmt.Sprintf("(%s);", result), lines[1])
		require.Equal(t, float64(1), ReadGauge(finishedRowsGauge, conf.Labels))
		require.Equal(t, float64(len(bf.String())), ReadGauge(finishedSizeGauge, conf.Labels))

		RemoveLabelValuesWithTaskInMetrics(conf.Labels)
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
	cfg.FileSize = UnspecifiedSize
	return cfg
}

func createMockConfig(t *testing.T) (cfg *Config, clean func()) {
	cfg = &Config{
		FileSize: UnspecifiedSize,
	}

	InitMetricsVector(cfg.Labels)

	clean = func() {
		RemoveLabelValuesWithTaskInMetrics(cfg.Labels)
		require.Equal(t, float64(0), ReadGauge(finishedRowsGauge, cfg.Labels))
		require.Equal(t, float64(0), ReadGauge(finishedSizeGauge, cfg.Labels))
	}

	return
}
