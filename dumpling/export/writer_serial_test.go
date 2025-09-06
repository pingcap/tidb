// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"database/sql/driver"
	"fmt"
	"regexp"
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
	conf.IsStringChunking = false
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
	conf.IsStringChunking = false
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
	conf.IsStringChunking = false
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
	conf.IsStringChunking = false
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
		conf.IsStringChunking = false
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

// TestWriteInsertWithStatementSizeLimit tests the fix for the issue where
// duplicate INSERT statements were generated when statement size limits were reached.
//
// Original issue: When the statement size limit was reached and the writer needed to
// switch to a new statement, the INSERT statement prefix was written on every outer
// loop iteration, causing duplicate INSERT INTO statements in the output.
//
// Fix: Added isFirstChunk flag to ensure INSERT statement prefix is only written
// once per statement, and properly reset the flag when switching statements.
func TestWriteInsertWithStatementSizeLimit(t *testing.T) {
	cfg := createMockConfig()

	// Create test data with enough rows to trigger statement size switching
	data := [][]driver.Value{
		{"1", "user1", "user1@example.com"},
		{"2", "user2", "user2@example.com"},
		{"3", "user3", "user3@example.com"},
		{"4", "user4", "user4@example.com"},
		{"5", "user5", "user5@example.com"},
		{"6", "user6", "user6@example.com"},
	}
	colTypes := []string{"INT", "VARCHAR", "VARCHAR"}
	specCmts := []string{"/*!40101 SET NAMES binary*/;"}

	tableIR := newMockTableIR("test", "users", data, specCmts, colTypes)
	bf := storage.NewBufferWriter()

	// Set a very small statement size limit to force statement switching
	// This should cause the writer to create multiple INSERT statements
	statementSizeLimit := uint64(150) // Small enough to fit only 2-3 rows per statement
	conf := configForWriteSQL(cfg, UnspecifiedSize, statementSizeLimit)
	conf.IsStringChunking = false
	m := newMetrics(conf.PromFactory, conf.Labels)

	n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf, m)
	require.NoError(t, err)
	require.Equal(t, uint64(6), n)

	output := bf.String()

	// Verify that we have multiple INSERT statements (due to size limit)
	insertCount := strings.Count(output, "INSERT INTO `users` VALUES")
	require.Greater(t, insertCount, 1, "Expected multiple INSERT statements due to size limit")

	// Verify that each INSERT statement is properly formed and not duplicated
	lines := strings.Split(output, "\n")

	// Count consecutive INSERT statements - there should be no consecutive duplicates
	var consecutiveInserts int
	var maxConsecutiveInserts int
	for _, line := range lines {
		if strings.Contains(line, "INSERT INTO `users` VALUES") {
			consecutiveInserts++
		} else {
			if consecutiveInserts > maxConsecutiveInserts {
				maxConsecutiveInserts = consecutiveInserts
			}
			consecutiveInserts = 0
		}
	}
	if consecutiveInserts > maxConsecutiveInserts {
		maxConsecutiveInserts = consecutiveInserts
	}

	// There should never be more than 1 consecutive INSERT statement
	require.Equal(t, 1, maxConsecutiveInserts, "Found consecutive duplicate INSERT statements")

	// Verify the structure: each INSERT should be followed by data rows and a semicolon
	insertFound := false
	for i, line := range lines {
		if strings.Contains(line, "INSERT INTO `users` VALUES") {
			insertFound = true
			// The next non-empty lines should be data rows, ending with a semicolon
			require.True(t, i < len(lines)-1, "INSERT statement should be followed by data")
		} else if insertFound && strings.HasSuffix(line, ";") {
			// Found the end of this INSERT statement
			insertFound = false
		}
	}
}

// TestWriteInsertWithoutStatementSizeLimit verifies normal behavior when no size limit is set
func TestWriteInsertWithoutStatementSizeLimit(t *testing.T) {
	cfg := createMockConfig()

	data := [][]driver.Value{
		{"1", "user1", "user1@example.com"},
		{"2", "user2", "user2@example.com"},
		{"3", "user3", "user3@example.com"},
	}
	colTypes := []string{"INT", "VARCHAR", "VARCHAR"}
	specCmts := []string{"/*!40101 SET NAMES binary*/;"}

	tableIR := newMockTableIR("test", "users", data, specCmts, colTypes)
	bf := storage.NewBufferWriter()

	// No statement size limit
	conf := configForWriteSQL(cfg, UnspecifiedSize, UnspecifiedSize)
	conf.IsStringChunking = false
	m := newMetrics(conf.PromFactory, conf.Labels)

	n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf, m)
	require.NoError(t, err)
	require.Equal(t, uint64(3), n)

	output := bf.String()

	// Should have exactly one INSERT statement
	insertCount := strings.Count(output, "INSERT INTO `users` VALUES")
	require.Equal(t, 1, insertCount, "Expected exactly one INSERT statement when no size limit")

	// Verify the expected output format
	expected := "/*!40101 SET NAMES binary*/;\n" +
		"INSERT INTO `users` VALUES\n" +
		"(1,'user1','user1@example.com'),\n" +
		"(2,'user2','user2@example.com'),\n" +
		"(3,'user3','user3@example.com');\n"
	require.Equal(t, expected, output)
}

// TestWriteInsertFirstChunkTracking tests the isFirstChunk flag behavior directly
func TestWriteInsertFirstChunkTracking(t *testing.T) {
	cfg := createMockConfig()

	// Create a scenario that will definitely trigger statement switching
	// by using a very restrictive statement size limit
	data := [][]driver.Value{
		{"1", "a"},
		{"2", "b"},
		{"3", "c"},
		{"4", "d"},
	}
	colTypes := []string{"INT", "VARCHAR"}

	tableIR := newMockTableIR("test", "items", data, nil, colTypes)
	bf := storage.NewBufferWriter()

	// Set statement size to a very small value to force multiple statements
	statementSizeLimit := uint64(50)
	conf := configForWriteSQL(cfg, UnspecifiedSize, statementSizeLimit)
	conf.IsStringChunking = false
	m := newMetrics(conf.PromFactory, conf.Labels)

	n, err := WriteInsert(tcontext.Background(), conf, tableIR, tableIR, bf, m)
	require.NoError(t, err)
	require.Equal(t, uint64(4), n)

	output := bf.String()
	t.Logf("Debug output:\n%s", output)

	// The key test: look for the pattern that indicates the bug
	// With the original bug, we would see consecutive INSERT statements
	// This regex looks for INSERT followed immediately by another INSERT (possibly with whitespace)
	insertPattern := `INSERT INTO .*? VALUES\s*\n\s*INSERT INTO .*? VALUES`
	matched, err := regexp.MatchString(insertPattern, output)
	require.NoError(t, err)

	// This should NOT match in the fixed version - consecutive INSERTs indicate the bug
	require.False(t, matched, "Found consecutive INSERT statements - this indicates the original bug")

	// Also verify the structure is correct
	parts := strings.Split(output, "INSERT INTO `items` VALUES")

	// First part should be empty (before first INSERT)
	require.Equal(t, "", parts[0])

	// Each subsequent part should start with newline and contain data ending with semicolon
	for i := 1; i < len(parts); i++ {
		part := parts[i]
		require.True(t, strings.HasPrefix(part, "\n"), "INSERT should be followed by newline")
		require.True(t, strings.Contains(part, ";"), "Each statement should end with semicolon")
		// Should not contain another INSERT statement prefix
		require.False(t, strings.Contains(part, "INSERT INTO"), "No nested INSERT statements")
	}
}
