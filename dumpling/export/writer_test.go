// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path"
	"strings"
	"sync"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/version"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/pingcap/tidb/pkg/testkit/testfailpoint"
	"github.com/pingcap/tidb/pkg/util/promutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type writerTestSummary struct {
	summary.LogCollector
	data map[string]uint64
}

func (s *writerTestSummary) CollectSuccessUnit(name string, _ int, value any) {
	s.data[name] += value.(uint64)
}

type closeErrorWriter struct {
	objectio.Writer
	closeFn func() error
}

func (w *closeErrorWriter) Close(ctx context.Context) error {
	if err := w.Writer.Close(ctx); err != nil {
		return err
	}
	return w.closeFn()
}

func TestWriterCloseErrorAccounting(t *testing.T) {
	for _, format := range []string{FileFormatCSVString, FileFormatSQLTextString, FileFormatParquetString} {
		for _, tc := range []struct {
			name      string
			fileSize  uint64
			failClose int
		}{
			{name: "first_file", failClose: 1},
			{name: "later_file", fileSize: 1, failClose: 2},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				conf := defaultConfigForTest(t)
				conf.OutputDirPath = t.TempDir()
				conf.FileType = format
				conf.FileSize = tc.fileSize
				conf.Consistency = ConsistencyTypeSnapshot
				conf.NoHeader = true
				w := createTestWriter(conf, t)
				collector := &writerTestSummary{data: make(map[string]uint64)}
				summary.SetLogCollector(collector)
				t.Cleanup(func() { summary.SetLogCollector(summary.NewLogCollector(appLogger.Info)) })

				// Preserve credits from another chunk while rolling back this attempt.
				const previousRows, previousBytes = uint64(7), uint64(19)
				collector.data["total rows"] = previousRows
				collector.data[summary.TotalBytes] = previousBytes
				AddGauge(w.metrics.finishedRowsGauge, float64(previousRows))
				AddGauge(w.metrics.finishedSizeGauge, float64(previousBytes))
				checkAccounting := func(rows, size uint64) {
					t.Helper()
					assert.Equal(t, rows, collector.data["total rows"], "summary rows")
					assert.Equal(t, size, collector.data[summary.TotalBytes], "summary bytes")
					assert.Equal(t, float64(rows), ReadGauge(w.metrics.finishedRowsGauge), "progress rows")
					assert.Equal(t, float64(size), ReadGauge(w.metrics.finishedSizeGauge), "progress bytes")
				}
				rebuilds := 0
				w.rebuildConnFn = func(conn *sql.Conn, _ bool) (*sql.Conn, error) {
					rebuilds++
					checkAccounting(previousRows, previousBytes)
					return conn, nil
				}
				closes := 0
				w.extStorage = &workerTestStorage{
					Storage: w.extStorage,
					wrap: func(output objectio.Writer) objectio.Writer {
						return &closeErrorWriter{Writer: output, closeFn: func() error {
							closes++
							if closes == tc.failClose {
								return errors.New("upload failed during close")
							}
							return nil
						}}
					},
				}
				meta := newMockTableIRWithColumnInfo("test", "retry", nil, nil,
					[]*ColumnInfo{{Name: "value", DatabaseTypeName: "TEXT"}})
				data := &workerTestTableData{
					mockTableIR: meta,
					newIter: func(_ int) *workerTestRowIter {
						return &workerTestRowIter{values: [][]byte{[]byte("first"), []byte("second"), []byte("third")}}
					},
				}

				require.NoError(t, runWorkerTestTask(w, meta, data))
				require.Equal(t, 1, rebuilds)
				require.Equal(t, 2, data.attempts)
				contents := workerTestOutput(t, conf.OutputDirPath)
				fileCount := 1
				if tc.fileSize != UnspecifiedSize {
					fileCount = 3
				}
				require.Len(t, contents, fileCount, "retry must overwrite the same object names")
				require.Equal(t, fileCount+tc.failClose, closes)
				var size uint64
				for _, content := range contents {
					size += uint64(len(content))
				}
				checkAccounting(previousRows+3, previousBytes+size)
			})
		}
	}
}

type captureWriterOptionsStorage struct {
	storeapi.Storage
	options []*storeapi.WriterOption
}

func (s *captureWriterOptionsStorage) Create(ctx context.Context, name string, option *storeapi.WriterOption) (objectio.Writer, error) {
	s.options = append(s.options, option)
	return s.Storage.Create(ctx, name, option)
}

func TestWriteTableDataDirectIO(t *testing.T) {
	for _, format := range []string{FileFormatSQLTextString, FileFormatCSVString, FileFormatParquetString} {
		t.Run(format, func(t *testing.T) {
			conf := defaultConfigForTest(t)
			conf.OutputDirPath = t.TempDir()
			conf.FileType = format
			writer := createTestWriter(conf, t)
			storage := &captureWriterOptionsStorage{Storage: writer.extStorage}
			writer.extStorage = storage
			tableIR := newMockTableIRWithColumnInfo("test", "t", [][]driver.Value{{"1"}}, nil,
				[]*ColumnInfo{{Name: "id", DatabaseTypeName: "INT"}})
			require.NoError(t, writer.WriteTableData(tableIR, tableIR, 0))
			require.Len(t, storage.options, 1)
			require.NotNil(t, storage.options[0])
			require.True(t, storage.options[0].DirectIO)
			if format != FileFormatParquetString {
				require.EqualValues(t, uploadPartSize, storage.options[0].PartSize)
				require.Equal(t, uploadConcurrency, storage.options[0].Concurrency)
			}
		})
	}
}

func TestWriteDatabaseMeta(t *testing.T) {
	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir

	writer := createTestWriter(config, t)

	err := writer.WriteDatabaseMeta("test", "CREATE DATABASE `test`")
	require.NoError(t, err)

	p := path.Join(dir, "test-schema-create.sql")
	_, err = os.Stat(p)
	require.NoError(t, err)

	bytes, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n/*!40101 SET NAMES binary*/;\nCREATE DATABASE `test`;\n", string(bytes))
}

func TestWritePolicyMeta(t *testing.T) {
	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir

	writer := createTestWriter(config, t)

	err := writer.WritePolicyMeta("testpolicy", "create placement policy `y` followers=2")
	require.NoError(t, err)

	p := path.Join(dir, "testpolicy-placement-policy-create.sql")
	_, err = os.Stat(p)
	require.NoError(t, err)

	bytes, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n/*!40101 SET NAMES binary*/;\ncreate placement policy `y` followers=2;\n", string(bytes))
}

func TestWriteTableMeta(t *testing.T) {
	dir := t.TempDir()

	config := defaultConfigForTest(t)
	config.OutputDirPath = dir

	writer := createTestWriter(config, t)

	err := writer.WriteTableMeta("test", "t", "CREATE TABLE t (a INT)")
	require.NoError(t, err)
	p := path.Join(dir, "test.t-schema.sql")
	_, err = os.Stat(p)
	require.NoError(t, err)
	bytes, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n/*!40101 SET NAMES binary*/;\nCREATE TABLE t (a INT);\n", string(bytes))

	testfailpoint.Enable(t, "github.com/pingcap/tidb/dumpling/export/FailToCloseMetaFile", "return(true)")

	err = writer.WriteTableMeta("test", "t", "CREATE TABLE t (a INT)")
	require.ErrorContains(t, err, "injected error: fail to close meta file")
}

func TestWriteViewMeta(t *testing.T) {
	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir

	writer := createTestWriter(config, t)

	specCmt := "/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n/*!40101 SET NAMES binary*/;\n"
	createTableSQL := "CREATE TABLE `v`(\n`a` int\n)ENGINE=MyISAM;\n"
	createViewSQL := "DROP TABLE IF EXISTS `v`;\nDROP VIEW IF EXISTS `v`;\nSET @PREV_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT;\nSET @PREV_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS;\nSET @PREV_COLLATION_CONNECTION=@@COLLATION_CONNECTION;\nSET character_set_client = utf8;\nSET character_set_results = utf8;\nSET collation_connection = utf8_general_ci;\nCREATE ALGORITHM=UNDEFINED DEFINER=`root`@`localhost` SQL SECURITY DEFINER VIEW `v` (`a`) AS SELECT `t`.`a` AS `a` FROM `test`.`t`;\nSET character_set_client = @PREV_CHARACTER_SET_CLIENT;\nSET character_set_results = @PREV_CHARACTER_SET_RESULTS;\nSET collation_connection = @PREV_COLLATION_CONNECTION;\n"
	err := writer.WriteViewMeta("test", "v", createTableSQL, createViewSQL)
	require.NoError(t, err)

	p := path.Join(dir, "test.v-schema.sql")
	_, err = os.Stat(p)
	require.NoError(t, err)
	bytes, err := os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, specCmt+createTableSQL, string(bytes))

	p = path.Join(dir, "test.v-schema-view.sql")
	_, err = os.Stat(p)
	require.NoError(t, err)
	bytes, err = os.ReadFile(p)
	require.NoError(t, err)
	require.Equal(t, specCmt+createViewSQL, string(bytes))
}

func TestWriteTableData(t *testing.T) {
	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir

	writer := createTestWriter(config, t)

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
	err := writer.WriteTableData(tableIR, tableIR, 0)
	require.NoError(t, err)

	p := path.Join(dir, "test.employee.000000000.sql")
	_, err = os.Stat(p)
	require.NoError(t, err)
	bytes, err := os.ReadFile(p)
	require.NoError(t, err)

	expected := "/*!40101 SET NAMES binary*/;\n" +
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
		"INSERT INTO `employee` VALUES\n" +
		"(1,'male','bob@mail.com','020-1234',NULL),\n" +
		"(2,'female','sarah@mail.com','020-1253','healthy'),\n" +
		"(3,'male','john@mail.com','020-1256','healthy'),\n" +
		"(4,'female','sarah@mail.com','020-1235','healthy');\n"
	require.Equal(t, expected, string(bytes))

	testfailpoint.Enable(t, "github.com/pingcap/tidb/dumpling/export/FailToCloseDataFile", "return(true)")

	tableIR = newMockTableIR("test", "employee", data, specCmts, colTypes)
	err = writer.WriteTableData(tableIR, tableIR, 0)
	require.ErrorContains(t, err, "injected error: fail to close data file")
}

func TestWriteTableDataWithFileSize(t *testing.T) {
	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir
	config.FileSize = 50
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	config.FileSize += uint64(len(specCmts[0]) + 1)
	config.FileSize += uint64(len(specCmts[1]) + 1)
	config.FileSize += uint64(len("INSERT INTO `employees` VALUES\n"))

	writer := createTestWriter(config, t)

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	err := writer.WriteTableData(tableIR, tableIR, 0)
	require.NoError(t, err)

	cases := map[string]string{
		"test.employee.000000000.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(1,'male','bob@mail.com','020-1234',NULL),\n" +
			"(2,'female','sarah@mail.com','020-1253','healthy');\n",
		"test.employee.000000001.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(3,'male','john@mail.com','020-1256','healthy'),\n" +
			"(4,'female','sarah@mail.com','020-1235','healthy');\n",
	}

	for p, expected := range cases {
		p = path.Join(dir, p)
		_, err := os.Stat(p)
		require.NoError(t, err)
		bytes, err := os.ReadFile(p)
		require.NoError(t, err)
		require.Equal(t, expected, string(bytes))
	}

	t.Run("parquet split files should keep parquet codec suffix", func(t *testing.T) {
		parquetDir := t.TempDir()
		parquetConfig := defaultConfigForTest(t)
		parquetConfig.OutputDirPath = parquetDir
		parquetConfig.FileType = FileFormatParquetString
		parquetConfig.FileSize = 1
		parquetConfig.ParquetCompressType = compressedio.Gzip

		parquetWriter := createTestWriter(parquetConfig, t)
		parquetData := [][]driver.Value{
			{"1"},
			{"2"},
			{"3"},
		}
		colInfos := []*ColumnInfo{{Name: "id", DatabaseTypeName: "INT"}}
		parquetTableIR := newMockTableIRWithColumnInfo("test", "employee", parquetData, nil, colInfos)

		require.NoError(t, parquetWriter.WriteTableData(parquetTableIR, parquetTableIR, 0))

		entries, err := os.ReadDir(parquetDir)
		require.NoError(t, err)
		require.Greater(t, len(entries), 1)
		for _, entry := range entries {
			require.Truef(t, strings.HasSuffix(entry.Name(), ".gz.parquet"), "unexpected file name: %s", entry.Name())
		}
	})
}

func TestWriteTableDataCsvWithFileSize(t *testing.T) {
	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}

	writeCSV := func(fileSize uint64) []string {
		dir := t.TempDir()
		config := defaultConfigForTest(t)
		config.OutputDirPath = dir
		config.FileType = FileFormatCSVString
		config.NoHeader = true
		config.FileSize = fileSize
		writer := createTestWriter(config, t)
		tableIR := newMockTableIR("test", "employee", data, nil, colTypes)
		require.NoError(t, writer.WriteTableData(tableIR, tableIR, 0))
		entries, err := os.ReadDir(dir) // ReadDir returns entries sorted by name.
		require.NoError(t, err)
		contents := make([]string, 0, len(entries))
		for _, e := range entries {
			b, err := os.ReadFile(path.Join(dir, e.Name()))
			require.NoError(t, err)
			contents = append(contents, string(b))
		}
		return contents
	}

	// A small FileSize forces rotation on row boundaries. Concatenating the split
	// files in name order must reproduce the single unsplit file exactly: rows
	// are cut on boundaries, never dropped or corrupted.
	split := writeCSV(40)
	whole := writeCSV(UnspecifiedSize)

	require.Len(t, whole, 1)
	require.Greater(t, len(split), 1, "small FileSize should split into multiple files")
	require.Equal(t, whole[0], strings.Join(split, ""))
}

func TestWriteTableDataWithFileSizeAndRows(t *testing.T) {
	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir
	config.FileSize = 50
	config.Rows = 4
	specCmts := []string{
		"/*!40101 SET NAMES binary*/;",
		"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;",
	}
	config.FileSize += uint64(len(specCmts[0]) + 1)
	config.FileSize += uint64(len(specCmts[1]) + 1)
	config.FileSize += uint64(len("INSERT INTO `employees` VALUES\n"))

	writer := createTestWriter(config, t)

	data := [][]driver.Value{
		{"1", "male", "bob@mail.com", "020-1234", nil},
		{"2", "female", "sarah@mail.com", "020-1253", "healthy"},
		{"3", "male", "john@mail.com", "020-1256", "healthy"},
		{"4", "female", "sarah@mail.com", "020-1235", "healthy"},
	}
	colTypes := []string{"INT", "SET", "VARCHAR", "VARCHAR", "TEXT"}
	tableIR := newMockTableIR("test", "employee", data, specCmts, colTypes)
	err := writer.WriteTableData(tableIR, tableIR, 0)
	require.NoError(t, err)

	cases := map[string]string{
		"test.employee.0000000000000.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(1,'male','bob@mail.com','020-1234',NULL),\n" +
			"(2,'female','sarah@mail.com','020-1253','healthy');\n",
		"test.employee.0000000000001.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(3,'male','john@mail.com','020-1256','healthy'),\n" +
			"(4,'female','sarah@mail.com','020-1235','healthy');\n",
	}

	for p, expected := range cases {
		p = path.Join(dir, p)
		_, err = os.Stat(p)
		require.NoError(t, err)
		bytes, err := os.ReadFile(p)
		require.NoError(t, err)
		require.Equal(t, expected, string(bytes))
	}
}

func TestWriteTableDataWithStatementSize(t *testing.T) {
	dir := t.TempDir()
	config := defaultConfigForTest(t)
	config.OutputDirPath = dir
	config.StatementSize = 50
	config.StatementSize += uint64(len("INSERT INTO `employee` VALUES\n"))
	var err error
	config.OutputFileTemplate, err = ParseOutputFileTemplate("specified-name")
	require.NoError(t, err)

	writer := createTestWriter(config, t)

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
	tableIR := newMockTableIR("te%/st", "employee", data, specCmts, colTypes)
	err = writer.WriteTableData(tableIR, tableIR, 0)
	require.NoError(t, err)

	// only with statement size
	cases := map[string]string{
		"specified-name.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(1,'male','bob@mail.com','020-1234',NULL),\n" +
			"(2,'female','sarah@mail.com','020-1253','healthy');\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(3,'male','john@mail.com','020-1256','healthy'),\n" +
			"(4,'female','sarah@mail.com','020-1235','healthy');\n",
	}

	for p, expected := range cases {
		p = path.Join(config.OutputDirPath, p)
		_, err = os.Stat(p)
		require.NoError(t, err)
		bytes, err1 := os.ReadFile(p)
		require.NoError(t, err1)
		require.Equal(t, expected, string(bytes))
	}

	// with file size and statement size
	config.FileSize = 204
	config.StatementSize = 95
	config.FileSize += uint64(len(specCmts[0]) + 1)
	config.FileSize += uint64(len(specCmts[1]) + 1)
	config.StatementSize += uint64(len("INSERT INTO `employee` VALUES\n"))
	// test specifying filename format
	config.OutputFileTemplate, err = ParseOutputFileTemplate("{{.Index}}-{{.Table}}-{{fn .DB}}")
	require.NoError(t, err)
	err = os.RemoveAll(config.OutputDirPath)
	require.NoError(t, err)
	config.OutputDirPath, err = os.MkdirTemp("", "dumpling")

	writer = createTestWriter(config, t)

	cases = map[string]string{
		"000000000-employee-te%25%2Fst.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(1,'male','bob@mail.com','020-1234',NULL),\n" +
			"(2,'female','sarah@mail.com','020-1253','healthy');\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(3,'male','john@mail.com','020-1256','healthy');\n",
		"000000001-employee-te%25%2Fst.sql": "/*!40101 SET NAMES binary*/;\n" +
			"/*!40014 SET FOREIGN_KEY_CHECKS=0*/;\n" +
			"INSERT INTO `employee` VALUES\n" +
			"(4,'female','sarah@mail.com','020-1235','healthy');\n",
	}

	tableIR = newMockTableIR("te%/st", "employee", data, specCmts, colTypes)
	require.NoError(t, writer.WriteTableData(tableIR, tableIR, 0))
	require.NoError(t, err)
	for p, expected := range cases {
		p = path.Join(config.OutputDirPath, p)
		_, err = os.Stat(p)
		require.NoError(t, err)
		bytes, err := os.ReadFile(p)
		require.NoError(t, err)
		require.Equal(t, expected, string(bytes))
	}
}

var mu sync.Mutex

func createTestWriter(conf *Config, t *testing.T) *Writer {
	t.Helper()
	conf.ServerInfo.ServerType = version.ServerTypeMySQL

	mu.Lock()
	extStore, err := conf.createExternalStorage(context.Background())
	mu.Unlock()

	require.NoError(t, err)
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	conn, err := db.Conn(context.Background())
	require.NoError(t, err)

	metrics := newMetrics(promutil.NewDefaultFactory(), nil)
	w := NewWriter(tcontext.Background(), 0, conf, conn, extStore, metrics)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})
	return w
}
