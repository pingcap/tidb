// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/br/pkg/summary"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/dumpling/log"
	"github.com/pingcap/tidb/pkg/dumpformat/csvfile"
	"github.com/pingcap/tidb/pkg/dumpformat/parquetfile"
	"github.com/pingcap/tidb/pkg/dumpformat/sqlfile"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/objstore/compressedio"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"go.uber.org/zap"
)

// WriteMeta writes MetaIR to an objectio.Writer
func WriteMeta(tctx *tcontext.Context, meta MetaIR, w objectio.Writer) error {
	tctx.L().Debug("start dumping meta data", zap.String("target", meta.TargetName()))

	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(tctx, w, fmt.Sprintf("%s\n", specCmtIter.Next())); err != nil {
			return err
		}
	}

	if err := write(tctx, w, meta.MetaSQL()); err != nil {
		return err
	}

	tctx.L().Debug("finish dumping meta data", zap.String("target", meta.TargetName()))
	return nil
}

// WriteInsert writes TableDataIR to an objectio.Writer in SQL type
func WriteInsert(
	pCtx *tcontext.Context,
	cfg *Config,
	meta TableMeta,
	tblIR TableDataIR,
	w objectio.Writer,
	metrics *metrics,
) (n uint64, err error) {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return 0, fileRowIter.Error()
	}

	// sink writes directly into the object store writer, mirroring the CSV/parquet
	// paths. The writer's multipart uploader (enabled via WriterOption.Concurrency
	// in buildInterceptFileWriter) overlaps upload with encoding, so no writerPipe
	// goroutine is needed; SQLWriter owns the per-statement framing and sizing and
	// counts written bytes for file-size rotation via EstimateFileSize.
	sink := &wrappedWriter{ctx: pCtx.Context, w: w}

	selectedField := meta.SelectedField()
	var insertStatementPrefix string
	if selectedField != "" && selectedField != "*" {
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s (%s) VALUES\n",
			wrapBackTicks(escapeString(meta.TableName())), selectedField)
	} else {
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s VALUES\n",
			wrapBackTicks(escapeString(meta.TableName())))
	}
	var kinds []sqlfile.FieldKind
	if selectedField != "" {
		kinds = sqlColumnKinds(meta.ColumnTypes())
	}
	statementSize := int64(0)
	if cfg.StatementSize != UnspecifiedSize {
		statementSize = int64(cfg.StatementSize)
	}
	sw := sqlfile.NewSQLWriter(sink, []byte(insertStatementPrefix), kinds, &sqlfile.Config{
		StatementSize:   statementSize,
		EscapeBackslash: cfg.EscapeBackslash,
	})

	var (
		row          = MakeRowReceiver(meta.ColumnTypes())
		counter      uint64
		lastCounter  uint64
		finishedSize uint64
	)

	defer func() {
		if err != nil {
			pCtx.L().Warn("fail to dumping table(chunk), will revert some metrics and start a retry if possible",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", lastCounter),
				zap.Uint64("finished size", finishedSize),
				log.ShortError(err))
			SubGauge(metrics.finishedRowsGauge, float64(lastCounter))
			SubGauge(metrics.finishedSizeGauge, float64(finishedSize))
		} else {
			pCtx.L().Debug("finish dumping table(chunk)",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", counter),
				zap.Uint64("finished size", finishedSize))
			summary.CollectSuccessUnit(summary.TotalBytes, 1, finishedSize)
			summary.CollectSuccessUnit("total rows", 1, counter)
		}
	}()

	// preambleSize counts the special comments written straight to the sink, which
	// SQLWriter's EstimateFileSize does not see, so finishedSize covers the whole
	// file.
	var preambleSize int
	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		cmt := []byte(specCmtIter.Next())
		if _, err = sink.Write(cmt); err != nil {
			return 0, errors.Trace(err)
		}
		if _, err = sink.Write([]byte{'\n'}); err != nil {
			return 0, errors.Trace(err)
		}
		preambleSize += len(cmt) + 1
	}

	// rawRow is reused across rows to avoid a per-row slice allocation.
	rawRow := row.GetRawBytes()[:0]
	for fileRowIter.HasNext() {
		if selectedField != "" {
			if err = fileRowIter.Decode(row); err != nil {
				return counter, errors.Trace(err)
			}
			rawRow = row.AppendRawBytes(rawRow[:0])
			if err = sw.Write(rawRow); err != nil {
				return counter, errors.Trace(err)
			}
		} else {
			// All columns are generated; emit an empty tuple "()".
			if err = sw.Write(nil); err != nil {
				return counter, errors.Trace(err)
			}
		}
		counter++
		if counter%1000 == 0 {
			AddGauge(metrics.finishedRowsGauge, float64(counter-lastCounter))
			lastCounter = counter
		}
		failpoint.Inject("ChaosBrokenWriterConn", func(_ failpoint.Value) {
			failpoint.Return(0, errors.New("connection is closed"))
		})
		failpoint.Inject("AtEveryRow", nil)

		fileRowIter.Next()
		if cfg.FileSize != UnspecifiedSize && uint64(preambleSize)+sw.EstimateFileSize() >= cfg.FileSize {
			break
		}
	}
	AddGauge(metrics.finishedRowsGauge, float64(counter-lastCounter))
	lastCounter = counter

	if err = sw.Close(); err != nil {
		return counter, errors.Trace(err)
	}
	finishedSize = uint64(preambleSize) + sw.EstimateFileSize()
	AddGauge(metrics.finishedSizeGauge, float64(finishedSize))
	if err = fileRowIter.Error(); err != nil {
		return counter, errors.Trace(err)
	}
	return counter, nil
}

// columnKinds maps Dumpling column type names to csvfile FieldKinds: binary and
// numeric types keep their kind, everything else defaults to string.
func columnKinds(colTypes []string) []csvfile.FieldKind {
	kinds := make([]csvfile.FieldKind, len(colTypes))
	for i, ct := range colTypes {
		if _, ok := dataTypeBin[ct]; ok {
			kinds[i] = csvfile.KindBytes
		} else if _, ok := dataTypeNum[ct]; ok {
			kinds[i] = csvfile.KindNumber
		} else {
			kinds[i] = csvfile.KindString
		}
	}
	return kinds
}

// toCSVBinaryFormat maps the export BinaryFormat to its csvfile counterpart.
func toCSVBinaryFormat(f BinaryFormat) csvfile.BinaryFormat {
	switch f {
	case BinaryFormatHEX:
		return csvfile.BinaryFormatHEX
	case BinaryFormatBase64:
		return csvfile.BinaryFormatBase64
	default:
		return csvfile.BinaryFormatUTF8
	}
}

// sqlColumnKinds is the sqlfile counterpart of columnKinds, with the same
// binary/numeric/string-default classification.
func sqlColumnKinds(colTypes []string) []sqlfile.FieldKind {
	kinds := make([]sqlfile.FieldKind, len(colTypes))
	for i, ct := range colTypes {
		if _, ok := dataTypeBin[ct]; ok {
			kinds[i] = sqlfile.KindBytes
		} else if _, ok := dataTypeNum[ct]; ok {
			kinds[i] = sqlfile.KindNumber
		} else {
			kinds[i] = sqlfile.KindString
		}
	}
	return kinds
}

// WriteInsertInCsv writes TableDataIR to an objectio.Writer in CSV format.
func WriteInsertInCsv(
	pCtx *tcontext.Context,
	cfg *Config,
	meta TableMeta,
	tblIR TableDataIR,
	w objectio.Writer,
	metrics *metrics,
) (n uint64, err error) {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return 0, fileRowIter.Error()
	}

	csvCfg := &csvfile.Config{
		NullValue:       []byte(cfg.CsvNullValue),
		Separator:       []byte(cfg.CsvSeparator),
		Delimiter:       []byte(cfg.CsvDelimiter),
		LineTerminator:  []byte(cfg.CsvLineTerminator),
		BinaryFormat:    toCSVBinaryFormat(DialectBinaryFormatMap[cfg.CsvOutputDialect]),
		EscapeBackslash: cfg.EscapeBackslash,
	}
	// CSVWriter writes directly into the object store writer, whose concurrent
	// multipart upload replaces the writerPipe.
	selectedFields := meta.SelectedField()
	var kinds []csvfile.FieldKind
	if selectedFields != "" {
		kinds = columnKinds(meta.ColumnTypes())
	}
	cw := csvfile.NewCSVWriter(&wrappedWriter{ctx: pCtx.Context, w: w}, kinds, csvCfg)

	var (
		row          = MakeRowReceiver(meta.ColumnTypes())
		counter      uint64
		lastCounter  uint64
		finishedSize uint64
	)

	defer func() {
		if err != nil {
			pCtx.L().Warn("fail to dumping table(chunk), will revert some metrics and start a retry if possible",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", lastCounter),
				zap.Uint64("finished size", finishedSize),
				log.ShortError(err))
			SubGauge(metrics.finishedRowsGauge, float64(lastCounter))
			SubGauge(metrics.finishedSizeGauge, float64(finishedSize))
		} else {
			pCtx.L().Debug("finish dumping table(chunk)",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", counter),
				zap.Uint64("finished size", finishedSize))
			summary.CollectSuccessUnit(summary.TotalBytes, 1, finishedSize)
			summary.CollectSuccessUnit("total rows", 1, counter)
		}
	}()

	if !cfg.NoHeader && len(meta.ColumnNames()) != 0 && selectedFields != "" {
		colNames := meta.ColumnNames()
		nameBytes := make([][]byte, len(colNames))
		for i, col := range colNames {
			nameBytes[i] = []byte(col)
		}
		if err = cw.WriteHeader(nameBytes); err != nil {
			return 0, errors.Trace(err)
		}
	}

	// rawRow is reused across rows to avoid a per-row slice allocation.
	rawRow := row.GetRawBytes()[:0]
	for fileRowIter.HasNext() {
		// When all table columns are generated, selectedFields is empty.
		// Dumpling still iterates source rows via SELECT '' and emits only
		// line terminators here.
		if selectedFields != "" {
			if err = fileRowIter.Decode(row); err != nil {
				return counter, errors.Trace(err)
			}
			rawRow = row.AppendRawBytes(rawRow[:0])
			if err = cw.Write(rawRow); err != nil {
				return counter, errors.Trace(err)
			}
		} else {
			// All columns are generated; emit an empty row (just the line
			// terminator) through the writer so it is counted for rotation.
			if err = cw.Write(nil); err != nil {
				return counter, errors.Trace(err)
			}
		}
		counter++
		if counter%1000 == 0 {
			AddGauge(metrics.finishedRowsGauge, float64(counter-lastCounter))
			lastCounter = counter
		}

		fileRowIter.Next()
		if cfg.FileSize != UnspecifiedSize && cw.EstimateFileSize() >= cfg.FileSize {
			break
		}
	}
	AddGauge(metrics.finishedRowsGauge, float64(counter-lastCounter))
	lastCounter = counter

	if err = cw.Close(); err != nil {
		return counter, errors.Trace(err)
	}
	finishedSize = cw.EstimateFileSize()
	AddGauge(metrics.finishedSizeGauge, float64(finishedSize))
	if err = fileRowIter.Error(); err != nil {
		return counter, errors.Trace(err)
	}
	return counter, nil
}

func write(tctx *tcontext.Context, writer objectio.Writer, str string) error {
	_, err := writer.Write(tctx, []byte(str))
	if err != nil {
		// str might be very long, only output the first 200 chars
		outputLength := min(len(str), 200)
		tctx.L().Warn("fail to write",
			zap.String("heading 200 characters", str[:outputLength]),
			zap.Error(err))
	}
	return errors.Trace(err)
}

func buildFileWriter(tctx *tcontext.Context, s storeapi.Storage, fileName string, compressType compressedio.CompressType) (objectio.Writer, func(ctx context.Context) error, error) {
	fileName += compressType.FileSuffix()
	fullPath := s.URI() + "/" + fileName
	writer, err := objstore.WithCompression(s, compressType, compressedio.DecompressConfig{}).Create(tctx, fileName, nil)
	if err != nil {
		tctx.L().Warn("fail to open file",
			zap.String("path", fullPath),
			zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	tctx.L().Debug("opened file", zap.String("path", fullPath))
	tearDownRoutine := func(ctx context.Context) error {
		err := writer.Close(ctx)
		failpoint.Inject("FailToCloseMetaFile", func(_ failpoint.Value) {
			err = errors.New("injected error: fail to close meta file")
		})
		if err == nil {
			return nil
		}
		err = errors.Trace(err)
		tctx.L().Warn("fail to close file",
			zap.String("path", fullPath),
			zap.Error(err))
		return err
	}
	return writer, tearDownRoutine, nil
}

func buildInterceptFileWriter(pCtx *tcontext.Context, s storeapi.Storage, fileName string, compressType compressedio.CompressType, wo *storeapi.WriterOption) (objectio.Writer, func(context.Context) error) {
	fileName += compressType.FileSuffix()
	var writer objectio.Writer
	fullPath := s.URI() + "/" + fileName
	fileWriter := &InterceptFileWriter{}
	initRoutine := func() error {
		// use separated context pCtx here to make sure context used in ExternalFile won't be canceled before close,
		// which will cause a context canceled error when closing gcs's Writer
		w, err := objstore.WithCompression(s, compressType, compressedio.DecompressConfig{}).Create(pCtx, fileName, wo)
		if err != nil {
			pCtx.L().Warn("fail to open file",
				zap.String("path", fullPath),
				zap.Error(err))
			return newWriterError(err)
		}
		writer = w
		pCtx.L().Debug("opened file", zap.String("path", fullPath))
		fileWriter.Writer = writer
		return nil
	}
	fileWriter.initRoutine = initRoutine

	tearDownRoutine := func(ctx context.Context) error {
		if writer == nil {
			return nil
		}
		pCtx.L().Debug("tear down lazy file writer...", zap.String("path", fullPath))
		err := writer.Close(ctx)
		failpoint.Inject("FailToCloseDataFile", func(_ failpoint.Value) {
			err = errors.New("injected error: fail to close data file")
		})
		if err != nil {
			pCtx.L().Warn("fail to close file",
				zap.String("path", fullPath),
				zap.Error(err))
		}
		return err
	}
	return fileWriter, tearDownRoutine
}

// LazyStringWriter is an interceptor of io.StringWriter,
// will lazily create file the first time StringWriter need to write something.
type LazyStringWriter struct {
	initRoutine func() error
	sync.Once
	io.StringWriter
	err error
}

// WriteString implements io.StringWriter. It check whether writer has written something and init a file at first time
func (l *LazyStringWriter) WriteString(str string) (int, error) {
	l.Do(func() { l.err = l.initRoutine() })
	if l.err != nil {
		return 0, errors.Errorf("open file error: %s", l.err.Error())
	}
	return l.StringWriter.WriteString(str)
}

type writerError struct {
	error
}

func (e *writerError) Error() string {
	return e.error.Error()
}

func newWriterError(err error) error {
	if err == nil {
		return nil
	}
	return &writerError{error: err}
}

// InterceptFileWriter is an interceptor of os.File,
// tracking whether a StringWriter has written something.
type InterceptFileWriter struct {
	objectio.Writer
	sync.Once
	SomethingIsWritten bool

	initRoutine func() error
	err         error
}

// Write implements objectio.Writer. It checks whether writer has written something and init a file at first time
func (w *InterceptFileWriter) Write(ctx context.Context, p []byte) (int, error) {
	w.Do(func() { w.err = w.initRoutine() })
	if len(p) > 0 {
		w.SomethingIsWritten = true
	}
	if w.err != nil {
		return 0, errors.Annotate(w.err, "open file error")
	}
	n, err := w.Writer.Write(ctx, p)
	return n, newWriterError(err)
}

// Close closes the InterceptFileWriter
func (w *InterceptFileWriter) Close(ctx context.Context) error {
	return w.Writer.Close(ctx)
}

func wrapBackTicks(identifier string) string {
	if !strings.HasPrefix(identifier, "`") && !strings.HasSuffix(identifier, "`") {
		return wrapStringWith(identifier, "`")
	}
	return identifier
}

func wrapStringWith(str string, wrapper string) string {
	return fmt.Sprintf("%s%s%s", wrapper, str, wrapper)
}

type wrappedWriter struct {
	ctx context.Context
	w   objectio.Writer
}

func (w *wrappedWriter) Write(p []byte) (n int, err error) {
	return w.w.Write(w.ctx, p)
}

// WriteInsertInParquet writes table rows to parquet format.
func WriteInsertInParquet(
	pCtx *tcontext.Context,
	cfg *Config,
	meta TableMeta,
	tblIR TableDataIR,
	w objectio.Writer,
	metrics *metrics,
) (n uint64, err error) {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return 0, fileRowIter.Error()
	}

	// parquet need to get more information from tableMeta
	opts := []parquetfile.WriterOption{
		parquetfile.WithCompression(parquetfile.CompressionCodec(cfg.ParquetCompressType)),
		parquetfile.WithDataPageSize(cfg.ParquetPageSize),
		parquetfile.WithRowGroupMemoryLimit(cfg.ParquetRowGroupSize),
	}
	writer, err := parquetfile.NewWriter(&wrappedWriter{ctx: pCtx.Context, w: w}, meta.ColumnInfos(), opts...)
	if err != nil {
		return 0, errors.Trace(err)
	}

	var (
		row            = MakeRowReceiver(meta.ColumnTypes())
		counter        uint64
		lastCounter    uint64
		finishedSize   uint64
		selectedFields = meta.SelectedField()
	)

	defer func() {
		if err != nil {
			pCtx.L().Warn("fail to dumping table(chunk), will revert some metrics and start a retry if possible",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", lastCounter),
				zap.Uint64("finished size", finishedSize),
				log.ShortError(err))
			SubGauge(metrics.finishedRowsGauge, float64(lastCounter))
			SubGauge(metrics.finishedSizeGauge, float64(finishedSize))
		} else {
			pCtx.L().Debug("finish dumping table(chunk)",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", counter),
				zap.Uint64("finished size", finishedSize))
			summary.CollectSuccessUnit(summary.TotalBytes, 1, finishedSize)
			summary.CollectSuccessUnit("total rows", 1, counter)
		}
	}()

	// Add rows to parquet writer; it flushes when accounted in-memory bytes reach
	// the configured row-group memory limit.
	for fileRowIter.HasNext() {
		// When all table columns are generated, selectedFields is empty.
		// Dumpling still iterates source rows via SELECT '' and writes no parquet
		// rows in this branch.
		if selectedFields != "" {
			if err = fileRowIter.Decode(row); err != nil {
				return counter, errors.Trace(err)
			}
			err = writer.Write((*row).GetRawBytes())
			if err != nil {
				return counter, errors.Trace(err)
			}
		}
		counter++
		if counter%1000 == 0 {
			AddGauge(metrics.finishedRowsGauge, float64(counter-lastCounter))
			lastCounter = counter
		}
		fileRowIter.Next()
		if cfg.FileSize != UnspecifiedSize && writer.EstimateFileSize() >= cfg.FileSize {
			break
		}
	}
	AddGauge(metrics.finishedRowsGauge, float64(counter-lastCounter))
	lastCounter = counter

	// write remain data and meta file
	if err = writer.Close(); err != nil {
		return counter, errors.Trace(err)
	}
	finishedSize = writer.EstimateFileSize()
	AddGauge(metrics.finishedSizeGauge, float64(finishedSize))
	if err = fileRowIter.Error(); err != nil {
		return counter, errors.Trace(err)
	}
	return counter, nil
}

// FileFormat is the format that output to file, including SQL text, CSV, and parquet.
type FileFormat int32

const (
	// FileFormatUnknown indicates the given file type is unknown
	FileFormatUnknown FileFormat = iota
	// FileFormatSQLText indicates the given file type is sql type
	FileFormatSQLText
	// FileFormatCSV indicates the given file type is csv type
	FileFormatCSV
	// FileFormatParquet indicates the given file type is parquet type
	FileFormatParquet
)

const (
	// FileFormatSQLTextString indicates the string/suffix of sql type file
	FileFormatSQLTextString = "sql"
	// FileFormatCSVString indicates the string/suffix of csv type file
	FileFormatCSVString = "csv"
	// FileFormatParquetString indicates the string/suffix of parquet type file
	FileFormatParquetString = "parquet"
)

// String implement Stringer.String method.
func (f FileFormat) String() string {
	switch f {
	case FileFormatSQLText:
		return strings.ToUpper(FileFormatSQLTextString)
	case FileFormatCSV:
		return strings.ToUpper(FileFormatCSVString)
	case FileFormatParquet:
		return strings.ToUpper(FileFormatParquetString)
	default:
		return "unknown"
	}
}

// Extension returns the extension for specific format.
//
//	text -> "sql"
//	csv  -> "csv"
//	parquet -> "parquet"
func (f FileFormat) Extension() string {
	switch f {
	case FileFormatSQLText:
		return FileFormatSQLTextString
	case FileFormatCSV:
		return FileFormatCSVString
	case FileFormatParquet:
		return FileFormatParquetString
	default:
		return "unknown_format"
	}
}

// WriteInsert writes TableDataIR to objectio.Writer in SQL/CSV/parquet type.
func (f FileFormat) WriteInsert(
	pCtx *tcontext.Context,
	cfg *Config,
	meta TableMeta,
	tblIR TableDataIR,
	w objectio.Writer,
	metrics *metrics,
) (uint64, error) {
	switch f {
	case FileFormatSQLText:
		return WriteInsert(pCtx, cfg, meta, tblIR, w, metrics)
	case FileFormatCSV:
		return WriteInsertInCsv(pCtx, cfg, meta, tblIR, w, metrics)
	case FileFormatParquet:
		return WriteInsertInParquet(pCtx, cfg, meta, tblIR, w, metrics)
	default:
		return 0, errors.Errorf("unknown file format")
	}
}
