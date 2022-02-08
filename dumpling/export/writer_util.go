// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/summary"
	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/dumpling/log"
)

const lengthLimit = 1048576

var pool = sync.Pool{New: func() interface{} {
	return &bytes.Buffer{}
}}

type writerPipe struct {
	input  chan *bytes.Buffer
	closed chan struct{}
	errCh  chan error
	labels prometheus.Labels

	finishedFileSize     uint64
	currentFileSize      uint64
	currentStatementSize uint64

	fileSizeLimit      uint64
	statementSizeLimit uint64

	w storage.ExternalFileWriter
}

func newWriterPipe(w storage.ExternalFileWriter, fileSizeLimit, statementSizeLimit uint64, labels prometheus.Labels) *writerPipe {
	return &writerPipe{
		input:  make(chan *bytes.Buffer, 8),
		closed: make(chan struct{}),
		errCh:  make(chan error, 1),
		w:      w,
		labels: labels,

		currentFileSize:      0,
		currentStatementSize: 0,
		fileSizeLimit:        fileSizeLimit,
		statementSizeLimit:   statementSizeLimit,
	}
}

func (b *writerPipe) Run(tctx *tcontext.Context) {
	defer close(b.closed)
	var errOccurs bool
	receiveChunkTime := time.Now()
	for {
		select {
		case s, ok := <-b.input:
			if !ok {
				return
			}
			if errOccurs {
				continue
			}
			ObserveHistogram(receiveWriteChunkTimeHistogram, b.labels, time.Since(receiveChunkTime).Seconds())
			receiveChunkTime = time.Now()
			err := writeBytes(tctx, b.w, s.Bytes())
			ObserveHistogram(writeTimeHistogram, b.labels, time.Since(receiveChunkTime).Seconds())
			AddGauge(finishedSizeGauge, b.labels, float64(s.Len()))
			b.finishedFileSize += uint64(s.Len())
			s.Reset()
			pool.Put(s)
			if err != nil {
				errOccurs = true
				b.errCh <- err
			}
			receiveChunkTime = time.Now()
		case <-tctx.Done():
			return
		}
	}
}

func (b *writerPipe) AddFileSize(fileSize uint64) {
	b.currentFileSize += fileSize
	b.currentStatementSize += fileSize
}

func (b *writerPipe) Error() error {
	select {
	case err := <-b.errCh:
		return err
	default:
		return nil
	}
}

func (b *writerPipe) ShouldSwitchFile() bool {
	return b.fileSizeLimit != UnspecifiedSize && b.currentFileSize >= b.fileSizeLimit
}

func (b *writerPipe) ShouldSwitchStatement() bool {
	return (b.fileSizeLimit != UnspecifiedSize && b.currentFileSize >= b.fileSizeLimit) ||
		(b.statementSizeLimit != UnspecifiedSize && b.currentStatementSize >= b.statementSizeLimit)
}

// WriteMeta writes MetaIR to a storage.ExternalFileWriter
func WriteMeta(tctx *tcontext.Context, meta MetaIR, w storage.ExternalFileWriter) error {
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

// WriteInsert writes TableDataIR to a storage.ExternalFileWriter in sql type
func WriteInsert(pCtx *tcontext.Context, cfg *Config, meta TableMeta, tblIR TableDataIR, w storage.ExternalFileWriter) (n uint64, err error) {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return 0, fileRowIter.Error()
	}

	bf := pool.Get().(*bytes.Buffer)
	if bfCap := bf.Cap(); bfCap < lengthLimit {
		bf.Grow(lengthLimit - bfCap)
	}

	wp := newWriterPipe(w, cfg.FileSize, cfg.StatementSize, cfg.Labels)

	// use context.Background here to make sure writerPipe can deplete all the chunks in pipeline
	ctx, cancel := tcontext.Background().WithLogger(pCtx.L()).WithCancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wp.Run(ctx)
		wg.Done()
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		bf.WriteString(specCmtIter.Next())
		bf.WriteByte('\n')
	}
	wp.currentFileSize += uint64(bf.Len())

	var (
		insertStatementPrefix string
		row                   = MakeRowReceiver(meta.ColumnTypes())
		counter               uint64
		lastCounter           uint64
		escapeBackslash       = cfg.EscapeBackslash
	)

	defer func() {
		if err != nil {
			pCtx.L().Warn("fail to dumping table(chunk), will revert some metrics and start a retry if possible",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", lastCounter),
				zap.Uint64("finished size", wp.finishedFileSize),
				log.ShortError(err))
			SubGauge(finishedRowsGauge, cfg.Labels, float64(lastCounter))
			SubGauge(finishedSizeGauge, cfg.Labels, float64(wp.finishedFileSize))
		} else {
			pCtx.L().Debug("finish dumping table(chunk)",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", counter),
				zap.Uint64("finished size", wp.finishedFileSize))
			summary.CollectSuccessUnit(summary.TotalBytes, 1, wp.finishedFileSize)
			summary.CollectSuccessUnit("total rows", 1, counter)
		}
	}()

	selectedField := meta.SelectedField()

	// if has generated column
	if selectedField != "" && selectedField != "*" {
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s (%s) VALUES\n",
			wrapBackTicks(escapeString(meta.TableName())), selectedField)
	} else {
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s VALUES\n",
			wrapBackTicks(escapeString(meta.TableName())))
	}
	insertStatementPrefixLen := uint64(len(insertStatementPrefix))

	for fileRowIter.HasNext() {
		wp.currentStatementSize = 0
		bf.WriteString(insertStatementPrefix)
		wp.AddFileSize(insertStatementPrefixLen)

		for fileRowIter.HasNext() {
			lastBfSize := bf.Len()
			if selectedField != "" {
				if err = fileRowIter.Decode(row); err != nil {
					return counter, errors.Trace(err)
				}
				row.WriteToBuffer(bf, escapeBackslash)
			} else {
				bf.WriteString("()")
			}
			counter++
			wp.AddFileSize(uint64(bf.Len()-lastBfSize) + 2) // 2 is for ",\n" and ";\n"
			failpoint.Inject("ChaosBrokenWriterConn", func(_ failpoint.Value) {
				failpoint.Return(0, errors.New("connection is closed"))
			})

			fileRowIter.Next()
			shouldSwitch := wp.ShouldSwitchStatement()
			if fileRowIter.HasNext() && !shouldSwitch {
				bf.WriteString(",\n")
			} else {
				bf.WriteString(";\n")
			}
			if bf.Len() >= lengthLimit {
				select {
				case <-pCtx.Done():
					return counter, pCtx.Err()
				case err = <-wp.errCh:
					return counter, err
				case wp.input <- bf:
					bf = pool.Get().(*bytes.Buffer)
					if bfCap := bf.Cap(); bfCap < lengthLimit {
						bf.Grow(lengthLimit - bfCap)
					}
					AddGauge(finishedRowsGauge, cfg.Labels, float64(counter-lastCounter))
					lastCounter = counter
				}
			}

			if shouldSwitch {
				break
			}
		}
		if wp.ShouldSwitchFile() {
			break
		}
	}
	if bf.Len() > 0 {
		wp.input <- bf
	}
	close(wp.input)
	<-wp.closed
	AddGauge(finishedRowsGauge, cfg.Labels, float64(counter-lastCounter))
	lastCounter = counter
	if err = fileRowIter.Error(); err != nil {
		return counter, errors.Trace(err)
	}
	return counter, wp.Error()
}

// WriteInsertInCsv writes TableDataIR to a storage.ExternalFileWriter in csv type
func WriteInsertInCsv(pCtx *tcontext.Context, cfg *Config, meta TableMeta, tblIR TableDataIR, w storage.ExternalFileWriter) (n uint64, err error) {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return 0, fileRowIter.Error()
	}

	bf := pool.Get().(*bytes.Buffer)
	if bfCap := bf.Cap(); bfCap < lengthLimit {
		bf.Grow(lengthLimit - bfCap)
	}

	wp := newWriterPipe(w, cfg.FileSize, UnspecifiedSize, cfg.Labels)
	opt := &csvOption{
		nullValue: cfg.CsvNullValue,
		separator: []byte(cfg.CsvSeparator),
		delimiter: []byte(cfg.CsvDelimiter),
	}

	// use context.Background here to make sure writerPipe can deplete all the chunks in pipeline
	ctx, cancel := tcontext.Background().WithLogger(pCtx.L()).WithCancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		wp.Run(ctx)
		wg.Done()
	}()
	defer func() {
		cancel()
		wg.Wait()
	}()

	var (
		row             = MakeRowReceiver(meta.ColumnTypes())
		counter         uint64
		lastCounter     uint64
		escapeBackslash = cfg.EscapeBackslash
		selectedFields  = meta.SelectedField()
	)

	defer func() {
		if err != nil {
			pCtx.L().Warn("fail to dumping table(chunk), will revert some metrics and start a retry if possible",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", lastCounter),
				zap.Uint64("finished size", wp.finishedFileSize),
				log.ShortError(err))
			SubGauge(finishedRowsGauge, cfg.Labels, float64(lastCounter))
			SubGauge(finishedSizeGauge, cfg.Labels, float64(wp.finishedFileSize))
		} else {
			pCtx.L().Debug("finish dumping table(chunk)",
				zap.String("database", meta.DatabaseName()),
				zap.String("table", meta.TableName()),
				zap.Uint64("finished rows", counter),
				zap.Uint64("finished size", wp.finishedFileSize))
			summary.CollectSuccessUnit(summary.TotalBytes, 1, wp.finishedFileSize)
			summary.CollectSuccessUnit("total rows", 1, counter)
		}
	}()

	if !cfg.NoHeader && len(meta.ColumnNames()) != 0 && selectedFields != "" {
		for i, col := range meta.ColumnNames() {
			bf.Write(opt.delimiter)
			escapeCSV([]byte(col), bf, escapeBackslash, opt)
			bf.Write(opt.delimiter)
			if i != len(meta.ColumnTypes())-1 {
				bf.Write(opt.separator)
			}
		}
		bf.WriteByte('\r' + '\n')
	}
	wp.currentFileSize += uint64(bf.Len())

	for fileRowIter.HasNext() {
		lastBfSize := bf.Len()
		if selectedFields != "" {
			if err = fileRowIter.Decode(row); err != nil {
				return counter, errors.Trace(err)
			}
			row.WriteToBufferInCsv(bf, escapeBackslash, opt)
		}
		counter++
		wp.currentFileSize += uint64(bf.Len()-lastBfSize) + 1 // 1 is for "\n"

		bf.WriteByte('\r' + '\n')
		if bf.Len() >= lengthLimit {
			select {
			case <-pCtx.Done():
				return counter, pCtx.Err()
			case err = <-wp.errCh:
				return counter, err
			case wp.input <- bf:
				bf = pool.Get().(*bytes.Buffer)
				if bfCap := bf.Cap(); bfCap < lengthLimit {
					bf.Grow(lengthLimit - bfCap)
				}
				AddGauge(finishedRowsGauge, cfg.Labels, float64(counter-lastCounter))
				lastCounter = counter
			}
		}

		fileRowIter.Next()
		if wp.ShouldSwitchFile() {
			break
		}
	}

	if bf.Len() > 0 {
		wp.input <- bf
	}
	close(wp.input)
	<-wp.closed
	AddGauge(finishedRowsGauge, cfg.Labels, float64(counter-lastCounter))
	lastCounter = counter
	if err = fileRowIter.Error(); err != nil {
		return counter, errors.Trace(err)
	}
	return counter, wp.Error()
}

func write(tctx *tcontext.Context, writer storage.ExternalFileWriter, str string) error {
	_, err := writer.Write(tctx, []byte(str))
	if err != nil {
		// str might be very long, only output the first 200 chars
		outputLength := len(str)
		if outputLength >= 200 {
			outputLength = 200
		}
		tctx.L().Warn("fail to write",
			zap.String("heading 200 characters", str[:outputLength]),
			zap.Error(err))
	}
	return errors.Trace(err)
}

func writeBytes(tctx *tcontext.Context, writer storage.ExternalFileWriter, p []byte) error {
	_, err := writer.Write(tctx, p)
	if err != nil {
		// str might be very long, only output the first 200 chars
		outputLength := len(p)
		if outputLength >= 200 {
			outputLength = 200
		}
		tctx.L().Warn("fail to write",
			zap.ByteString("heading 200 characters", p[:outputLength]),
			zap.Error(err))
		if strings.Contains(err.Error(), "Part number must be an integer between 1 and 10000") {
			err = errors.Annotate(err, "workaround: dump file exceeding 50GB, please specify -F=256MB -r=200000 to avoid this problem")
		}
	}
	return errors.Trace(err)
}

func buildFileWriter(tctx *tcontext.Context, s storage.ExternalStorage, fileName string, compressType storage.CompressType) (storage.ExternalFileWriter, func(ctx context.Context), error) {
	fileName += compressFileSuffix(compressType)
	fullPath := path.Join(s.URI(), fileName)
	writer, err := storage.WithCompression(s, compressType).Create(tctx, fileName)
	if err != nil {
		tctx.L().Warn("fail to open file",
			zap.String("path", fullPath),
			zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	tctx.L().Debug("opened file", zap.String("path", fullPath))
	tearDownRoutine := func(ctx context.Context) {
		err := writer.Close(ctx)
		if err == nil {
			return
		}
		err = errors.Trace(err)
		tctx.L().Warn("fail to close file",
			zap.String("path", fullPath),
			zap.Error(err))
	}
	return writer, tearDownRoutine, nil
}

func buildInterceptFileWriter(pCtx *tcontext.Context, s storage.ExternalStorage, fileName string, compressType storage.CompressType) (storage.ExternalFileWriter, func(context.Context)) {
	fileName += compressFileSuffix(compressType)
	var writer storage.ExternalFileWriter
	fullPath := path.Join(s.URI(), fileName)
	fileWriter := &InterceptFileWriter{}
	initRoutine := func() error {
		// use separated context pCtx here to make sure context used in ExternalFile won't be canceled before close,
		// which will cause a context canceled error when closing gcs's Writer
		w, err := storage.WithCompression(s, compressType).Create(pCtx, fileName)
		if err != nil {
			pCtx.L().Warn("fail to open file",
				zap.String("path", fullPath),
				zap.Error(err))
			return newWriterError(err)
		}
		writer = w
		pCtx.L().Debug("opened file", zap.String("path", fullPath))
		fileWriter.ExternalFileWriter = writer
		return nil
	}
	fileWriter.initRoutine = initRoutine

	tearDownRoutine := func(ctx context.Context) {
		if writer == nil {
			return
		}
		pCtx.L().Debug("tear down lazy file writer...", zap.String("path", fullPath))
		err := writer.Close(ctx)
		if err != nil {
			pCtx.L().Warn("fail to close file",
				zap.String("path", fullPath),
				zap.Error(err))
		}
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
	storage.ExternalFileWriter
	sync.Once
	SomethingIsWritten bool

	initRoutine func() error
	err         error
}

// Write implements storage.ExternalFileWriter.Write. It check whether writer has written something and init a file at first time
func (w *InterceptFileWriter) Write(ctx context.Context, p []byte) (int, error) {
	w.Do(func() { w.err = w.initRoutine() })
	if len(p) > 0 {
		w.SomethingIsWritten = true
	}
	if w.err != nil {
		return 0, errors.Annotate(w.err, "open file error")
	}
	n, err := w.ExternalFileWriter.Write(ctx, p)
	return n, newWriterError(err)
}

// Close closes the InterceptFileWriter
func (w *InterceptFileWriter) Close(ctx context.Context) error {
	return w.ExternalFileWriter.Close(ctx)
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

func compressFileSuffix(compressType storage.CompressType) string {
	switch compressType {
	case storage.NoCompression:
		return ""
	case storage.Gzip:
		return ".gz"
	default:
		return ""
	}
}

// FileFormat is the format that output to file. Currently we support SQL text and CSV file format.
type FileFormat int32

const (
	// FileFormatUnknown indicates the given file type is unknown
	FileFormatUnknown FileFormat = iota
	// FileFormatSQLText indicates the given file type is sql type
	FileFormatSQLText
	// FileFormatCSV indicates the given file type is csv type
	FileFormatCSV
)

const (
	// FileFormatSQLTextString indicates the string/suffix of sql type file
	FileFormatSQLTextString = "sql"
	// FileFormatCSVString indicates the string/suffix of csv type file
	FileFormatCSVString = "csv"
)

// String implement Stringer.String method.
func (f FileFormat) String() string {
	switch f {
	case FileFormatSQLText:
		return strings.ToUpper(FileFormatSQLTextString)
	case FileFormatCSV:
		return strings.ToUpper(FileFormatCSVString)
	default:
		return "unknown"
	}
}

// Extension returns the extension for specific format.
//  text -> "sql"
//  csv  -> "csv"
func (f FileFormat) Extension() string {
	switch f {
	case FileFormatSQLText:
		return FileFormatSQLTextString
	case FileFormatCSV:
		return FileFormatCSVString
	default:
		return "unknown_format"
	}
}

// WriteInsert writes TableDataIR to a storage.ExternalFileWriter in sql/csv type
func (f FileFormat) WriteInsert(pCtx *tcontext.Context, cfg *Config, meta TableMeta, tblIR TableDataIR, w storage.ExternalFileWriter) (uint64, error) {
	switch f {
	case FileFormatSQLText:
		return WriteInsert(pCtx, cfg, meta, tblIR, w)
	case FileFormatCSV:
		return WriteInsertInCsv(pCtx, cfg, meta, tblIR, w)
	default:
		return 0, errors.Errorf("unknown file format")
	}
}
