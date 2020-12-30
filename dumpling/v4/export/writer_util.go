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

	"github.com/pingcap/br/pkg/storage"
	"github.com/pingcap/br/pkg/summary"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/pingcap/dumpling/v4/log"
)

const lengthLimit = 1048576

// TODO make this configurable, 5 mb is a good minimum size but on low latency/high bandwidth network you can go a lot bigger
const hardcodedS3ChunkSize = 5 * 1024 * 1024

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

	w storage.Writer
}

func newWriterPipe(w storage.Writer, fileSizeLimit, statementSizeLimit uint64, labels prometheus.Labels) *writerPipe {
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

func (b *writerPipe) Run(ctx context.Context) {
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
			receiveWriteChunkTimeHistogram.With(b.labels).Observe(time.Since(receiveChunkTime).Seconds())
			receiveChunkTime = time.Now()
			err := writeBytes(ctx, b.w, s.Bytes())
			writeTimeHistogram.With(b.labels).Observe(time.Since(receiveChunkTime).Seconds())
			finishedSizeCounter.With(b.labels).Add(float64(s.Len()))
			b.finishedFileSize += uint64(s.Len())
			s.Reset()
			pool.Put(s)
			if err != nil {
				errOccurs = true
				b.errCh <- err
			}
			receiveChunkTime = time.Now()
		case <-ctx.Done():
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

// WriteMeta writes MetaIR to a storage.Writer
func WriteMeta(ctx context.Context, meta MetaIR, w storage.Writer) error {
	log.Debug("start dumping meta data", zap.String("target", meta.TargetName()))

	specCmtIter := meta.SpecialComments()
	for specCmtIter.HasNext() {
		if err := write(ctx, w, fmt.Sprintf("%s\n", specCmtIter.Next())); err != nil {
			return err
		}
	}

	if err := write(ctx, w, meta.MetaSQL()); err != nil {
		return err
	}

	log.Debug("finish dumping meta data", zap.String("target", meta.TargetName()))
	return nil
}

// WriteInsert writes TableDataIR to a storage.Writer in sql type
func WriteInsert(pCtx context.Context, cfg *Config, meta TableMeta, tblIR TableDataIR, w storage.Writer) error {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return nil
	}

	bf := pool.Get().(*bytes.Buffer)
	if bfCap := bf.Cap(); bfCap < lengthLimit {
		bf.Grow(lengthLimit - bfCap)
	}

	wp := newWriterPipe(w, cfg.FileSize, cfg.StatementSize, cfg.Labels)

	ctx, cancel := context.WithCancel(context.Background())
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
		err                   error
	)

	selectedField := meta.SelectedField()

	// if has generated column
	if selectedField != "" && selectedField != "*" {
		insertStatementPrefix = fmt.Sprintf("INSERT INTO %s %s VALUES\n",
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
					log.Error("fail to scan from sql.Row", zap.Error(err))
					return errors.Trace(err)
				}
				row.WriteToBuffer(bf, escapeBackslash)
			} else {
				bf.WriteString("()")
			}
			counter++
			wp.AddFileSize(uint64(bf.Len()-lastBfSize) + 2) // 2 is for ",\n" and ";\n"
			failpoint.Inject("ChaosBrokenMySQLConn", func(_ failpoint.Value) {
				failpoint.Return(errors.New("connection is closed"))
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
					return pCtx.Err()
				case err = <-wp.errCh:
					return err
				case wp.input <- bf:
					bf = pool.Get().(*bytes.Buffer)
					if bfCap := bf.Cap(); bfCap < lengthLimit {
						bf.Grow(lengthLimit - bfCap)
					}
					finishedRowsCounter.With(cfg.Labels).Add(float64(counter - lastCounter))
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
	log.Debug("finish dumping table(chunk)",
		zap.String("database", meta.DatabaseName()),
		zap.String("table", meta.TableName()),
		zap.Uint64("total rows", counter))
	if bf.Len() > 0 {
		wp.input <- bf
	}
	close(wp.input)
	<-wp.closed
	summary.CollectSuccessUnit(summary.TotalBytes, 1, wp.finishedFileSize)
	summary.CollectSuccessUnit("total rows", 1, counter)
	finishedRowsCounter.With(cfg.Labels).Add(float64(counter - lastCounter))
	if err = fileRowIter.Error(); err != nil {
		return errors.Trace(err)
	}
	return wp.Error()
}

// WriteInsertInCsv writes TableDataIR to a storage.Writer in csv type
func WriteInsertInCsv(pCtx context.Context, cfg *Config, meta TableMeta, tblIR TableDataIR, w storage.Writer) error {
	fileRowIter := tblIR.Rows()
	if !fileRowIter.HasNext() {
		return nil
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

	ctx, cancel := context.WithCancel(context.Background())
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
		err             error
	)

	if !cfg.NoHeader && len(meta.ColumnNames()) != 0 && selectedFields != "" {
		for i, col := range meta.ColumnNames() {
			bf.Write(opt.delimiter)
			escapeCSV([]byte(col), bf, escapeBackslash, opt)
			bf.Write(opt.delimiter)
			if i != len(meta.ColumnTypes())-1 {
				bf.Write(opt.separator)
			}
		}
		bf.WriteByte('\n')
	}
	wp.currentFileSize += uint64(bf.Len())

	for fileRowIter.HasNext() {
		lastBfSize := bf.Len()
		if selectedFields != "" {
			if err = fileRowIter.Decode(row); err != nil {
				log.Error("fail to scan from sql.Row", zap.Error(err))
				return errors.Trace(err)
			}
			row.WriteToBufferInCsv(bf, escapeBackslash, opt)
		}
		counter++
		wp.currentFileSize += uint64(bf.Len()-lastBfSize) + 1 // 1 is for "\n"

		bf.WriteByte('\n')
		if bf.Len() >= lengthLimit {
			select {
			case <-pCtx.Done():
				return pCtx.Err()
			case err = <-wp.errCh:
				return err
			case wp.input <- bf:
				bf = pool.Get().(*bytes.Buffer)
				if bfCap := bf.Cap(); bfCap < lengthLimit {
					bf.Grow(lengthLimit - bfCap)
				}
				finishedRowsCounter.With(cfg.Labels).Add(float64(counter - lastCounter))
				lastCounter = counter
			}
		}

		fileRowIter.Next()
		if wp.ShouldSwitchFile() {
			break
		}
	}

	log.Debug("finish dumping table(chunk)",
		zap.String("database", meta.DatabaseName()),
		zap.String("table", meta.TableName()),
		zap.Uint64("total rows", counter))
	if bf.Len() > 0 {
		wp.input <- bf
	}
	close(wp.input)
	<-wp.closed
	summary.CollectSuccessUnit(summary.TotalBytes, 1, wp.finishedFileSize)
	summary.CollectSuccessUnit("total rows", 1, counter)
	finishedRowsCounter.With(cfg.Labels).Add(float64(counter - lastCounter))
	if err = fileRowIter.Error(); err != nil {
		return errors.Trace(err)
	}
	return wp.Error()
}

func write(ctx context.Context, writer storage.Writer, str string) error {
	_, err := writer.Write(ctx, []byte(str))
	if err != nil {
		// str might be very long, only output the first 200 chars
		outputLength := len(str)
		if outputLength >= 200 {
			outputLength = 200
		}
		log.Error("writing failed",
			zap.String("string", str[:outputLength]),
			zap.Error(err))
	}
	return errors.Trace(err)
}

func writeBytes(ctx context.Context, writer storage.Writer, p []byte) error {
	_, err := writer.Write(ctx, p)
	if err != nil {
		// str might be very long, only output the first 200 chars
		outputLength := len(p)
		if outputLength >= 200 {
			outputLength = 200
		}
		log.Error("writing failed",
			zap.ByteString("string", p[:outputLength]),
			zap.String("writer", fmt.Sprintf("%#v", writer)),
			zap.Error(err))
	}
	return errors.Trace(err)
}

func buildFileWriter(ctx context.Context, s storage.ExternalStorage, fileName string, compressType storage.CompressType) (storage.Writer, func(ctx context.Context), error) {
	fileName += compressFileSuffix(compressType)
	fullPath := path.Join(s.URI(), fileName)
	uploader, err := s.CreateUploader(ctx, fileName)
	if err != nil {
		log.Error("open file failed",
			zap.String("path", fullPath),
			zap.Error(err))
		return nil, nil, errors.Trace(err)
	}
	writer := storage.NewUploaderWriter(uploader, hardcodedS3ChunkSize, compressType)
	log.Debug("opened file", zap.String("path", fullPath))
	tearDownRoutine := func(ctx context.Context) {
		err := writer.Close(ctx)
		if err == nil {
			return
		}
		err = errors.Trace(err)
		log.Error("close file failed",
			zap.String("path", fullPath),
			zap.Error(err))
	}
	return writer, tearDownRoutine, nil
}

func buildInterceptFileWriter(s storage.ExternalStorage, fileName string, compressType storage.CompressType) (storage.Writer, func(context.Context)) {
	fileName += compressFileSuffix(compressType)
	var writer storage.Writer
	fullPath := path.Join(s.URI(), fileName)
	fileWriter := &InterceptFileWriter{}
	initRoutine := func(ctx context.Context) error {
		uploader, err := s.CreateUploader(ctx, fileName)
		if err != nil {
			log.Error("open file failed",
				zap.String("path", fullPath),
				zap.Error(err))
			return newWriterError(err)
		}
		w := storage.NewUploaderWriter(uploader, hardcodedS3ChunkSize, compressType)
		writer = w
		log.Debug("opened file", zap.String("path", fullPath))
		fileWriter.Writer = writer
		return nil
	}
	fileWriter.initRoutine = initRoutine

	tearDownRoutine := func(ctx context.Context) {
		if writer == nil {
			return
		}
		log.Debug("tear down lazy file writer...", zap.String("path", fullPath))
		err := writer.Close(ctx)
		if err != nil {
			log.Error("close file failed", zap.String("path", fullPath))
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
	storage.Writer
	sync.Once
	SomethingIsWritten bool

	initRoutine func(context.Context) error
	err         error
}

// Write implements storage.Writer.Write. It check whether writer has written something and init a file at first time
func (w *InterceptFileWriter) Write(ctx context.Context, p []byte) (int, error) {
	w.Do(func() { w.err = w.initRoutine(ctx) })
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

// WriteInsert writes TableDataIR to a storage.Writer in sql/csv type
func (f FileFormat) WriteInsert(pCtx context.Context, cfg *Config, meta TableMeta, tblIR TableDataIR, w storage.Writer) error {
	switch f {
	case FileFormatSQLText:
		return WriteInsert(pCtx, cfg, meta, tblIR, w)
	case FileFormatCSV:
		return WriteInsertInCsv(pCtx, cfg, meta, tblIR, w)
	default:
		return errors.Errorf("unknown file format")
	}
}
