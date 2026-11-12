// Copyright 2026 PingCAP, Inc. Licensed under Apache-2.0.

package export

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	tcontext "github.com/pingcap/tidb/dumpling/context"
	"github.com/pingcap/tidb/pkg/objstore/objectio"
	"github.com/pingcap/tidb/pkg/objstore/storeapi"
	"github.com/stretchr/testify/require"
)

func TestWriterReadsWhileOutputBlocked(t *testing.T) {
	conf := defaultConfigForTest(t)
	conf.OutputDirPath = t.TempDir()
	conf.StatementSize = UnspecifiedSize
	w := createTestWriter(conf, t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w.tctx = w.tctx.WithContext(ctx)

	writeStarted := make(chan struct{})
	sourceAdvanced := make(chan struct{})
	iter := &overlapRowIter{
		rowCount:       64,
		buffer:         make([]byte, 64*1024),
		args:           make([]any, 1),
		writeStarted:   writeStarted,
		sourceAdvanced: sourceAdvanced,
	}
	meta := newMockTableIR("test", "overlap", nil, nil, []string{"TEXT"})
	data := &overlapTableData{mockTableIR: meta, iter: iter}
	w.extStorage = &overlapStorage{
		Storage:        w.extStorage,
		writeStarted:   writeStarted,
		sourceAdvanced: sourceAdvanced,
	}
	tasks := make(chan Task, 1)
	tasks <- &TaskTableData{Meta: meta, Data: data, TotalChunks: 1}
	close(tasks)

	require.NoError(t, w.run(tasks), "the source must keep advancing while an output write is blocked")
	output, err := os.ReadFile(filepath.Join(conf.OutputDirPath, "test.overlap.000000000.sql"))
	require.NoError(t, err)
	var expected strings.Builder
	expected.WriteString("INSERT INTO `overlap` VALUES\n")
	for i := range iter.rowCount {
		expected.WriteString("('")
		expected.WriteString(strings.Repeat(string(rune('a'+i%26)), len(iter.buffer)))
		if i+1 == iter.rowCount {
			expected.WriteString("');\n")
		} else {
			expected.WriteString("'),\n")
		}
	}
	require.Equal(t, expected.String(), string(output), "source buffers must remain owned by their queued rows")
}

type overlapTableData struct {
	*mockTableIR
	iter *overlapRowIter
}

func (d *overlapTableData) Start(ctx *tcontext.Context, _ *sql.Conn) error {
	d.iter.ctx = ctx
	return nil
}

func (d *overlapTableData) Rows() SQLRowIter { return d.iter }

type overlapRowIter struct {
	ctx            context.Context
	rowCount       int
	index          int
	buffer         []byte
	args           []any
	err            error
	writeStarted   <-chan struct{}
	sourceAdvanced chan<- struct{}
}

func (i *overlapRowIter) Decode(row RowReceiver) error {
	if err := i.ctx.Err(); err != nil {
		return err
	}
	for j := range i.buffer {
		i.buffer[j] = byte('a' + i.index%26)
	}
	row.BindAddress(i.args)
	*i.args[0].(*sql.RawBytes) = i.buffer
	return nil
}

func (i *overlapRowIter) Next() {
	i.index++
	// Each row is 64 KiB, so this point is beyond the batch containing the
	// first row. Wait for the output to block before proving source progress.
	if i.index == 32 {
		select {
		case <-i.writeStarted:
			close(i.sourceAdvanced)
		case <-i.ctx.Done():
			i.err = i.ctx.Err()
		}
	}
}

func (i *overlapRowIter) HasNext() bool { return i.err == nil && i.index < i.rowCount }
func (i *overlapRowIter) Error() error  { return i.err }
func (*overlapRowIter) Close() error    { return nil }

type overlapStorage struct {
	storeapi.Storage
	writeStarted   chan<- struct{}
	sourceAdvanced <-chan struct{}
}

func (s *overlapStorage) Create(ctx context.Context, name string, option *storeapi.WriterOption) (objectio.Writer, error) {
	w, err := s.Storage.Create(ctx, name, option)
	if err != nil {
		return nil, err
	}
	return &overlapOutputWriter{Writer: w, writeStarted: s.writeStarted, sourceAdvanced: s.sourceAdvanced}, nil
}

type overlapOutputWriter struct {
	objectio.Writer
	once           sync.Once
	err            error
	writeStarted   chan<- struct{}
	sourceAdvanced <-chan struct{}
}

func (w *overlapOutputWriter) Write(ctx context.Context, p []byte) (int, error) {
	w.once.Do(func() {
		close(w.writeStarted)
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		select {
		case <-w.sourceAdvanced:
		case <-ctx.Done():
			w.err = ctx.Err()
		case <-timer.C:
			w.err = errors.New("source did not advance while output write was blocked")
		}
	})
	if w.err != nil {
		return 0, w.err
	}
	return w.Writer.Write(ctx, p)
}

func TestWriterCSVReusedRowsAcrossFiles(t *testing.T) {
	conf := defaultConfigForTest(t)
	conf.OutputDirPath = t.TempDir()
	conf.FileType = FileFormatCSVString
	conf.NoHeader = true
	conf.CsvLineTerminator = "\n"
	conf.FileSize = 1
	w := createTestWriter(conf, t)
	meta := newMockTableIR("test", "reused", nil, nil, []string{"TEXT"})
	// Both rows fit individually, but together overflow a partially filled batch.
	largeFirst := strings.Repeat("x", 3*readBatchBytes/4)
	largeSecond := strings.Repeat("y", readBatchBytes/2)
	data := &workerTestTableData{
		mockTableIR: meta,
		newIter: func(_ int) *workerTestRowIter {
			return &workerTestRowIter{values: [][]byte{[]byte("first"), nil, {}, []byte(largeFirst), []byte(largeSecond), []byte("last")}}
		},
	}

	require.NoError(t, runWorkerTestTask(w, meta, data))
	require.Equal(t, []string{
		"\"first\"\n", "\\N\n", "\"\"\n", "\"" + largeFirst + "\"\n", "\"" + largeSecond + "\"\n", "\"last\"\n",
	}, workerTestOutput(t, conf.OutputDirPath))
	require.True(t, data.iter.closed, "the source must be closed before the worker returns")
}

func TestWriterReadsWhileCSVOutputBlocked(t *testing.T) {
	conf := defaultConfigForTest(t)
	conf.OutputDirPath = t.TempDir()
	conf.FileType = FileFormatCSVString
	conf.NoHeader = true
	conf.CsvLineTerminator = "\n"
	w := createTestWriter(conf, t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w.tctx = w.tctx.WithContext(ctx)
	writeStarted, sourceAdvanced := make(chan struct{}), make(chan struct{})
	iter := &overlapRowIter{
		rowCount: 64, buffer: make([]byte, 64*1024), args: make([]any, 1),
		writeStarted: writeStarted, sourceAdvanced: sourceAdvanced,
	}
	meta := newMockTableIR("test", "csv_overlap", nil, nil, []string{"TEXT"})
	data := &overlapTableData{mockTableIR: meta, iter: iter}
	w.extStorage = &overlapStorage{Storage: w.extStorage, writeStarted: writeStarted, sourceAdvanced: sourceAdvanced}

	require.NoError(t, runWorkerTestTask(w, meta, data))
	var expected strings.Builder
	for i := range iter.rowCount {
		expected.WriteByte('"')
		expected.WriteString(strings.Repeat(string(rune('a'+i%26)), len(iter.buffer)))
		expected.WriteString("\"\n")
	}
	require.Equal(t, []string{expected.String()}, workerTestOutput(t, conf.OutputDirPath))
}

func TestWriterSourceErrors(t *testing.T) {
	for _, stage := range []string{"decode", "next"} {
		t.Run(stage, func(t *testing.T) {
			conf := defaultConfigForTest(t)
			conf.OutputDirPath = t.TempDir()
			w := createTestWriter(conf, t)
			sourceErr := errors.New("source " + stage + " failure")
			meta := newMockTableIR("test", "failure", nil, nil, []string{"TEXT"})
			data := &workerTestTableData{
				mockTableIR: meta,
				newIter: func(_ int) *workerTestRowIter {
					iter := &workerTestRowIter{values: [][]byte{[]byte("first"), []byte("second")}, errorIndex: 1}
					if stage == "decode" {
						iter.decodeErr = sourceErr
					} else {
						iter.readErr = sourceErr
					}
					return iter
				},
			}

			require.ErrorContains(t, runWorkerTestTask(w, meta, data), sourceErr.Error())
			require.True(t, data.iter.closed, "the failed source must be closed before the worker returns")
		})
	}
}

func TestWriterDecodeErrorAfterFileRotation(t *testing.T) {
	conf := defaultConfigForTest(t)
	conf.OutputDirPath = t.TempDir()
	conf.FileType = FileFormatCSVString
	conf.NoHeader = true
	conf.CsvLineTerminator = "\n"
	conf.FileSize = 1
	w := createTestWriter(conf, t)
	sourceErr := errors.New("decode failure in next file")
	meta := newMockTableIR("test", "decode_boundary", nil, nil, []string{"TEXT"})
	data := &workerTestTableData{
		mockTableIR: meta,
		newIter: func(_ int) *workerTestRowIter {
			return &workerTestRowIter{
				values:     [][]byte{[]byte("first"), []byte("second")},
				errorIndex: 1,
				decodeErr:  sourceErr,
			}
		},
	}

	require.ErrorContains(t, runWorkerTestTask(w, meta, data), sourceErr.Error())
	// Next reached the second row successfully, so the preceding file and its
	// metrics are committed before decoding the first row of the next file.
	const firstFile = "\"first\"\n"
	require.Equal(t, []string{firstFile}, workerTestOutput(t, conf.OutputDirPath))
	require.Equal(t, float64(1), ReadGauge(w.metrics.finishedRowsGauge))
	require.Equal(t, float64(len(firstFile)), ReadGauge(w.metrics.finishedSizeGauge))
	require.True(t, data.iter.closed)
}

func TestWriterFailureCancelsBlockedSource(t *testing.T) {
	conf := defaultConfigForTest(t)
	conf.OutputDirPath = t.TempDir()
	w := createTestWriter(conf, t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	w.tctx = w.tctx.WithContext(ctx)
	sourceBlocked := make(chan struct{})
	sourceExited := make(chan struct{})
	consumerErr := errors.New("consumer write failure")
	meta := newMockTableIR("test", "cancel", nil, nil, []string{"TEXT"})
	data := &workerTestTableData{
		mockTableIR: meta,
		newIter: func(_ int) *workerTestRowIter {
			values := make([][]byte, 64)
			for i := range values {
				values[i] = []byte(strings.Repeat("x", 64*1024))
			}
			return &workerTestRowIter{
				values:        values,
				blockAt:       32,
				sourceBlocked: sourceBlocked,
				sourceExited:  sourceExited,
			}
		},
	}
	w.extStorage = &workerTestStorage{
		Storage: w.extStorage,
		wrap: func(output objectio.Writer) objectio.Writer {
			return &workerTestOutputWriter{Writer: output, write: func(ctx context.Context, _ []byte) (int, error) {
				timer := time.NewTimer(3 * time.Second)
				defer timer.Stop()
				select {
				case <-sourceBlocked:
					return 0, consumerErr
				case <-ctx.Done():
					return 0, ctx.Err()
				case <-timer.C:
					return 0, errors.New("source did not enter blocked Next")
				}
			}}
		},
	}

	require.ErrorContains(t, runWorkerTestTask(w, meta, data), consumerErr.Error(), "source cancellation must not replace the write error")
	select {
	case <-sourceExited:
	default:
		t.Fatal("worker returned before its blocked source exited")
	}
	require.True(t, data.iter.closed)
	require.NoError(t, ctx.Err(), "only the attempt's source context should be canceled")
}

func TestWriterRetryStartsFreshSource(t *testing.T) {
	conf := defaultConfigForTest(t)
	conf.OutputDirPath = t.TempDir()
	conf.Consistency = ConsistencyTypeSnapshot
	conf.StatementSize = UnspecifiedSize
	w := createTestWriter(conf, t)
	rebuilds := 0
	w.rebuildConnFn = func(conn *sql.Conn, _ bool) (*sql.Conn, error) {
		rebuilds++
		return conn, nil
	}
	meta := newMockTableIR("test", "retry", nil, nil, []string{"TEXT"})
	var attempts []*workerTestRowIter
	data := &workerTestTableData{
		mockTableIR: meta,
		newIter: func(attempt int) *workerTestRowIter {
			iter := &workerTestRowIter{values: [][]byte{[]byte("fresh"), nil, {}}, errorIndex: 1}
			if attempt == 1 {
				iter.values = [][]byte{[]byte("stale"), []byte("unreachable")}
				iter.decodeErr = errors.New("retry source failure")
			}
			attempts = append(attempts, iter)
			return iter
		},
	}

	require.NoError(t, runWorkerTestTask(w, meta, data))
	require.Equal(t, 1, rebuilds)
	require.Len(t, attempts, 2)
	for _, iter := range attempts {
		require.True(t, iter.closed, "each attempt must close its source")
	}
	require.Equal(t, []string{"INSERT INTO `retry` VALUES\n('fresh'),\n(NULL),\n('');\n"}, workerTestOutput(t, conf.OutputDirPath))
}

func runWorkerTestTask(w *Writer, meta TableMeta, data TableDataIR) error {
	tasks := make(chan Task, 1)
	tasks <- &TaskTableData{Meta: meta, Data: data, TotalChunks: 1}
	close(tasks)
	return w.run(tasks)
}

func workerTestOutput(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	contents := make([]string, 0, len(entries))
	for _, entry := range entries {
		content, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		require.NoError(t, err)
		contents = append(contents, string(content))
	}
	return contents
}

type workerTestTableData struct {
	*mockTableIR
	newIter  func(int) *workerTestRowIter
	iter     *workerTestRowIter
	attempts int
}

func (d *workerTestTableData) Start(ctx *tcontext.Context, _ *sql.Conn) error {
	d.attempts++
	d.iter = d.newIter(d.attempts)
	d.iter.ctx = ctx
	d.iter.args = make([]any, 1)
	d.iter.buffer = make([]byte, 0, 64*1024)
	return nil
}

func (d *workerTestTableData) Rows() SQLRowIter { return d.iter }
func (d *workerTestTableData) Close() error     { return d.iter.Close() }

type workerTestRowIter struct {
	ctx           context.Context
	values        [][]byte
	index         int
	buffer        []byte
	args          []any
	err           error
	errorIndex    int
	decodeErr     error
	readErr       error
	blockAt       int
	sourceBlocked chan struct{}
	sourceExited  chan struct{}
	closed        bool
}

func (i *workerTestRowIter) Decode(row RowReceiver) error {
	if err := i.ctx.Err(); err != nil {
		return err
	}
	if i.decodeErr != nil && i.index == i.errorIndex {
		return i.decodeErr
	}
	row.BindAddress(i.args)
	value := i.values[i.index]
	if value == nil {
		*i.args[0].(*sql.RawBytes) = nil
	} else {
		i.buffer = append(i.buffer[:0], value...)
		*i.args[0].(*sql.RawBytes) = i.buffer
	}
	return nil
}

func (i *workerTestRowIter) Next() {
	i.index++
	if i.blockAt > 0 && i.index == i.blockAt {
		close(i.sourceBlocked)
		<-i.ctx.Done()
		i.err = i.ctx.Err()
		close(i.sourceExited)
	}
	if i.readErr != nil && i.index == i.errorIndex {
		i.err = i.readErr
	}
}

func (i *workerTestRowIter) HasNext() bool { return i.err == nil && i.index < len(i.values) }
func (i *workerTestRowIter) Error() error  { return i.err }
func (i *workerTestRowIter) Close() error {
	i.closed = true
	return nil
}

type workerTestStorage struct {
	storeapi.Storage
	wrap func(objectio.Writer) objectio.Writer
}

func (s *workerTestStorage) Create(ctx context.Context, name string, option *storeapi.WriterOption) (objectio.Writer, error) {
	w, err := s.Storage.Create(ctx, name, option)
	if err != nil {
		return nil, err
	}
	return s.wrap(w), nil
}

type workerTestOutputWriter struct {
	objectio.Writer
	write func(context.Context, []byte) (int, error)
}

func (w *workerTestOutputWriter) Write(ctx context.Context, p []byte) (int, error) {
	return w.write(ctx, p)
}
