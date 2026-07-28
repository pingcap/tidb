package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"path"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/extension"
	"github.com/pingcap/tidb/pkg/sessionctx/variable"
	"github.com/pingcap/tidb/pkg/util/intest"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

func createAllUsersRuleBundle(name string, specs ...FilterSpec) *LogFilterRuleBundle {
	rule := &LogFilterRule{
		User:       "%@%",
		FilterName: name,
		Enabled:    true,
		Filter: &LogFilter{
			Name:   name,
			Filter: specs,
		},
	}
	return NewLogFilterRuleBundle([]*LogFilterRule{rule})
}

func createTestLogger(tb testing.TB) (*Logger, *LogManager) {
	tempDir := tb.TempDir()
	m, err := NewAuditLogManager()
	require.NoError(tb, err)
	tb.Cleanup(m.Stop)
	require.NoError(tb, m.SetEnabled(true))
	require.NoError(tb, m.SetLogConfigPath(path.Join(tempDir, DefAuditLogName)))
	require.NoError(tb, m.SetFilter(createAllUsersRuleBundle("all")))
	return m.logger, m
}

func TestLogEntryDoesNotBlockEachOther(t *testing.T) {
	logger, m := createTestLogger(t)

	concurrency := 12
	doneCh := make(chan struct{})
	var fieldsFuncCalls atomic.Int64
	for i := 0; i < concurrency; i++ {
		id := newEntryIDGenerator()
		force := (i % 2) == 0
		logEntryFunc := func(entry *LogEntry) {
			logger.LogEntry(entry, id, force)
		}

		if i >= 8 {
			if i%2 == 0 {
				logEntryFunc = func(entry *LogEntry) {
					m.LogEntryWithFilter(func() *LogEntry { return entry }, id)
				}
			} else {
				logEntryFunc = func(entry *LogEntry) {
					m.ForceLogEntry(entry)
				}
			}
		} else if i >= 4 {
			logEntryFunc = func(entry *LogEntry) {
				logger.LogEntryFunc(func() *LogEntry {
					return entry
				}, id, force)
			}
		}

		go func() {
			logEntryFunc(&LogEntry{
				FieldsFunc: func(fields []zap.Field, redact bool) []zap.Field {
					// If LogEntry blocks other entries, this waits until timeout.
					if fieldsFuncCalls.Add(1) == int64(concurrency) {
						close(doneCh)
					}
					select {
					case <-doneCh:
						return []zap.Field{zap.String("test", "test")}
					case <-time.After(20 * time.Second):
						panic("timeout")
					}
				},
			})
		}()
	}

	select {
	case _, ok := <-doneCh:
		require.False(t, ok, "doneCh should be closed")
		require.Equal(t, concurrency, int(fieldsFuncCalls.Load()))
	case <-time.After(10 * time.Second):
		require.FailNow(t, "timeout")
	}
}

type mockLogWriter struct {
	LogWriter
	mock.Mock
}

func (w *mockLogWriter) Write(p []byte) (n int, err error) {
	w.Called(p)
	return len(p), nil
}

func (w *mockLogWriter) Close() error {
	return w.Called().Error(0)
}

func (w *mockLogWriter) Rotate() error {
	return w.Called().Error(0)
}

func (w *mockLogWriter) Sync() error {
	return w.Called().Error(0)
}

func TestAuditLogger(t *testing.T) {
	require.True(t, intest.InTest, "requires test hooks enabled by -tags=intest")
	defer func() {
		injectWriterForTest = nil
	}()

	type mockTestAuditLoggerWriter struct {
		mockLogWriter
		zapCfg *log.Config
		w      *lumberjack.Logger
	}

	mockWriter := &mockTestAuditLoggerWriter{}
	injectWriterForTest = func(zapCfg *log.Config, w *lumberjack.Logger) LogWriter {
		mockWriter.zapCfg = zapCfg
		mockWriter.w = w
		mockWriter.MethodCalled("set")
		return mockWriter
	}

	checkLoggerCfg := func(logger *Logger, cfg LoggerConfig) {
		require.NotNil(t, mockWriter)
		require.Equal(t, cfg, logger.cfg)
		require.Same(t, mockWriter, logger.writer)
		require.Equal(t, &log.Config{
			Level:         "info",
			DisableCaller: true,
			File: log.FileLogConfig{
				Filename:    cfg.Filepath,
				MaxSize:     int(cfg.FileMaxSize),
				MaxBackups:  cfg.FileReservedBackups,
				MaxDays:     cfg.FileReservedDays,
				Compression: "",
			},
		}, mockWriter.zapCfg)
		require.Equal(t, &lumberjack.Logger{
			Filename:   mockWriter.zapCfg.File.Filename,
			MaxSize:    mockWriter.zapCfg.File.MaxSize,
			MaxBackups: mockWriter.zapCfg.File.MaxBackups,
			MaxAge:     mockWriter.zapCfg.File.MaxDays,
			LocalTime:  true,
			Compress:   false,
		}, mockWriter.w)
	}

	cfg := LoggerConfig{
		zapLoggerConfig: zapLoggerConfig{
			Filepath:            "/tmp/tidb-audit.log",
			FileMaxSize:         1234,
			FileMaxLifetime:     5678,
			FileReservedBackups: 9012,
			FileReservedDays:    3456,
			LogFormat:           LogFormatText,
		},
		Enabled:    true,
		ConfigPath: "tidb-audit.log",
		Redact:     true,
		Filter:     createAllUsersRuleBundle("all"),
	}

	testResetLoggerCfg := func(lg *Logger, shouldReplaceWriter bool) {
		if shouldReplaceWriter {
			var err1, err2 error
			if rand.Int()%2 == 0 {
				err1 = errors.New("mockErr1")
			}
			if rand.Int()%2 == 0 {
				err2 = errors.New("mockErr2")
			}
			mockWriter.On("Sync").Return(err1).Once()
			mockWriter.On("Close").Return(err2).Once()
			mockWriter.On("set").Once()
		}
		require.NoError(t, lg.SetConfig(cfg))
		mockWriter.AssertExpectations(t)
		checkLoggerCfg(lg, cfg)
	}

	dedup := make(map[string]struct{})
	idGen := newEntryIDGenerator()
	var entryCall mock.Mock
	var nextLogID string
	entry := &LogEntry{
		User:    "root",
		Host:    "host1",
		ConnID:  123,
		Classes: []EventClass{ClassQuery},
		FieldsFunc: func(fields []zap.Field, redact bool) []zap.Field {
			fieldMap := make(map[string]any)
			for _, f := range fields {
				fieldMap[f.Key] = f
			}
			logKeyID, ok := fieldMap[LogKeyID].(zap.Field)
			require.True(t, ok)
			require.Equal(t, zapcore.StringType, logKeyID.Type)
			require.NotEmpty(t, logKeyID.String)
			require.NotContains(t, dedup, logKeyID.String)
			dedup[logKeyID.String] = struct{}{}
			nextLogID = logKeyID.String
			entryCall.MethodCalled("fieldsFunc", fields, redact)
			return fields
		},
	}

	testLogEntry := func(lg *Logger, force bool, shouldLog bool) {
		for i := 0; i < 2; i++ {
			if shouldLog {
				entryCall.On("fieldsFunc", mock.Anything, cfg.Redact).Once()
				mockWriter.On("Write", mock.Anything).Once().Run(func(args mock.Arguments) {
					bs, ok := args.Get(0).([]byte)
					require.True(t, ok)
					switch cfg.LogFormat {
					case LogFormatJSON:
						events := make([]any, 0, len(entry.Classes))
						for _, c := range entry.Classes {
							events = append(events, c.String())
						}
						var entryMap map[string]any
						err := json.Unmarshal(bs, &entryMap)
						require.NoError(t, err)
						require.Contains(t, entryMap, LogKeyTime)
						require.Equal(t, nextLogID, entryMap[LogKeyID])
						require.Equal(t, events, entryMap[LogKeyEvent])
						require.Equal(t, entry.User, entryMap[LogKeyUser])
						require.Equal(t, strconv.Itoa(int(entry.ConnID)), entryMap[LogKeyConnectionID])
					default:
						s := string(bs)
						events := make([]string, 0, len(entry.Classes))
						for _, c := range entry.Classes {
							events = append(events, c.String())
						}
						require.Contains(t, s, "] [INFO] [")
						require.Contains(t, s, "ID="+nextLogID)
						require.Contains(t, s, fmt.Sprintf(`EVENT="[%s]"`, strings.Join(events, ",")))
						require.Contains(t, s, "USER="+entry.User)
						require.Contains(t, s, "CONNECTION_ID="+strconv.Itoa(int(entry.ConnID)))
					}
				})
			}
			switch i {
			case 0:
				lg.LogEntry(entry, idGen, force)
			default:
				lg.LogEntryFunc(func() *LogEntry { return entry }, idGen, force)
			}
			entryCall.AssertExpectations(t)
			mockWriter.AssertExpectations(t)
		}
	}

	testRotate := func(lg *Logger) {
		mockWriter.On("Rotate").Return(nil).Once()
		require.NoError(t, lg.Rotate())
		mockWriter.AssertExpectations(t)

		mockErr := errors.New("mockErr")
		mockWriter.On("Rotate").Return(mockErr).Once()
		err := lg.Rotate()
		require.True(t, errors.ErrorEqual(err, mockErr))
		mockWriter.AssertExpectations(t)

		rotated, err := lg.RotateOrNot(func(cfg LoggerConfig) bool {
			return false
		})
		require.NoError(t, err)
		require.False(t, rotated)
		mockWriter.AssertExpectations(t)

		mockWriter.On("Rotate").Return(nil).Once()
		rotated, err = lg.RotateOrNot(func(cfg LoggerConfig) bool {
			return true
		})
		require.NoError(t, err)
		require.True(t, rotated)
		mockWriter.AssertExpectations(t)

		mockErr = errors.New("mockErr2")
		mockWriter.On("Rotate").Return(mockErr).Once()
		rotated, err = lg.RotateOrNot(func(cfg LoggerConfig) bool {
			return true
		})
		require.True(t, rotated)
		require.True(t, errors.ErrorEqual(err, mockErr))
		mockWriter.AssertExpectations(t)
	}

	mockWriter.On("set").Once()
	lg, err := NewAuditLogger(cfg)
	require.NoError(t, err)
	checkLoggerCfg(lg, cfg)
	mockWriter.AssertExpectations(t)

	testLogEntry(lg, false, true)
	testLogEntry(lg, false, true)
	testRotate(lg)

	nonNilEntry := entry
	entry = nil
	testLogEntry(lg, true, false)
	testLogEntry(lg, false, false)
	entry = nonNilEntry

	cfg.Filter = createAllUsersRuleBundle("all", FilterSpec{Classes: []string{ClassDDL.String()}})
	testResetLoggerCfg(lg, false)
	testLogEntry(lg, false, false)
	testLogEntry(lg, true, true)
	entry.Classes = []EventClass{ClassQuery, ClassDDL}
	testLogEntry(lg, false, true)
	testLogEntry(lg, true, true)
	testRotate(lg)

	cfg.Enabled = false
	testResetLoggerCfg(lg, false)
	testLogEntry(lg, false, false)
	testLogEntry(lg, true, true)
	testRotate(lg)

	cfg.Enabled = true
	testResetLoggerCfg(lg, false)
	testResetLoggerCfg(lg, false)

	for i, fn := range []func(){
		func() { cfg.LogFormat = LogFormatJSON },
		func() { cfg.LogFormat = "" },
		func() { cfg.LogFormat = LogFormatText },
		func() { cfg.Filepath = "/tmp/change/audit2.log" },
		func() { cfg.FileMaxSize++ },
		func() { cfg.FileMaxLifetime++ },
		func() { cfg.FileReservedBackups++ },
		func() { cfg.FileReservedDays++ },
	} {
		t.Run("test-config-change-should-replace-internal-logger-"+strconv.Itoa(i), func(t *testing.T) {
			fn()
			testResetLoggerCfg(lg, true)
			testLogEntry(lg, false, true)
		})
	}

	for i, fn := range []func(){
		func() { cfg.Redact = false },
		func() { cfg.Redact = true },
		func() { cfg.ConfigPath = "./another.log" },
	} {
		t.Run("test-config-change-should-not-replace-internal-logger-"+strconv.Itoa(i), func(t *testing.T) {
			fn()
			testResetLoggerCfg(lg, false)
			testLogEntry(lg, false, true)
		})
	}

	var syncErr error
	if rand.Int()%2 == 0 {
		syncErr = errors.New("mockSyncErr")
	}
	mockWriter.On("Sync").Return(syncErr).Once()
	lg.Stop()
	mockWriter.AssertExpectations(t)

	cfg.Enabled = true
	cfg.Filter = createAllUsersRuleBundle("all")
	cfg.FileMaxSize++
	testResetLoggerCfg(lg, true)
	testLogEntry(lg, false, true)
	lg.Stop()
}

func TestMaxLifeTimeRotateTicker(t *testing.T) {
	checkChNotClosed := func(ch <-chan time.Time) {
		for {
			select {
			case _, ok := <-ch:
				require.False(t, ok, "channel should not be closed")
			case <-time.After(time.Millisecond):
				return
			}
		}
	}
	idleTicker := &maxLifeTimeRotateTicker{}

	ticker := newMaxLifeTimeRotateTicker(LoggerConfig{})
	require.Equal(t, idleTicker, ticker)
	checkChNotClosed(ticker.C())
	require.True(t, ticker.C() == idleTickerC)

	cfg := LoggerConfig{Enabled: true, zapLoggerConfig: zapLoggerConfig{
		FileMaxLifetime: 1,
		Filepath:        "/tmp1/tidb-audit.log",
	}}
	ticker = newMaxLifeTimeRotateTicker(cfg)
	rawTicker := ticker.C()
	start := time.Now()
	select {
	case _, ok := <-rawTicker:
		require.True(t, ok)
		require.GreaterOrEqual(t, time.Since(start), time.Second)
	case <-time.After(time.Second * 2):
		require.FailNow(t, "ticker should be triggered")
	}
	require.Equal(t, int64(1), ticker.rotateMaxLifeTime)
	require.Equal(t, "/tmp1/tidb-audit.log", ticker.rotateFile)

	cfg.FileMaxLifetime = 2
	require.False(t, ticker.CheckCfg(cfg))
	ticker.SyncConfig(cfg)
	require.True(t, rawTicker != ticker.C())
	checkChNotClosed(ticker.C())
	rawTicker = ticker.C()
	start = time.Now()
	select {
	case _, ok := <-rawTicker:
		require.True(t, ok)
		require.GreaterOrEqual(t, time.Since(start), time.Second)
	case <-time.After(time.Second * 3):
		require.FailNow(t, "ticker should be triggered")
	}
	require.Equal(t, int64(2), ticker.rotateMaxLifeTime)
	require.Equal(t, "/tmp1/tidb-audit.log", ticker.rotateFile)

	cfg.Filepath = "/tmp2/tidb-audit.log"
	require.False(t, ticker.CheckCfg(cfg))
	ticker.SyncConfig(cfg)
	require.True(t, rawTicker != ticker.C())
	rawTicker = ticker.C()
	checkChNotClosed(ticker.C())
	require.Equal(t, int64(2), ticker.rotateMaxLifeTime)
	require.Equal(t, "/tmp2/tidb-audit.log", ticker.rotateFile)

	cfg.Redact = !cfg.Redact
	cfg.FileMaxSize++
	cfg.FileReservedDays++
	cfg.Filter = createAllUsersRuleBundle("another")
	require.True(t, ticker.CheckCfg(cfg))
	ticker.SyncConfig(cfg)
	require.True(t, rawTicker == ticker.C())
	checkChNotClosed(ticker.C())
	require.Equal(t, int64(2), ticker.rotateMaxLifeTime)
	require.Equal(t, "/tmp2/tidb-audit.log", ticker.rotateFile)

	require.True(t, ticker.CheckCfg(cfg))
	ticker.SyncConfig(cfg)
	checkChNotClosed(rawTicker)
	require.True(t, rawTicker == ticker.C())
	checkChNotClosed(ticker.C())
	require.Equal(t, int64(2), ticker.rotateMaxLifeTime)
	require.Equal(t, "/tmp2/tidb-audit.log", ticker.rotateFile)

	cfg.Enabled = false
	require.False(t, ticker.CheckCfg(cfg))
	ticker.SyncConfig(cfg)
	require.Equal(t, idleTicker, ticker)
	require.True(t, ticker.C() == idleTickerC)
	checkChNotClosed(ticker.C())

	cfg.Filepath = "/path/to/tidb-audit.log"
	cfg.FileMaxLifetime = 256
	require.True(t, ticker.CheckCfg(cfg))
	ticker.SyncConfig(cfg)
	require.Equal(t, idleTicker, ticker)
	require.True(t, ticker.C() == idleTickerC)
	checkChNotClosed(ticker.C())

	cfg.Enabled = true
	require.False(t, ticker.CheckCfg(cfg))
	ticker.SyncConfig(cfg)
	require.Equal(t, int64(256), ticker.rotateMaxLifeTime)
	require.Equal(t, "/path/to/tidb-audit.log", ticker.rotateFile)
	rawTicker = ticker.C()
	require.True(t, rawTicker != idleTickerC)
	checkChNotClosed(ticker.C())

	cfg.FileMaxLifetime = 0
	require.False(t, ticker.CheckCfg(cfg))
	ticker.SyncConfig(cfg)
	require.Equal(t, idleTicker, ticker)
	require.True(t, ticker.C() == idleTickerC)
	checkChNotClosed(ticker.C())
}

func TestAuditLoggerMaxLifeTimeRotateLoop(t *testing.T) {
	require.True(t, intest.InTest, "requires test hooks enabled by -tags=intest")
	defer func() {
		injectWriterForTest = nil
		rotateNotifySyncChForTest = nil
	}()

	mockWriter := &mockLogWriter{}
	mockWriter.On("Sync").Return(nil).Maybe()
	mockWriter.On("Close").Return(nil).Maybe()
	injectWriterForTest = func(zapCfg *log.Config, w *lumberjack.Logger) LogWriter {
		return mockWriter
	}

	cfg := LoggerConfig{
		zapLoggerConfig: zapLoggerConfig{
			Filepath:            "/tmp/tidb-audit.log",
			FileMaxSize:         1234,
			FileMaxLifetime:     5678,
			FileReservedBackups: 9012,
			FileReservedDays:    3456,
			LogFormat:           LogFormatText,
		},
		Enabled:    true,
		ConfigPath: "tidb-audit.log",
		Redact:     true,
		Filter:     createAllUsersRuleBundle("all"),
	}

	ch := make(chan *maxLifeTimeRotateTicker, 1)
	rotateNotifySyncChForTest = ch
	consumeCh := func() *maxLifeTimeRotateTicker {
		select {
		case ticker, ok := <-ch:
			require.NotNil(t, ticker)
			require.True(t, ok)
			select {
			case <-ch:
				require.FailNow(t, "channel should not have more than one element")
			default:
			}
			return ticker
		case <-time.After(time.Second):
			require.FailNow(t, "timeout")
			panic("")
		}
	}

	logger, err := NewAuditLogger(cfg)
	require.NoError(t, err)
	defer logger.Stop()
	ticker := consumeCh()
	require.NotNil(t, ticker.C())
	require.Equal(t, int64(5678), ticker.rotateMaxLifeTime)
	require.Equal(t, "/tmp/tidb-audit.log", ticker.rotateFile)

	cfg.FileMaxLifetime = 1234
	require.NoError(t, logger.SetConfig(cfg))
	ticker = consumeCh()
	require.NotNil(t, ticker.C())
	require.Equal(t, int64(1234), ticker.rotateMaxLifeTime)
	require.Equal(t, "/tmp/tidb-audit.log", ticker.rotateFile)

	require.NoError(t, logger.SetConfig(cfg))
	require.NotNil(t, ticker.C())
	require.Equal(t, int64(1234), ticker.rotateMaxLifeTime)
	require.Equal(t, "/tmp/tidb-audit.log", ticker.rotateFile)
	select {
	case <-ch:
		require.FailNow(t, "should not trigger loop when cfg does not change")
	case <-time.After(100 * time.Millisecond):
	}

	cfg.Enabled = false
	require.NoError(t, logger.SetConfig(cfg))
	ticker = consumeCh()
	require.NotNil(t, ticker.C())
	require.Equal(t, &maxLifeTimeRotateTicker{}, ticker)

	rotateCalled := make(chan struct{})
	mockWriter.On("Rotate").Return(errors.New("mockErr")).Once().Run(func(_ mock.Arguments) {
		close(rotateCalled)
	})
	cfg.Enabled = true
	cfg.FileMaxLifetime = 1
	require.NoError(t, logger.SetConfig(cfg))
	ticker = consumeCh()
	require.Equal(t, int64(1), ticker.rotateMaxLifeTime)
	require.Equal(t, "/tmp/tidb-audit.log", ticker.rotateFile)
	select {
	case <-rotateCalled:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timeout")
	}
	mockWriter.AssertExpectations(t)
}

func TestConcurrentAuditOperations(t *testing.T) {
	logger, m := createTestLogger(t)
	require.NoError(t, m.SetFileReservedBackups(10))

	maxFileSizeMB := int64(5)
	require.NoError(t, m.SetFileMaxSize(maxFileSizeMB))

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 6; i++ {
		wg.Add(1)
		connID := uint64(i + 1)
		go func(i int) {
			var sb strings.Builder
			for j := 0; j < 1024*100; j++ {
				sb.WriteString("1234567890")
			}
			str := sb.String()
			id := newEntryIDGenerator()
			entry := &LogEntry{
				User:         "test",
				Host:         "localhost",
				ConnID:       connID,
				Classes:      []EventClass{ClassQuery},
				SessionAlias: str,
			}
			var entryFunc func() *LogEntry
			if i%2 == 0 {
				entryFunc = func() *LogEntry {
					return entry
				}
			}

			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if entryFunc != nil {
						logger.LogEntryFunc(entryFunc, id, true)
					} else {
						logger.LogEntry(entry, id, true)
					}
				}
			}
		}(i)
	}

	var startRotateOnce sync.Once
	start := time.Now()
	for {
		time.Sleep(5 * time.Millisecond)
		duration := time.Since(start)
		if duration > 10*time.Second {
			cancel()
			break
		}

		if duration > 5*time.Second {
			startRotateOnce.Do(func() {
				for i := 0; i < 4; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							select {
							case <-ctx.Done():
								return
							default:
								switch rand.Intn(10) {
								case 0:
									require.NoError(t, m.SetFileMaxLifetime(int64(rand.Intn(10))))
								case 1:
									require.NoError(t, m.SetFilter(nil))
								case 2:
									require.NoError(t, m.SetEnabled(rand.Intn(2) == 0))
								case 3:
									cfg := m.GetConfig()
									cfg.Redact = rand.Intn(2) == 0
									require.NoError(t, logger.SetConfig(cfg))
								case 4:
									_, err := logger.RotateOrNot(func(cfg LoggerConfig) bool {
										return rand.Intn(2) == 0
									})
									require.NoError(t, err)
								default:
									require.NoError(t, logger.Rotate())
								}
							}
						}
					}()
				}
			})
		}

		nextFileMaxSize := m.GetFileMaxSize() - 1
		if nextFileMaxSize <= 0 {
			nextFileMaxSize = maxFileSizeMB
		}
		require.NoError(t, m.SetFileMaxSize(nextFileMaxSize))
	}
	wg.Wait()
}

func BenchmarkLogEntry(b *testing.B) {
	logger, m := createTestLogger(b)
	require.NoError(b, m.SetFileMaxSize(1024*1024*1024*1024))
	require.NoError(b, m.SetEnabled(true))

	var n atomic.Int64
	n.Store(int64(b.N))
	concurrency := runtime.NumCPU() * 2
	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		id := newEntryIDGenerator()
		connInfo := extension.ConnEventInfo{
			ConnectionInfo: &variable.ConnectionInfo{
				ConnectionID: uint64(i) + 1,
				User:         "root",
				Host:         "localhost",
			},
			SessionAlias: fmt.Sprintf("session-%d", i),
		}
		go func() {
			defer wg.Done()
			var current int64
			for {
				if current == 0 {
					current = 128
					if left := n.Add(-current); left < 0 {
						current += left
					}

					if current <= 0 {
						return
					}
				}
				current--
				logger.LogEntryFunc(func() *LogEntry {
					return newConnEventEntry(extension.ConnHandshakeAccepted, &connInfo)
				}, id, false)
			}
		}()
	}
	wg.Wait()
}
