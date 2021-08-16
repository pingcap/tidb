package logutil

import (
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var _pool2 = buffer.NewPool()

const (
	logBatchSize = 102400
	flushTimeout = 1000 * time.Millisecond
)

type GeneralLog struct {
	logger  *zap.Logger
	logChan chan string
}

func newGeneralLog(logger *zap.Logger) *GeneralLog {
	gl := &GeneralLog{
		logger:  logger,
		logChan: make(chan string, logBatchSize),
	}
	go gl.startWorker()
	return gl
}

// startWorker starts a log flushing worker that flushes log periodically or when batch is full
func (gl *GeneralLog) startWorker() {
	var buf strings.Builder
	logCount := 0
	timeout := time.After(flushTimeout)
	for {
		select {
		case logText := <-gl.logChan:
			if logCount > 0 {
				buf.WriteByte('\n')
			}
			buf.WriteString(logText)
			logCount += 1
			if logCount == logBatchSize {
				gl.logger.Info(buf.String())
				//gl.logger.Info(fmt.Sprintf("buf size 1: %d", buf.Len()))
				buf.Reset()
				logCount = 0
			}
		case <-timeout:
			if logCount > 0 {
				gl.logger.Info(buf.String())
				//gl.logger.Info(fmt.Sprintf("buf size 2: %d", buf.Len()))
				buf.Reset()
				logCount = 0
			}
			timeout = time.After(flushTimeout)
		}
	}
}

func newGeneralLogLogger(cfg *LogConfig) (*zap.Logger, error) {
	// reuse global config and override general log file
	// if general log filename is empty, general log will behave the same as the global log
	glConfig := &cfg.Config
	glConfig.File = log.FileLogConfig{
		MaxSize: 1000,
		// Filename: "general_log",
		Filename: "",
	}

	// create the slow query logger
	generalLogLogger, prop, err := log.InitLogger(glConfig)
	if err != nil {
		return nil, errors.Trace(err)
	}

	generalLogLogger = generalLogLogger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return log.NewTextCore(&generalLogEncoder{}, prop.Syncer, prop.Level)
	}))

	return generalLogLogger, nil
}

type generalLogEncoder struct{}

func (e *generalLogEncoder) EncodeEntry(entry zapcore.Entry, _ []zapcore.Field) (*buffer.Buffer, error) {
	b := _pool2.Get()
	// t := entry.Time
	// b.WriteByte('[')
	// b.WriteString(strconv.FormatInt(int64(t.Year()), 10))
	// b.WriteByte('/')
	// if t.Month() < 10 {
	// 	b.WriteByte('0')
	// }
	// b.WriteString(strconv.FormatInt(int64(t.Month()), 10))
	// b.WriteByte('/')
	// if t.Day() < 10 {
	// 	b.WriteByte('0')
	// }
	// b.WriteString(strconv.FormatInt(int64(t.Day()), 10))
	// b.WriteByte(' ')
	// if t.Hour() < 10 {
	// 	b.WriteByte('0')
	// }
	// b.WriteString(strconv.FormatInt(int64(t.Hour()), 10))
	// b.WriteByte(':')
	// if t.Minute() < 10 {
	// 	b.WriteByte('0')
	// }
	// b.WriteString(strconv.FormatInt(int64(t.Minute()), 10))
	// b.WriteByte(':')
	// if t.Second() < 10 {
	// 	b.WriteByte('0')
	// }
	// b.WriteString(strconv.FormatInt(int64(t.Second()), 10))
	// b.WriteByte('.')
	// b.WriteString(strconv.FormatInt(int64(t.Nanosecond()/1000000), 10))
	// b.WriteByte(' ')
	// _, offset := t.Zone()
	// offsetAbs := offset
	// if offsetAbs < 0 {
	// 	offsetAbs = -offsetAbs
	// }
	// offsetHour := offset / 3600
	// offsetHourAbs := offsetHour
	// if offsetHourAbs < 0 {
	// 	offsetHourAbs = -offsetHourAbs
	// }
	// offsetMinuteAbs := offsetAbs/60 - offsetHourAbs*60
	// if offsetMinuteAbs < 0 {
	// 	offsetMinuteAbs = -offsetMinuteAbs
	// }
	// if offsetHour > 0 {
	// 	b.WriteByte('+')
	// }
	// if offsetHourAbs < 10 {
	// 	b.WriteByte('0')
	// }
	// b.WriteString(strconv.FormatInt(int64(offsetHour), 10))
	// b.WriteByte(':')
	// if offsetMinuteAbs < 10 {
	// 	b.WriteByte('0')
	// }
	// b.WriteString(strconv.FormatInt(int64(offsetMinuteAbs), 10))
	// b.WriteString("] ")
	b.WriteString(entry.Message)
	b.WriteByte('\n')
	// fmt.Fprintf(b, "[%s] %s\n", entry.Time.Format("2006/01/02 15:04:05.000 -07:00") /* keep up with pingcap/log */, entry.Message)
	return b, nil
}

func (e *generalLogEncoder) Clone() zapcore.Encoder                          { return e }
func (e *generalLogEncoder) AddArray(string, zapcore.ArrayMarshaler) error   { return nil }
func (e *generalLogEncoder) AddObject(string, zapcore.ObjectMarshaler) error { return nil }
func (e *generalLogEncoder) AddBinary(string, []byte)                        {}
func (e *generalLogEncoder) AddByteString(string, []byte)                    {}
func (e *generalLogEncoder) AddBool(string, bool)                            {}
func (e *generalLogEncoder) AddComplex128(string, complex128)                {}
func (e *generalLogEncoder) AddComplex64(string, complex64)                  {}
func (e *generalLogEncoder) AddDuration(string, time.Duration)               {}
func (e *generalLogEncoder) AddFloat64(string, float64)                      {}
func (e *generalLogEncoder) AddFloat32(string, float32)                      {}
func (e *generalLogEncoder) AddInt(string, int)                              {}
func (e *generalLogEncoder) AddInt64(string, int64)                          {}
func (e *generalLogEncoder) AddInt32(string, int32)                          {}
func (e *generalLogEncoder) AddInt16(string, int16)                          {}
func (e *generalLogEncoder) AddInt8(string, int8)                            {}
func (e *generalLogEncoder) AddString(string, string)                        {}
func (e *generalLogEncoder) AddTime(string, time.Time)                       {}
func (e *generalLogEncoder) AddUint(string, uint)                            {}
func (e *generalLogEncoder) AddUint64(string, uint64)                        {}
func (e *generalLogEncoder) AddUint32(string, uint32)                        {}
func (e *generalLogEncoder) AddUint16(string, uint16)                        {}
func (e *generalLogEncoder) AddUint8(string, uint8)                          {}
func (e *generalLogEncoder) AddUintptr(string, uintptr)                      {}
func (e *generalLogEncoder) AddReflected(string, interface{}) error          { return nil }
func (e *generalLogEncoder) OpenNamespace(string)                            {}
