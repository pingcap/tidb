// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/exp/zapslog"
	"go.uber.org/zap/zapcore"
)

var _pool = buffer.NewPool()

func newSlowQueryLogger(cfg *LogConfig) (*slog.Logger, error) {
	// create the slow query logger
	cfg1 := newSlowQueryLogConfig(cfg)
	output, err := newWriteSyncer(cfg1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	level := zap.NewAtomicLevel()
	err = level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		return nil, err
	}
	// replace 2018-12-19-unified-log-format text encoder with slow log encoder
	core := log.NewTextCore(&slowLogEncoder{}, output, level)
	handler := zapslog.NewHandler(core, nil)
	return slog.New(handler), nil
}

func newSlowQueryLogConfig(cfg *LogConfig) *log.Config {
	// copy the global log config to slow log config
	// if the filename of slow log config is empty, slow log will behave the same as global log.
	sqConfig := cfg.Config
	// level of the global log config doesn't affect the slow query logger which determines whether to
	// log by execution duration.
	sqConfig.Level = LogConfig{}.Level
	if len(cfg.SlowQueryFile) != 0 {
		sqConfig.File = cfg.File
		sqConfig.File.Filename = cfg.SlowQueryFile
	}
	return &sqConfig
}

type slowLogEncoder struct{}

func (*slowLogEncoder) EncodeEntry(entry zapcore.Entry, _ []zapcore.Field) (*buffer.Buffer, error) {
	b := _pool.Get()
	fmt.Fprintf(b, "# Time: %s\n", entry.Time.Format(SlowLogTimeFormat))
	fmt.Fprintf(b, "%s\n", entry.Message)
	return b, nil
}

func (e *slowLogEncoder) Clone() zapcore.Encoder                        { return e }
func (*slowLogEncoder) AddArray(string, zapcore.ArrayMarshaler) error   { return nil }
func (*slowLogEncoder) AddObject(string, zapcore.ObjectMarshaler) error { return nil }
func (*slowLogEncoder) AddBinary(string, []byte)                        {}
func (*slowLogEncoder) AddByteString(string, []byte)                    {}
func (*slowLogEncoder) AddBool(string, bool)                            {}
func (*slowLogEncoder) AddComplex128(string, complex128)                {}
func (*slowLogEncoder) AddComplex64(string, complex64)                  {}
func (*slowLogEncoder) AddDuration(string, time.Duration)               {}
func (*slowLogEncoder) AddFloat64(string, float64)                      {}
func (*slowLogEncoder) AddFloat32(string, float32)                      {}
func (*slowLogEncoder) AddInt(string, int)                              {}
func (*slowLogEncoder) AddInt64(string, int64)                          {}
func (*slowLogEncoder) AddInt32(string, int32)                          {}
func (*slowLogEncoder) AddInt16(string, int16)                          {}
func (*slowLogEncoder) AddInt8(string, int8)                            {}
func (*slowLogEncoder) AddString(string, string)                        {}
func (*slowLogEncoder) AddTime(string, time.Time)                       {}
func (*slowLogEncoder) AddUint(string, uint)                            {}
func (*slowLogEncoder) AddUint64(string, uint64)                        {}
func (*slowLogEncoder) AddUint32(string, uint32)                        {}
func (*slowLogEncoder) AddUint16(string, uint16)                        {}
func (*slowLogEncoder) AddUint8(string, uint8)                          {}
func (*slowLogEncoder) AddUintptr(string, uintptr)                      {}
func (*slowLogEncoder) AddReflected(string, interface{}) error          { return nil }
func (*slowLogEncoder) OpenNamespace(string)                            {}
