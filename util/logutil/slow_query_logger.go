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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

var _pool = buffer.NewPool()

func newSlowQueryLogger(cfg *LogConfig) (*zap.Logger, *log.ZapProperties, error) {

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

	// create the slow query logger
	sqLogger, prop, err := log.InitLogger(&sqConfig)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	// replace 2018-12-19-unified-log-format text encoder with slow log encoder
	newCore := log.NewTextCore(&slowLogEncoder{}, prop.Syncer, prop.Level)
	sqLogger = sqLogger.WithOptions(zap.WrapCore(func(core zapcore.Core) zapcore.Core {
		return newCore
	}))
	prop.Core = newCore

	return sqLogger, prop, nil
}

type slowLogEncoder struct{}

func (e *slowLogEncoder) EncodeEntry(entry zapcore.Entry, _ []zapcore.Field) (*buffer.Buffer, error) {
	b := _pool.Get()
	fmt.Fprintf(b, "# Time: %s\n", entry.Time.Format(SlowLogTimeFormat))
	fmt.Fprintf(b, "%s\n", entry.Message)
	return b, nil
}

func (e *slowLogEncoder) Clone() zapcore.Encoder                          { return e }
func (e *slowLogEncoder) AddArray(string, zapcore.ArrayMarshaler) error   { return nil }
func (e *slowLogEncoder) AddObject(string, zapcore.ObjectMarshaler) error { return nil }
func (e *slowLogEncoder) AddBinary(string, []byte)                        {}
func (e *slowLogEncoder) AddByteString(string, []byte)                    {}
func (e *slowLogEncoder) AddBool(string, bool)                            {}
func (e *slowLogEncoder) AddComplex128(string, complex128)                {}
func (e *slowLogEncoder) AddComplex64(string, complex64)                  {}
func (e *slowLogEncoder) AddDuration(string, time.Duration)               {}
func (e *slowLogEncoder) AddFloat64(string, float64)                      {}
func (e *slowLogEncoder) AddFloat32(string, float32)                      {}
func (e *slowLogEncoder) AddInt(string, int)                              {}
func (e *slowLogEncoder) AddInt64(string, int64)                          {}
func (e *slowLogEncoder) AddInt32(string, int32)                          {}
func (e *slowLogEncoder) AddInt16(string, int16)                          {}
func (e *slowLogEncoder) AddInt8(string, int8)                            {}
func (e *slowLogEncoder) AddString(string, string)                        {}
func (e *slowLogEncoder) AddTime(string, time.Time)                       {}
func (e *slowLogEncoder) AddUint(string, uint)                            {}
func (e *slowLogEncoder) AddUint64(string, uint64)                        {}
func (e *slowLogEncoder) AddUint32(string, uint32)                        {}
func (e *slowLogEncoder) AddUint16(string, uint16)                        {}
func (e *slowLogEncoder) AddUint8(string, uint8)                          {}
func (e *slowLogEncoder) AddUintptr(string, uintptr)                      {}
func (e *slowLogEncoder) AddReflected(string, interface{}) error          { return nil }
func (e *slowLogEncoder) OpenNamespace(string)                            {}
