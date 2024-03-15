// Copyright 2024 PingCAP, Inc.
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
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

func newGeneralLogger(cfg *LogConfig) (*zap.Logger, *log.ZapProperties, error) {
	// create the general logger
	sqLogger, prop, err := log.InitLogger(newGeneralLogConfig(cfg))
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	return sqLogger, prop, nil
}

func newGeneralLogConfig(cfg *LogConfig) *log.Config {
	// copy the global log config to general log config
	// if the filename of general log config is empty, general log will behave the same as global log.
	sqConfig := cfg.Config
	// level of the global log config doesn't affect the general logger which determines whether to
	// log by execution duration.
	sqConfig.Level = LogConfig{}.Level
	if len(cfg.GeneralLogFile) != 0 {
		sqConfig.File = cfg.File
		sqConfig.File.Filename = cfg.GeneralLogFile
	}
	return &sqConfig
}
