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

//go:build !codes

package testsetup

import (
	"fmt"
	"os"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SetupForCommonTest runs before all the tests.
func SetupForCommonTest() {
	applyOSLogLevel()
}

func applyOSLogLevel() {
	osLoglevel := os.Getenv("log_level")
	if len(osLoglevel) > 0 {
		cfg := log.Config{
			Level:  osLoglevel,
			Format: "text",
			File:   log.FileLogConfig{},
		}
		gl, props, err := log.InitLogger(&cfg, zap.AddStacktrace(zapcore.FatalLevel))
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "applyOSLogLevel failed: %v", err)
			os.Exit(-1)
		}
		log.ReplaceGlobals(gl, props)
	}
}
