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
// +build !codes

package testbridge

import (
	"flag"
	"fmt"
	"os"

	"github.com/pingcap/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SetupForCommonTest runs before all the tests.
func SetupForCommonTest() {
	workaroundGoCheckFlags()
	applyOSLogLevel()
}

// workaroundGoCheckFlags registers flags of go-check for pkg does not import go-check
// to workaround the go-check flags passed in Makefile.
//
// TODO: Remove this function when the migration from go-check to testify[1] is done.
// [1] https://github.com/pingcap/tidb/issues/26022
func workaroundGoCheckFlags() {
	if flag.Lookup("check.timeout") == nil {
		_ = flag.Duration("check.timeout", 0, "workaroundGoCheckFlags: check.timeout")
	}
	if flag.Lookup("check.p") == nil {
		_ = flag.Bool("check.p", false, "workaroundGoCheckFlags: check.p")
	}
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
