// Copyright 2019 PingCAP, Inc.
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

package log_test

import (
	"io"
	"os"
	"testing"

	"github.com/pingcap/tidb/br/pkg/lightning/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestConfigAdjust(t *testing.T) {
	cfg := &log.Config{}
	cfg.Adjust()
	require.Equal(t, "info", cfg.Level)

	cfg.File = "."
	err := log.InitLogger(cfg, "info")
	require.EqualError(t, err, "can't use directory as log file name")
}

func TestTestLogger(t *testing.T) {
	logger, buffer := log.MakeTestLogger()
	logger.Warn("the message", zap.Int("number", 123456), zap.Ints("array", []int{7, 8, 9}))
	require.Equal(t, `{"$lvl":"WARN","$msg":"the message","number":123456,"array":[7,8,9]}`, buffer.Stripped())
}

func TestInitStdoutLogger(t *testing.T) {
	r, w, err := os.Pipe()
	require.NoError(t, err)
	oldStdout := os.Stdout
	os.Stdout = w

	msg := "logger is initialized to stdout"
	outputC := make(chan string, 1)
	go func() {
		buf := make([]byte, 4096)
		n := 0
		for {
			nn, err := r.Read(buf[n:])
			if nn == 0 || err == io.EOF {
				break
			}
			require.NoError(t, err)
			n += nn
		}
		outputC <- string(buf[:n])
	}()

	logCfg := &log.Config{File: "-"}
	err = log.InitLogger(logCfg, "info")
	require.NoError(t, err)
	log.L().Info(msg)

	os.Stdout = oldStdout
	require.NoError(t, w.Close())
	output := <-outputC
	require.NoError(t, r.Close())
	require.Contains(t, output, msg)
}
