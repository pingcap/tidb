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
	"context"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/pingcap/errors"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/tidb/pkg/lightning/log"
	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	// filter packages on default
	require.Equal(t, "", os.Getenv(logutil.GRPCDebugEnvName))
	require.IsType(t, &log.FilterCore{}, log.L().Logger.Core())
	// output all packages when EnableDiagnoseLogs=true
	logCfg.EnableDiagnoseLogs = true
	require.NoError(t, log.InitLogger(logCfg, "info"))
	require.IsType(t, &zaplog.TextIOCore{}, log.L().Logger.Core())
	require.Equal(t, "true", os.Getenv(logutil.GRPCDebugEnvName))
	// reset GRPCDebugEnvName
	require.NoError(t, os.Unsetenv(logutil.GRPCDebugEnvName))
}

func TestIsContextCanceledError(t *testing.T) {
	require.True(t, log.IsContextCanceledError(context.Canceled))
	require.True(t, log.IsContextCanceledError(status.Error(codes.Canceled, "")))
	require.True(t, log.IsContextCanceledError(errors.Annotate(context.Canceled, "foo")))
	require.True(t, log.IsContextCanceledError(awserr.New(request.CanceledErrorCode, "", context.Canceled)))
	require.True(t, log.IsContextCanceledError(awserr.New(
		"MultipartUpload", "upload multipart failed",
		awserr.New(request.CanceledErrorCode, "", context.Canceled))))
	require.True(t, log.IsContextCanceledError(awserr.New(request.ErrCodeRequestError, "", context.Canceled)))

	require.False(t, log.IsContextCanceledError(nil))
}
