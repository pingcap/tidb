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

package util

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestWaitGroupWrapperRun(t *testing.T) {
	var expect int32 = 4
	var val atomic.Int32
	var wg WaitGroupWrapper
	for i := int32(0); i < expect; i++ {
		wg.Run(func() {
			val.Inc()
		})
	}
	wg.Wait()
	require.Equal(t, expect, val.Load())

	val.Store(0)
	wg2 := NewWaitGroupEnhancedWrapper("", nil, false)
	for i := int32(0); i < expect; i++ {
		wg2.Run(func() {
			val.Inc()
		}, fmt.Sprintf("test_%v", i))
	}
	wg2.Wait()
	require.Equal(t, expect, val.Load())
}

func TestWaitGroupWrapperRunWithRecover(t *testing.T) {
	var expect int32 = 2
	var val atomic.Int32
	var wg WaitGroupWrapper
	for i := int32(0); i < expect; i++ {
		wg.RunWithRecover(func() {
			panic("test1")
		}, func(r any) {
			val.Inc()
		})
	}
	wg.Wait()
	require.Equal(t, expect, val.Load())

	val.Store(0)
	wg2 := NewWaitGroupEnhancedWrapper("", nil, false)
	for i := int32(0); i < expect; i++ {
		wg2.RunWithRecover(func() {
			panic("test1")
		}, func(r any) {
			val.Inc()
		}, fmt.Sprintf("test_%v", i))
	}
	wg2.Wait()
	require.Equal(t, expect, val.Load())
}

func TestWaitGroupWrapperCheck(t *testing.T) {
	exit := make(chan struct{})
	wg := NewWaitGroupEnhancedWrapper("", exit, false)
	quit := make(chan struct{})
	wg.Run(func() {
		<-quit
	}, "test")

	// need continue check as existed unexited process
	close(exit)
	require.True(t, wg.check())

	// no need to continue check as all process exited
	quit <- struct{}{}
	time.Sleep(1 * time.Second)
	require.False(t, wg.check())
}

func TestWaitGroupWrapperGo(t *testing.T) {
	file, fileName := prepareStdoutLogger(t)
	var wg WaitGroupWrapper
	wg.RunWithLog(func() {
		middleF()
	})
	wg.Wait()
	require.NoError(t, file.Close())
	content, err := os.ReadFile(fileName)
	require.NoError(t, err)
	require.Contains(t, string(content), "pkg/util.middleF")
}

func prepareStdoutLogger(t *testing.T) (*os.File, string) {
	bak := os.Stdout
	t.Cleanup(func() {
		os.Stdout = bak
	})
	tempDir := t.TempDir()
	fileName := path.Join(tempDir, "test.log")
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY, 0644)
	require.NoError(t, err)
	os.Stdout = file
	// InitLogger contains zap.AddStacktrace(zapcore.FatalLevel), so log level
	// below fatal will not contain stack automatically.
	require.NoError(t, logutil.InitLogger(&logutil.LogConfig{}))

	return file, fileName
}

func middleF() {
	var a int
	_ = 10 / a
}

func TestNewErrorGroupWithRecover(t *testing.T) {
	file, fileName := prepareStdoutLogger(t)
	eg := NewErrorGroupWithRecover()
	eg.Go(func() error {
		middleF()
		return nil
	})
	err := eg.Wait()
	require.ErrorContains(t, err, "runtime error: integer divide by zero")
	require.NoError(t, file.Close())
	content, err := os.ReadFile(fileName)
	require.NoError(t, err)
	require.Contains(t, string(content), "pkg/util.middleF")
}
