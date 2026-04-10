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

package cpuprofile_test

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/pkg/testkit/testsetup"
	"github.com/pingcap/tidb/pkg/util/cpuprofile"
	"github.com/pingcap/tidb/pkg/util/cpuprofile/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func RunMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	// To speed up testing
	*cpuprofile.ExportedDefProfileDuration = time.Millisecond * 200
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*fileSink).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/bazelbuild/rules_go/go/tools/bzltestutil.RegisterTimeoutHandler.func1"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
	testsetup.SetupForCommonTest()
	goleak.VerifyTestMain(m, opts...)
}

func RunBasicAPI(t *testing.T) {
	err := cpuprofile.ExportedStartCPUProfiler()
	require.NoError(t, err)
	defer cpuprofile.ExportedStopCPUProfiler()

	err = cpuprofile.ExportedStartCPUProfiler()
	require.Equal(t, err, *cpuprofile.ExportedErrProfilerAlreadyStarted)

	// Test for close multiple times.
	cpuprofile.ExportedStopCPUProfiler()
	cpuprofile.ExportedStopCPUProfiler()

	*cpuprofile.ExportedGlobalCPUProfiler = cpuprofile.ExportedNewParallelCPUProfiler()
	cpuprofile.ExportedStopCPUProfiler()
	err = cpuprofile.ExportedStartCPUProfiler()
	require.NoError(t, err)
	err = cpuprofile.ExportedStartCPUProfiler()
	require.Equal(t, err, *cpuprofile.ExportedErrProfilerAlreadyStarted)
}

func RunParallelCPUProfiler(t *testing.T) {
	err := cpuprofile.ExportedStartCPUProfiler()
	require.NoError(t, err)
	defer cpuprofile.ExportedStopCPUProfiler()

	// Test register/unregister nil
	cpuprofile.ExportedRegister(nil)
	require.Equal(t, 0, cpuprofile.ExportedConsumersCount(*cpuprofile.ExportedGlobalCPUProfiler))
	cpuprofile.ExportedUnregister(nil)
	require.Equal(t, 0, cpuprofile.ExportedConsumersCount(*cpuprofile.ExportedGlobalCPUProfiler))

	// Test profile error and duplicate register.
	dataCh := make(cpuprofile.ExportedProfileConsumer, 10)
	err = pprof.StartCPUProfile(bytes.NewBuffer(nil))
	require.NoError(t, err)

	// Test for duplicate register.
	cpuprofile.ExportedRegister(dataCh)
	cpuprofile.ExportedRegister(dataCh)
	require.Equal(t, 1, cpuprofile.ExportedConsumersCount(*cpuprofile.ExportedGlobalCPUProfiler))

	// Test profile error
	data := <-dataCh
	require.Equal(t, "cpu profiling already in use", data.Error.Error())
	cpuprofile.ExportedUnregister(dataCh)
	require.Equal(t, 0, cpuprofile.ExportedConsumersCount(*cpuprofile.ExportedGlobalCPUProfiler))

	// shouldn't receive data from a unregistered consumer.
	data = nil
	select {
	case data = <-dataCh:
	default:
	}
	require.Nil(t, data)

	// unregister not exist consumer
	cpuprofile.ExportedUnregister(dataCh)
	require.Equal(t, 0, cpuprofile.ExportedConsumersCount(*cpuprofile.ExportedGlobalCPUProfiler))

	// Test register a closed consumer
	dataCh = make(cpuprofile.ExportedProfileConsumer, 10)
	close(dataCh)
	cpuprofile.ExportedRegister(dataCh)
	require.Equal(t, 1, cpuprofile.ExportedConsumersCount(*cpuprofile.ExportedGlobalCPUProfiler))
	data, ok := <-dataCh
	require.Nil(t, data)
	require.False(t, ok)
	cpuprofile.ExportedUnregister(dataCh)
	require.Equal(t, 0, cpuprofile.ExportedConsumersCount(*cpuprofile.ExportedGlobalCPUProfiler))
	pprof.StopCPUProfile()

	// Test successfully get profile data.
	dataCh = make(cpuprofile.ExportedProfileConsumer, 10)
	cpuprofile.ExportedRegister(dataCh)
	data = <-dataCh
	require.NoError(t, data.Error)
	profileData, err := profile.ParseData(data.Data.Bytes())
	require.NoError(t, err)
	require.NotNil(t, profileData)
	cpuprofile.ExportedUnregister(dataCh)
	require.Equal(t, 0, cpuprofile.ExportedConsumersCount(*cpuprofile.ExportedGlobalCPUProfiler))

	// Test stop profiling when no consumer.
	cpuprofile.ExportedRegister(dataCh)
	for {
		// wait for parallelCPUProfiler do profiling successfully
		err = pprof.StartCPUProfile(bytes.NewBuffer(nil))
		if err != nil {
			break
		}
		pprof.StopCPUProfile()
		time.Sleep(time.Millisecond)
	}
	cpuprofile.ExportedUnregister(dataCh)
	require.Equal(t, 0, cpuprofile.ExportedConsumersCount(*cpuprofile.ExportedGlobalCPUProfiler))

	// wait for parallelCPUProfiler stop profiling
	start := time.Now()
	for {
		err = pprof.StartCPUProfile(bytes.NewBuffer(nil))
		if err == nil || time.Since(start) >= time.Second*2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	require.NoError(t, err)
	pprof.StopCPUProfile()
}

func getCPUProfile(d time.Duration, w io.Writer) error {
	pc := cpuprofile.ExportedNewCollector()
	err := pc.StartCPUProfile(w)
	if err != nil {
		return err
	}
	time.Sleep(d)
	return pc.StopCPUProfile()
}

func RunGetCPUProfile(t *testing.T) {
	err := cpuprofile.ExportedStartCPUProfiler()
	require.NoError(t, err)
	defer cpuprofile.ExportedStopCPUProfiler()

	// Test profile error
	err = pprof.StartCPUProfile(bytes.NewBuffer(nil))
	require.NoError(t, err)
	err = getCPUProfile(time.Millisecond*50, bytes.NewBuffer(nil))
	require.Error(t, err)
	require.Equal(t, "cpu profiling already in use", err.Error())
	pprof.StopCPUProfile()

	// test parallel get CPU profile.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testutil.MockCPULoad(ctx, "sql", "sql_digest", "plan_digest")
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			buf := bytes.NewBuffer(nil)
			err = getCPUProfile(time.Millisecond*1000, buf)
			require.NoError(t, err)
			profileData, err := profile.Parse(buf)
			require.NoError(t, err)

			labelCnt := 0
			for _, s := range profileData.Sample {
				for k := range s.Label {
					require.Equal(t, "sql", k)
					labelCnt++
				}
			}
			require.True(t, labelCnt > 0)
		}()
	}
	wg.Wait()
}

func RunProfileHTTPHandler(t *testing.T) {
	err := cpuprofile.ExportedStartCPUProfiler()
	require.NoError(t, err)
	defer cpuprofile.ExportedStopCPUProfiler()

	// setup http server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	router := http.NewServeMux()
	router.HandleFunc("/debug/pprof/profile", cpuprofile.ExportedProfileHTTPHandler)
	httpServer := &http.Server{Handler: router, WriteTimeout: time.Second * 60}
	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			require.NoError(t, err)
		}
	}()
	defer func() {
		err := httpServer.Close()
		require.NoError(t, err)
	}()

	address := listener.Addr().String()

	// Test for get profile success.
	resp, err := http.Get("http://" + address + "/debug/pprof/profile?seconds=1")
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	profileData, err := profile.Parse(resp.Body)
	require.NoError(t, err)
	require.NotNil(t, profileData)
	require.NoError(t, resp.Body.Close())

	// Test for get profile failed.
	resp, err = http.Get("http://" + address + "/debug/pprof/profile?seconds=100000")
	require.NoError(t, err)
	require.Equal(t, 400, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "profile duration exceeds server's WriteTimeout\n", string(body))
	require.NoError(t, resp.Body.Close())
}
