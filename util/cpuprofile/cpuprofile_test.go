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

package cpuprofile

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/util/testbridge"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()
	goleak.VerifyTestMain(m)
}

func TestParallelCPUProfiler(t *testing.T) {
	if GlobalCPUProfiler != nil {
		GlobalCPUProfiler.Close()
	}
	GlobalCPUProfiler = NewParallelCPUProfiler()
	GlobalCPUProfiler.Start()
	defer GlobalCPUProfiler.Close()

	// Test register/unregister nil
	Register(nil)
	require.Equal(t, 0, GlobalCPUProfiler.ConsumersCount())
	Unregister(nil)
	require.Equal(t, 0, GlobalCPUProfiler.ConsumersCount())
	require.False(t, GlobalCPUProfiler.inProfilingStatus())

	// Test profile error and duplicate register.
	dataCh := make(ProfileConsumer, 10)
	err := pprof.StartCPUProfile(bytes.NewBuffer(nil))
	require.NoError(t, err)

	// Test for duplicate register.
	Register(dataCh)
	Register(dataCh)
	require.Equal(t, 1, GlobalCPUProfiler.ConsumersCount())
	require.Equal(t, true, GlobalCPUProfiler.needProfile())

	// Test profile error
	data := <-dataCh
	require.Equal(t, "cpu profiling already in use", data.Error.Error())
	Unregister(dataCh)
	require.Equal(t, 0, GlobalCPUProfiler.ConsumersCount())

	// shouldn't receive data from a unregistered consumer.
	data = nil
	select {
	case data = <-dataCh:
	default:
	}
	require.Nil(t, data)

	// unregister not exist consumer
	Unregister(dataCh)
	require.Equal(t, 0, GlobalCPUProfiler.ConsumersCount())

	// Test register a closed consumer
	dataCh = make(ProfileConsumer, 10)
	close(dataCh)
	Register(dataCh)
	require.Equal(t, 1, GlobalCPUProfiler.ConsumersCount())
	data, ok := <-dataCh
	require.Nil(t, data)
	require.False(t, ok)
	Unregister(dataCh)
	require.Equal(t, 0, GlobalCPUProfiler.ConsumersCount())
	pprof.StopCPUProfile()

	// Test successfully get profile data.
	dataCh = make(ProfileConsumer, 10)
	Register(dataCh)
	data = <-dataCh
	require.NoError(t, data.Error)
	profileData, err := profile.ParseData(data.Data.Bytes())
	require.NoError(t, err)
	require.NotNil(t, profileData)
	Unregister(dataCh)
	require.Equal(t, 0, GlobalCPUProfiler.ConsumersCount())

	// Test stop profiling when no consumer.
	Register(dataCh)
	for {
		// wait for ParallelCPUProfiler do profiling successfully
		err = pprof.StartCPUProfile(bytes.NewBuffer(nil))
		if err != nil {
			break
		}
		pprof.StopCPUProfile()
		time.Sleep(time.Millisecond)
	}
	Unregister(dataCh)
	require.Equal(t, 0, GlobalCPUProfiler.ConsumersCount())

	// wait for ParallelCPUProfiler stop profiling
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

func TestGetCPUProfile(t *testing.T) {
	if GlobalCPUProfiler != nil {
		GlobalCPUProfiler.Close()
	}
	GlobalCPUProfiler = NewParallelCPUProfiler()
	GlobalCPUProfiler.Start()
	defer GlobalCPUProfiler.Close()

	// Test profile error
	err := pprof.StartCPUProfile(bytes.NewBuffer(nil))
	require.NoError(t, err)
	err = GetCPUProfile(context.Background(), 1, bytes.NewBuffer(nil))
	require.Error(t, err)
	require.Equal(t, "cpu profiling already in use", err.Error())
	pprof.StopCPUProfile()

	// test parallel get CPU profile.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockCPULoad(ctx, "sql", "sql_digest", "plan_digest")
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := bytes.NewBuffer(nil)
			err = GetCPUProfile(context.Background(), 2, buf)
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
			require.True(t, labelCnt > 1)
		}()
	}
	wg.Wait()

	// test for ctx timeout.
	ctx1, cancel1 := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel1()
	err = GetCPUProfile(ctx1, 2, bytes.NewBuffer(nil))
	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
}

func TestProfileHTTPHandler(t *testing.T) {
	if GlobalCPUProfiler != nil {
		GlobalCPUProfiler.Close()
	}
	GlobalCPUProfiler = NewParallelCPUProfiler()
	GlobalCPUProfiler.Start()
	defer GlobalCPUProfiler.Close()

	// setup http server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	router := http.NewServeMux()
	router.HandleFunc("/debug/pprof/profile", ProfileHTTPHandler)
	httpServer := &http.Server{Handler: router, WriteTimeout: time.Second * 60}
	go func() {
		if err = httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			require.NoError(t, err)
		}
	}()
	defer func() {
		err = httpServer.Close()
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
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "profile duration exceeds server's WriteTimeout\n", string(body))
	require.NoError(t, resp.Body.Close())
}

func mockCPULoad(ctx context.Context, labels ...string) {
	lvs := []string{}
	for _, label := range labels {
		lvs = append(lvs, label)
		lvs = append(lvs, label+"-value")
		// start goroutine with only 1 label.
		go mockCPULoadByGoroutineWithLabel(ctx, label, label+"-value")
	}
	// start goroutine with all labels.
	go mockCPULoadByGoroutineWithLabel(ctx, lvs...)
}

func mockCPULoadByGoroutineWithLabel(ctx context.Context, labels ...string) {
	ctx = pprof.WithLabels(ctx, pprof.Labels(labels...))
	pprof.SetGoroutineLabels(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		sum := 0
		for i := 0; i < 1000000; i++ {
			sum = sum + i*2
		}
	}
}
