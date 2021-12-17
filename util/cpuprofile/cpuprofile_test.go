package cpuprofile

import (
	"bytes"
	"context"
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
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/util/cpuprofile.(*ParallelCPUProfiler).profilingLoop"),
		goleak.IgnoreTopFunction("time.Sleep"),
		goleak.IgnoreTopFunction("runtime/pprof.readProfile"),
		goleak.IgnoreTopFunction("runtime/pprof.profileWriter"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestParallelCPUProfiler(t *testing.T) {
	if globalProfiler != nil {
		globalProfiler.close()
	}
	globalProfiler = NewParallelCPUProfiler()
	globalProfiler.Start()

	// Test register/unregister nil
	Register(nil)
	require.Equal(t, 0, globalProfiler.consumersCount())
	Unregister(nil)
	require.Equal(t, 0, globalProfiler.consumersCount())

	// Test profile error and duplicate register.
	dataCh := make(ProfileConsumer, 10)
	err := pprof.StartCPUProfile(bytes.NewBuffer(nil))
	require.NoError(t, err)

	// Test for duplicate register.
	Register(dataCh)
	Register(dataCh)
	require.Equal(t, 1, globalProfiler.consumersCount())

	// Test profile error
	data := <-dataCh
	require.Equal(t, "cpu profiling already in use", data.Error.Error())
	Unregister(dataCh)
	require.Equal(t, 0, globalProfiler.consumersCount())

	// shouldn't receive data from a unregistered consumer.
	data = nil
	select {
	case data = <-dataCh:
	default:
	}
	require.Nil(t, data)

	// unregister not exist consumer
	Unregister(dataCh)
	require.Equal(t, 0, globalProfiler.consumersCount())

	// Test register a closed consumer
	dataCh = make(ProfileConsumer, 10)
	close(dataCh)
	Register(dataCh)
	require.Equal(t, 1, globalProfiler.consumersCount())
	data, ok := <-dataCh
	require.Nil(t, data)
	require.False(t, ok)
	Unregister(dataCh)
	require.Equal(t, 0, globalProfiler.consumersCount())
	pprof.StopCPUProfile()

	// Test successfully get profile data.
	dataCh = make(ProfileConsumer, 10)
	Register(dataCh)
	data = <-dataCh
	require.NoError(t, data.Error)
	profileData, err := data.Parse()
	require.NoError(t, err)
	require.NotNil(t, profileData)
	Unregister(dataCh)
	require.Equal(t, 0, globalProfiler.consumersCount())

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
	require.Equal(t, 0, globalProfiler.consumersCount())

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
	if globalProfiler != nil {
		globalProfiler.close()
	}
	globalProfiler = NewParallelCPUProfiler()
	globalProfiler.Start()

	// Test profile error
	err := pprof.StartCPUProfile(bytes.NewBuffer(nil))
	require.NoError(t, err)
	err = GetCPUProfile(1, bytes.NewBuffer(nil))
	require.Error(t, err)
	require.Equal(t, "cpu profiling already in use", err.Error())
	pprof.StopCPUProfile()

	// test parallel get CPU profile.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mockCPULoad(ctx, labelSQL, "other")
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := bytes.NewBuffer(nil)
			err = GetCPUProfile(2, buf)
			require.NoError(t, err)
			profileData, err := profile.Parse(buf)
			require.NoError(t, err)

			labelCnt := 0
			for _, s := range profileData.Sample {
				for k := range s.Label {
					require.Equal(t, labelSQL, k)
					labelCnt++
				}
			}
			require.True(t, labelCnt > 1)
		}()
	}
	wg.Wait()
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
