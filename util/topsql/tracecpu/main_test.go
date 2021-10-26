package tracecpu_test

import (
	"bytes"
	"testing"

	"github.com/google/pprof/profile"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/util/testbridge"
	"github.com/pingcap/tidb/util/topsql/tracecpu"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testbridge.WorkaroundGoCheckFlags()

	variable.TopSQLVariable.Enable.Store(false)
	config.UpdateGlobal(func(conf *config.Config) {
		conf.TopSQL.ReceiverAddress = "mock"
	})
	variable.TopSQLVariable.PrecisionSeconds.Store(1)
	tracecpu.GlobalSQLCPUProfiler.Run()

	opts := []goleak.Option{
		goleak.IgnoreTopFunction("time.Sleep"),
		goleak.IgnoreTopFunction("runtime/pprof.readProfile"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreTopFunction("github.com/pingcap/tidb/util/topsql/tracecpu.(*sqlCPUProfiler).startAnalyzeProfileWorker"),
	}

	goleak.VerifyTestMain(m, opts...)
}

func TestPProfCPUProfile(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := tracecpu.StartCPUProfile(buf)
	require.NoError(t, err)
	// enable top sql.
	variable.TopSQLVariable.Enable.Store(true)
	err = tracecpu.StopCPUProfile()
	require.NoError(t, err)
	_, err = profile.Parse(buf)
	require.NoError(t, err)
}
