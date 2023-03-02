package example

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tidb/distribute_framework/dispatcher"
	"github.com/pingcap/tidb/distribute_framework/handle"
	"github.com/pingcap/tidb/distribute_framework/proto"
	"github.com/pingcap/tidb/distribute_framework/scheduler"
	"github.com/pingcap/tidb/distribute_framework/storage"
	"github.com/pingcap/tidb/testkit"
	"github.com/pingcap/tidb/testkit/testsetup"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/util"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	testsetup.SetupForCommonTest()
	opts := []goleak.Option{
		goleak.IgnoreTopFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
		goleak.IgnoreTopFunction("github.com/lestrrat-go/httprc.runFetchWorker"),
		goleak.IgnoreTopFunction("go.etcd.io/etcd/client/pkg/v3/logutil.(*MergeLogger).outputLoop"),
		goleak.IgnoreTopFunction("go.opencensus.io/stats/view.(*worker).start"),
		goleak.IgnoreTopFunction("syscall.syscall"),
	}
	goleak.VerifyTestMain(m, opts...)
}

func TestSimple(t *testing.T) {
	store := testkit.CreateMockStore(t)

	gtk := testkit.NewTestKit(t, store)
	stk := testkit.NewTestKit(t, store)
	ctx := context.Background()
	gm := storage.NewGlobalTaskManager(util.WithInternalSourceType(ctx, "globalTaskManager"), gtk.Session())
	storage.SetGlobalTaskManager(gm)
	sm := storage.NewSubTaskManager(util.WithInternalSourceType(ctx, "subTaskManager"), stk.Session())
	storage.SetSubTaskManager(sm)

	dsp, err := dispatcher.NewDispatcher(util.WithInternalSourceType(ctx, "dispatcher"), gm, sm)
	require.NoError(t, err)
	dsp.Start()

	// Start 3 schedulers.
	scheManager1 := scheduler.NewManager(util.WithInternalSourceType(ctx, "scheduler"), "id1", gm, sm)
	scheManager1.Start()
	scheManager2 := scheduler.NewManager(util.WithInternalSourceType(ctx, "scheduler"), "id2", gm, sm)
	scheManager2.Start()
	scheManager3 := scheduler.NewManager(util.WithInternalSourceType(ctx, "scheduler"), "id3", gm, sm)
	scheManager3.Start()

	// Create a task.
	handleTk := testkit.NewTestKit(t, store)
	hd, err := handle.NewHandle(util.WithInternalSourceType(context.Background(), "handle"), handleTk.Session())
	require.NoError(t, err)
	id, doneCh, err := hd.SubmitGlobalTaskAndRun(&proto.SimpleNumberGTaskMeta{})
	require.NoError(t, err)
	require.Greater(t, int(id), 0)
	<-doneCh

	time.Sleep(3 * time.Second)

	// cleanup
	dsp.Stop()
	scheManager1.Stop()
	scheManager2.Stop()
	scheManager3.Stop()
}
