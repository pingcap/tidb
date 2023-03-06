package example

import (
	"context"
	"github.com/pingcap/failpoint"
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

	dispatcher.MockTiDBId = []string{"id1", "id2", "id3"}

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
	globalNumberCounter.Store(0)
	id, doneCh, err := hd.SubmitGlobalTaskAndRun(&proto.SimpleNumberGTaskMeta{})
	require.NoError(t, err)
	require.Greater(t, int(id), 0)
	<-doneCh

	time.Sleep(1 * time.Second)

	require.Equal(t, int64(-450), globalNumberCounter.Load())

	// cleanup
	dsp.Stop()
	scheManager1.Stop()
	scheManager2.Stop()
	scheManager3.Stop()
}

func TestMockErrorOnStep1(t *testing.T) {
	store := testkit.CreateMockStore(t)

	dispatcher.MockTiDBId = []string{"id1", "id2", "id3"}

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

	err = failpoint.Enable("github.com/pingcap/tidb/distribute_framework/example/mockStepOneError", `return(true)`)
	require.NoError(t, err)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/distribute_framework/example/mockStepOneError")
		require.NoError(t, err)
	}()

	// Create a task.
	handleTk := testkit.NewTestKit(t, store)
	hd, err := handle.NewHandle(util.WithInternalSourceType(context.Background(), "handle"), handleTk.Session())
	require.NoError(t, err)
	globalNumberCounter.Store(0)
	id, doneCh, err := hd.SubmitGlobalTaskAndRun(&proto.SimpleNumberGTaskMeta{})
	require.NoError(t, err)
	require.Greater(t, int(id), 0)
	<-doneCh

	time.Sleep(1 * time.Second)

	require.Less(t, int64(0), globalNumberCounter.Load())
	require.Greater(t, int64(450), globalNumberCounter.Load())

	// cleanup
	dsp.Stop()
	scheManager1.Stop()
	scheManager2.Stop()
	scheManager3.Stop()
}

func TestMockErrorOnStep2(t *testing.T) {
	store := testkit.CreateMockStore(t)

	dispatcher.MockTiDBId = []string{"id1", "id2", "id3"}

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

	err = failpoint.Enable("github.com/pingcap/tidb/distribute_framework/example/mockStepTwoError", `return(true)`)
	require.NoError(t, err)
	defer func() {
		err := failpoint.Disable("github.com/pingcap/tidb/distribute_framework/example/mockStepTwoError")
		require.NoError(t, err)
	}()

	// Create a task.
	handleTk := testkit.NewTestKit(t, store)
	hd, err := handle.NewHandle(util.WithInternalSourceType(context.Background(), "handle"), handleTk.Session())
	require.NoError(t, err)
	globalNumberCounter.Store(0)
	id, doneCh, err := hd.SubmitGlobalTaskAndRun(&proto.SimpleNumberGTaskMeta{})
	require.NoError(t, err)
	require.Greater(t, int(id), 0)
	<-doneCh

	time.Sleep(1 * time.Second)

	require.Greater(t, int64(450), globalNumberCounter.Load())
	require.Less(t, int64(0), globalNumberCounter.Load())

	// cleanup
	dsp.Stop()
	scheManager1.Stop()
	scheManager2.Stop()
	scheManager3.Stop()
}
