package executor

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/executor/internal/exec"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/mock"
	"github.com/stretchr/testify/require"
	tikvutil "github.com/tikv/client-go/v2/util"
)

type recordingBatchGetter struct {
	callCount int
	lastCtx   context.Context
}

func (b *recordingBatchGetter) BatchGet(ctx context.Context, keys []kv.Key) (map[string][]byte, error) {
	b.callCount++
	b.lastCtx = ctx
	values := make(map[string][]byte, len(keys))
	for _, key := range keys {
		values[string(key)] = []byte("v")
	}
	return values, nil
}

type alwaysValidTxn struct{ kv.Transaction }

func (alwaysValidTxn) Valid() bool { return true }

type dirtyContentContext struct {
	*mock.Context
	dirty map[int64]bool
}

func (c *dirtyContentContext) HasDirtyContent(tid int64) bool { return c.dirty[tid] }

func TestBatchPointGetInTxnStoreBatchGetFlag(t *testing.T) {
	t.Run("clean table", func(t *testing.T) {
		sctx := &dirtyContentContext{Context: mock.NewContext(), dirty: map[int64]bool{}}
		tblInfo := &model.TableInfo{ID: 1}
		bg := &recordingBatchGetter{}
		e := &BatchPointGetExec{
			BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0),
			tblInfo:      tblInfo,
			batchGetter:  bg,
			handles:      []kv.Handle{kv.IntHandle(1), kv.IntHandle(2)},
			txn:          alwaysValidTxn{},
		}
		err := e.initialize(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, bg.callCount)
		require.False(t, tikvutil.IsStoreBatchGetDisabled(bg.lastCtx))
	})

	t.Run("dirty table", func(t *testing.T) {
		sctx := &dirtyContentContext{Context: mock.NewContext(), dirty: map[int64]bool{1: true}}
		tblInfo := &model.TableInfo{ID: 1}
		bg := &recordingBatchGetter{}
		e := &BatchPointGetExec{
			BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0),
			tblInfo:      tblInfo,
			batchGetter:  bg,
			handles:      []kv.Handle{kv.IntHandle(1), kv.IntHandle(2)},
			txn:          alwaysValidTxn{},
		}
		err := e.initialize(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, bg.callCount)
		require.True(t, tikvutil.IsStoreBatchGetDisabled(bg.lastCtx))
	})

	t.Run("partitioned dirty table", func(t *testing.T) {
		sctx := &dirtyContentContext{Context: mock.NewContext(), dirty: map[int64]bool{11: true}}
		tblInfo := &model.TableInfo{
			ID: 1,
			Partition: &model.PartitionInfo{
				Enable: true,
				Definitions: []model.PartitionDefinition{
					{ID: 11},
					{ID: 12},
				},
			},
		}
		bg := &recordingBatchGetter{}
		e := &BatchPointGetExec{
			BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0),
			tblInfo:      tblInfo,
			batchGetter:  bg,
			handles:      []kv.Handle{kv.IntHandle(1), kv.IntHandle(2)},
			txn:          alwaysValidTxn{},
			planPhysIDs:  []int64{11, 12},
		}
		err := e.initialize(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, bg.callCount)
		require.True(t, tikvutil.IsStoreBatchGetDisabled(bg.lastCtx))
	})

	t.Run("partitioned clean table", func(t *testing.T) {
		sctx := &dirtyContentContext{Context: mock.NewContext(), dirty: map[int64]bool{}}
		tblInfo := &model.TableInfo{
			ID: 1,
			Partition: &model.PartitionInfo{
				Enable: true,
				Definitions: []model.PartitionDefinition{
					{ID: 11},
					{ID: 12},
				},
			},
		}
		bg := &recordingBatchGetter{}
		e := &BatchPointGetExec{
			BaseExecutor: exec.NewBaseExecutor(sctx, nil, 0),
			tblInfo:      tblInfo,
			batchGetter:  bg,
			handles:      []kv.Handle{kv.IntHandle(1), kv.IntHandle(2)},
			txn:          alwaysValidTxn{},
			planPhysIDs:  []int64{11, 12},
		}
		err := e.initialize(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1, bg.callCount)
		require.False(t, tikvutil.IsStoreBatchGetDisabled(bg.lastCtx))
	})
}
