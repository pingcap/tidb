package stream

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"go.etcd.io/etcd/clientv3"
)

// MetaDataClient is the client for operations over metadata.
type MetaDataClient struct {
	*clientv3.Client
}

// PutTask put a task to the metadata storage.
func (c *MetaDataClient) PutTask(ctx context.Context, task TaskInfo) error {
	data, err := task.StreamBackupTaskInfo.Marshal()
	if err != nil {
		return errors.Annotatef(err, "failed to marshal task %s", task.Name)
	}

	ops := make([]clientv3.Op, 0, 2+len(task.Ranges))
	ops = append(ops, clientv3.OpPut(TaskOf(task.GetName()), string(data)))
	for _, r := range task.Ranges {
		ops = append(ops, clientv3.OpPut(RangeKeyOf(task.Name, r[0]), string(r[1])))
	}
	if task.Pausing {
		ops = append(ops, clientv3.OpPut(Pause(task.Name), ""))
	}

	txn := c.KV.Txn(ctx)
	_, err = txn.Then(ops...).Commit()
	if err != nil {
		return errors.Annotatef(err, "failed to commit the change for task %s", task.Name)
	}
	return nil
}

// DeleteTask deletes a task, along with its metadata.
func (c *MetaDataClient) DeleteTask(ctx context.Context, taskName string) error {
	_, err := c.KV.Txn(ctx).
		Then(clientv3.OpDelete(TaskOf(taskName)),
			clientv3.OpDelete(RangesOf(taskName), clientv3.WithPrefix()),
			clientv3.OpDelete(Pause(taskName))).
		Commit()
	if err != nil {
		return errors.Annotatef(err, "failed to delete task itself %s", taskName)
	}
	return nil
}

func (c *MetaDataClient) PauseTask(ctx context.Context, taskName string) error {
	_, err := c.KV.Put(ctx, Pause(taskName), "")
	if err != nil {
		return errors.Annotatef(err, "failed to pause task %s", taskName)
	}
	return nil
}

func (c *MetaDataClient) ResumeTask(ctx context.Context, taskName string) error {
	_, err := c.KV.Delete(ctx, Pause(taskName))
	if err != nil {
		return errors.Annotatef(err, "failed to resume task %s", taskName)
	}
	return nil
}

// Task presents a remote "task" object.
// returned by a query of task.
// Associated to the client created it, hence be able to fetch remote fields like `ranges`.
type Task struct {
	cli  *MetaDataClient
	Info backuppb.StreamBackupTaskInfo
}

// Ranges tries to fetch the range from the metadata storage.
func (t *Task) Ranges(ctx context.Context) (Ranges, error) {
	ranges := make(Ranges, 0, 64)
	resp, err := t.cli.KV.Get(ctx, RangesOf(t.Info.Name), clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Annotatef(err, "failed to fetch ranges of task %s", t.Info.Name)
	}
	commonPrefix := []byte(RangesOf(t.Info.Name))
	for _, kvs := range resp.Kvs {
		// Given we scan the key `RangesOf(t.Info.Name)` with `WithPrefix()`,
		// The prefix should always be RangesOf(t.Info.Name).
		// It would be safe to cut the prefix directly. (instead of use TrimPrefix)
		// But the rule not apply for the slash. Maybe scan the prefix RangesOf(t.Info.Name) + "/"?
		startKey := bytes.TrimPrefix(kvs.Key[len(commonPrefix):], []byte("/"))
		ranges = append(ranges, [...][]byte{startKey, kvs.Value})
	}
	return ranges, nil
}

// MinNextBackupTS query the all next backup ts of a store, returning the minimal next backup ts of the store.
// FIXME: this would probably exceed the gRPC max request size (1.5M), maybe we need page scanning,
//        but we cannot both do `WithPrefix` and `WithStart`. Maybe impl a `PrefixScanner` with `kv.NextPrefixKey()`?
func (t *Task) MinNextBackupTS(ctx context.Context, store uint64) (uint64, error) {
	min := uint64(0xffffffff)
	resp, err := t.cli.KV.Get(ctx, CheckPointsOf(t.Info.Name, store), clientv3.WithPrefix())
	if err != nil {
		return 0, errors.Annotatef(err, "failed to get checkpoints of %s", t.Info.Name)
	}
	for _, kv := range resp.Kvs {
		if len(kv.Value) != 8 {
			return 0, errors.Annotatef(berrors.ErrPiTRMalformedMetadata,
				"the next backup ts of store %d isn't 64bits (it is %d bytes, key = %s)",
				store,
				len(kv.Value),
				hex.EncodeToString(kv.Key))
		}
		nextBackupTS := binary.BigEndian.Uint64(kv.Value)
		if nextBackupTS < min {
			min = nextBackupTS
		}
	}
	return min, nil
}

// Step forwards the progress (next_backup_ts) of some region.
// The task should be done by TiKV. This function should only be used for test cases.
func (t *Task) Step(ctx context.Context, store uint64, region uint64, ts uint64) error {
	_, err := t.cli.KV.Put(ctx, CheckpointOf(t.Info.Name, store, region), string(encodeUint64(ts)))
	if err != nil {
		return errors.Annotatef(err, "failed forward the progress of %s to %d", t.Info.Name, ts)
	}
	return nil
}

// GetTask get the basic task handle from the metadata storage.
func (c *MetaDataClient) GetTask(ctx context.Context, taskName string) (*Task, error) {
	resp, err := c.Get(ctx, TaskOf(taskName))
	if err != nil {
		return nil, errors.Annotatef(err, "failed to fetch task %s", taskName)
	}
	if len(resp.Kvs) == 0 {
		return nil, errors.Annotatef(berrors.ErrPiTRTaskNotFound, "no such task %s", taskName)
	}
	var taskInfo backuppb.StreamBackupTaskInfo
	err = proto.Unmarshal(resp.Kvs[0].Value, &taskInfo)
	if err != nil {
		return nil, errors.Annotatef(err, "invalid binary presentation of task info (name = %s)", taskName)
	}
	task := &Task{
		cli:  c,
		Info: taskInfo,
	}
	return task, nil
}
