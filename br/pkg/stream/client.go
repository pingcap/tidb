// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.
package stream

import (
	"bytes"
	"context"
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/kv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// MetaDataClient is the client for operations over metadata.
type MetaDataClient struct {
	*clientv3.Client
}

func NewMetaDataClient(c *clientv3.Client) *MetaDataClient {
	return &MetaDataClient{c}
}

// PutTask put a task to the metadata storage.
func (c *MetaDataClient) PutTask(ctx context.Context, task TaskInfo) error {
	data, err := task.PBInfo.Marshal()
	if err != nil {
		return errors.Annotatef(err, "failed to marshal task %s", task.PBInfo.Name)
	}

	ops := make([]clientv3.Op, 0, 2+len(task.Ranges))
	ops = append(ops, clientv3.OpPut(TaskOf(task.PBInfo.Name), string(data)))
	for _, r := range task.Ranges {
		ops = append(ops, clientv3.OpPut(RangeKeyOf(task.PBInfo.Name, r.StartKey), string(r.EndKey)))
		log.Debug("range info",
			zap.String("task-name", task.PBInfo.Name),
			zap.String("start-key", redact.Key(r.StartKey)),
			zap.String("end-key", redact.Key(r.EndKey)),
		)
	}
	if task.Pausing {
		ops = append(ops, clientv3.OpPut(Pause(task.PBInfo.Name), ""))
	}

	txn := c.KV.Txn(ctx)
	_, err = txn.Then(ops...).Commit()
	if err != nil {
		return errors.Annotatef(err, "failed to commit the change for task %s", task.PBInfo.Name)
	}
	return nil
}

// DeleteTask deletes a task, along with its metadata.
func (c *MetaDataClient) DeleteTask(ctx context.Context, taskName string) error {
	_, err := c.KV.Txn(ctx).
		Then(
			clientv3.OpDelete(TaskOf(taskName)),
			clientv3.OpDelete(RangesOf(taskName), clientv3.WithPrefix()),
			clientv3.OpDelete(CheckPointsOf(taskName), clientv3.WithPrefix()),
			clientv3.OpDelete(Pause(taskName)),
			clientv3.OpDelete(LastErrorPrefixOf(taskName), clientv3.WithPrefix()),
		).
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

func (c *MetaDataClient) CleanLastErrorOfTask(ctx context.Context, taskName string) error {
	_, err := c.KV.Delete(ctx, LastErrorPrefixOf(taskName), clientv3.WithPrefix())
	if err != nil {
		return errors.Annotatef(err, "failed to clean last error of task %s", taskName)
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

func (c *MetaDataClient) GetTaskWithPauseStatus(ctx context.Context, taskName string) (*Task, bool, error) {
	resps, err := c.KV.Txn(ctx).
		Then(
			clientv3.OpGet(TaskOf(taskName)),
			clientv3.OpGet(Pause(taskName)),
		).Commit()
	if err != nil {
		return nil, false, errors.Annotatef(err, "failed to fetch task %s", taskName)
	}

	if len(resps.Responses) == 0 || len(resps.Responses[0].GetResponseRange().Kvs) == 0 {
		return nil, false, errors.Annotatef(berrors.ErrPiTRTaskNotFound, "no such task %s", taskName)
	}

	var taskInfo backuppb.StreamBackupTaskInfo
	err = proto.Unmarshal(resps.Responses[0].GetResponseRange().Kvs[0].Value, &taskInfo)
	if err != nil {
		return nil, false, errors.Annotatef(err, "invalid binary presentation of task info (name = %s)", taskName)
	}

	paused := false
	if len(resps.Responses) > 1 && len(resps.Responses[1].GetResponseRange().Kvs) > 0 {
		paused = true
	}
	return &Task{cli: c, Info: taskInfo}, paused, nil
}

// GetAllTasks get all of tasks from metadata storage.
func (c *MetaDataClient) GetAllTasks(ctx context.Context) ([]Task, error) {
	scanner := scanEtcdPrefix(c.Client, PrefixOfTask())
	kvs, err := scanner.AllPages(ctx, 1)
	if err != nil {
		return nil, errors.Trace(err)
	} else if len(kvs) == 0 {
		return nil, nil
	}

	tasks := make([]Task, len(kvs))
	for idx, kv := range kvs {
		err = proto.Unmarshal(kv.Value, &tasks[idx].Info)
		if err != nil {
			return nil, errors.Trace(err)
		}
		tasks[idx].cli = c
	}
	return tasks, nil
}

// GetTaskCount get the count of tasks from metadata storage.
func (c *MetaDataClient) GetTaskCount(ctx context.Context) (int, error) {
	scanner := scanEtcdPrefix(c.Client, PrefixOfTask())
	kvs, err := scanner.AllPages(ctx, 1)
	return len(kvs), errors.Trace(err)
}

// Task presents a remote "task" object.
// returned by a query of task.
// Associated to the client created it, hence be able to fetch remote fields like `ranges`.
type Task struct {
	cli  *MetaDataClient
	Info backuppb.StreamBackupTaskInfo
}

// Pause is a shorthand for `metaCli.PauseTask`.
func (t *Task) Pause(ctx context.Context) error {
	return t.cli.PauseTask(ctx, t.Info.Name)
}

// Resume is a shorthand for `metaCli.ResumeTask`
func (t *Task) Resume(ctx context.Context) error {
	return t.cli.ResumeTask(ctx, t.Info.Name)
}

func (t *Task) IsPaused(ctx context.Context) (bool, error) {
	resp, err := t.cli.KV.Get(ctx, Pause(t.Info.Name), clientv3.WithCountOnly())
	if err != nil {
		return false, errors.Annotatef(err, "failed to fetch the status of task %s", t.Info.Name)
	}
	return resp.Count > 0, nil
}

// Ranges tries to fetch the range from the metadata storage.
func (t *Task) Ranges(ctx context.Context) (Ranges, error) {
	ranges := make(Ranges, 0, 64)
	kvs, err := scanEtcdPrefix(t.cli.Client, RangesOf(t.Info.Name)).AllPages(ctx, 64)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to fetch ranges of task %s", t.Info.Name)
	}
	commonPrefix := []byte(RangesOf(t.Info.Name))
	for _, kvp := range kvs {
		// The prefix must matches.
		startKey := kvp.Key[len(commonPrefix):]
		ranges = append(ranges, kv.KeyRange{StartKey: startKey, EndKey: kvp.Value})
	}
	return ranges, nil
}

// NextBackupTSList lists the backup ts of each store.
func (t *Task) NextBackupTSList(ctx context.Context) (map[uint64]uint64, error) {
	result := map[uint64]uint64{}
	prefix := CheckPointsOf(t.Info.Name)
	scanner := scanEtcdPrefix(t.cli.Client, prefix)
	kvs, err := scanner.AllPages(ctx, 1024)
	if err != nil {
		return nil, errors.Annotatef(err, "failed to get checkpoints of %s", t.Info.Name)
	}
	for _, kv := range kvs {
		if len(kv.Value) != 8 {
			return nil, errors.Annotatef(berrors.ErrPiTRMalformedMetadata,
				"the next backup ts isn't 64bits (it is %d bytes, value = %s)",
				len(kv.Value),
				redact.Key(kv.Value))
		}
		nextBackupTS := binary.BigEndian.Uint64(kv.Value)

		storeIDBinary := bytes.TrimPrefix(kv.Key, []byte(prefix))
		if len(storeIDBinary) != 8 {
			return nil, errors.Annotatef(berrors.ErrPiTRMalformedMetadata,
				"the store id isn't 64bits (it is %d bytes, value = %s)",
				len(storeIDBinary),
				redact.Key(storeIDBinary))
		}
		storeID := binary.BigEndian.Uint64(storeIDBinary)
		result[storeID] = nextBackupTS
	}
	return result, nil
}

// MinNextBackupTS query the all next backup ts of a store, returning the minimal next backup ts of the store.
func (t *Task) MinNextBackupTS(ctx context.Context, store uint64) (uint64, error) {
	key := CheckPointOf(t.Info.Name, store)
	resp, err := t.cli.KV.Get(ctx, key)
	if err != nil {
		return 0, errors.Annotatef(err, "failed to get checkpoints of %s", t.Info.Name)
	}
	if resp.Count != 1 {
		return 0, nil
	}
	kv := resp.Kvs[0]
	if len(kv.Value) != 8 {
		return 0, errors.Annotatef(berrors.ErrPiTRMalformedMetadata,
			"the next backup ts of store %d isn't 64bits (it is %d bytes, value = %s)",
			store,
			len(kv.Value),
			redact.Key(kv.Value))
	}
	nextBackupTS := binary.BigEndian.Uint64(kv.Value)
	return nextBackupTS, nil
}

// GetGlobalCheckPointTS gets the global checkpoint timestamp according to log task.
func (t *Task) GetGlobalCheckPointTS(ctx context.Context) (uint64, error) {
	checkPointMap, err := t.NextBackupTSList(ctx)
	if err != nil {
		return 0, errors.Trace(err)
	}

	initialized := false
	checkpoint := t.Info.StartTs
	for _, ts := range checkPointMap {
		if !initialized || ts < checkpoint {
			initialized = true
			checkpoint = ts
		}
	}

	return checkpoint, nil
}

// Step forwards the progress (next_backup_ts) of some region.
// The task should be done by TiKV. This function should only be used for test cases.
func (t *Task) Step(ctx context.Context, store uint64, ts uint64) error {
	_, err := t.cli.KV.Put(ctx, CheckPointOf(t.Info.Name, store), string(encodeUint64(ts)))
	if err != nil {
		return errors.Annotatef(err, "failed forward the progress of %s to %d", t.Info.Name, ts)
	}
	return nil
}

func (t *Task) LastError(ctx context.Context) (map[uint64]backuppb.StreamBackupError, error) {
	storeToError := map[uint64]backuppb.StreamBackupError{}
	prefix := LastErrorPrefixOf(t.Info.Name)
	result, err := t.cli.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Annotatef(err, "failed to get the last error for task %s", t.Info.GetName())
	}
	for _, r := range result.Kvs {
		storeIDStr := strings.TrimPrefix(string(r.Key), prefix)
		storeID, err := strconv.ParseUint(storeIDStr, 10, 64)
		if err != nil {
			return nil, errors.Annotatef(err, "failed to parse the store ID string %s", storeIDStr)
		}
		var lastErr backuppb.StreamBackupError
		if err := proto.Unmarshal(r.Value, &lastErr); err != nil {
			return nil, errors.Annotatef(err, "failed to parse wire encoding for store %d", storeID)
		}
		storeToError[storeID] = lastErr
	}
	return storeToError, nil
}
