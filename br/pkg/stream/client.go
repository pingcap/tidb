package stream

import (
	"context"

	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/kv"
)

type Ranges = []kv.KeyRange

// TaskInfo is a task info with extra information.
type TaskInfo struct {
	backuppb.StreamBackupTaskInfo
	Ranges  Ranges
	Pausing bool
}

type MetaDataClient interface {
	PutTask(ctx context.Context, task TaskInfo) error
	DeleteTask(ctx context.Context, taskName string) error
	PauseTask(ctx context.Context, taskName string) error
	ResumeTask(ctx context.Context, taskName string) error
	GetTask(ctx context.Context, taskName string) (*Task, error)
}

type metaDataClient struct {
}

func NewMetaDataClient() MetaDataClient {
	return &metaDataClient{}
}

func (*metaDataClient) PutTask(ctx context.Context, task TaskInfo) error {
	return nil
}

func (*metaDataClient) DeleteTask(ctx context.Context, taskName string) error {
	return nil
}

func (*metaDataClient) PauseTask(ctx context.Context, taskName string) error {
	return nil
}

func (*metaDataClient) ResumeTask(ctx context.Context, taskName string) error {
	return nil
}

func (*metaDataClient) GetTask(ctx context.Context, taskName string) (*Task, error) {
	return nil, nil
}

type Task interface {
	Pause(ctx context.Context) error
	Resume(ctx context.Context) error
	Paused(ctx context.Context) (bool, error)
	Ranges(ctx context.Context) (Ranges, error)
}

type task struct {
}

func (t *task) Pause(ctx context.Context) error {
	return nil
}

func (t *task) Resume(ctx context.Context) error {
	return nil
}

func (t *task) Paused(ctx context.Context) (bool, error) {
	return false, nil
}

func (t *task) Ranges(ctx context.Context) (Ranges, error) {
	return nil, nil
}
