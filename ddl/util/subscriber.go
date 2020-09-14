// Copyright 202- PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"context"
	"strconv"

	"go.etcd.io/etcd/clientv3"
)

// DDLJobSubscriber is used to notify DDL job queue state change through etcd.
type DDLJobSubscriber interface {
	// WaitNewDDLJob return a channel for new ddl job notify
	WaitNewDDLJob(ctx context.Context) clientv3.WatchChan
	// NotifyNewDDLJob when dll job enqueue
	NotifyNewDDLJob(ctx context.Context) error
	// WaitDDLJobDone return a channel for ddl job doned notify
	WaitDDLJobDone(ctx context.Context, jobID int64) clientv3.WatchChan
	// NotifyDDLJobDone when dll job dequeue
	NotifyDDLJobDone(ctx context.Context, jobID int64) error
	// ReleaseDonedDDLJob after ddl job done notification delivered
	ReleaseDonedDDLJob(jobID int64) error
}

// NewDDLJobSubscriber creates a new instance of DDLJobSubscriber
func NewDDLJobSubscriber(ctx context.Context, etcdCli *clientv3.Client) DDLJobSubscriber {
	childCtx, cancelFunc := context.WithCancel(ctx)
	return &ddlJobSubscirber{
		ctx:     childCtx,
		etcdCli: etcdCli,
		cancel:  cancelFunc,
	}
}

type ddlJobSubscirber struct {
	ctx     context.Context
	etcdCli *clientv3.Client
	cancel  context.CancelFunc
}

func (sub *ddlJobSubscirber) WaitNewDDLJob(ctx context.Context) clientv3.WatchChan {
	return sub.etcdCli.Watch(ctx, "/tidb//ddl/job/reverse")
}

func (sub *ddlJobSubscirber) NotifyNewDDLJob(ctx context.Context) error {
	return PutKVToEtcd(ctx, sub.etcdCli, putKeyRetryUnlimited, "/tidb/ddl/job/reverse", "1")
}

func (sub *ddlJobSubscirber) WaitDDLJobDone(ctx context.Context, jobID int64) clientv3.WatchChan {
	return sub.etcdCli.Watch(ctx, "/tidb/ddl/job/done/"+strconv.FormatInt(jobID, 10))
}

func (sub *ddlJobSubscirber) NotifyDDLJobDone(ctx context.Context, jobID int64) error {
	return PutKVToEtcd(ctx, sub.etcdCli, putKeyRetryUnlimited, "/tidb/ddl/job/done/"+strconv.FormatInt(jobID, 10), "1")
}

func (sub *ddlJobSubscirber) ReleaseDonedDDLJob(jobID int64) error {
	return DeleteKeyFromEtcd("/tidb/ddl/job/done/"+strconv.FormatInt(jobID, 10), sub.etcdCli, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
}

type mockDDLJobSubscriber struct {
	ctx    context.Context
	cancel context.CancelFunc
}

// NewMockDDLJobSubscribe creates a new mock of DDLJobSubscriber
func NewMockDDLJobSubscribe(ctx context.Context) DDLJobSubscriber {
	childCtx, cancelFunc := context.WithCancel(ctx)
	return &mockDDLJobSubscriber{childCtx, cancelFunc}
}

func (sub *mockDDLJobSubscriber) WaitNewDDLJob(ctx context.Context) clientv3.WatchChan {
	return make(clientv3.WatchChan)
}

func (sub *mockDDLJobSubscriber) NotifyNewDDLJob(ctx context.Context) error {
	return nil
}

func (sub *mockDDLJobSubscriber) WaitDDLJobDone(ctx context.Context, jobID int64) clientv3.WatchChan {
	return make(clientv3.WatchChan)
}

func (sub *mockDDLJobSubscriber) NotifyDDLJobDone(ctx context.Context, jobID int64) error {
	return nil
}

func (sub *mockDDLJobSubscriber) ReleaseDonedDDLJob(jobID int64) error {
	return nil
}
