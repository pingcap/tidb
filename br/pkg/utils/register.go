// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// RegisterTaskType for the sub-prefix path for key
type RegisterTaskType int

const (
	RegisterRestore RegisterTaskType = iota
	RegisterLightning
	RegisterImportInto
)

func (tp RegisterTaskType) String() string {
	switch tp {
	case RegisterRestore:
		return "restore"
	case RegisterLightning:
		return "lightning"
	case RegisterImportInto:
		return "import-into"
	}
	return "default"
}

// The key format should be {RegisterImportTaskPrefix}/{RegisterTaskType}/{taskName}
const (
	// RegisterImportTaskPrefix is the prefix of the key for task register
	// todo: remove "/import" suffix, it's confusing to have a key like "/tidb/brie/import/restore/restore-xxx"
	RegisterImportTaskPrefix = "/tidb/brie/import"

	RegisterRetryInternal  = 10 * time.Second
	defaultTaskRegisterTTL = 3 * time.Minute // 3 minutes
)

// TaskRegister can register the task to PD with a lease.
type TaskRegister interface {
	// Close closes the background task if using RegisterTask
	// and revoke the lease.
	// NOTE: we don't close the etcd client here, call should do it.
	Close(ctx context.Context) (err error)
	// RegisterTask firstly put its key to PD with a lease,
	// and start to keepalive the lease in the background.
	// DO NOT mix calls to RegisterTask and RegisterTaskOnce.
	RegisterTask(c context.Context) error
	// RegisterTaskOnce put its key to PD with a lease if the key does not exist,
	// else we refresh the lease.
	// you have to call this method periodically to keep the lease alive.
	// DO NOT mix calls to RegisterTask and RegisterTaskOnce.
	RegisterTaskOnce(ctx context.Context) error
}

type taskRegister struct {
	client    *clientv3.Client
	ttl       time.Duration
	secondTTL int64
	key       string

	// leaseID used to revoke the lease
	curLeaseID clientv3.LeaseID
	wg         sync.WaitGroup
	cancel     context.CancelFunc
}

// NewTaskRegisterWithTTL build a TaskRegister with key format {RegisterTaskPrefix}/{RegisterTaskType}/{taskName}
func NewTaskRegisterWithTTL(client *clientv3.Client, ttl time.Duration, tp RegisterTaskType, taskName string) TaskRegister {
	return &taskRegister{
		client:    client,
		ttl:       ttl,
		secondTTL: int64(ttl / time.Second),
		key:       path.Join(RegisterImportTaskPrefix, tp.String(), taskName),

		curLeaseID: clientv3.NoLease,
	}
}

// NewTaskRegister build a TaskRegister with key format {RegisterTaskPrefix}/{RegisterTaskType}/{taskName}
func NewTaskRegister(client *clientv3.Client, tp RegisterTaskType, taskName string) TaskRegister {
	return NewTaskRegisterWithTTL(client, defaultTaskRegisterTTL, tp, taskName)
}

// Close implements the TaskRegister interface
func (tr *taskRegister) Close(ctx context.Context) (err error) {
	// not needed if using RegisterTaskOnce
	if tr.cancel != nil {
		tr.cancel()
	}
	tr.wg.Wait()
	if tr.curLeaseID != clientv3.NoLease {
		_, err = tr.client.Lease.Revoke(ctx, tr.curLeaseID)
		if err != nil {
			log.Warn("failed to revoke the lease", zap.Error(err), zap.Int64("lease-id", int64(tr.curLeaseID)))
		}
	}
	return err
}

func (tr *taskRegister) grant(ctx context.Context) (*clientv3.LeaseGrantResponse, error) {
	lease, err := tr.client.Lease.Grant(ctx, tr.secondTTL)
	if err != nil {
		return nil, err
	}
	if len(lease.Error) > 0 {
		return nil, errors.New(lease.Error)
	}
	return lease, nil
}

// RegisterTaskOnce implements the TaskRegister interface
func (tr *taskRegister) RegisterTaskOnce(ctx context.Context) error {
	resp, err := tr.client.Get(ctx, tr.key)
	if err != nil {
		return errors.Trace(err)
	}
	if len(resp.Kvs) == 0 {
		lease, err2 := tr.grant(ctx)
		if err2 != nil {
			return errors.Annotatef(err2, "failed grant a lease")
		}
		tr.curLeaseID = lease.ID
		_, err2 = tr.client.KV.Put(ctx, tr.key, "", clientv3.WithLease(lease.ID))
		if err2 != nil {
			return errors.Trace(err2)
		}
	} else {
		// if the task is run distributively, like IMPORT INTO, we should refresh the lease ID,
		// in case the owner changed during the registration, and the new owner create the key.
		tr.curLeaseID = clientv3.LeaseID(resp.Kvs[0].Lease)
		_, err2 := tr.client.Lease.KeepAliveOnce(ctx, tr.curLeaseID)
		if err2 != nil {
			return errors.Trace(err2)
		}
	}
	return nil
}

// RegisterTask implements the TaskRegister interface
func (tr *taskRegister) RegisterTask(c context.Context) error {
	cctx, cancel := context.WithCancel(c)
	tr.cancel = cancel
	lease, err := tr.grant(cctx)
	if err != nil {
		return errors.Annotatef(err, "failed grant a lease")
	}
	tr.curLeaseID = lease.ID
	_, err = tr.client.KV.Put(cctx, tr.key, "", clientv3.WithLease(lease.ID))
	if err != nil {
		return errors.Trace(err)
	}

	// KeepAlive interval equals to ttl/3
	respCh, err := tr.client.Lease.KeepAlive(cctx, lease.ID)
	if err != nil {
		return errors.Trace(err)
	}
	tr.wg.Add(1)
	go tr.keepaliveLoop(cctx, respCh)
	return nil
}

func (tr *taskRegister) keepaliveLoop(ctx context.Context, ch <-chan *clientv3.LeaseKeepAliveResponse) {
	defer tr.wg.Done()
	const minTimeLeftThreshold time.Duration = 20 * time.Second
	var (
		timeLeftThreshold time.Duration = tr.ttl / 4
		lastUpdateTime    time.Time     = time.Now()
		err               error
	)
	if timeLeftThreshold < minTimeLeftThreshold {
		timeLeftThreshold = minTimeLeftThreshold
	}
	failpoint.Inject("brie-task-register-always-grant", func(_ failpoint.Value) {
		timeLeftThreshold = tr.ttl
	})
	for {
	CONSUMERESP:
		for {
			failpoint.Inject("brie-task-register-keepalive-stop", func(_ failpoint.Value) {
				if _, err = tr.client.Lease.Revoke(ctx, tr.curLeaseID); err != nil {
					log.Warn("brie-task-register-keepalive-stop", zap.Error(err))
				}
			})
			select {
			case <-ctx.Done():
				return
			case _, ok := <-ch:
				if !ok {
					break CONSUMERESP
				}
				lastUpdateTime = time.Now()
			}
		}
		log.Warn("the keepalive channel is closed, try to recreate it")
		needReputKV := false
	RECREATE:
		for {
			timeGap := time.Since(lastUpdateTime)
			if tr.ttl-timeGap <= timeLeftThreshold {
				lease, err := tr.grant(ctx)
				failpoint.Inject("brie-task-register-failed-to-grant", func(_ failpoint.Value) {
					err = errors.New("failpoint-error")
				})
				if err != nil {
					select {
					case <-ctx.Done():
						return
					default:
					}
					log.Warn("failed to grant lease", zap.Error(err))
					time.Sleep(RegisterRetryInternal)
					continue
				}
				tr.curLeaseID = lease.ID
				lastUpdateTime = time.Now()
				needReputKV = true
			}
			if needReputKV {
				// if the lease has expired, need to put the key again
				_, err := tr.client.KV.Put(ctx, tr.key, "", clientv3.WithLease(tr.curLeaseID))
				failpoint.Inject("brie-task-register-failed-to-reput", func(_ failpoint.Value) {
					err = errors.New("failpoint-error")
				})
				if err != nil {
					select {
					case <-ctx.Done():
						return
					default:
					}
					log.Warn("failed to put new kv", zap.Error(err))
					time.Sleep(RegisterRetryInternal)
					continue
				}
				needReputKV = false
			}
			// recreate keepalive
			ch, err = tr.client.Lease.KeepAlive(ctx, tr.curLeaseID)
			if err != nil {
				select {
				case <-ctx.Done():
					return
				default:
				}
				log.Warn("failed to create new kv", zap.Error(err))
				time.Sleep(RegisterRetryInternal)
				continue
			}

			break RECREATE
		}
	}
}

// RegisterTask saves the task's information
type RegisterTask struct {
	Key     string
	LeaseID int64
	TTL     int64
}

// MessageToUser marshal the task to user message
func (task RegisterTask) MessageToUser() string {
	return fmt.Sprintf("[ key: %s, lease-id: %x, ttl: %ds ]", task.Key, task.LeaseID, task.TTL)
}

type RegisterTasksList struct {
	Tasks []RegisterTask
}

func (list RegisterTasksList) MessageToUser() string {
	var tasksMsgBuf strings.Builder
	for _, task := range list.Tasks {
		tasksMsgBuf.WriteString(task.MessageToUser())
		tasksMsgBuf.WriteString(", ")
	}
	return tasksMsgBuf.String()
}

func (list RegisterTasksList) Empty() bool {
	return len(list.Tasks) == 0
}

// GetImportTasksFrom try to get all the import tasks with prefix `RegisterTaskPrefix`
func GetImportTasksFrom(ctx context.Context, client *clientv3.Client) (RegisterTasksList, error) {
	resp, err := client.KV.Get(ctx, RegisterImportTaskPrefix, clientv3.WithPrefix())
	if err != nil {
		return RegisterTasksList{}, errors.Trace(err)
	}

	list := RegisterTasksList{
		Tasks: make([]RegisterTask, 0, len(resp.Kvs)),
	}
	for _, kv := range resp.Kvs {
		leaseResp, err := client.Lease.TimeToLive(ctx, clientv3.LeaseID(kv.Lease))
		if err != nil {
			return list, errors.Annotatef(err, "failed to get time-to-live of lease: %x", kv.Lease)
		}
		// the lease has expired
		if leaseResp.TTL <= 0 {
			continue
		}
		list.Tasks = append(list.Tasks, RegisterTask{
			Key:     string(kv.Key),
			LeaseID: kv.Lease,
			TTL:     leaseResp.TTL,
		})
	}
	return list, nil
}
