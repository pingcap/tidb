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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/logutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	ttlCmdKeyLeaseSeconds   int64 = 180
	ttlCmdKeyRequestPrefix        = "/tidb/ttl/cmd/req/"
	ttlCmdKeyResponsePrefix       = "/tidb/ttl/cmd/resp/"
	ttlCmdTypeTriggerTTLJob       = "trigger_ttl_job"
)

// CmdRequest is the request for a TTL command
type CmdRequest struct {
	RequestID string          `json:"request_id"`
	CmdType   string          `json:"cmd_type"`
	Data      json.RawMessage `json:"data"`
}

// GetTriggerTTLJobRequest returns the `TriggerNewTTLJobRequest` object if command type is 'trigger_ttl_job',
// otherwise, (nil, false) will be returned
func (r *CmdRequest) GetTriggerTTLJobRequest() (*TriggerNewTTLJobRequest, bool) {
	if r.CmdType != ttlCmdTypeTriggerTTLJob {
		return nil, false
	}

	var req TriggerNewTTLJobRequest
	if err := json.Unmarshal(r.Data, &req); err != nil {
		return nil, false
	}
	return &req, true
}

type cmdResponse struct {
	RequestID    string          `json:"request_id"`
	ErrorMessage string          `json:"error_message"`
	Data         json.RawMessage `json:"data"`
}

// TriggerNewTTLJobRequest is the command detail to trigger a TTL job
type TriggerNewTTLJobRequest struct {
	DBName    string `json:"db_name"`
	TableName string `json:"table_name"`
}

// TriggerNewTTLJobTableResult is the table detail of `TriggerNewTTLJobResponse`
type TriggerNewTTLJobTableResult struct {
	TableID       int64  `json:"table_id"`
	DBName        string `json:"db_name"`
	TableName     string `json:"table_name"`
	PartitionName string `json:"partition_name,omitempty"`
	JobID         string `json:"job_id,omitempty"`
	ErrorMessage  string `json:"error_message,omitempty"`
}

// TriggerNewTTLJobResponse is the response detail for trigger_ttl_job command
type TriggerNewTTLJobResponse struct {
	TableResult []*TriggerNewTTLJobTableResult `json:"table_result"`
}

// CommandClient is an interface used to send and response command of TTL jobs
type CommandClient interface {
	// Command sends a command and waits for response. The first value of the return is the requestID, it always not empty.
	Command(ctx context.Context, cmdType string, obj any, response any) (string, error)
	// WatchCommand watches the commands that are sent
	WatchCommand(ctx context.Context) <-chan *CmdRequest
	// TakeCommand takes a command to ensure only one can handle the command.
	// If the first return value is true, it means you have taken the command successfully,
	// and you should call `ResponseCommand`
	// after processed the command. Otherwise, you should not process this command because it is not belong to you.
	TakeCommand(ctx context.Context, reqID string) (bool, error)
	// ResponseCommand responses the result of the command. `TakeCommand` must be called first before `ResponseCommand`
	// obj is the response object to the sender, if obj is an error, the sender will receive an error too.
	ResponseCommand(ctx context.Context, reqID string, obj any) error
}

// TriggerNewTTLJob triggers a new TTL job
func TriggerNewTTLJob(ctx context.Context, cli CommandClient, dbName, tableName string) (
	*TriggerNewTTLJobResponse, error) {
	var resp TriggerNewTTLJobResponse
	_, err := cli.Command(ctx, ttlCmdTypeTriggerTTLJob, &TriggerNewTTLJobRequest{
		DBName:    dbName,
		TableName: tableName,
	}, &resp)

	if err != nil {
		return nil, err
	}
	return &resp, nil
}

// etcdClient is the client of etcd which implements the commandCli and notificationCli interface
type etcdClient struct {
	etcdCli *clientv3.Client
}

// NewCommandClient creates a command client with etcd
func NewCommandClient(etcdCli *clientv3.Client) CommandClient {
	return &etcdClient{
		etcdCli: etcdCli,
	}
}

func (c *etcdClient) sendCmd(ctx context.Context, cmdType string, obj any) (string, error) {
	reqID := uuid.New().String()
	data, err := json.Marshal(obj)
	if err != nil {
		return reqID, err
	}

	requestJSON, err := json.Marshal(&CmdRequest{
		RequestID: reqID,
		CmdType:   cmdType,
		Data:      data,
	})
	if err != nil {
		return reqID, err
	}

	lease, err := c.etcdCli.Grant(ctx, ttlCmdKeyLeaseSeconds)
	if err != nil {
		return reqID, err
	}

	_, err = c.etcdCli.Put(ctx, ttlCmdKeyRequestPrefix+reqID, string(requestJSON), clientv3.WithLease(lease.ID))
	if err != nil {
		return reqID, err
	}

	return reqID, nil
}

func (c *etcdClient) waitCmdResponse(ctx context.Context, reqID string, obj any) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(ttlCmdKeyLeaseSeconds))
	defer cancel()

	key := ttlCmdKeyResponsePrefix + reqID
	ch := c.etcdCli.Watch(ctx, key)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var respData []byte
loop:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			response, err := c.etcdCli.Get(ctx, key)
			if err != nil {
				return err
			}

			if len(response.Kvs) > 0 {
				respData = response.Kvs[0].Value
				break loop
			}
		case resp, ok := <-ch:
			if !ok {
				logutil.BgLogger().Info("watcher is closed")
				// to make ch always block
				ch = make(chan clientv3.WatchResponse)
				time.Sleep(time.Second)
				continue loop
			}

			for _, event := range resp.Events {
				if event.Type == clientv3.EventTypePut {
					respData = event.Kv.Value
					break loop
				}
			}
		}
	}

	var cmdResp cmdResponse
	if err := json.Unmarshal(respData, &cmdResp); err != nil {
		return err
	}

	if cmdResp.ErrorMessage != "" {
		return errors.New(cmdResp.ErrorMessage)
	}

	return json.Unmarshal(cmdResp.Data, obj)
}

// Command implements the CommandClient
func (c *etcdClient) Command(ctx context.Context, cmdType string, request any, response any) (
	string, error) {
	requestID, err := c.sendCmd(ctx, cmdType, request)
	if err != nil {
		return requestID, err
	}
	return requestID, c.waitCmdResponse(ctx, requestID, &response)
}

// TakeCommand implements the CommandClient
func (c *etcdClient) TakeCommand(ctx context.Context, reqID string) (bool, error) {
	resp, err := c.etcdCli.Delete(ctx, ttlCmdKeyRequestPrefix+reqID)
	if err != nil {
		return false, err
	}
	return resp.Deleted > 0, nil
}

// ResponseCommand implements the CommandClient
func (c *etcdClient) ResponseCommand(ctx context.Context, reqID string, obj any) error {
	resp := &cmdResponse{
		RequestID: reqID,
	}

	if err, ok := obj.(error); ok {
		resp.ErrorMessage = err.Error()
	} else {
		data, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		resp.Data = data
	}

	respJSON, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	lease, err := c.etcdCli.Grant(ctx, ttlCmdKeyLeaseSeconds)
	if err != nil {
		return err
	}

	_, err = c.etcdCli.Put(ctx, ttlCmdKeyResponsePrefix+reqID, string(respJSON), clientv3.WithLease(lease.ID))
	return err
}

// WatchCommand implements the CommandClient
func (c *etcdClient) WatchCommand(ctx context.Context) <-chan *CmdRequest {
	ch := make(chan *CmdRequest)
	go func() {
		ctx, cancel := context.WithCancel(ctx)
		defer func() {
			cancel()
			close(ch)
		}()

		etcdCh := c.etcdCli.Watch(ctx, ttlCmdKeyRequestPrefix, clientv3.WithPrefix())
		for resp := range etcdCh {
			for _, event := range resp.Events {
				if event.Type != clientv3.EventTypePut {
					continue
				}

				var request CmdRequest
				if err := json.Unmarshal(event.Kv.Value, &request); err != nil {
					logutil.BgLogger().Error(
						"failed to parse ttl cmd payload",
						zap.Error(err),
						zap.ByteString("key", event.Kv.Key),
						zap.ByteString("value", event.Kv.Value),
					)
				}

				select {
				case ch <- &request:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch
}

// mockClient is a mock implementation for CommandCli and NotificationCli
type mockClient struct {
	sync.Mutex
	store                map[string]any
	commandWatchers      []chan *CmdRequest
	notificationWatchers map[string][]chan clientv3.WatchResponse
}

// NewMockCommandClient creates a mock command client
func NewMockCommandClient() CommandClient {
	return &mockClient{
		store:                make(map[string]any),
		commandWatchers:      make([]chan *CmdRequest, 0, 1),
		notificationWatchers: make(map[string][]chan clientv3.WatchResponse),
	}
}

// Command implements the CommandClient
func (c *mockClient) Command(ctx context.Context, cmdType string, request any, response any) (
	string, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(ttlCmdKeyLeaseSeconds))
	defer cancel()

	reqID, err := c.sendCmd(ctx, cmdType, request)
	if err != nil {
		return reqID, err
	}

	responseKey := ttlCmdKeyResponsePrefix + reqID
	for ctx.Err() == nil {
		time.Sleep(time.Second)
		c.Lock()
		val, ok := c.store[responseKey]
		c.Unlock()

		if !ok {
			continue
		}

		res, ok := val.(*cmdResponse)
		if !ok {
			return reqID, errors.New("response cannot be casted to *cmdResponse")
		}

		if res.ErrorMessage != "" {
			return reqID, errors.New(res.ErrorMessage)
		}

		if err = json.Unmarshal(res.Data, response); err != nil {
			return reqID, err
		}
		return reqID, nil
	}
	return reqID, ctx.Err()
}

func (c *mockClient) sendCmd(ctx context.Context, cmdType string, request any) (string, error) {
	reqID := uuid.New().String()
	data, err := json.Marshal(request)
	if err != nil {
		return reqID, err
	}

	req := &CmdRequest{
		RequestID: reqID,
		CmdType:   cmdType,
		Data:      data,
	}

	c.Lock()
	defer c.Unlock()
	key := ttlCmdKeyRequestPrefix + reqID
	c.store[key] = req
	for _, ch := range c.commandWatchers {
		select {
		case <-ctx.Done():
			return reqID, ctx.Err()
		case ch <- req:
		default:
			return reqID, errors.New("watcher channel is blocked")
		}
	}
	return reqID, nil
}

// TakeCommand implements the CommandClient
func (c *mockClient) TakeCommand(_ context.Context, reqID string) (bool, error) {
	c.Lock()
	defer c.Unlock()
	key := ttlCmdKeyRequestPrefix + reqID
	if _, ok := c.store[key]; ok {
		delete(c.store, key)
		return true, nil
	}
	return false, nil
}

// ResponseCommand implements the CommandClient
func (c *mockClient) ResponseCommand(_ context.Context, reqID string, obj any) error {
	c.Lock()
	defer c.Unlock()

	resp := &cmdResponse{
		RequestID: reqID,
	}

	if respErr, ok := obj.(error); ok {
		resp.ErrorMessage = respErr.Error()
	} else {
		jsonData, err := json.Marshal(obj)
		if err != nil {
			return err
		}
		resp.Data = jsonData
	}

	c.store[ttlCmdKeyResponsePrefix+reqID] = resp
	return nil
}

// WatchCommand implements the CommandClient
func (c *mockClient) WatchCommand(ctx context.Context) <-chan *CmdRequest {
	c.Lock()
	defer c.Unlock()
	ch := make(chan *CmdRequest, 16+len(c.store))
	c.commandWatchers = append(c.commandWatchers, ch)
	for key, val := range c.store {
		if strings.HasPrefix(key, ttlCmdKeyRequestPrefix) {
			if req, ok := val.(*CmdRequest); ok {
				ch <- req
			}
		}
	}
	go func() {
		<-ctx.Done()
		c.Lock()
		defer c.Unlock()
		for i, chItem := range c.commandWatchers {
			if chItem == ch {
				c.commandWatchers = append(c.commandWatchers[:i], c.commandWatchers[i+1:]...)
				break
			}
		}
		close(ch)
	}()
	return ch
}
