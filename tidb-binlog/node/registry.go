// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package node

import (
	"context"
	"encoding/json"
	"path"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// EtcdRegistry wraps the reactions with etcd
type EtcdRegistry struct {
	client     *etcd.Client
	reqTimeout time.Duration
}

// NewEtcdRegistry returns an EtcdRegistry client
func NewEtcdRegistry(cli *etcd.Client, reqTimeout time.Duration) *EtcdRegistry {
	return &EtcdRegistry{
		client:     cli,
		reqTimeout: reqTimeout,
	}
}

// Close closes the etcd client
func (r *EtcdRegistry) Close() error {
	err := r.client.Close()
	return errors.Trace(err)
}

func (r *EtcdRegistry) prefixed(p ...string) string {
	return path.Join(p...)
}

// Node returns the nodeStatus that matchs nodeID in the etcd
func (r *EtcdRegistry) Node(pctx context.Context, prefix, nodeID string) (status *Status, revision int64, err error) {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	data, revision, err := r.client.Get(ctx, r.prefixed(prefix, nodeID))
	if err != nil {
		return nil, -1, errors.Trace(err)
	}

	if err = json.Unmarshal(data, &status); err != nil {
		return nil, -1, errors.Annotatef(err, "Invalid nodeID(%s)", nodeID)
	}
	return status, revision, nil
}

// Nodes retruns all the nodeStatuses in the etcd
func (r *EtcdRegistry) Nodes(pctx context.Context, prefix string) (status []*Status, revision int64, err error) {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	resp, revision, err := r.client.List(ctx, r.prefixed(prefix))
	if err != nil {
		return nil, -1, errors.Trace(err)
	}

	status, err = NodesStatusFromEtcdNode(resp)
	if err != nil {
		return nil, -1, errors.Trace(err)
	}

	return status, revision, nil
}

// UpdateNode update the node information.
func (r *EtcdRegistry) UpdateNode(pctx context.Context, prefix string, status *Status) error {
	ctx, cancel := context.WithTimeout(pctx, r.reqTimeout)
	defer cancel()

	if exists, err := r.checkNodeExists(ctx, prefix, status.NodeID); err != nil {
		return errors.Trace(err)
	} else if !exists {
		// not found then create a new node
		log.Info("node dosen't exist, will create one", zap.String("NodeID", status.NodeID))
		return r.createNode(ctx, prefix, status)
	} else {
		// found it, update status information of the node
		return r.updateNode(ctx, prefix, status)
	}
}

func (r *EtcdRegistry) checkNodeExists(ctx context.Context, prefix, nodeID string) (bool, error) {
	_, _, err := r.client.Get(ctx, r.prefixed(prefix, nodeID))
	if err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, errors.Trace(err)
	}
	return true, nil
}

func (r *EtcdRegistry) updateNode(ctx context.Context, prefix string, status *Status) error {
	objstr, err := json.Marshal(status)
	if err != nil {
		return errors.Annotatef(err, "error marshal NodeStatus(%v)", status)
	}
	key := r.prefixed(prefix, status.NodeID)
	err = r.client.Update(ctx, key, string(objstr), 0)
	return errors.Trace(err)
}

func (r *EtcdRegistry) createNode(ctx context.Context, prefix string, status *Status) error {
	objstr, err := json.Marshal(status)
	if err != nil {
		return errors.Annotatef(err, "error marshal NodeStatus(%v)", status)
	}
	key := r.prefixed(prefix, status.NodeID)
	_, err = r.client.Create(ctx, key, string(objstr), nil)
	return errors.Trace(err)
}

// WatchNode watchs node's event
func (r *EtcdRegistry) WatchNode(pctx context.Context, prefix string, revision int64) clientv3.WatchChan {
	return r.client.Watch(pctx, prefix, revision)
}

func nodeStatusFromEtcdNode(id string, node *etcd.Node) (*Status, error) {
	status := &Status{}

	if err := json.Unmarshal(node.Value, &status); err != nil {
		return nil, errors.Annotatef(err, "error unmarshal NodeStatus with nodeID(%s), node value(%s)", id, node.Value)
	}

	return status, nil
}

// NodesStatusFromEtcdNode returns nodes' status under root node.
func NodesStatusFromEtcdNode(root *etcd.Node) ([]*Status, error) {
	statuses := make([]*Status, len(root.Childs))
	idx := 0
	for id, n := range root.Childs {
		status, err := nodeStatusFromEtcdNode(id, n)
		if err != nil {
			return nil, err
		}
		if status == nil {
			continue
		}
		statuses[idx] = status
		idx = idx + 1
	}
	return statuses, nil
}

// AnalyzeNodeID returns nodeID by analyze key path.
func AnalyzeNodeID(key string) string {
	// the key looks like: /tidb-binlog/v1/pumps/nodeID, or /tidb-binlog/pumps/nodeID for old binlog version.
	paths := strings.Split(key, "/")
	nodeIDOffset := 3

	if len(paths) >= 2 {
		// version string start with 'v'
		if !strings.HasPrefix(paths[1], "v") {
			nodeIDOffset = 2
		}
	} else {
		log.Error("can't get nodeID or node type", zap.String("key", key))
		return ""
	}

	if len(paths) < nodeIDOffset+1 {
		log.Error("can't get nodeID or node type", zap.String("key", key))
		return ""
	}

	return paths[nodeIDOffset]
}
