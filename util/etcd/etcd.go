// Copyright 2018 PingCAP, Inc.
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

package etcd

import (
	"context"
	"crypto/tls"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
)

// Node organizes the ectd query result as a Trie tree
type Node struct {
	Childs map[string]*Node
	Value  []byte
}

// OpType is operation's type in etcd
type OpType string

var (
	// CreateOp is create operation type
	CreateOp OpType = "create"

	// UpdateOp is update operation type
	UpdateOp OpType = "update"

	// DeleteOp is delete operation type
	DeleteOp OpType = "delete"
)

// Operation represents an operation in etcd, include create, update and delete.
type Operation struct {
	Tp         OpType
	Key        string
	Value      string
	Opts       []clientv3.OpOption
	TTL        int64
	WithPrefix bool
}

// String implements Stringer interface.
func (o *Operation) String() string {
	return fmt.Sprintf("{Tp: %s, Key: %s, Value: %s, TTL: %d, WithPrefix: %v, Opts: %v}", o.Tp, o.Key, o.Value, o.TTL, o.WithPrefix, o.Opts)
}

// Client is a wrapped etcd client that support some simple method
type Client struct {
	client   *clientv3.Client
	rootPath string
}

// NewClient returns a wrapped etcd client
func NewClient(cli *clientv3.Client, root string) *Client {
	return &Client{
		client:   cli,
		rootPath: root,
	}
}

// NewClientFromCfg returns a wrapped etcd client
func NewClientFromCfg(endpoints []string, dialTimeout time.Duration, root string, security *tls.Config) (*Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
		TLS:         security,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Client{
		client:   cli,
		rootPath: root,
	}, nil
}

// Close shutdowns the connection to etcd
func (e *Client) Close() error {
	if err := e.client.Close(); err != nil {
		return errors.Trace(err)
	}
	return nil
}

// GetClient returns client
func (e *Client) GetClient() *clientv3.Client {
	return e.client
}

// Create guarantees to set a key = value with some options(like ttl)
func (e *Client) Create(ctx context.Context, key string, val string, opts []clientv3.OpOption) (int64, error) {
	key = keyWithPrefix(e.rootPath, key)
	txnResp, err := e.client.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(key), "=", 0),
	).Then(
		clientv3.OpPut(key, val, opts...),
	).Commit()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if !txnResp.Succeeded {
		return 0, errors.AlreadyExistsf("key %s in etcd", key)
	}

	if txnResp.Header != nil {
		return txnResp.Header.Revision, nil
	}

	// impossible to happen
	return 0, errors.New("revision is unknown")
}

// Get returns a key/value matchs the given key
func (e *Client) Get(ctx context.Context, key string) (value []byte, revision int64, err error) {
	key = keyWithPrefix(e.rootPath, key)
	resp, err := e.client.KV.Get(ctx, key)
	if err != nil {
		return nil, -1, errors.Trace(err)
	}

	if len(resp.Kvs) == 0 {
		return nil, -1, errors.NotFoundf("key %s in etcd", key)
	}

	return resp.Kvs[0].Value, resp.Header.Revision, nil
}

// Update updates a key/value.
// set ttl 0 to disable the Lease ttl feature
func (e *Client) Update(ctx context.Context, key string, val string, ttl int64) error {
	key = keyWithPrefix(e.rootPath, key)

	var opts []clientv3.OpOption
	if ttl > 0 {
		lcr, err := e.client.Lease.Grant(ctx, ttl)
		if err != nil {
			return errors.Trace(err)
		}

		opts = []clientv3.OpOption{clientv3.WithLease(lcr.ID)}
	}

	txnResp, err := e.client.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.ModRevision(key), ">", 0),
	).Then(
		clientv3.OpPut(key, val, opts...),
	).Commit()
	if err != nil {
		return errors.Trace(err)
	}

	if !txnResp.Succeeded {
		return errors.NotFoundf("key %s in etcd", key)
	}

	return nil
}

// UpdateOrCreate updates a key/value, if the key does not exist then create, or update
func (e *Client) UpdateOrCreate(ctx context.Context, key string, val string, ttl int64) error {
	key = keyWithPrefix(e.rootPath, key)

	var opts []clientv3.OpOption
	if ttl > 0 {
		lcr, err := e.client.Lease.Grant(ctx, ttl)
		if err != nil {
			return errors.Trace(err)
		}

		opts = []clientv3.OpOption{clientv3.WithLease(lcr.ID)}
	}

	_, err := e.client.KV.Do(ctx, clientv3.OpPut(key, val, opts...))
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

// List returns the trie struct that constructed by the key/value with same prefix
func (e *Client) List(ctx context.Context, key string) (node *Node, revision int64, err error) {
	key = keyWithPrefix(e.rootPath, key)
	if !strings.HasSuffix(key, "/") {
		key += "/"
	}

	resp, err := e.client.KV.Get(ctx, key, clientv3.WithPrefix())
	if err != nil {
		return nil, -1, errors.Trace(err)
	}

	root := new(Node)
	length := len(key)
	for _, kv := range resp.Kvs {
		key := string(kv.Key)
		if len(key) <= length {
			continue
		}

		keyTail := key[length:]
		tailNode := parseToDirTree(root, keyTail)
		tailNode.Value = kv.Value
	}

	return root, resp.Header.Revision, nil
}

// Delete deletes the key/values with matching prefix or key
func (e *Client) Delete(ctx context.Context, key string, withPrefix bool) error {
	key = keyWithPrefix(e.rootPath, key)
	var opts []clientv3.OpOption
	if withPrefix {
		opts = []clientv3.OpOption{clientv3.WithPrefix()}
	}

	_, err := e.client.KV.Delete(ctx, key, opts...)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// Watch watchs the events of key with prefix.
func (e *Client) Watch(ctx context.Context, prefix string, revision int64) clientv3.WatchChan {
	if revision > 0 {
		return e.client.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
	}
	return e.client.Watch(ctx, prefix, clientv3.WithPrefix())
}

// DoTxn does some operation in one transaction.
// Note: should only have one opereration for one key, otherwise will get duplicate key error.
func (e *Client) DoTxn(ctx context.Context, operations []*Operation) (int64, error) {
	cmps := make([]clientv3.Cmp, 0, len(operations))
	ops := make([]clientv3.Op, 0, len(operations))

	for _, operation := range operations {
		operation.Key = keyWithPrefix(e.rootPath, operation.Key)

		if operation.TTL > 0 {
			if operation.Tp == DeleteOp {
				return 0, errors.Errorf("unexpected TTL in delete operation")
			}

			lcr, err := e.client.Lease.Grant(ctx, operation.TTL)
			if err != nil {
				return 0, errors.Trace(err)
			}
			operation.Opts = append(operation.Opts, clientv3.WithLease(lcr.ID))
		}

		if operation.WithPrefix {
			operation.Opts = append(operation.Opts, clientv3.WithPrefix())
		}

		switch operation.Tp {
		case CreateOp:
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(operation.Key), "=", 0))
			ops = append(ops, clientv3.OpPut(operation.Key, operation.Value, operation.Opts...))
		case UpdateOp:
			cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(operation.Key), ">", 0))
			ops = append(ops, clientv3.OpPut(operation.Key, operation.Value, operation.Opts...))
		case DeleteOp:
			ops = append(ops, clientv3.OpDelete(operation.Key, operation.Opts...))
		default:
			return 0, errors.Errorf("unknown operation type %s", operation.Tp)
		}
	}

	txnResp, err := e.client.KV.Txn(ctx).If(
		cmps...,
	).Then(
		ops...,
	).Commit()
	if err != nil {
		return 0, errors.Trace(err)
	}

	if !txnResp.Succeeded {
		return 0, errors.Errorf("do transaction failed, operations: %+v", operations)
	}

	return txnResp.Header.Revision, nil
}

func parseToDirTree(root *Node, p string) *Node {
	pathDirs := strings.Split(p, "/")
	current := root
	var next *Node
	var ok bool

	for _, dir := range pathDirs {
		if current.Childs == nil {
			current.Childs = make(map[string]*Node)
		}

		next, ok = current.Childs[dir]
		if !ok {
			current.Childs[dir] = new(Node)
			next = current.Childs[dir]
		}

		current = next
	}

	return current
}

func keyWithPrefix(prefix, key string) string {
	if strings.HasPrefix(key, prefix) {
		return key
	}

	return path.Join(prefix, key)
}

// SetEtcdCliByNamespace is used to add a etcd namespace prefix befor etcd path.
func SetEtcdCliByNamespace(cli *clientv3.Client, namespacePrefix string) {
	cli.KV = namespace.NewKV(cli.KV, namespacePrefix)
	cli.Watcher = namespace.NewWatcher(cli.Watcher, namespacePrefix)
	cli.Lease = namespace.NewLease(cli.Lease, namespacePrefix)

}
