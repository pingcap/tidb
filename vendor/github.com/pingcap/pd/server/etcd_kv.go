// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"path"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

const (
	kvRangeLimit      = 10000
	kvRequestTimeout  = time.Second * 10
	kvSlowRequestTime = time.Second * 1
)

var (
	errTxnFailed = errors.New("failed to commit transaction")
)

type etcdKVBase struct {
	server   *Server
	client   *clientv3.Client
	rootPath string
}

func newEtcdKVBase(s *Server) *etcdKVBase {
	return &etcdKVBase{
		server:   s,
		client:   s.client,
		rootPath: s.rootPath,
	}
}

func (kv *etcdKVBase) Load(key string) (string, error) {
	key = path.Join(kv.rootPath, key)

	resp, err := kvGet(kv.server.client, key)
	if err != nil {
		return "", errors.Trace(err)
	}
	if n := len(resp.Kvs); n == 0 {
		return "", nil
	} else if n > 1 {
		return "", errors.Errorf("load more than one kvs: key %v kvs %v", key, n)
	}
	return string(resp.Kvs[0].Value), nil
}

func (kv *etcdKVBase) LoadRange(key, endKey string, limit int) ([]string, error) {
	key = path.Join(kv.rootPath, key)
	endKey = path.Join(kv.rootPath, endKey)

	withRange := clientv3.WithRange(endKey)
	withLimit := clientv3.WithLimit(int64(limit))
	resp, err := kvGet(kv.server.client, key, withRange, withLimit)
	if err != nil {
		return nil, errors.Trace(err)
	}
	res := make([]string, 0, len(resp.Kvs))
	for _, item := range resp.Kvs {
		res = append(res, string(item.Value))
	}
	return res, nil
}

func (kv *etcdKVBase) Save(key, value string) error {
	key = path.Join(kv.rootPath, key)

	resp, err := kv.server.leaderTxn().Then(clientv3.OpPut(key, value)).Commit()
	if err != nil {
		log.Errorf("save to etcd error: %v", err)
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.Trace(errTxnFailed)
	}
	return nil
}

func kvGet(c *clientv3.Client, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := context.WithTimeout(c.Ctx(), kvRequestTimeout)
	defer cancel()

	start := time.Now()
	resp, err := clientv3.NewKV(c).Get(ctx, key, opts...)
	if err != nil {
		log.Errorf("load from etcd error: %v", err)
	}
	if cost := time.Since(start); cost > kvSlowRequestTime {
		log.Warnf("kv gets too slow: key %v cost %v err %v", key, cost, err)
	}

	return resp, errors.Trace(err)
}
