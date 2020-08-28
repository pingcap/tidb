// Copyright 2020 PingCAP, Inc.
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
	"fmt"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/util/logutil"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
	"strings"
	"time"
)

type DeadLockChecker struct {
	etcdCli *clientv3.Client
}

func NewDeadLockChecker(etcdCli *clientv3.Client) DeadLockChecker {
	return DeadLockChecker{
		etcdCli: etcdCli,
	}
}

func (d *DeadLockChecker) getAliveServers() (map[string]struct{}, error) {
	var err error
	var resp *clientv3.GetResponse
	allInfo := make(map[string]struct{})
	retryCnt := 5
	retryInterval := time.Millisecond * 200
	timeout := time.Second
	for i := 0; i < retryCnt; i++ {
		childCtx, cancel := context.WithTimeout(context.Background(), timeout)
		resp, err = d.etcdCli.Get(childCtx, DDLAllSchemaVersions, clientv3.WithPrefix())
		cancel()
		if err != nil {
			logutil.BgLogger().Info("[ddl] lock cleaner get alive servers failed.", zap.Error(err))
			time.Sleep(retryInterval)
			continue
		}
		for _, kv := range resp.Kvs {
			serverID := strings.TrimPrefix(string(kv.Key), DDLAllSchemaVersions)
			allInfo[serverID] = struct{}{}
		}
		fmt.Printf("%v---\n", allInfo)
		return allInfo, nil
	}
	return nil, errors.Trace(err)
}

func (d *DeadLockChecker) GetDeadLockTables(schemas []*model.DBInfo) (map[model.SessionInfo][]model.TableLockTpInfo, error) {
	if d.etcdCli == nil {
		return nil, nil
	}
	aliveServers, err := d.getAliveServers()
	if err != nil {
		return nil, err
	}
	deadLockTables := make(map[model.SessionInfo][]model.TableLockTpInfo)
	for _, schema := range schemas {
		for _, tbl := range schema.Tables {
			if tbl.Lock == nil {
				continue
			}
			for _, se := range tbl.Lock.Sessions {
				if _, ok := aliveServers[se.ServerID]; !ok {
					deadLockTables[se] = append(deadLockTables[se], model.TableLockTpInfo{
						SchemaID: schema.ID,
						TableID:  tbl.ID,
						Tp:       tbl.Lock.Tp,
					})
				}
			}
		}
	}
	return deadLockTables, nil
}
