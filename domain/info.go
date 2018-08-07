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
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/printer"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	// ServerInformation store server information such as IP, port and so on.
	ServerInformation = "/tidb/server/info"
)

// infoSyncer stores server info to PD when server start and delete when server down.
type infoSyncer struct {
	etcdCli *clientv3.Client
	info    *ServerInfo
}

// ServerInfo is server static information.
// It will not update when server running. So please only put static information in ServerInfo struct.
type ServerInfo struct {
	ServerVersionInfo
	ID         string `json:"ddl_id"`
	IP         string `json:"ip"`
	StatusPort uint   `json:"status_port"`
	Lease      string `json:"lease"`
}

// ServerVersionInfo is the server version and git_hash.
type ServerVersionInfo struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}

// NewInfoSyncer return new infoSyncer. It export for tidb-test test.
func NewInfoSyncer(id string, etcdCli *clientv3.Client) *infoSyncer {
	return &infoSyncer{
		etcdCli: etcdCli,
		info:    getServerInfo(id),
	}
}

func getServerInfo(id string) *ServerInfo {
	cfg := config.GetGlobalConfig()
	info := &ServerInfo{
		ID:         id,
		IP:         cfg.AdvertiseAddress,
		StatusPort: cfg.Status.StatusPort,
		Lease:      cfg.Lease,
	}
	info.Version = mysql.ServerVersion
	info.GitHash = printer.TiDBGitHash
	return info
}

//GetServerInfo gets self server static information.
func (is *infoSyncer) GetServerInfo() *ServerInfo {
	return is.info
}

// GetOwnerServerInfoFromPD gets owner server static information from PD.
func (is *infoSyncer) GetOwnerServerInfoFromPD(ownerID string) (*ServerInfo, error) {
	if is.etcdCli == nil || ownerID == is.info.ID {
		return is.info, nil
	}
	ctx := context.Background()
	key := fmt.Sprintf("%s/%s", ServerInformation, ownerID)
	infoMap, err := getInfo(ctx, is.etcdCli, key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	info, ok := infoMap[ownerID]
	if !ok {
		return nil, errors.New(fmt.Sprintf("[infoSyncer] get %s failed", key))
	}
	return info, nil
}

// GetAllServerInfoFromPD gets all servers static information from PD.
func (is *infoSyncer) GetAllServerInfoFromPD() (map[string]*ServerInfo, error) {
	allInfo := make(map[string]*ServerInfo)
	if is.etcdCli == nil {
		allInfo[is.info.ID] = is.info
		return allInfo, nil
	}
	ctx := context.Background()
	allInfo, err := getInfo(ctx, is.etcdCli, ServerInformation, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return allInfo, nil
}

// StoreServerInfoToPD stores self server static information to PD when domain Init.
func (is *infoSyncer) StoreServerInfoToPD() error {
	if is.etcdCli == nil {
		return nil
	}
	infoBuf, err := json.Marshal(is.info)
	if err != nil {
		return errors.Trace(err)
	}
	ctx := context.Background()
	key := fmt.Sprintf("%s/%s", ServerInformation, is.info.ID)
	err = ddl.PutKV(ctx, is.etcdCli, ddl.KeyOpDefaultRetryCnt, key, hack.String(infoBuf))
	return errors.Trace(err)
}

// RemoveServerInfoFromPD remove self server static information from PD when domain close.
func (is *infoSyncer) RemoveServerInfoFromPD() {
	if is.etcdCli == nil {
		return
	}
	key := fmt.Sprintf("%s/%s", ServerInformation, is.info.ID)
	err := ddl.DeleteKey(key, is.etcdCli)
	if err != nil {
		log.Errorf("[infoSyncer] remove self server info failed %v", err)
	}
}

func getInfo(ctx context.Context, etcdCli *clientv3.Client, key string, opts ...clientv3.OpOption) (map[string]*ServerInfo, error) {
	var err error
	allInfo := make(map[string]*ServerInfo)
	for {
		select {
		case <-ctx.Done():
			err = errors.Trace(ctx.Err())
			return nil, err
		default:
		}

		resp, err := etcdCli.Get(ctx, key, opts...)
		if err != nil {
			log.Infof("[infoSyncer] get %s failed %v, continue checking.", key, err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, kv := range resp.Kvs {
			info := &ServerInfo{}
			err := json.Unmarshal(kv.Value, info)
			if err != nil {
				log.Infof("[infoSyncer] get %s, json.Unmarshal %v failed %v.", kv.Key, kv.Value, err)
				return nil, errors.Trace(err)
			}
			allInfo[info.ID] = info
		}
		return allInfo, nil
	}
}
