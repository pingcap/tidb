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
	"github.com/coreos/etcd/clientv3/concurrency"
	"github.com/juju/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/owner"
	"github.com/pingcap/tidb/util/hack"
	"github.com/pingcap/tidb/util/printer"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

const (
	// ServerInformationPath store server information such as IP, port and so on.
	ServerInformationPath = "/tidb/server/info"
	// keyOpDefaultRetryCnt is the default retry count for etcd store.
	keyOpDefaultRetryCnt = 2
	// keyOpDefaultTimeout is the default time out for etcd store.
	keyOpDefaultTimeout = 1 * time.Second
)

// InfoSessionTTL is the etcd session's TTL in seconds. It's exported for testing.
var InfoSessionTTL = 1 * 60

// InfoSyncer stores server info to etcd when the tidb-server starts and delete when tidb-server shuts down.
type InfoSyncer struct {
	etcdCli        *clientv3.Client
	info           *ServerInfo
	serverInfoPath string
	session        *concurrency.Session
}

// ServerInfo is server static information.
// It will not be updated when tidb-server running. So please only put static information in ServerInfo struct.
type ServerInfo struct {
	ServerVersionInfo
	ID         string `json:"ddl_id"`
	IP         string `json:"ip"`
	Port       uint   `json:"listening_port"`
	StatusPort uint   `json:"status_port"`
	Lease      string `json:"lease"`
}

// ServerVersionInfo is the server version and git_hash.
type ServerVersionInfo struct {
	Version string `json:"version"`
	GitHash string `json:"git_hash"`
}

// NewInfoSyncer return new InfoSyncer. It is exported for testing.
func NewInfoSyncer(id string, etcdCli *clientv3.Client) *InfoSyncer {
	return &InfoSyncer{
		etcdCli:        etcdCli,
		info:           getServerInfo(id),
		serverInfoPath: fmt.Sprintf("%s/%s", ServerInformationPath, id),
	}
}

// Init creates a new etcd session and stores server info to etcd.
func (is *InfoSyncer) Init(ctx context.Context) error {
	return errors.Trace(is.newSessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt))
}

// GetServerInfo gets self server static information.
func (is *InfoSyncer) GetServerInfo() *ServerInfo {
	return is.info
}

// GetServerInfoByID gets server static information from etcd.
func (is *InfoSyncer) GetServerInfoByID(ctx context.Context, id string) (*ServerInfo, error) {
	if is.etcdCli == nil || id == is.info.ID {
		return is.info, nil
	}
	key := fmt.Sprintf("%s/%s", ServerInformationPath, id)
	infoMap, err := getInfo(ctx, is.etcdCli, key, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		return nil, errors.Trace(err)
	}
	info, ok := infoMap[id]
	if !ok {
		return nil, errors.Errorf("[info-syncer] get %s failed", key)
	}
	return info, nil
}

// GetAllServerInfo gets all servers static information from etcd.
func (is *InfoSyncer) GetAllServerInfo(ctx context.Context) (map[string]*ServerInfo, error) {
	allInfo := make(map[string]*ServerInfo)
	if is.etcdCli == nil {
		allInfo[is.info.ID] = is.info
		return allInfo, nil
	}
	allInfo, err := getInfo(ctx, is.etcdCli, ServerInformationPath, keyOpDefaultRetryCnt, keyOpDefaultTimeout, clientv3.WithPrefix())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return allInfo, nil
}

// storeServerInfo stores self server static information to etcd.
func (is *InfoSyncer) storeServerInfo(ctx context.Context) error {
	if is.etcdCli == nil {
		return nil
	}
	infoBuf, err := json.Marshal(is.info)
	if err != nil {
		return errors.Trace(err)
	}
	err = ddl.PutKVToEtcd(ctx, is.etcdCli, keyOpDefaultRetryCnt, is.serverInfoPath, hack.String(infoBuf), clientv3.WithLease(is.session.Lease()))
	return errors.Trace(err)
}

// RemoveServerInfo remove self server static information from etcd.
func (is *InfoSyncer) RemoveServerInfo() {
	if is.etcdCli == nil {
		return
	}
	err := ddl.DeleteKeyFromEtcd(is.serverInfoPath, is.etcdCli, keyOpDefaultRetryCnt, keyOpDefaultTimeout)
	if err != nil {
		log.Errorf("[info-syncer] remove server info failed %v", err)
	}
}

// Done returns a channel that closes when the info syncer is no longer being refreshed.
func (is InfoSyncer) Done() <-chan struct{} {
	if is.etcdCli == nil {
		return make(chan struct{}, 1)
	}
	return is.session.Done()
}

// Restart restart the info syncer with new session leaseID and store server info to etcd again.
func (is *InfoSyncer) Restart(ctx context.Context) error {
	return errors.Trace(is.newSessionAndStoreServerInfo(ctx, owner.NewSessionDefaultRetryCnt))
}

// newSessionAndStoreServerInfo creates a new etcd session and stores server info to etcd.
func (is *InfoSyncer) newSessionAndStoreServerInfo(ctx context.Context, retryCnt int) error {
	if is.etcdCli == nil {
		return nil
	}
	logPrefix := fmt.Sprintf("[Info-syncer] %s", is.serverInfoPath)
	session, err := owner.NewSession(ctx, logPrefix, is.etcdCli, retryCnt, InfoSessionTTL)
	if err != nil {
		return errors.Trace(err)
	}
	is.session = session

	err = is.storeServerInfo(ctx)
	return errors.Trace(err)
}

// getInfo gets server information from etcd according to the key and opts.
func getInfo(ctx context.Context, etcdCli *clientv3.Client, key string, retryCnt int, timeout time.Duration, opts ...clientv3.OpOption) (map[string]*ServerInfo, error) {
	var err error
	var resp *clientv3.GetResponse
	allInfo := make(map[string]*ServerInfo)
	for i := 0; i < retryCnt; i++ {
		select {
		case <-ctx.Done():
			err = errors.Trace(ctx.Err())
			return nil, err
		default:
		}
		childCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err = etcdCli.Get(childCtx, key, opts...)
		cancel()
		if err != nil {
			log.Infof("[info-syncer] get %s failed %v, continue checking.", key, err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		for _, kv := range resp.Kvs {
			info := &ServerInfo{}
			err = json.Unmarshal(kv.Value, info)
			if err != nil {
				log.Infof("[info-syncer] get %s, json.Unmarshal %v failed %v.", kv.Key, kv.Value, err)
				return nil, errors.Trace(err)
			}
			allInfo[info.ID] = info
		}
		return allInfo, nil
	}
	return nil, errors.Trace(err)
}

// getServerInfo gets self tidb server information.
func getServerInfo(id string) *ServerInfo {
	cfg := config.GetGlobalConfig()
	info := &ServerInfo{
		ID:         id,
		IP:         cfg.AdvertiseAddress,
		Port:       cfg.Port,
		StatusPort: cfg.Status.StatusPort,
		Lease:      cfg.Lease,
	}
	info.Version = mysql.ServerVersion
	info.GitHash = printer.TiDBGitHash
	return info
}
