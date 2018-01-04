// Copyright 2016 PingCAP, Inc.
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

package etcdutil

import (
	"context"
	"crypto/tls"
	"net/http"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver"
	"github.com/coreos/etcd/pkg/types"
	"github.com/juju/errors"
	log "github.com/sirupsen/logrus"
)

const (
	// DefaultDialTimeout is the maximum amount of time a dial will wait for a
	// connection to setup. 30s is long enough for most of the network conditions.
	DefaultDialTimeout = 30 * time.Second

	// DefaultRequestTimeout 10s is long enough for most of etcd clusters.
	DefaultRequestTimeout = 10 * time.Second

	// DefaultSlowRequestTime 1s for the threshold for normal request, for those
	// longer then 1s, they are considered as slow requests.
	DefaultSlowRequestTime = 1 * time.Second

	maxCheckEtcdRunningCount = 60
	checkEtcdRunningDelay    = 1 * time.Second
)

// CheckClusterID checks Etcd's cluster ID, returns an error if mismatch.
// This function will never block even quorum is not satisfied.
func CheckClusterID(localClusterID types.ID, um types.URLsMap, tlsConfig *tls.Config) error {
	if len(um) == 0 {
		return nil
	}

	var peerURLs []string
	for _, urls := range um {
		peerURLs = append(peerURLs, urls.StringSlice()...)
	}

	for _, u := range peerURLs {
		trp := &http.Transport{
			TLSClientConfig: tlsConfig,
		}
		remoteCluster, gerr := etcdserver.GetClusterFromRemotePeers([]string{u}, trp)
		trp.CloseIdleConnections()
		if gerr != nil {
			// Do not return error, because other members may be not ready.
			log.Error(gerr)
			continue
		}

		remoteClusterID := remoteCluster.ID()
		if remoteClusterID != localClusterID {
			return errors.Errorf("Etcd cluster ID mismatch, expect %d, got %d", localClusterID, remoteClusterID)
		}
	}
	return nil
}

// AddEtcdMember adds an etcd member.
func AddEtcdMember(client *clientv3.Client, urls []string) (*clientv3.MemberAddResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	addResp, err := client.MemberAdd(ctx, urls)
	cancel()
	return addResp, errors.Trace(err)
}

// ListEtcdMembers returns a list of internal etcd members.
func ListEtcdMembers(client *clientv3.Client) (*clientv3.MemberListResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	listResp, err := client.MemberList(ctx)
	cancel()
	return listResp, errors.Trace(err)
}

// RemoveEtcdMember removes a member by the given id.
func RemoveEtcdMember(client *clientv3.Client, id uint64) (*clientv3.MemberRemoveResponse, error) {
	ctx, cancel := context.WithTimeout(client.Ctx(), DefaultRequestTimeout)
	rmResp, err := client.MemberRemove(ctx, id)
	cancel()
	return rmResp, errors.Trace(err)
}
