// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metaservice

import (
	"context"
	"errors"
	"fmt"

	"github.com/pingcap/kvproto/pkg/keyspacepb"
	"github.com/pingcap/tidb/pkg/keyspace"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/etcd"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/opt"
	"github.com/tikv/pd/client/pkg/caller"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const getAllMembersBackoffMs = 5000

// PDClientFactory constructs a PD client for one keyspace-aware etcd dial attempt.
type PDClientFactory func(
	ctx context.Context,
	apiCtx pd.APIContext,
	callerComponent caller.Component,
	svrAddrs []string,
	security pd.SecurityOption,
	opts ...opt.ClientOption,
) (pd.Client, error)

// NewEtcdMetaServiceClient creates a ServiceClient backed by etcd and PD clients.
// When etcdCli is nil but pdCli is not, it returns a PD-only client that still
// implements GetPDAddrs and GetPDHttpAddrs.
func NewEtcdMetaServiceClient(etcdCli *clientv3.Client, pdCli pd.Client) ServiceClient {
	return newClient(etcdCli, pdCli)
}

// EtcdDialInfo describes how a direct etcd client should connect for a keyspace.
type EtcdDialInfo struct {
	Endpoints []string
	Namespace string
}

// ResolveEtcdDialInfo resolves the etcd endpoints and namespace for a keyspace.
func ResolveEtcdDialInfo(ctx context.Context, pdCli pd.Client, keyspaceMeta *keyspacepb.KeyspaceMeta) (EtcdDialInfo, error) {
	_, endpoints, err := GetInfoAndGroupAddrs(ctx, pdCli, keyspaceMeta)
	if err != nil {
		return EtcdDialInfo{}, err
	}

	info := EtcdDialInfo{Endpoints: endpoints}
	if keyspaceMeta != nil {
		codec, err := tikv.NewCodecV2(tikv.ModeTxn, keyspaceMeta)
		if err != nil {
			return EtcdDialInfo{}, err
		}
		info.Namespace = keyspace.MakeKeyspaceEtcdNamespace(codec)
	}
	return info, nil
}

// NewEtcdClientFromPDClient resolves the meta service group from an existing PD
// client and returns a namespaced etcd client.
func NewEtcdClientFromPDClient(
	ctx context.Context,
	pdCli pd.Client,
	keyspaceMeta *keyspacepb.KeyspaceMeta,
	etcdCfg clientv3.Config,
) (*clientv3.Client, error) {
	dialInfo, err := ResolveEtcdDialInfo(ctx, pdCli, keyspaceMeta)
	if err != nil {
		return nil, err
	}

	etcdCfg.Context = ctx
	etcdCfg.Endpoints = dialInfo.Endpoints
	etcdCli, err := clientv3.New(etcdCfg)
	if err != nil {
		return nil, err
	}
	if dialInfo.Namespace != "" {
		etcd.SetEtcdCliByNamespace(etcdCli, dialInfo.Namespace)
	}
	return etcdCli, nil
}

// DialEtcdClient resolves the target meta service group and returns a namespaced etcd client.
func DialEtcdClient(
	ctx context.Context,
	keyspaceName string,
	pdAddrs []string,
	security pd.SecurityOption,
	pdClientFactory PDClientFactory,
	callerComponent caller.Component,
	pdClientOpts []opt.ClientOption,
	etcdCfg clientv3.Config,
) (*clientv3.Client, error) {
	if pdClientFactory == nil {
		pdClientFactory = pd.NewClientWithAPIContext
	}

	pdCli, err := pdClientFactory(
		ctx, keyspace.BuildAPIContext(keyspaceName), callerComponent, pdAddrs, security, pdClientOpts...,
	)
	if err != nil {
		return nil, err
	}
	defer pdCli.Close()

	var keyspaceMeta *keyspacepb.KeyspaceMeta
	if keyspaceName != "" {
		keyspaceMeta, err = pdCli.LoadKeyspace(ctx, keyspaceName)
		if err != nil {
			return nil, err
		}
		if keyspaceMeta == nil {
			return nil, errors.New("keyspace meta not found")
		}
	}

	return NewEtcdClientFromPDClient(ctx, pdCli, keyspaceMeta, etcdCfg)
}

// client is used to implement etcd meta service.
type client struct {
	pdCli           pd.Client
	keyspaceEtcdCli *clientv3.Client
}

// newClient is used to implement etcd meta service.
func newClient(etcdCli *clientv3.Client, pdCli pd.Client) ServiceClient {
	if etcdCli == nil && pdCli == nil {
		return nil
	}
	if etcdCli != nil {
		return &client{
			keyspaceEtcdCli: etcdCli,
			pdCli:           pdCli,
		}
	}
	return &client{
		pdCli: pdCli,
	}
}

// GetKeyspaceEtcdCli return etcd client.
func (n *client) GetKeyspaceEtcdCli() *clientv3.Client {
	return n.keyspaceEtcdCli
}

// GetPDAddrs implements ServiceClient interface.
func (n *client) GetPDAddrs(ctx context.Context) ([]string, error) {
	addrs, err := GetPDAddrs(ctx, n.pdCli, false)
	if err != nil {
		return nil, err
	}
	return addrs, err
}

// GetPDAddrs returns dialable PD endpoints from PD members.
// For http/https members, it returns host:port unless withSchema is true.
// For unix members, it keeps the unix:// scheme because embedded etcd tests
// publish unix:// endpoints and stripping the scheme would make them undialable.
func GetPDAddrs(ctx context.Context, pdClient pd.Client, withSchema bool) ([]string, error) {
	if pdClient == nil {
		return nil, errors.New("PD client not found")
	}
	pdAddrs := make([]string, 0)
	bo := tikv.NewBackoffer(ctx, getAllMembersBackoffMs)

	for {
		members, err := pdClient.GetAllMembers(ctx)
		if err != nil {
			err := bo.Backoff(tikv.BoRegionMiss(), err)
			if err != nil {
				return nil, err
			}
			continue
		}
		for _, member := range members.GetMembers() {
			if len(member.ClientUrls) > 0 {
				endpoint, err := util.ParseServiceURL(member.ClientUrls[0])
				if err != nil {
					return nil, fmt.Errorf("parse client url from pd members %q: %w", member.ClientUrls[0], err)
				}
				pdAddrs = append(pdAddrs, endpoint.Endpoint(withSchema))
			}
		}
		if len(pdAddrs) == 0 {
			return nil, errors.New("no usable PD client URL found in PD members")
		}
		return pdAddrs, nil
	}
}

// ParseURL parses the given URL to get a dialable endpoint.
func ParseURL(rawURL string) (prefix string, hostPort string, err error) {
	endpoint, err := util.ParseServiceURL(rawURL)
	if err != nil {
		return "", "", err
	}
	return endpoint.SchemePrefix(), endpoint.Address(), nil
}

// GetPDHttpAddrs is used to get PD http addrs.
func (n *client) GetPDHttpAddrs(ctx context.Context) ([]string, error) {
	addrs, err := GetPDAddrs(ctx, n.pdCli, true)
	if err != nil {
		return nil, err
	}
	return addrs, err
}
