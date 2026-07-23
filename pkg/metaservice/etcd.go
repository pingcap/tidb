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

var defaultPDClientFactory PDClientFactory = func(
	ctx context.Context,
	_ pd.APIContext,
	callerComponent caller.Component,
	svrAddrs []string,
	security pd.SecurityOption,
	opts ...opt.ClientOption,
) (pd.Client, error) {
	return pd.NewClientWithContext(ctx, callerComponent, svrAddrs, security, opts...)
}

// NewEtcdMetaServiceClient creates a ServiceClient backed by etcd and PD clients.
// When etcdCli is nil but pdCli is not, it returns a PD-only client that still
// implements GetPDAddrs and GetPDServiceURLs.
func NewEtcdMetaServiceClient(etcdCli *clientv3.Client, pdCli pd.Client) ServiceClient {
	return newClient(etcdCli, pdCli)
}

type etcdDialInfo struct {
	endpoints []string
	namespace string
}

func resolveEtcdDialInfo(
	ctx context.Context,
	pdCli pd.Client,
	keyspaceMeta *keyspacepb.KeyspaceMeta,
	callerPDAddrs []string,
) (etcdDialInfo, error) {
	pdAddrs := filterEmptyAddrs(callerPDAddrs)
	if len(pdAddrs) == 0 && usesGlobalMetaServiceGroup(keyspaceMeta) {
		var err error
		pdAddrs, err = GetPDAddrs(ctx, pdCli, false)
		if err != nil {
			return etcdDialInfo{}, err
		}
	}

	info, err := GetInfo(keyspaceMeta, pdAddrs)
	if err != nil {
		return etcdDialInfo{}, err
	}

	dialInfo := etcdDialInfo{endpoints: info.GroupAddrs()}
	if keyspaceMeta != nil {
		codec, err := tikv.NewCodecV2(tikv.ModeTxn, keyspaceMeta)
		if err != nil {
			return etcdDialInfo{}, err
		}
		dialInfo.namespace = keyspace.MakeKeyspaceEtcdNamespace(codec)
	}
	return dialInfo, nil
}

func filterEmptyAddrs(addrs []string) []string {
	if len(addrs) == 0 {
		return nil
	}
	filtered := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		if addr != "" {
			filtered = append(filtered, addr)
		}
	}
	return filtered
}

func usesGlobalMetaServiceGroup(keyspaceMeta *keyspacepb.KeyspaceMeta) bool {
	if keyspaceMeta == nil {
		return true
	}
	_, ok := keyspaceMeta.Config[GroupIDKey]
	return !ok
}

// NewEtcdClientFromPDClient resolves the meta service group from an existing PD
// client and returns a namespaced etcd client. callerPDAddrs are used when the
// keyspace does not have a dedicated meta service group.
func NewEtcdClientFromPDClient(
	ctx context.Context,
	pdCli pd.Client,
	keyspaceMeta *keyspacepb.KeyspaceMeta,
	callerPDAddrs []string,
	etcdCfg clientv3.Config,
) (*clientv3.Client, error) {
	return newEtcdClientFromPDClient(ctx, pdCli, keyspaceMeta, callerPDAddrs, etcdCfg)
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
	apiCtx := keyspace.BuildAPIContext(keyspaceName)
	if pdClientFactory == nil {
		// DialEtcdClient only needs member discovery plus one explicit keyspace
		// metadata lookup. Using a V2 PD client here would issue another
		// LoadKeyspace RPC during client initialization.
		pdClientFactory = defaultPDClientFactory
		apiCtx = pd.NewAPIContextV1()
	}

	pdCli, err := pdClientFactory(
		ctx, apiCtx, callerComponent, pdAddrs, security, pdClientOpts...,
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
			return nil, fmt.Errorf("keyspace meta not found for keyspace %q", keyspaceName)
		}
	}

	return newEtcdClientFromPDClient(ctx, pdCli, keyspaceMeta, pdAddrs, etcdCfg)
}

func newEtcdClientFromPDClient(
	ctx context.Context,
	pdCli pd.Client,
	keyspaceMeta *keyspacepb.KeyspaceMeta,
	callerPDAddrs []string,
	etcdCfg clientv3.Config,
) (*clientv3.Client, error) {
	dialInfo, err := resolveEtcdDialInfo(ctx, pdCli, keyspaceMeta, callerPDAddrs)
	if err != nil {
		return nil, err
	}

	etcdCfg.Context = ctx
	etcdCfg.Endpoints = dialInfo.endpoints
	etcdCli, err := clientv3.New(etcdCfg)
	if err != nil {
		return nil, err
	}
	if dialInfo.namespace != "" {
		etcd.SetEtcdCliByNamespace(etcdCli, dialInfo.namespace)
	}
	return etcdCli, nil
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
// For http/https members, it returns host:port unless withScheme is true.
// For unix-family members, it keeps the scheme because the scheme is required
// for dialing. Malformed member URLs are skipped, and an error is returned only
// when no usable PD client URL remains.
func GetPDAddrs(ctx context.Context, pdClient pd.Client, withScheme bool) ([]string, error) {
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
			for _, clientURL := range member.ClientUrls {
				endpoint, err := util.ParseServiceURL(clientURL)
				if err != nil {
					continue
				}
				pdAddrs = append(pdAddrs, endpoint.Endpoint(withScheme))
			}
		}
		if len(pdAddrs) == 0 {
			return nil, errors.New("no usable PD client URL found in PD members")
		}
		return pdAddrs, nil
	}
}

// ParseURL parses the given URL to get a dialable endpoint.
func ParseURL(rawURL string) (prefix string, address string, err error) {
	endpoint, err := util.ParseServiceURL(rawURL)
	if err != nil {
		return "", "", err
	}
	return endpoint.SchemePrefix(), endpoint.Address(), nil
}

// GetPDServiceURLs returns PD member client URLs with the scheme included.
func (n *client) GetPDServiceURLs(ctx context.Context) ([]string, error) {
	addrs, err := GetPDAddrs(ctx, n.pdCli, true)
	if err != nil {
		return nil, err
	}
	return addrs, err
}
