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
	"net"
	"net/url"

	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const getAllMembersBackoffMs = 5000

// client is used to implement etcd meta service.
type client struct {
	pdCli           pd.Client
	keyspaceEtcdCli *clientv3.Client
}

// newClient is used to implement etcd meta service.
func newClient(etcdCli *clientv3.Client, pdCli pd.Client) ServiceClient {
	if etcdCli == nil {
		return nil
	}
	return &client{
		keyspaceEtcdCli: etcdCli,
		pdCli:           pdCli,
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

// GetPDAddrs returns the PD addresses from PD client.
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
				prefix, hostPort, err := ParseURL(member.ClientUrls[0])
				if err != nil {
					return nil, fmt.Errorf("parse client url from pd members %q: %w", member.ClientUrls[0], err)
				}
				var pdAddr string
				if withSchema {
					pdAddr = prefix + hostPort // http://ip:port
				} else {
					pdAddr = hostPort // ip:port
				}

				pdAddrs = append(pdAddrs, pdAddr)
			}
		}
		if len(pdAddrs) == 0 {
			return nil, errors.New("no usable PD client URL found in PD members")
		}
		return pdAddrs, nil
	}
}

// ParseURL parses the given URL to get the host:port.
func ParseURL(rawURL string) (prefix string, hostPort string, err error) {
	u, parseErr := url.Parse(rawURL)
	if parseErr != nil {
		return "", "", invalidURLHostPortErr()
	}

	switch u.Scheme {
	case "http":
		prefix = "http://"
	case "https":
		prefix = "https://"
	default:
		return "", "", fmt.Errorf("invalid URL prefix")
	}

	host, port, splitErr := net.SplitHostPort(u.Host)
	if splitErr != nil || host == "" || port == "" {
		return "", "", invalidURLHostPortErr()
	}

	return prefix, u.Host, nil
}

func invalidURLHostPortErr() error {
	return fmt.Errorf("invalid URL format, expect host:port")
}

// GetPDHttpAddrs is used to get PD http addrs.
func (n *client) GetPDHttpAddrs(ctx context.Context) ([]string, error) {
	addrs, err := GetPDAddrs(ctx, n.pdCli, true)
	if err != nil {
		return nil, err
	}
	return addrs, err
}
