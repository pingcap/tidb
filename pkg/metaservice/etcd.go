// Copyright 2025 PingCAP, Inc.
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
	"strings"

	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const getAllMembersBackoff = 5000

// EtcdMetaServiceClient is used to implement etcd meta service.
type EtcdMetaServiceClient struct {
	pdCli           pd.Client
	KeyspaceEtcdCli *clientv3.Client
}

// NewEtcdMetaServiceClient is used to implement etcd meta service.
func NewEtcdMetaServiceClient(etcdCli *clientv3.Client, pdCli pd.Client) ServiceClient {
	if etcdCli == nil {
		return nil
	}
	return &EtcdMetaServiceClient{
		KeyspaceEtcdCli: etcdCli,
		pdCli:           pdCli,
	}
}

// GetKeyspaceEtcdCli return etcd client.
func (n *EtcdMetaServiceClient) GetKeyspaceEtcdCli() *clientv3.Client {
	return n.KeyspaceEtcdCli
}

// GetPDAddrs implements ServiceClient interface.
func (n *EtcdMetaServiceClient) GetPDAddrs() ([]string, error) {
	addrs, err := GetPDHostPorts(context.Background(), n.pdCli, false)
	if err != nil {
		return nil, err
	}
	return addrs, err
}

// GetPDLeaderAddrs implements ServiceClient interface.
func (n *EtcdMetaServiceClient) GetPDLeaderAddrs(ctx context.Context) (string, zap.Field) {
	// todo: PD GetAllMembers should directly return which is the pd leader.
	// Don't use etcd client to get PD leader.

	var (
		leaderAddr string
		errMsgMap  = map[string]string{}
	)
	for _, addr := range n.KeyspaceEtcdCli.Endpoints() {
		status, err := n.KeyspaceEtcdCli.Status(ctx, addr)
		if err != nil {
			errMsgMap[addr] = err.Error()
			continue
		}
		if status.Leader == status.Header.MemberId {
			leaderAddr = addr
			break
		}
	}

	errMsgField := zap.Skip()
	if len(errMsgMap) > 0 {
		errMsgField = zap.Any("errors when find leader", errMsgMap)
	}
	return leaderAddr, errMsgField
}

// GetPDHostPorts returns the PD addresses from PD client.
func GetPDHostPorts(ctx context.Context, pdClient pd.Client, hasPrefix bool) ([]string, error) {
	pdAddrs := make([]string, 0)
	bo := tikv.NewBackoffer(ctx, getAllMembersBackoff)
	if pdClient == nil {
		return nil, errors.New("PD client not found")
	}
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
				prefix, host, port, err := ParseURL(member.ClientUrls[0])
				if err != nil {
					return nil, fmt.Errorf("parse client url from pd members %q: %w", member.ClientUrls[0], err)
				}
				var pdAddr string
				if hasPrefix {
					pdAddr = prefix + host + ":" + port // http://ip:port
				} else {
					pdAddr = host + ":" + port // ip:port
				}

				pdAddrs = append(pdAddrs, pdAddr)
			}
		}
		return pdAddrs, nil
	}
}

// ParseURL parses the given URL to get the host and port.
func ParseURL(rawURL string) (prefix string, host string, port string, err error) {
	var trimmedURL string

	// Check the URL prefix and remove it
	switch {
	case strings.HasPrefix(rawURL, "unix://"):
		prefix = "unix://"
		trimmedURL = strings.TrimPrefix(rawURL, "unix://")
	case strings.HasPrefix(rawURL, "http://"):
		prefix = "http://"
		trimmedURL = strings.TrimPrefix(rawURL, "http://")
	case strings.HasPrefix(rawURL, "https://"):
		prefix = "https://"
		trimmedURL = strings.TrimPrefix(rawURL, "https://")
	default:
		return "", "", "", fmt.Errorf("invalid URL prefix")
	}

	// Split host and port
	parts := strings.Split(trimmedURL, ":")
	if len(parts) == 0 {
		return "", "", "", fmt.Errorf("invalid URL format, expect host:port")
	}

	if len(parts) > 2 {
		return "", "", "", fmt.Errorf("invalid URL format, expect host:port")
	}

	host = parts[0]
	if host == "" {
		return "", "", "", fmt.Errorf("invalid URL format, expect host:port")
	}

	if len(parts) == 2 {
		port = parts[1]
	} else if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") {
		port = "80" // Default HTTP/HTTPS port
	} else {
		return "", "", "", fmt.Errorf("invalid URL format, expect host:port")
	}

	return prefix, host, port, nil
}

// GetPDHttpAddrs is used to get PD http addrs.
func (n *EtcdMetaServiceClient) GetPDHttpAddrs() ([]string, error) {
	addrs, err := GetPDHostPorts(context.Background(), n.pdCli, true)
	if err != nil {
		return nil, err
	}
	return addrs, err
}
