// Copyright 2016 The etcd Authors
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

package clientv3

import (
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type (
	Member               pb.Member
	MemberListResponse   pb.MemberListResponse
	MemberAddResponse    pb.MemberAddResponse
	MemberRemoveResponse pb.MemberRemoveResponse
	MemberUpdateResponse pb.MemberUpdateResponse
)

type Cluster interface {
	// MemberList lists the current cluster membership.
	MemberList(ctx context.Context) (*MemberListResponse, error)

	// MemberAdd adds a new member into the cluster.
	MemberAdd(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error)

	// MemberRemove removes an existing member from the cluster.
	MemberRemove(ctx context.Context, id uint64) (*MemberRemoveResponse, error)

	// MemberUpdate updates the peer addresses of the member.
	MemberUpdate(ctx context.Context, id uint64, peerAddrs []string) (*MemberUpdateResponse, error)
}

type cluster struct {
	remote   pb.ClusterClient
	callOpts []grpc.CallOption
}

func NewCluster(c *Client) Cluster {
	api := &cluster{remote: RetryClusterClient(c)}
	if c != nil {
		api.callOpts = c.callOpts
	}
	return api
}

func NewClusterFromClusterClient(remote pb.ClusterClient, c *Client) Cluster {
	api := &cluster{remote: remote}
	if c != nil {
		api.callOpts = c.callOpts
	}
	return api
}

func (c *cluster) MemberAdd(ctx context.Context, peerAddrs []string) (*MemberAddResponse, error) {
	r := &pb.MemberAddRequest{PeerURLs: peerAddrs}
	resp, err := c.remote.MemberAdd(ctx, r, c.callOpts...)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	return (*MemberAddResponse)(resp), nil
}

func (c *cluster) MemberRemove(ctx context.Context, id uint64) (*MemberRemoveResponse, error) {
	r := &pb.MemberRemoveRequest{ID: id}
	resp, err := c.remote.MemberRemove(ctx, r, c.callOpts...)
	if err != nil {
		return nil, toErr(ctx, err)
	}
	return (*MemberRemoveResponse)(resp), nil
}

func (c *cluster) MemberUpdate(ctx context.Context, id uint64, peerAddrs []string) (*MemberUpdateResponse, error) {
	// it is safe to retry on update.
	r := &pb.MemberUpdateRequest{ID: id, PeerURLs: peerAddrs}
	resp, err := c.remote.MemberUpdate(ctx, r, c.callOpts...)
	if err == nil {
		return (*MemberUpdateResponse)(resp), nil
	}
	return nil, toErr(ctx, err)
}

func (c *cluster) MemberList(ctx context.Context) (*MemberListResponse, error) {
	// it is safe to retry on list.
	resp, err := c.remote.MemberList(ctx, &pb.MemberListRequest{}, c.callOpts...)
	if err == nil {
		return (*MemberListResponse)(resp), nil
	}
	return nil, toErr(ctx, err)
}
