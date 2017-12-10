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

package grpcproxy

import (
	"io"

	"golang.org/x/net/context"

	"github.com/coreos/etcd/clientv3"
	pb "github.com/coreos/etcd/etcdserver/etcdserverpb"
)

type maintenanceProxy struct {
	client *clientv3.Client
}

func NewMaintenanceProxy(c *clientv3.Client) pb.MaintenanceServer {
	return &maintenanceProxy{
		client: c,
	}
}

func (mp *maintenanceProxy) Defragment(ctx context.Context, dr *pb.DefragmentRequest) (*pb.DefragmentResponse, error) {
	conn := mp.client.ActiveConnection()
	return pb.NewMaintenanceClient(conn).Defragment(ctx, dr)
}

func (mp *maintenanceProxy) Snapshot(sr *pb.SnapshotRequest, stream pb.Maintenance_SnapshotServer) error {
	conn := mp.client.ActiveConnection()
	ctx, cancel := context.WithCancel(stream.Context())
	defer cancel()

	sc, err := pb.NewMaintenanceClient(conn).Snapshot(ctx, sr)
	if err != nil {
		return err
	}

	for {
		rr, err := sc.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		err = stream.Send(rr)
		if err != nil {
			return err
		}
	}
}

func (mp *maintenanceProxy) Hash(ctx context.Context, r *pb.HashRequest) (*pb.HashResponse, error) {
	conn := mp.client.ActiveConnection()
	return pb.NewMaintenanceClient(conn).Hash(ctx, r)
}

func (mp *maintenanceProxy) Alarm(ctx context.Context, r *pb.AlarmRequest) (*pb.AlarmResponse, error) {
	conn := mp.client.ActiveConnection()
	return pb.NewMaintenanceClient(conn).Alarm(ctx, r)
}

func (mp *maintenanceProxy) Status(ctx context.Context, r *pb.StatusRequest) (*pb.StatusResponse, error) {
	conn := mp.client.ActiveConnection()
	return pb.NewMaintenanceClient(conn).Status(ctx, r)
}
