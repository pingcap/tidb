// Copyright 2025 PingCAP, Inc.
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

package autoid

import (
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/pingcap/kvproto/pkg/autoid"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/owner"
)

// ExportNewWithCli exports newWithCli for mega test framework.
func ExportNewWithCli(selfAddr string, cli *clientv3.Client, store kv.Storage) *Service {
	return newWithCli(selfAddr, cli, store)
}

// ExportLeaderShip exports the leaderShip field for testing.
func (s *Service) ExportLeaderShip() owner.Manager {
	return s.leaderShip
}

// ExportRegisterAutoIDAllocServer registers the service as a gRPC AutoIDAllocServer.
func ExportRegisterAutoIDAllocServer(srv *grpc.Server, svc *Service) {
	autoid.RegisterAutoIDAllocServer(srv, svc)
}