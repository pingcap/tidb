// Copyright 2019-present PingCAP, Inc.
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

package tikv

import (
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/pingcap/tidb/store/mockstore/unistore/pd"
	"github.com/pingcap/tidb/store/mockstore/unistore/tikv/mvcc"
)

// InnerServer defines the inner server interface.
type InnerServer interface {
	Setup(pdClient pd.Client)
	Start(pdClient pd.Client) error
	Stop() error
	Raft(stream tikvpb.Tikv_RaftServer) error
	BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error
	Snapshot(stream tikvpb.Tikv_SnapshotServer) error
}

// StandAlongInnerServer implements the InnerServer interface.
type StandAlongInnerServer struct {
	bundle *mvcc.DBBundle
}

// NewStandAlongInnerServer returns a new StandAlongInnerServer.
func NewStandAlongInnerServer(bundle *mvcc.DBBundle) *StandAlongInnerServer {
	return &StandAlongInnerServer{
		bundle: bundle,
	}
}

// Raft implements the InnerServer Raft method.
func (is *StandAlongInnerServer) Raft(stream tikvpb.Tikv_RaftServer) error {
	return nil
}

// BatchRaft implements the InnerServer BatchRaft method.
func (is *StandAlongInnerServer) BatchRaft(stream tikvpb.Tikv_BatchRaftServer) error {
	return nil
}

// Snapshot implements the InnerServer Snapshot method.
func (is *StandAlongInnerServer) Snapshot(stream tikvpb.Tikv_SnapshotServer) error {
	return nil
}

// Setup implements the InnerServer Setup method.
func (is *StandAlongInnerServer) Setup(pdClient pd.Client) {}

// Start implements the InnerServer Start method.
func (is *StandAlongInnerServer) Start(pdClient pd.Client) error {
	return nil
}

// Stop implements the InnerServer Stop method.
func (is *StandAlongInnerServer) Stop() error {
	return is.bundle.DB.Close()
}
