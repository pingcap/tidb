// Copyright 2021 PingCAP, Inc.
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

package mocktikv

import "github.com/pingcap/tidb/store/tikv/mockstore/mocktikv"

// Type alias to keep br working. Remove it after BR updates.
type (
	// MVCCStore is a mvcc key-value storage.
	MVCCStore = mocktikv.MVCCStore
	// Cluster simulates a TiKV cluster. It focuses on management and the change of
	// meta data. A Cluster mainly includes following 3 kinds of meta data:
	// 1) Region: A Region is a fragment of TiKV's data whose range is [start, end).
	//    The data of a Region is duplicated to multiple Peers and distributed in
	//    multiple Stores.
	// 2) Peer: A Peer is a replica of a Region's data. All peers of a Region form
	//    a group, each group elects a Leader to provide services.
	// 3) Store: A Store is a storage/service node. Try to think it as a TiKV server
	//    process. Only the store with request's Region's leader Peer could respond
	//    to client's request.
	Cluster = mocktikv.Cluster
)

// Variable alias to keep br working. Remove it after BR updates.
var (
	// MustNewMVCCStore is used for testing, use NewMVCCLevelDB instead.
	MustNewMVCCStore = mocktikv.MustNewMVCCStore
	// NewCluster creates an empty cluster. It needs to be bootstrapped before
	// providing service.
	NewCluster = mocktikv.NewCluster
	// NewPDClient creates a mock pd.Client that uses local timestamp and meta data
	// from a Cluster.
	NewPDClient = mocktikv.NewPDClient
)
