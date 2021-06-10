// Copyright 2019 PingCAP, Inc.
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

package kv

import (
	"go.uber.org/atomic"
)

// StoreLimit will update from config reload and global variable set.
var StoreLimit atomic.Int64

// ReplicaReadType is the type of replica to read data from
type ReplicaReadType byte

const (
	// ReplicaReadLeader stands for 'read from leader'.
	ReplicaReadLeader ReplicaReadType = iota
	// ReplicaReadFollower stands for 'read from follower'.
	ReplicaReadFollower
	// ReplicaReadMixed stands for 'read from leader and follower and learner'.
	ReplicaReadMixed
)

// IsFollowerRead checks if follower is going to be used to read data.
func (r ReplicaReadType) IsFollowerRead() bool {
	return r != ReplicaReadLeader
}
