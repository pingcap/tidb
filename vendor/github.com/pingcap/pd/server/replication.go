// Copyright 2016 PingCAP, Inc.
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

package server

import "sync/atomic"

// Replication provides some help to do replication.
type Replication struct {
	replicateCfg atomic.Value
}

func newReplication(cfg *ReplicationConfig) *Replication {
	r := &Replication{}
	r.store(cfg)
	return r
}

func (r *Replication) load() *ReplicationConfig {
	return r.replicateCfg.Load().(*ReplicationConfig)
}

func (r *Replication) store(cfg *ReplicationConfig) {
	r.replicateCfg.Store(cfg)
}

// GetMaxReplicas returns the number of replicas for each region.
func (r *Replication) GetMaxReplicas() int {
	return int(r.load().MaxReplicas)
}

// SetMaxReplicas set the replicas for each region.
func (r *Replication) SetMaxReplicas(replicas int) {
	c := r.load()
	v := c.clone()
	v.MaxReplicas = uint64(replicas)
	r.store(v)
}

// GetLocationLabels returns the location labels for each region
func (r *Replication) GetLocationLabels() []string {
	return r.load().LocationLabels
}
