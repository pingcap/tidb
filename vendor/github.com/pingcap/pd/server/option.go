// Copyright 2017 PingCAP, Inc.
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

import (
	"reflect"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
)

// scheduleOption is a wrapper to access the configuration safely.
type scheduleOption struct {
	v   atomic.Value
	rep *Replication
	ns  map[string]*namespaceOption
}

func newScheduleOption(cfg *Config) *scheduleOption {
	o := &scheduleOption{}
	o.store(&cfg.Schedule)
	o.ns = make(map[string]*namespaceOption)
	for name, nsCfg := range cfg.Namespace {
		nsCfg := nsCfg
		o.ns[name] = newNamespaceOption(&nsCfg)
	}
	o.rep = newReplication(&cfg.Replication)
	return o
}

func (o *scheduleOption) load() *ScheduleConfig {
	return o.v.Load().(*ScheduleConfig)
}

func (o *scheduleOption) store(cfg *ScheduleConfig) {
	o.v.Store(cfg)
}

func (o *scheduleOption) GetReplication() *Replication {
	return o.rep
}

func (o *scheduleOption) GetMaxReplicas(name string) int {
	if n, ok := o.ns[name]; ok {
		return n.GetMaxReplicas()
	}
	return o.rep.GetMaxReplicas()
}

func (o *scheduleOption) SetMaxReplicas(replicas int) {
	o.rep.SetMaxReplicas(replicas)
}

func (o *scheduleOption) GetLocationLabels() []string {
	return o.rep.GetLocationLabels()
}

func (o *scheduleOption) GetMaxSnapshotCount() uint64 {
	return o.load().MaxSnapshotCount
}

func (o *scheduleOption) GetMaxPendingPeerCount() uint64 {
	return o.load().MaxPendingPeerCount
}

func (o *scheduleOption) GetMaxStoreDownTime() time.Duration {
	return o.load().MaxStoreDownTime.Duration
}

func (o *scheduleOption) GetLeaderScheduleLimit(name string) uint64 {
	if n, ok := o.ns[name]; ok {
		return n.GetLeaderScheduleLimit()
	}
	return o.load().LeaderScheduleLimit
}

func (o *scheduleOption) GetRegionScheduleLimit(name string) uint64 {
	if n, ok := o.ns[name]; ok {
		return n.GetRegionScheduleLimit()
	}
	return o.load().RegionScheduleLimit
}

func (o *scheduleOption) GetReplicaScheduleLimit(name string) uint64 {
	if n, ok := o.ns[name]; ok {
		return n.GetReplicaScheduleLimit()
	}
	return o.load().ReplicaScheduleLimit
}

func (o *scheduleOption) GetTolerantSizeRatio() float64 {
	return o.load().TolerantSizeRatio
}

func (o *scheduleOption) GetSchedulers() SchedulerConfigs {
	return o.load().Schedulers
}

func (o *scheduleOption) AddSchedulerCfg(tp string, args []string) error {
	c := o.load()
	v := c.clone()
	for _, schedulerCfg := range v.Schedulers {
		// comparing args is to cover the case that there are schedulers in same type but not with same name
		// such as two schedulers of type "evict-leader",
		// one name is "evict-leader-scheduler-1" and the other is "evict-leader-scheduler-2"
		if reflect.DeepEqual(schedulerCfg, SchedulerConfig{tp, args}) {
			return nil
		}
	}
	v.Schedulers = append(v.Schedulers, SchedulerConfig{Type: tp, Args: args})
	o.store(v)
	return nil
}

func (o *scheduleOption) RemoveSchedulerCfg(name string) error {
	c := o.load()
	v := c.clone()
	for i, schedulerCfg := range v.Schedulers {
		// To create a temporary scheduler is just used to get scheduler's name
		tmp, err := schedule.CreateScheduler(schedulerCfg.Type, schedule.NewLimiter(), schedulerCfg.Args...)
		if err != nil {
			return errors.Trace(err)
		}
		if tmp.GetName() == name {
			v.Schedulers = append(v.Schedulers[:i], v.Schedulers[i+1:]...)
			o.store(v)
			return nil
		}
	}
	return nil
}

func (o *scheduleOption) persist(kv *core.KV) error {
	namespaces := make(map[string]NamespaceConfig)
	for name, ns := range o.ns {
		namespaces[name] = *ns.load()
	}
	cfg := &Config{
		Schedule:    *o.load(),
		Replication: *o.rep.load(),
		Namespace:   namespaces,
	}
	err := kv.SaveConfig(cfg)
	return errors.Trace(err)
}

func (o *scheduleOption) reload(kv *core.KV) error {
	namespaces := make(map[string]NamespaceConfig)
	for name, ns := range o.ns {
		namespaces[name] = *ns.load()
	}
	cfg := &Config{
		Schedule:    *o.load(),
		Replication: *o.rep.load(),
		Namespace:   namespaces,
	}
	isExist, err := kv.LoadConfig(cfg)
	if err != nil {
		return errors.Trace(err)
	}
	if isExist {
		o.store(&cfg.Schedule)
		o.rep.store(&cfg.Replication)
		for name, nsCfg := range cfg.Namespace {
			nsCfg := nsCfg
			o.ns[name] = newNamespaceOption(&nsCfg)
		}
	}
	return nil
}

func (o *scheduleOption) GetHotRegionLowThreshold() int {
	return schedule.HotRegionLowThreshold
}

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

// namespaceOption is a wrapper to access the configuration safely.
type namespaceOption struct {
	namespaceCfg atomic.Value
}

func newNamespaceOption(cfg *NamespaceConfig) *namespaceOption {
	n := &namespaceOption{}
	n.store(cfg)
	return n
}

func (n *namespaceOption) load() *NamespaceConfig {
	return n.namespaceCfg.Load().(*NamespaceConfig)
}

func (n *namespaceOption) store(cfg *NamespaceConfig) {
	n.namespaceCfg.Store(cfg)
}

// GetMaxReplicas returns the number of replicas for each region.
func (n *namespaceOption) GetMaxReplicas() int {
	return int(n.load().MaxReplicas)
}

// GetLeaderScheduleLimit returns the number of replicas for each region.
func (n *namespaceOption) GetLeaderScheduleLimit() uint64 {
	return n.load().LeaderScheduleLimit
}

// GetRegionScheduleLimit returns the number of replicas for each region.
func (n *namespaceOption) GetRegionScheduleLimit() uint64 {
	return n.load().RegionScheduleLimit
}

// GetReplicaScheduleLimit returns the number of replicas for each region.
func (n *namespaceOption) GetReplicaScheduleLimit() uint64 {
	return n.load().ReplicaScheduleLimit
}
