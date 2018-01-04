// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/eraftpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/server/cache"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/namespace"
	"github.com/pingcap/pd/server/schedule"
	log "github.com/sirupsen/logrus"
)

const (
	runSchedulerCheckInterval = 3 * time.Second
	collectFactor             = 0.8
	historiesCacheSize        = 1000
	eventsCacheSize           = 1000
	maxScheduleRetries        = 10

	regionheartbeatSendChanCap = 1024
	hotRegionScheduleName      = "balance-hot-region-scheduler"
)

var (
	errSchedulerExisted  = errors.New("scheduler existed")
	errSchedulerNotFound = errors.New("scheduler not found")
)

type coordinator struct {
	sync.RWMutex

	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc

	cluster          *clusterInfo
	limiter          *schedule.Limiter
	replicaChecker   *schedule.ReplicaChecker
	namespaceChecker *schedule.NamespaceChecker
	operators        map[uint64]*schedule.Operator
	schedulers       map[string]*scheduleController
	classifier       namespace.Classifier
	histories        cache.Cache
	hbStreams        *heartbeatStreams
}

func newCoordinator(cluster *clusterInfo, hbStreams *heartbeatStreams, classifier namespace.Classifier) *coordinator {
	ctx, cancel := context.WithCancel(context.Background())
	return &coordinator{
		ctx:              ctx,
		cancel:           cancel,
		cluster:          cluster,
		limiter:          schedule.NewLimiter(),
		replicaChecker:   schedule.NewReplicaChecker(cluster, classifier),
		namespaceChecker: schedule.NewNamespaceChecker(cluster, classifier),
		operators:        make(map[uint64]*schedule.Operator),
		schedulers:       make(map[string]*scheduleController),
		classifier:       classifier,
		histories:        cache.NewDefaultCache(historiesCacheSize),
		hbStreams:        hbStreams,
	}
}

func (c *coordinator) dispatch(region *core.RegionInfo) {
	// Check existed operator.
	if op := c.getOperator(region.GetId()); op != nil {
		timeout := op.IsTimeout()
		if step := op.Check(region); step != nil && !timeout {
			operatorCounter.WithLabelValues(op.Desc(), "check").Inc()
			c.sendScheduleCommand(region, step)
			return
		}
		if op.IsFinish() {
			log.Infof("[region %v] operator finish: %s", region.GetId(), op)
			operatorCounter.WithLabelValues(op.Desc(), "finish").Inc()
			c.removeOperator(op)
		} else if timeout {
			log.Infof("[region %v] operator timeout: %s", region.GetId(), op)
			operatorCounter.WithLabelValues(op.Desc(), "timeout").Inc()
			c.removeOperator(op)
		}
	}

	// Check replica operator.
	if c.limiter.OperatorCount(schedule.OpReplica) >= c.cluster.GetReplicaScheduleLimit() {
		return
	}
	// Generate Operator which moves region to targeted namespace store
	if op := c.namespaceChecker.Check(region); op != nil {
		c.addOperator(op)
	}
	if op := c.replicaChecker.Check(region); op != nil {
		c.addOperator(op)
	}
}

func (c *coordinator) run() {
	ticker := time.NewTicker(runSchedulerCheckInterval)
	defer ticker.Stop()
	log.Info("coordinator: Start collect cluster information")
	for {
		if c.shouldRun() {
			log.Info("coordinator: Cluster information is prepared")
			break
		}
		select {
		case <-ticker.C:
		case <-c.ctx.Done():
			log.Info("coordinator: Stopped coordinator")
			return
		}
	}
	log.Info("coordinator: Run scheduler")

	k := 0
	scheduleCfg := c.cluster.opt.load()
	for _, schedulerCfg := range scheduleCfg.Schedulers {
		s, err := schedule.CreateScheduler(schedulerCfg.Type, c.limiter, schedulerCfg.Args...)
		if err != nil {
			log.Errorf("can not create scheduler %s: %v", schedulerCfg.Type, err)
		} else {
			log.Infof("create scheduler %s", s.GetName())
			if err = c.addScheduler(s, schedulerCfg.Args...); err != nil {
				log.Errorf("can not add scheduler %s: %v", s.GetName(), err)
			}
		}

		// only record valid scheduler config
		if err == nil {
			scheduleCfg.Schedulers[k] = schedulerCfg
			k++
		}
	}

	// remove invalid scheduler config and persist
	scheduleCfg.Schedulers = scheduleCfg.Schedulers[:k]
	if err := c.cluster.opt.persist(c.cluster.kv); err != nil {
		log.Errorf("can't persist schedule config: %v", err)
	}

}

func (c *coordinator) stop() {
	c.cancel()
	c.wg.Wait()
}

// Hack to retrive info from scheduler.
// TODO: remove it.
type hasHotStatus interface {
	GetHotReadStatus() *core.StoreHotRegionInfos
	GetHotWriteStatus() *core.StoreHotRegionInfos
}

func (c *coordinator) getHotWriteRegions() *core.StoreHotRegionInfos {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[hotRegionScheduleName]
	if !ok {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotWriteStatus()
	}
	return nil
}

func (c *coordinator) getHotReadRegions() *core.StoreHotRegionInfos {
	c.RLock()
	defer c.RUnlock()
	s, ok := c.schedulers[hotRegionScheduleName]
	if !ok {
		return nil
	}
	if h, ok := s.Scheduler.(hasHotStatus); ok {
		return h.GetHotReadStatus()
	}
	return nil
}

func (c *coordinator) getSchedulers() []string {
	c.RLock()
	defer c.RUnlock()

	names := make([]string, 0, len(c.schedulers))
	for name := range c.schedulers {
		names = append(names, name)
	}
	return names
}

func (c *coordinator) collectSchedulerMetrics() {
	c.RLock()
	defer c.RUnlock()
	for _, s := range c.schedulers {
		var allowScheduler float64
		if s.AllowSchedule() {
			allowScheduler = 1
		}
		schedulerStatusGauge.WithLabelValues(s.GetName(), "allow").Set(allowScheduler)
	}
}

func (c *coordinator) collectHotSpotMetrics() {
	c.RLock()
	defer c.RUnlock()
	// collect hot write region metrics
	s, ok := c.schedulers[hotRegionScheduleName]
	if !ok {
		return
	}
	status := s.Scheduler.(hasHotStatus).GetHotWriteStatus()
	for _, s := range c.cluster.GetStores() {
		store := fmt.Sprintf("store_%d", s.GetId())
		stat, ok := status.AsPeer[s.GetId()]
		if ok {
			totalWriteBytes := float64(stat.TotalFlowBytes)
			hotWriteRegionCount := float64(stat.RegionsCount)

			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_peer").Set(totalWriteBytes)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_peer").Set(hotWriteRegionCount)
		} else {
			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_peer").Set(0)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_peer").Set(0)
		}

		stat, ok = status.AsLeader[s.GetId()]
		if ok {
			totalWriteBytes := float64(stat.TotalFlowBytes)
			hotWriteRegionCount := float64(stat.RegionsCount)

			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_leader").Set(totalWriteBytes)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_leader").Set(hotWriteRegionCount)
		} else {
			hotSpotStatusGauge.WithLabelValues(store, "total_written_bytes_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(store, "hot_write_region_as_leader").Set(0)
		}
	}

	// collect hot read region metrics
	status = s.Scheduler.(hasHotStatus).GetHotReadStatus()
	for _, s := range c.cluster.GetStores() {
		store := fmt.Sprintf("store_%d", s.GetId())
		stat, ok := status.AsLeader[s.GetId()]
		if ok {
			totalReadBytes := float64(stat.TotalFlowBytes)
			hotReadRegionCount := float64(stat.RegionsCount)

			hotSpotStatusGauge.WithLabelValues(store, "total_read_bytes_as_leader").Set(totalReadBytes)
			hotSpotStatusGauge.WithLabelValues(store, "hot_read_region_as_leader").Set(hotReadRegionCount)
		} else {
			hotSpotStatusGauge.WithLabelValues(store, "total_read_bytes_as_leader").Set(0)
			hotSpotStatusGauge.WithLabelValues(store, "hot_read_region_as_leader").Set(0)
		}
	}
}

func (c *coordinator) shouldRun() bool {
	return c.cluster.isPrepared()
}

func (c *coordinator) addScheduler(scheduler schedule.Scheduler, args ...string) error {
	c.Lock()
	defer c.Unlock()

	if _, ok := c.schedulers[scheduler.GetName()]; ok {
		return errSchedulerExisted
	}

	s := newScheduleController(c, scheduler)
	if err := s.Prepare(c.cluster); err != nil {
		return errors.Trace(err)
	}

	c.wg.Add(1)
	go c.runScheduler(s)
	c.schedulers[s.GetName()] = s
	c.cluster.opt.AddSchedulerCfg(s.GetType(), args)

	return nil
}

func (c *coordinator) removeScheduler(name string) error {
	c.Lock()
	defer c.Unlock()

	s, ok := c.schedulers[name]
	if !ok {
		return errSchedulerNotFound
	}

	s.Stop()
	delete(c.schedulers, name)

	if err := c.cluster.opt.RemoveSchedulerCfg(name); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (c *coordinator) runScheduler(s *scheduleController) {
	defer c.wg.Done()
	defer s.Cleanup(c.cluster)

	timer := time.NewTimer(s.GetInterval())
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			timer.Reset(s.GetInterval())
			if !s.AllowSchedule() {
				continue
			}
			opInfluence := schedule.NewOpInfluence(c.getOperators(), c.cluster)
			if op := s.Schedule(c.cluster, opInfluence); op != nil {
				c.addOperator(op)
			}

		case <-s.Ctx().Done():
			log.Infof("%v stopped: %v", s.GetName(), s.Ctx().Err())
			return
		}
	}
}

func (c *coordinator) addOperator(op *schedule.Operator) bool {
	c.Lock()
	defer c.Unlock()
	regionID := op.RegionID()

	log.Infof("[region %v] add operator: %s", regionID, op)

	// If the new operator passed in has higher priorities than the old one,
	// then replace the old operator.
	if old, ok := c.operators[regionID]; ok {
		if !isHigherPriorityOperator(op, old) {
			log.Infof("[region %v] cancel add operator, old: %s", regionID, old)
			return false
		}
		log.Infof("[region %v] replace old operator: %s", regionID, old)
		operatorCounter.WithLabelValues(old.Desc(), "replaced").Inc()
		c.removeOperatorLocked(old)
	}

	c.histories.Put(regionID, op)
	c.operators[regionID] = op
	c.limiter.UpdateCounts(c.operators)

	if region := c.cluster.GetRegion(op.RegionID()); region != nil {
		if step := op.Check(region); step != nil {
			c.sendScheduleCommand(region, step)
		}
	}

	operatorCounter.WithLabelValues(op.Desc(), "create").Inc()
	return true
}

func isHigherPriorityOperator(new, old *schedule.Operator) bool {
	return new.GetPriorityLevel() < old.GetPriorityLevel()
}

func (c *coordinator) removeOperator(op *schedule.Operator) {
	c.Lock()
	defer c.Unlock()
	c.removeOperatorLocked(op)
}

func (c *coordinator) removeOperatorLocked(op *schedule.Operator) {
	regionID := op.RegionID()
	delete(c.operators, regionID)
	c.limiter.UpdateCounts(c.operators)
	c.histories.Put(regionID, op)
	operatorCounter.WithLabelValues(op.Desc(), "remove").Inc()
}

func (c *coordinator) getOperator(regionID uint64) *schedule.Operator {
	c.RLock()
	defer c.RUnlock()
	return c.operators[regionID]
}

func (c *coordinator) getOperators() []*schedule.Operator {
	c.RLock()
	defer c.RUnlock()

	operators := make([]*schedule.Operator, 0, len(c.operators))
	for _, op := range c.operators {
		operators = append(operators, op)
	}

	return operators
}

func (c *coordinator) getHistories() []*schedule.Operator {
	c.RLock()
	defer c.RUnlock()

	var operators []*schedule.Operator
	for _, elem := range c.histories.Elems() {
		operators = append(operators, elem.Value.(*schedule.Operator))
	}

	return operators
}

func (c *coordinator) getHistoriesOfKind(mask schedule.OperatorKind) []*schedule.Operator {
	c.RLock()
	defer c.RUnlock()

	var operators []*schedule.Operator
	for _, elem := range c.histories.Elems() {
		op := elem.Value.(*schedule.Operator)
		if op.Kind()&mask != 0 {
			operators = append(operators, op)
		}
	}

	return operators
}

func (c *coordinator) sendScheduleCommand(region *core.RegionInfo, step schedule.OperatorStep) {
	log.Infof("[region %v] send schedule command: %s", region.GetId(), step)
	switch s := step.(type) {
	case schedule.TransferLeader:
		cmd := &pdpb.RegionHeartbeatResponse{
			TransferLeader: &pdpb.TransferLeader{
				Peer: region.GetStorePeer(s.ToStore),
			},
		}
		c.hbStreams.sendMsg(region, cmd)
	case schedule.AddPeer:
		if region.GetStorePeer(s.ToStore) != nil {
			// The newly added peer is pending.
			return
		}
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_AddNode,
				Peer: &metapb.Peer{
					Id:      s.PeerID,
					StoreId: s.ToStore,
				},
			},
		}
		c.hbStreams.sendMsg(region, cmd)
	case schedule.RemovePeer:
		cmd := &pdpb.RegionHeartbeatResponse{
			ChangePeer: &pdpb.ChangePeer{
				ChangeType: eraftpb.ConfChangeType_RemoveNode,
				Peer:       region.GetStorePeer(s.FromStore),
			},
		}
		c.hbStreams.sendMsg(region, cmd)
	default:
		log.Errorf("unknown operatorStep: %v", step)
	}
}

type scheduleController struct {
	schedule.Scheduler
	cluster      *clusterInfo
	limiter      *schedule.Limiter
	classifier   namespace.Classifier
	nextInterval time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
}

func newScheduleController(c *coordinator, s schedule.Scheduler) *scheduleController {
	ctx, cancel := context.WithCancel(c.ctx)
	return &scheduleController{
		Scheduler:    s,
		cluster:      c.cluster,
		limiter:      c.limiter,
		nextInterval: s.GetMinInterval(),
		classifier:   c.classifier,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (s *scheduleController) Ctx() context.Context {
	return s.ctx
}

func (s *scheduleController) Stop() {
	s.cancel()
}

func (s *scheduleController) Schedule(cluster schedule.Cluster, opInfluence schedule.OpInfluence) *schedule.Operator {
	for i := 0; i < maxScheduleRetries; i++ {
		// If we have schedule, reset interval to the minimal interval.
		if op := scheduleByNamespace(cluster, s.classifier, s.Scheduler, opInfluence); op != nil {
			s.nextInterval = s.Scheduler.GetMinInterval()
			return op
		}
	}
	s.nextInterval = s.Scheduler.GetNextInterval(s.nextInterval)
	return nil
}

func (s *scheduleController) GetInterval() time.Duration {
	return s.nextInterval
}

func (s *scheduleController) AllowSchedule() bool {
	return s.Scheduler.IsScheduleAllowed(s.cluster)
}
