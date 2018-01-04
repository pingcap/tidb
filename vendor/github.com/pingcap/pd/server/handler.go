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
	"strconv"

	"github.com/juju/errors"
	"github.com/pingcap/pd/server/core"
	"github.com/pingcap/pd/server/schedule"
	log "github.com/sirupsen/logrus"
)

var (
	errNotBootstrapped  = errors.New("TiKV cluster not bootstrapped, please start TiKV first")
	errOperatorNotFound = errors.New("operator not found")
)

// Handler is a helper to export methods to handle API/RPC requests.
type Handler struct {
	s   *Server
	opt *scheduleOption
}

func newHandler(s *Server) *Handler {
	return &Handler{s: s, opt: s.scheduleOpt}
}

func (h *Handler) getCoordinator() (*coordinator, error) {
	cluster := h.s.GetRaftCluster()
	if cluster == nil {
		return nil, errors.Trace(errNotBootstrapped)
	}
	return cluster.coordinator, nil
}

// GetSchedulers returns all names of schedulers.
func (h *Handler) GetSchedulers() ([]string, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.getSchedulers(), nil
}

// GetHotWriteRegions gets all hot write regions status
func (h *Handler) GetHotWriteRegions() *core.StoreHotRegionInfos {
	c, err := h.getCoordinator()
	if err != nil {
		return nil
	}
	return c.getHotWriteRegions()
}

// GetHotReadRegions gets all hot read regions status
func (h *Handler) GetHotReadRegions() *core.StoreHotRegionInfos {
	c, err := h.getCoordinator()
	if err != nil {
		return nil
	}
	return c.getHotReadRegions()
}

// GetHotWriteStores gets all hot write stores status
func (h *Handler) GetHotWriteStores() map[uint64]uint64 {
	return h.s.cluster.cachedCluster.getStoresWriteStat()
}

// GetHotReadStores gets all hot write stores status
func (h *Handler) GetHotReadStores() map[uint64]uint64 {
	return h.s.cluster.cachedCluster.getStoresReadStat()
}

// AddScheduler adds a scheduler.
func (h *Handler) AddScheduler(name string, args ...string) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}
	s, err := schedule.CreateScheduler(name, c.limiter, args...)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("create scheduler %s", s.GetName())
	if err = c.addScheduler(s, args...); err != nil {
		log.Errorf("can not add scheduler %v: %v", s.GetName(), err)
	} else if err = h.opt.persist(c.cluster.kv); err != nil {
		log.Errorf("can not persist scheduler config: %v", err)
	}
	return errors.Trace(err)
}

// RemoveScheduler removes a scheduler by name.
func (h *Handler) RemoveScheduler(name string) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}
	if err = c.removeScheduler(name); err != nil {
		log.Errorf("can not remove scheduler %v: %v", name, err)
	} else if err = h.opt.persist(c.cluster.kv); err != nil {
		log.Errorf("can not persist scheduler config: %v", err)
	}
	return errors.Trace(err)
}

// AddBalanceLeaderScheduler adds a balance-leader-scheduler.
func (h *Handler) AddBalanceLeaderScheduler() error {
	return h.AddScheduler("balance-leader")
}

// AddBalanceRegionScheduler adds a balance-region-scheduler.
func (h *Handler) AddBalanceRegionScheduler() error {
	return h.AddScheduler("balance-region")
}

// AddBalanceHotRegionScheduler adds a balance-hot-region-scheduler.
func (h *Handler) AddBalanceHotRegionScheduler() error {
	return h.AddScheduler("hot-region")
}

// AddAdjacentRegionScheduler adds a balance-adjacent-region-scheduler.
func (h *Handler) AddAdjacentRegionScheduler(args ...string) error {
	return h.AddScheduler("adjacent-region", args...)
}

// AddGrantLeaderScheduler adds a grant-leader-scheduler.
func (h *Handler) AddGrantLeaderScheduler(storeID uint64) error {
	return h.AddScheduler("grant-leader", strconv.FormatUint(storeID, 10))
}

// AddEvictLeaderScheduler adds an evict-leader-scheduler.
func (h *Handler) AddEvictLeaderScheduler(storeID uint64) error {
	return h.AddScheduler("evict-leader", strconv.FormatUint(storeID, 10))
}

// AddShuffleLeaderScheduler adds a shuffle-leader-scheduler.
func (h *Handler) AddShuffleLeaderScheduler() error {
	return h.AddScheduler("shuffle-leader")
}

// AddShuffleRegionScheduler adds a shuffle-region-scheduler.
func (h *Handler) AddShuffleRegionScheduler() error {
	return h.AddScheduler("shuffle-region")
}

// GetOperator returns the region operator.
func (h *Handler) GetOperator(regionID uint64) (*schedule.Operator, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, errors.Trace(err)
	}

	op := c.getOperator(regionID)
	if op == nil {
		return nil, errOperatorNotFound
	}

	return op, nil
}

// RemoveOperator removes the region operator.
func (h *Handler) RemoveOperator(regionID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}

	op := c.getOperator(regionID)
	if op == nil {
		return errOperatorNotFound
	}

	c.removeOperator(op)
	return nil
}

// GetOperators returns the running operators.
func (h *Handler) GetOperators() ([]*schedule.Operator, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.getOperators(), nil
}

// GetAdminOperators returns the running admin operators.
func (h *Handler) GetAdminOperators() ([]*schedule.Operator, error) {
	return h.GetOperatorsOfKind(schedule.OpAdmin)
}

// GetLeaderOperators returns the running leader operators.
func (h *Handler) GetLeaderOperators() ([]*schedule.Operator, error) {
	return h.GetOperatorsOfKind(schedule.OpLeader)
}

// GetRegionOperators returns the running region operators.
func (h *Handler) GetRegionOperators() ([]*schedule.Operator, error) {
	return h.GetOperatorsOfKind(schedule.OpRegion)
}

// GetOperatorsOfKind returns the running operators of the kind.
func (h *Handler) GetOperatorsOfKind(mask schedule.OperatorKind) ([]*schedule.Operator, error) {
	ops, err := h.GetOperators()
	if err != nil {
		return nil, errors.Trace(err)
	}
	var results []*schedule.Operator
	for _, op := range ops {
		if op.Kind()&mask != 0 {
			results = append(results, op)
		}
	}
	return results, nil
}

// GetHistoryOperators returns history operators
func (h *Handler) GetHistoryOperators() ([]*schedule.Operator, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.getHistories(), nil
}

// GetHistoryOperatorsOfKind returns history operators by Kind
func (h *Handler) GetHistoryOperatorsOfKind(mask schedule.OperatorKind) ([]*schedule.Operator, error) {
	c, err := h.getCoordinator()
	if err != nil {
		return nil, errors.Trace(err)
	}
	return c.getHistoriesOfKind(mask), nil
}

// AddTransferLeaderOperator adds an operator to transfer leader to the store.
func (h *Handler) AddTransferLeaderOperator(regionID uint64, storeID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return errRegionNotFound(regionID)
	}
	newLeader := region.GetStorePeer(storeID)
	if newLeader == nil {
		return errors.Errorf("region has no peer in store %v", storeID)
	}

	step := schedule.TransferLeader{FromStore: region.Leader.GetStoreId(), ToStore: newLeader.GetStoreId()}
	op := schedule.NewOperator("adminTransferLeader", regionID, schedule.OpAdmin|schedule.OpLeader, step)
	c.addOperator(op)
	return nil
}

// AddTransferRegionOperator adds an operator to transfer region to the stores.
func (h *Handler) AddTransferRegionOperator(regionID uint64, storeIDs map[uint64]struct{}) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return errRegionNotFound(regionID)
	}

	var steps []schedule.OperatorStep

	// Add missing peers.
	for id := range storeIDs {
		if c.cluster.GetStore(id) == nil {
			return core.ErrStoreNotFound(id)
		}
		if region.GetStorePeer(id) != nil {
			continue
		}
		peer, err := c.cluster.AllocPeer(id)
		if err != nil {
			return errors.Trace(err)
		}
		steps = append(steps, schedule.AddPeer{ToStore: id, PeerID: peer.Id})
	}

	// Remove redundant peers.
	for _, peer := range region.GetPeers() {
		if _, ok := storeIDs[peer.GetStoreId()]; ok {
			continue
		}
		steps = append(steps, schedule.RemovePeer{FromStore: peer.GetStoreId()})
	}

	op := schedule.NewOperator("adminMoveRegion", regionID, schedule.OpAdmin|schedule.OpRegion, steps...)
	c.addOperator(op)
	return nil
}

// AddTransferPeerOperator adds an operator to transfer peer.
func (h *Handler) AddTransferPeerOperator(regionID uint64, fromStoreID, toStoreID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return errRegionNotFound(regionID)
	}

	oldPeer := region.GetStorePeer(fromStoreID)
	if oldPeer == nil {
		return errors.Errorf("region has no peer in store %v", fromStoreID)
	}

	if c.cluster.GetStore(toStoreID) == nil {
		return core.ErrStoreNotFound(toStoreID)
	}
	newPeer, err := c.cluster.AllocPeer(toStoreID)
	if err != nil {
		return errors.Trace(err)
	}

	op := schedule.CreateMovePeerOperator("adminMovePeer", region, schedule.OpAdmin, fromStoreID, toStoreID, newPeer.GetId())
	c.addOperator(op)
	return nil
}

// AddAddPeerOperator adds an operator to add peer.
func (h *Handler) AddAddPeerOperator(regionID uint64, toStoreID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return errRegionNotFound(regionID)
	}

	if region.GetStorePeer(toStoreID) != nil {
		return errors.Errorf("region already has peer in store %v", toStoreID)
	}

	if c.cluster.GetStore(toStoreID) == nil {
		return core.ErrStoreNotFound(toStoreID)
	}
	newPeer, err := c.cluster.AllocPeer(toStoreID)
	if err != nil {
		return errors.Trace(err)
	}

	step := schedule.AddPeer{ToStore: toStoreID, PeerID: newPeer.GetId()}
	op := schedule.NewOperator("adminAddPeer", regionID, schedule.OpAdmin|schedule.OpRegion, step)
	c.addOperator(op)
	return nil
}

// AddRemovePeerOperator adds an operator to remove peer.
func (h *Handler) AddRemovePeerOperator(regionID uint64, fromStoreID uint64) error {
	c, err := h.getCoordinator()
	if err != nil {
		return errors.Trace(err)
	}

	region := c.cluster.GetRegion(regionID)
	if region == nil {
		return errRegionNotFound(regionID)
	}

	if region.GetStorePeer(fromStoreID) == nil {
		return errors.Errorf("region has no peer in store %v", fromStoreID)
	}

	op := schedule.CreateRemovePeerOperator("adminRemovePeer", schedule.OpAdmin, region, fromStoreID)
	c.addOperator(op)
	return nil
}
