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

import (
	"context"
	"path"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"github.com/juju/errors"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/pingcap/pd/pkg/etcdutil"
	log "github.com/sirupsen/logrus"
)

var (
	errNoLeader         = errors.New("no leader")
	resignLeaderTimeout = time.Second * 5
	nextLeaderTTL       = 10 // in seconds
)

// IsLeader returns whether server is leader or not.
func (s *Server) IsLeader() bool {
	return atomic.LoadInt64(&s.isLeader) == 1
}

func (s *Server) enableLeader(b bool) {
	value := int64(0)
	if b {
		value = 1
	}

	atomic.StoreInt64(&s.isLeader, value)
}

func (s *Server) getLeaderPath() string {
	return path.Join(s.rootPath, "leader")
}

func (s *Server) getNextLeaderPath() string {
	return path.Join(s.rootPath, "next_leader")
}

func (s *Server) leaderLoop() {
	defer s.wg.Done()

	for {
		if s.isClosed() {
			log.Infof("server is closed, return leader loop")
			return
		}

		leader, err := getLeader(s.client, s.getLeaderPath())
		if err != nil {
			log.Errorf("get leader err %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if leader != nil {
			if s.isSameLeader(leader) {
				// oh, we are already leader, we may meet something wrong
				// in previous campaignLeader. we can delete and campaign again.
				log.Warnf("leader is still %s, delete and campaign again", leader)
				if err = s.deleteLeaderKey(); err != nil {
					log.Errorf("delete leader key err %s", err)
					time.Sleep(200 * time.Millisecond)
					continue
				}
			} else {
				log.Infof("leader is %s, watch it", leader)
				s.watchLeader()
				log.Info("leader changed, try to campaign leader")
			}
		}

		// Check if current pd is expected to be next leader.
		nextLeaders, err := getNextLeaders(s.client, s.getNextLeaderPath())
		if err != nil {
			log.Errorf("check next leader failed: %v", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if len(nextLeaders) > 0 {
			if _, ok := nextLeaders[s.id]; !ok {
				time.Sleep(200 * time.Millisecond)
				continue
			}
		}

		if err = s.campaignLeader(); err != nil {
			log.Errorf("campaign leader err %s", errors.ErrorStack(err))
		}
	}
}

func getLeaderAddr(leader *pdpb.Member) string {
	return strings.Join(leader.GetClientUrls(), ",")
}

// getLeader gets server leader from etcd.
func getLeader(c *clientv3.Client, leaderPath string) (*pdpb.Member, error) {
	leader := &pdpb.Member{}
	ok, err := getProtoMsg(c, leaderPath, leader)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !ok {
		return nil, nil
	}

	return leader, nil
}

func getNextLeaders(c *clientv3.Client, path string) (map[uint64]struct{}, error) {
	val, err := getValue(c, path)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(val) == 0 {
		return nil, nil
	}
	ids := make(map[uint64]struct{})
	for _, idStr := range strings.Split(string(val), ",") {
		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		ids[id] = struct{}{}
	}
	return ids, nil
}

// GetLeader gets pd cluster leader.
func (s *Server) GetLeader() (*pdpb.Member, error) {
	if s.isClosed() {
		return nil, errors.New("server is closed")
	}
	leader, err := getLeader(s.client, s.getLeaderPath())
	if err != nil {
		return nil, errors.Trace(err)
	}
	if leader == nil {
		return nil, errors.Trace(errNoLeader)
	}
	return leader, nil
}

func (s *Server) isSameLeader(leader *pdpb.Member) bool {
	return leader.GetMemberId() == s.ID()
}

func (s *Server) marshalLeader() string {
	leader := &pdpb.Member{
		Name:       s.Name(),
		MemberId:   s.ID(),
		ClientUrls: strings.Split(s.cfg.AdvertiseClientUrls, ","),
		PeerUrls:   strings.Split(s.cfg.AdvertisePeerUrls, ","),
	}

	data, err := leader.Marshal()
	if err != nil {
		// can't fail, so panic here.
		log.Fatalf("marshal leader %s err %v", leader, err)
	}

	return string(data)
}

func (s *Server) campaignLeader() error {
	log.Debugf("begin to campaign leader %s", s.Name())

	lessor := clientv3.NewLease(s.client)
	defer lessor.Close()

	start := time.Now()
	ctx, cancel := context.WithTimeout(s.client.Ctx(), requestTimeout)
	leaseResp, err := lessor.Grant(ctx, s.cfg.LeaderLease)
	cancel()

	if cost := time.Since(start); cost > slowRequestTime {
		log.Warnf("lessor grants too slow, cost %s", cost)
	}

	if err != nil {
		return errors.Trace(err)
	}

	leaderKey := s.getLeaderPath()
	// The leader key must not exist, so the CreateRevision is 0.
	resp, err := s.txn().
		If(clientv3.Compare(clientv3.CreateRevision(leaderKey), "=", 0)).
		Then(clientv3.OpPut(leaderKey, s.leaderValue, clientv3.WithLease(clientv3.LeaseID(leaseResp.ID)))).
		Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("campaign leader failed, other server may campaign ok")
	}

	// Make the leader keepalived.
	ctx, cancel = context.WithCancel(s.client.Ctx())
	defer cancel()

	ch, err := lessor.KeepAlive(ctx, clientv3.LeaseID(leaseResp.ID))
	if err != nil {
		return errors.Trace(err)
	}
	log.Debugf("campaign leader ok %s", s.Name())

	err = s.scheduleOpt.reload(s.kv)
	if err != nil {
		return errors.Trace(err)
	}
	// Try to create raft cluster.
	err = s.createRaftCluster()
	if err != nil {
		return errors.Trace(err)
	}
	defer s.stopRaftCluster()

	log.Debug("sync timestamp for tso")
	if err = s.syncTimestamp(); err != nil {
		return errors.Trace(err)
	}
	defer s.ts.Store(&atomicObject{
		physical: zeroTime,
	})

	s.enableLeader(true)
	defer s.enableLeader(false)

	log.Infof("PD cluster leader %s is ready to serve", s.Name())

	tsTicker := time.NewTicker(updateTimestampStep)
	defer tsTicker.Stop()

	for {
		select {
		case _, ok := <-ch:
			if !ok {
				log.Info("keep alive channel is closed")
				return nil
			}
		case <-tsTicker.C:
			if err = s.updateTimestamp(); err != nil {
				return errors.Trace(err)
			}
		case <-s.resignCh:
			log.Infof("%s resigns leadership", s.Name())
			return nil
		case <-ctx.Done():
			return errors.New("server closed")
		}
	}
}

func (s *Server) watchLeader() {
	watcher := clientv3.NewWatcher(s.client)
	defer watcher.Close()

	ctx, cancel := context.WithCancel(s.client.Ctx())
	defer cancel()

	for {
		rch := watcher.Watch(ctx, s.getLeaderPath())
		for wresp := range rch {
			if wresp.Canceled {
				return
			}

			for _, ev := range wresp.Events {
				if ev.Type == mvccpb.DELETE {
					log.Info("leader is deleted")
					return
				}
			}
		}

		select {
		case <-ctx.Done():
			// server closed, return
			return
		default:
		}
	}
}

// ResignLeader resigns current PD's leadership. If nextLeader is empty, all
// other pd-servers can campaign.
func (s *Server) ResignLeader(nextLeader string) error {
	log.Infof("%s tries to resign leader with next leader directive: %v", s.Name(), nextLeader)
	// Determine next leaders.
	var leaderIDs []string
	res, err := etcdutil.ListEtcdMembers(s.client)
	if err != nil {
		return errors.Trace(err)
	}
	for _, member := range res.Members {
		if (nextLeader == "" && member.ID != s.id) || (nextLeader != "" && member.Name == nextLeader) {
			leaderIDs = append(leaderIDs, strconv.FormatUint(member.ID, 10))
		}
	}
	nextLeaderValue := strings.Join(leaderIDs, ",")

	// Save expect leader(s) to etcd.
	lease := clientv3.NewLease(s.client)
	defer lease.Close()
	ctx, cancel := context.WithTimeout(s.client.Ctx(), requestTimeout)
	leaseRes, err := lease.Grant(ctx, int64(nextLeaderTTL))
	cancel()
	if err != nil {
		return errors.Trace(err)
	}
	resp, err := s.leaderTxn().
		Then(clientv3.OpPut(s.getNextLeaderPath(), nextLeaderValue, clientv3.WithLease(clientv3.LeaseID(leaseRes.ID)))).
		Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("save next leader failed, maybe lost leadership")
	}

	log.Infof("%s ready to resign leader, expect next leaders: %v", s.Name(), nextLeaderValue)

	// Resign leader.
	select {
	case s.resignCh <- struct{}{}:
		return nil
	case <-time.After(resignLeaderTimeout):
		return errors.Errorf("failed to send resign signal, maybe not leader")
	}
}

func (s *Server) deleteLeaderKey() error {
	// delete leader itself and let others start a new election again.
	leaderKey := s.getLeaderPath()
	resp, err := s.leaderTxn().Then(clientv3.OpDelete(leaderKey)).Commit()
	if err != nil {
		return errors.Trace(err)
	}
	if !resp.Succeeded {
		return errors.New("resign leader failed, we are not leader already")
	}

	return nil
}

func (s *Server) leaderCmp() clientv3.Cmp {
	return clientv3.Compare(clientv3.Value(s.getLeaderPath()), "=", s.leaderValue)
}
