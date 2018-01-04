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

package pd

import (
	"context"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/coreos/etcd/clientv3"
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/testutil"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/server/api"
)

var _ = Suite(&testLeaderChangeSuite{})

type testLeaderChangeSuite struct{}

func (s *testLeaderChangeSuite) prepareClusterN(c *C, n int) (svrs map[string]*server.Server, endpoints []string, closeFunc func()) {
	cfgs := server.NewTestMultiConfig(n)

	ch := make(chan *server.Server, n)

	for i := 0; i < n; i++ {
		cfg := cfgs[i]

		go func() {
			svr, err := server.CreateServer(cfg, api.NewHandler)
			c.Assert(err, IsNil)
			err = svr.Run()
			c.Assert(err, IsNil)
			ch <- svr
		}()
	}

	svrs = make(map[string]*server.Server, n)
	for i := 0; i < n; i++ {
		svr := <-ch
		svrs[svr.GetAddr()] = svr
	}

	endpoints = make([]string, 0, n)
	for _, svr := range svrs {
		endpoints = append(endpoints, svr.GetEndpoints()...)
	}

	closeFunc = func() {
		for _, svr := range svrs {
			svr.Close()
		}
		for _, cfg := range cfgs {
			cleanServer(cfg)
		}
	}

	leaderPeer := mustWaitLeader(c, svrs)
	grpcClient := mustNewGrpcClient(c, leaderPeer.GetAddr())
	bootstrapServer(c, newHeader(leaderPeer), grpcClient)
	return
}

func (s *testLeaderChangeSuite) TestLeaderConfigChange(c *C) {
	svrs, endpoints, closeFunc := s.prepareClusterN(c, 3)
	defer closeFunc()

	cli, err := NewClient(endpoints, SecurityOption{})
	c.Assert(err, IsNil)
	defer cli.Close()

	leader := s.mustGetLeader(c, cli.(*client), endpoints)
	s.verifyLeader(c, cli.(*client), leader)

	r := server.ReplicationConfig{MaxReplicas: 5}
	svrs[leader].SetReplicationConfig(r)
	svrs[leader].Close()
	// wait leader changes
	changed := false
	for i := 0; i < 20; i++ {
		mustWaitLeader(c, svrs)
		newLeader := s.mustGetLeader(c, cli.(*client), endpoints)
		if newLeader != leader {
			s.verifyLeader(c, cli.(*client), newLeader)
			changed = true
			nr := svrs[newLeader].GetReplicationConfig().MaxReplicas
			c.Assert(nr, Equals, uint64(5))
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.Assert(changed, IsTrue)
}

func (s *testLeaderChangeSuite) TestMemberList(c *C) {
	_, endpoints, closeFunc := s.prepareClusterN(c, 2)
	defer closeFunc()

	cli, err := NewClient(endpoints[:1], SecurityOption{})
	c.Assert(err, IsNil)
	cli.Close()

	sort.Strings(cli.(*client).urls)
	sort.Strings(endpoints)
	c.Assert(cli.(*client).urls, DeepEquals, endpoints)
}

func (s *testLeaderChangeSuite) TestLeaderChange(c *C) {
	svrs, endpoints, closeFunc := s.prepareClusterN(c, 3)
	defer closeFunc()

	cli, err := NewClient(endpoints, SecurityOption{})
	c.Assert(err, IsNil)
	defer cli.Close()

	var p1, l1 int64
	testutil.WaitUntil(c, func(c *C) bool {
		var err error
		p1, l1, err = cli.GetTS(context.Background())
		if err == nil {
			return true
		}
		c.Log(err)
		return false
	})

	leader := s.mustGetLeader(c, cli.(*client), endpoints)
	s.verifyLeader(c, cli.(*client), leader)

	svrs[leader].Close()
	delete(svrs, leader)

	mustWaitLeader(c, svrs)
	newLeader := s.mustGetLeader(c, cli.(*client), endpoints)
	c.Assert(newLeader, Not(Equals), leader)
	s.verifyLeader(c, cli.(*client), newLeader)

	for i := 0; i < 20; i++ {
		p2, l2, err := cli.GetTS(context.Background())
		if err == nil {
			c.Assert(p1<<18+l1, Less, p2<<18+l2)
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	c.Error("failed getTS from new leader after 10 seconds")
}

func (s *testLeaderChangeSuite) TestLeaderTransfer(c *C) {
	servers, endpoints, closeFunc := s.prepareClusterN(c, 2)
	defer closeFunc()

	cli, err := NewClient(endpoints, SecurityOption{})
	c.Assert(err, IsNil)
	defer cli.Close()

	quit := make(chan struct{})
	var physical, logical int64
	testutil.WaitUntil(c, func(c *C) bool {
		physical, logical, err = cli.GetTS(context.Background())
		if err == nil {
			return true
		}
		c.Log(err)
		return false
	})
	lastTS := s.makeTS(physical, logical)
	go func() {
		for {
			select {
			case <-quit:
				return
			default:
			}

			physical, logical, err1 := cli.GetTS(context.Background())
			if err1 == nil {
				ts := s.makeTS(physical, logical)
				c.Assert(lastTS, Less, ts)
				lastTS = ts
			}
			time.Sleep(time.Millisecond)
		}
	}()

	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: time.Second,
	})
	c.Assert(err, IsNil)
	leaderPath := filepath.Join("/pd", strconv.FormatUint(cli.GetClusterID(context.Background()), 10), "leader")
	for i := 0; i < 10; i++ {
		mustWaitLeader(c, servers)
		_, err = etcdCli.Delete(context.TODO(), leaderPath)
		c.Assert(err, IsNil)
		// Sleep to make sure all servers are notified and starts campaign.
		time.Sleep(time.Second)
	}
	close(quit)
}

func (s *testLeaderChangeSuite) makeTS(physical, logical int64) uint64 {
	return uint64(physical<<18 + logical)
}

func (s *testLeaderChangeSuite) mustGetLeader(c *C, cli *client, urls []string) string {
	for _, u := range urls {
		members, err := cli.getMembers(context.Background(), u)
		if err != nil || members.GetLeader() == nil || len(members.GetLeader().GetClientUrls()) == 0 {
			continue
		}
		return members.GetLeader().GetClientUrls()[0]
	}
	c.Fatal("failed get leader")
	return ""
}

func (s *testLeaderChangeSuite) verifyLeader(c *C, cli *client, leader string) {
	cli.scheduleCheckLeader()
	time.Sleep(time.Millisecond * 500)

	cli.connMu.RLock()
	defer cli.connMu.RUnlock()
	c.Assert(cli.connMu.leader, Equals, leader)
}
