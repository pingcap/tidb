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
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/juju/errors"
	. "github.com/pingcap/check"
	"github.com/pingcap/pd/pkg/etcdutil"
)

var _ = Suite(&testJoinServerSuite{})

var (
	errTimeout = errors.New("timeout")
)

type testJoinServerSuite struct{}

func newTestMultiJoinConfig(count int) []*Config {
	cfgs := NewTestMultiConfig(count)
	for i := 0; i < count; i++ {
		cfgs[i].InitialCluster = ""
		if i == 0 {
			continue
		}
		cfgs[i].Join = cfgs[i-1].ClientUrls
	}
	return cfgs
}

func waitMembers(svrs []*Server, c int) error {
	// maxRetryTime * waitInterval = 5s
	maxRetryCount := 10
	waitInterval := 500 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), etcdutil.DefaultDialTimeout)
	defer cancel()

Outloop:
	for _, svr := range svrs {
		client := svr.GetClient()
		for retryCount := maxRetryCount; retryCount != 0; retryCount-- {
			time.Sleep(waitInterval)
			listResp, err := client.MemberList(ctx)
			if err != nil {
				continue
			}

			count := 0
			for _, memb := range listResp.Members {
				if len(memb.Name) == 0 {
					// unstarted, see:
					// https://github.com/coreos/etcd/blob/master/etcdctl/ctlv3/command/printer.go#L60
					// https://coreos.com/etcd/docs/latest/runtime-configuration.html#add-a-new-member
					continue
				}
				count++
			}

			if count >= c {
				continue Outloop
			}
		}
		return errors.New("waitMembers Timeout")
	}
	return nil
}

// Notice: cfg has changed.
func startPdWith(cfg *Config) (*Server, error) {
	svrCh := make(chan *Server)
	errCh := make(chan error, 1)
	abortCh := make(chan struct{}, 1)

	go func() {
		err := cfg.adjust()
		if err != nil {
			errCh <- errors.Trace(err)
			return
		}
		err = PrepareJoinCluster(cfg)
		if err != nil {
			errCh <- errors.Trace(err)
			return
		}
		svr, err := CreateServer(cfg, nil)
		if err != nil {
			errCh <- errors.Trace(err)
			return
		}
		err = svr.Run()
		if err != nil {
			errCh <- errors.Trace(err)
			svr.Close()
			return
		}

		select {
		case <-abortCh:
			svr.Close()
			return
		default:
		}

		svrCh <- svr
	}()

	// It should be enough for starting a PD.
	timer := time.NewTimer(etcdutil.DefaultRequestTimeout)
	defer timer.Stop()

	select {
	case s := <-svrCh:
		return s, nil
	case e := <-errCh:
		return nil, errors.Trace(e)
	case <-timer.C:
		abortCh <- struct{}{}
		return nil, errTimeout
	}
}

type cleanUpFunc func()

func mustNewJoinCluster(c *C, num int) ([]*Config, []*Server, cleanUpFunc) {
	svrs := make([]*Server, 0, num)
	cfgs := newTestMultiJoinConfig(num)

	for i, cfg := range cfgs {
		svr, err := startPdWith(cfg)
		c.Assert(err, IsNil)
		svrs = append(svrs, svr)
		waitMembers(svrs, i+1)
	}

	// Clean up.
	clean := func() {
		for _, s := range svrs {
			s.Close()
		}
		for _, cfg := range cfgs {
			cleanServer(cfg)
		}
	}

	return cfgs, svrs, clean
}

func isConnective(target, peer *Server) error {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	ch := make(chan error)
	go func() {
		// Put something to cluster.
		key := fmt.Sprintf("%d", rand.Int63())
		value := key
		client := peer.GetClient()
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		_, err := client.Put(ctx, key, value)
		cancel()
		if err != nil {
			ch <- errors.Trace(err)
			return
		}

		client = target.GetClient()
		ctx, cancel = context.WithTimeout(context.Background(), requestTimeout)
		resp, err := client.Get(ctx, key)
		cancel()
		if err != nil {
			ch <- errors.Trace(err)
			return
		}
		if len(resp.Kvs) == 0 {
			ch <- errors.Errorf("not match, got: %s, expect: %s", resp.Kvs, value)
			return
		}
		if string(resp.Kvs[0].Value) != value {
			ch <- errors.Errorf("not match, got: %s, expect: %s", resp.Kvs[0].Value, value)
			return
		}
		ch <- nil
	}()

	select {
	case err := <-ch:
		return err
	case <-timer.C:
		return errTimeout
	}
}

// A new PD joins an existing cluster.
func (s *testJoinServerSuite) TestNewPDJoinsExistingCluster(c *C) {
	_, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	err := waitMembers(svrs, 3)
	c.Assert(err, IsNil)
}

// A PD joins itself.
func (s *testJoinServerSuite) TestPDJoinsItself(c *C) {
	cfg := newTestMultiJoinConfig(1)[0]
	cfg.Join = cfg.AdvertiseClientUrls

	_, err := startPdWith(cfg)
	c.Assert(err, NotNil)
}

// A failed PD re-joins the previous cluster.
func (s *testJoinServerSuite) TestFailedPDJoinsPreviousCluster(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	target := 1
	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)
	err := os.RemoveAll(cfgs[target].DataDir)
	c.Assert(err, IsNil)

	cfgs[target].InitialCluster = ""
	_, err = startPdWith(cfgs[target])
	c.Assert(err, NotNil)
}

// A failed PD tries to join the previous cluster but it has been deleted
// during its downtime.
func (s *testJoinServerSuite) TestFailedAndDeletedPDJoinsPreviousCluster(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	target := 2
	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), etcdutil.DefaultDialTimeout)
	defer cancel()
	client := svrs[0].GetClient()
	client.MemberRemove(ctx, svrs[target].ID())

	cfgs[target].InitialCluster = ""
	_, err := startPdWith(cfgs[target])
	// Deleted PD will not start successfully.
	c.Assert(err, Equals, errTimeout)

	list, err := etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(len(list.Members), Equals, 2)
}

// A deleted PD joins the previous cluster.
func (s *testJoinServerSuite) TestDeletedPDJoinsPreviousCluster(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	target := 2
	ctx, cancel := context.WithTimeout(context.Background(), etcdutil.DefaultDialTimeout)
	defer cancel()
	client := svrs[0].GetClient()
	client.MemberRemove(ctx, svrs[target].ID())

	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)

	cfgs[target].InitialCluster = ""
	_, err := startPdWith(cfgs[target])
	// A deleted PD will not start successfully.
	c.Assert(err, Equals, errTimeout)

	list, err := etcdutil.ListEtcdMembers(client)
	c.Assert(err, IsNil)
	c.Assert(len(list.Members), Equals, 2)
}

// General join case.
func (s *testJoinServerSuite) TestGeneralJoin(c *C) {
	cfgs, svrs, clean := mustNewJoinCluster(c, 3)
	defer clean()

	target := rand.Intn(len(cfgs))
	other := 0
	for {
		if other != target {
			break
		}
		other = rand.Intn(len(cfgs))
	}
	// Put some data.
	err := isConnective(svrs[target], svrs[other])
	c.Assert(err, IsNil)

	svrs[target].Close()
	time.Sleep(500 * time.Millisecond)

	cfgs[target].InitialCluster = ""
	re, err := startPdWith(cfgs[target])
	c.Assert(err, IsNil)
	defer re.Close()

	svrs = append(svrs[:target], svrs[target+1:]...)
	svrs = append(svrs, re)
	mustWaitLeader(c, svrs)

	err = isConnective(re, svrs[0])
	c.Assert(err, IsNil)
}
