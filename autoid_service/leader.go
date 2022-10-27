package autoid

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

const (
	autoIDLeaderPath = "tidb/autoid/leader"
)

type leaderShip struct {
	cli      *clientv3.Client
	isLeader atomic.Bool

	mock bool // For test
}

func (ls *leaderShip) IsLeader() bool {
	if ls.mock {
		return true
	}
	return ls.isLeader.Load()
}

func (ls *leaderShip) campaignLoop(ctx context.Context, addr string) {
	s, err := newSession(ctx, ls.cli, 50, 15)
	if err != nil {
		log.Error("new session error?")
	}

	for {
		select {
		case <-s.Done():
			s, err = newSession(ctx, ls.cli, 50, 15)
			if err != nil {
				log.Error("new session error?")
			}
		default:
		}

		e := concurrency.NewElection(s, autoIDLeaderPath)
		// follower blocks here.

		err := e.Campaign(ctx, addr)
		if err != nil {
			log.Warn("fail to campaign?")
			continue
		}

		ls.isLeader.Store(true)
		ch := e.Observe(ctx)
		resp, ok := <-ch
		if !ok {
			log.Fatal("could not wait for first election; channel closed")
		}

		s := string(resp.Kvs[0].Value)
		if s != addr {
			log.Fatal("wrong election result. got %s, wanted id")
		}
		<-ch
		ls.isLeader.Store(false)
	}
}

func newSession(ctx context.Context, etcdCli *clientv3.Client, retryCnt, ttl int) (*concurrency.Session, error) {
	var err error

	var etcdSession *concurrency.Session
	failedCnt := 0
	for i := 0; i < retryCnt; i++ {
		etcdSession, err = concurrency.NewSession(etcdCli, concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
		if err == nil {
			break
		}

		if failedCnt%5 == 0 {
			log.Warn("failed to new session to etcd", zap.Error(err))
		}

		time.Sleep(200 * time.Millisecond)
		failedCnt++
	}
	return etcdSession, errors.Trace(err)
}
