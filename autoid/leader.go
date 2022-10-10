package autoid

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/juju/errors"
	"github.com/pingcap/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

type leaderShip struct {
	cli      *clientv3.Client
	isLeader atomic.Bool
}

func (ls *leaderShip) IsLeader() bool {
	return ls.isLeader.Load()
}

func (ls *leaderShip) campaignLoop(ctx context.Context) {
	fmt.Println("before new session!!!")
	s, err := newSession(ctx, "xxx", ls.cli, 50, 15)
	if err != nil {
		// TODO
		fmt.Println("error not printed???")
		log.Error("new session error?")
	}
	fmt.Println("enter campaign loop!!!")

	for {
		select {
		case <-s.Done():
			s, err = newSession(ctx, "xxx", ls.cli, 50, 15)
			if err != nil {
				// TODO
				log.Error("new session error?")
			}
		default:
		}

		e := concurrency.NewElection(s, "path")
		// follower blocks here.

		fmt.Println("before campaign leader!!!")
		err := e.Campaign(ctx, "id")
		if err != nil {
			log.Warn("fail to campaign?")
			continue
		}
		fmt.Println("campaign leader success!")

		ls.isLeader.Store(true)
		ch := e.Observe(ctx)
		resp, ok := <-ch
		if !ok {
			log.Fatal("could not wait for first election; channel closed")
		}

		s := string(resp.Kvs[0].Value)
		if s != "id" {
			log.Fatal("wrong election result. got %s, wanted id")
		}
		fmt.Println(resp)
		<-ch
		ls.isLeader.Store(false)
	}
}

func newSession(ctx context.Context, logPrefix string, etcdCli *clientv3.Client, retryCnt, ttl int) (*concurrency.Session, error) {
	var err error

	var etcdSession *concurrency.Session
	failedCnt := 0
	for i := 0; i < retryCnt; i++ {
		fmt.Println("once ..........")
		//		if err = contextDone(ctx, err); err != nil {
		//			return etcdSession, errors.Trace(err)
		//		}

		// startTime := time.Now()
		etcdSession, err = concurrency.NewSession(etcdCli, concurrency.WithTTL(ttl), concurrency.WithContext(ctx))
		// metrics.NewSessionHistogram.WithLabelValues(logPrefix, metrics.RetLabel(err)).Observe(time.Since(startTime).Seconds())
		if err == nil {
			fmt.Println("create session success?")
			break
		}

		fmt.Println("retry session success?")
		if failedCnt%5 == 0 {
			log.Warn("failed to new session to etcd", zap.String("ownerInfo", logPrefix), zap.Error(err))
		}

		time.Sleep(200 * time.Millisecond)
		failedCnt++
	}
	return etcdSession, errors.Trace(err)
}
