package autoid

import (
	"context"
	"fmt"
	"sync/atomic"
	"strconv"
	"runtime/debug"

	clientv3 "go.etcd.io/etcd/client/v3"
	"github.com/pingcap/errors"
)

type persist interface {
	syncID(ctx context.Context, dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) error
	loadID(ctx context.Context, dbID, tblID int64) (uint64, error)
}

type mockPersist struct {
	data map[autoIDKey]uint64
}

func (p *mockPersist) syncID(ctx context.Context, dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) error {
	key := autoIDKey{dbID: dbID, tblID: tblID}
	p.data[key] = val
	atomic.StoreInt64(addr, int64(val))
	<-done
	return nil
}

func (p *mockPersist) loadID(ctx context.Context, dbID, tblID int64) (uint64, error) {
	key := autoIDKey{dbID: dbID, tblID: tblID}
	return p.data[key], nil
}

const (
	AutoIDKeyPattern = "tidb/autoid/db/%d/tbl/%d"
)

type etcdPersist struct {
	cli      *clientv3.Client
}

func (p *etcdPersist) syncID(ctx context.Context, dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) error {
	key := fmt.Sprintf(AutoIDKeyPattern, dbID, tblID)
	fmt.Println("before syncID... put", key, val)
	_, err := p.cli.Put(ctx, key, strconv.FormatUint(val, 10))
	fmt.Println("after syncID... put", key, val)
	atomic.StoreInt64(addr, int64(val))
	<-done
	debug.PrintStack()
	if err != nil {
		fmt.Println("sync id error ==", err)
		return err
	}
	return nil
}

func (p *etcdPersist) loadID(ctx context.Context, dbID, tblID int64) (uint64, error) {
	key := fmt.Sprintf(AutoIDKeyPattern, dbID, tblID)
	resp, err := p.cli.Get(ctx, key)
	if err != nil {
		return 0, errors.Trace(err)
	}
	fmt.Println("load id get response ==", resp)
	if len(resp.Kvs) == 0 {
		fmt.Println(" LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLoad get empty response!!!!!!")
		return 0, nil
	}

	valStr := resp.Kvs[0].Value
	if len(valStr) == 0 {
		return 0, errors.New("invalid data")
	}
	val, err :=  strconv.ParseUint(string(valStr), 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}
