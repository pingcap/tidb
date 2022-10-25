package autoid

import (
	"context"
	"fmt"
	"runtime/debug"
	"strconv"
	"sync/atomic"

	"github.com/pingcap/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type Persist interface {
	SyncID(ctx context.Context, dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) error
	LoadID(ctx context.Context, dbID, tblID int64) (uint64, error)
}

var (
	_ Persist = &mockPersist{}
	_ Persist = &etcdPersist{}
)

type mockPersist struct {
	data map[autoIDKey]uint64
}

func (p *mockPersist) SyncID(ctx context.Context, dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) error {
	key := autoIDKey{dbID: dbID, tblID: tblID}
	p.data[key] = val
	atomic.StoreInt64(addr, int64(val))
	<-done
	return nil
}

func (p *mockPersist) LoadID(ctx context.Context, dbID, tblID int64) (uint64, error) {
	key := autoIDKey{dbID: dbID, tblID: tblID}
	return p.data[key], nil
}

const (
	autoIDKeyPattern = "tidb/autoid/db/%d/tbl/%d"
)

type etcdPersist struct {
	cli *clientv3.Client
}

func (p *etcdPersist) SyncID(ctx context.Context, dbID, tblID int64, val uint64, addr *int64, done <-chan struct{}) error {
	key := fmt.Sprintf(autoIDKeyPattern, dbID, tblID)
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

func (p *etcdPersist) LoadID(ctx context.Context, dbID, tblID int64) (uint64, error) {
	key := fmt.Sprintf(autoIDKeyPattern, dbID, tblID)
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
	val, err := strconv.ParseUint(string(valStr), 10, 64)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}
