// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.
package stream

import (
	"context"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/kv"
	kvutil "github.com/tikv/client-go/v2/kv"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// etcdSource is the adapter for etcd client and the `Source` interface.
type etcdSource struct {
	*clientv3.Client
}

// Scan ordered in range [from, to).
// return at most limit kvs at once.
// WARNING: The method for scanning keeps poor consistency: we may get different reversion of different pages.
// This might be acceptable just for calculating min backup ts or ranges to backup.
func (e etcdSource) Scan(
	ctx context.Context,
	from, to []byte,
	limit int,
) (kvs []kv.Entry, more bool, err error) {
	kvs = make([]kv.Entry, 0, limit)
	configs := []clientv3.OpOption{clientv3.WithRange(string(to)), clientv3.WithLimit(int64(limit))}
	resp, err := e.Get(ctx, string(from), configs...)
	if err != nil {
		err = errors.Annotatef(err, "failed to scan [%s, %s)", redact.Key(from), redact.Key(to))
		return
	}
	more = resp.More
	for _, kvp := range resp.Kvs {
		kvs = append(kvs, kv.Entry{Key: kvp.Key, Value: kvp.Value})
	}
	return
}

// source is the abstraction of some KV storage supports range scan.
type source interface {
	// Scan ordered in range [from, to).
	// return at most limit kvs at once.
	Scan(ctx context.Context, from, to []byte, limit int) (kvs []kv.Entry, more bool, err error)
}

type prefixScanner struct {
	src  source
	next []byte
	end  []byte
	done bool
}

// scanPrefix make a PrefixScanner from the source.
func scanPrefix(src source, prefix string) *prefixScanner {
	return &prefixScanner{
		src:  src,
		next: []byte(prefix),
		end:  kvutil.PrefixNextKey([]byte(prefix)),
	}
}

// scanEtcdPrefix is a shortcut for making a adaptor for etcd client and then create a PrefixScanner.
func scanEtcdPrefix(cli *clientv3.Client, prefix string) *prefixScanner {
	return scanPrefix(etcdSource{Client: cli}, prefix)
}

// Page scans one page of the source.
func (scan *prefixScanner) Page(ctx context.Context, size int) ([]kv.Entry, error) {
	kvs, more, err := scan.src.Scan(ctx, scan.next, scan.end, size)
	if err != nil {
		return nil, err
	}
	if !more {
		scan.done = true
	} else {
		// Append a 0x00 to the end, for getting the 'next key' of the last key.
		scan.next = append(kvs[len(kvs)-1].Key, 0)
	}
	return kvs, nil
}

// AllPages collects all pages into one vector.
// NOTE: like io.ReadAll, maybe the `size` parameter is redundant?
func (scan *prefixScanner) AllPages(ctx context.Context, size int) ([]kv.Entry, error) {
	kvs := make([]kv.Entry, 0, size)
	for !scan.Done() {
		newKvs, err := scan.Page(ctx, size)
		if err != nil {
			return kvs, err
		}
		kvs = append(kvs, newKvs...)
	}
	return kvs, nil
}

// Done checks whether the PrefixScanner scan all items in the range.
func (scan *prefixScanner) Done() bool {
	return scan.done
}
