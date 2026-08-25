// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/rawkv"
	"github.com/tikv/client-go/v2/txnkv"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func tagged(prefix []byte, suffix byte) []byte {
	key := append([]byte(nil), prefix...)
	return append(key, suffix)
}

func requireBytes(actual, expected []byte, label string) {
	if !bytes.Equal(actual, expected) {
		panic(fmt.Sprintf("%s: got %q, want %q", label, actual, expected))
	}
}

func requireKeys(actual [][]byte, expected ...[]byte) {
	if len(actual) != len(expected) {
		panic(fmt.Sprintf("keys: got %q, want %q", actual, expected))
	}
	for i := range expected {
		requireBytes(actual[i], expected[i], fmt.Sprintf("key[%d]", i))
	}
}

func main() {
	apiVersion := flag.Int("api-version", 0, "TiKV API version: 1 or 2")
	pd := flag.String("pd", "127.0.0.1:2379", "PD endpoint")
	flag.Parse()
	if *apiVersion != 1 && *apiVersion != 2 {
		fmt.Fprintln(os.Stderr, "--api-version must be 1 or 2")
		os.Exit(2)
	}

	ctx := context.Background()
	api := kvrpcpb.APIVersion_V1
	rawOptions := []rawkv.ClientOpt{rawkv.WithAPIVersion(api)}
	txnOptions := []txnkv.ClientOpt{txnkv.WithAPIVersion(api)}
	if *apiVersion == 2 {
		api = kvrpcpb.APIVersion_V2
		rawOptions = []rawkv.ClientOpt{
			rawkv.WithAPIVersion(api),
			rawkv.WithKeyspace("DEFAULT"),
		}
		txnOptions = []txnkv.ClientOpt{
			txnkv.WithAPIVersion(api),
			txnkv.WithKeyspace("DEFAULT"),
		}
	}

	prefix := []byte(fmt.Sprintf("client-parity/api%d/", *apiVersion))
	end := tagged(prefix, 0xff)
	a := tagged(prefix, 'a')
	b := tagged(prefix, 'b')
	c := tagged(prefix, 'c')
	d := tagged(prefix, 'd')
	missing := tagged(prefix, 'm')

	raw, err := rawkv.NewClientWithOpts(ctx, []string{*pd}, rawOptions...)
	must(err)
	defer raw.Close()
	must(raw.DeleteRange(ctx, prefix, end))
	must(raw.BatchPut(ctx, [][]byte{a, c}, [][]byte{[]byte("raw-a"), []byte("raw-c")}))
	values, err := raw.BatchGet(ctx, [][]byte{c, missing, a})
	must(err)
	if len(values) != 3 {
		panic(fmt.Sprintf("batch get length: got %d", len(values)))
	}
	requireBytes(values[0], []byte("raw-c"), "batch get c")
	if values[1] != nil {
		panic(fmt.Sprintf("batch get missing: got %q", values[1]))
	}
	requireBytes(values[2], []byte("raw-a"), "batch get a")
	keys, _, err := raw.Scan(ctx, prefix, end, 16)
	must(err)
	requireKeys(keys, a, c)
	keys, _, err = raw.ReverseScan(ctx, end, prefix, 16)
	must(err)
	requireKeys(keys, c, a)
	must(raw.DeleteRange(ctx, prefix, end))
	value, err := raw.Get(ctx, a)
	must(err)
	if value != nil {
		panic(fmt.Sprintf("raw delete range: got %q", value))
	}

	txnClient, err := txnkv.NewClient([]string{*pd}, txnOptions...)
	must(err)
	defer txnClient.Close()
	writer, err := txnClient.Begin()
	must(err)
	must(writer.Set(b, []byte("txn-b")))
	must(writer.Set(d, []byte("txn-d")))
	must(writer.Commit(ctx))

	reader, err := txnClient.Begin()
	must(err)
	entry, err := reader.Get(ctx, b)
	must(err)
	requireBytes(entry.Value, []byte("txn-b"), "txn get b")
	entry, err = reader.Get(ctx, missing)
	if !tikverr.IsErrNotFound(err) {
		panic(fmt.Sprintf("txn get missing: got entry=%q err=%v", entry.Value, err))
	}
	iter, err := reader.Iter(prefix, end)
	must(err)
	var iterKeys [][]byte
	for iter.Valid() {
		iterKeys = append(iterKeys, append([]byte(nil), iter.Key()...))
		must(iter.Next())
	}
	iter.Close()
	requireKeys(iterKeys, b, d)
	must(reader.Commit(ctx))

	cleaner, err := txnClient.Begin()
	must(err)
	must(cleaner.Delete(b))
	must(cleaner.Delete(d))
	must(cleaner.Commit(ctx))
	fmt.Printf(
		"client-parity api=%d raw=batch_get:c,-,a scan:a,c reverse:c,a txn=get:b,- scan:b,d\n",
		*apiVersion,
	)
}
