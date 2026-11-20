// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/tidb/pkg/ingestor/engineapi"
	"github.com/pingcap/tidb/pkg/ingestor/simplesst"
	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/objstore"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var errLostReceipt = errors.New("injected loss of successful ingest receipt")

type importer struct {
	client        *txnkv.Client
	pd            pd.Client
	dir           string
	workers       int
	mutations     int
	regions       map[uint64]bool
	files         int
	rpcs          int
	cfs           map[string]int
	mergeFixtures []mergeFixture
}

type intent struct {
	Kind     string `json:"kind"`
	Digest   string `json:"digest"`
	ImportTS uint64 `json:"import_ts"`
	Count    int    `json:"count"`
	Status   string `json:"status"`
}

func (im *importer) journal(ctx context.Context, p splitPlan, seq int, in intent) error {
	key := []byte(fmt.Sprintf("prototype/g%d/rpc/%08d", p.Generation, seq))
	phase, _, _ := strings.Cut(in.Kind, "/")
	return transact(ctx, im.client, func(txn *transaction.KVTxn) error {
		state, err := getOptional(ctx, txn, stateKey(p))
		if err != nil {
			return err
		}
		if !bytes.Equal(state, []byte("BUILDING")) {
			return fmt.Errorf("generation is not active: %s", state)
		}
		sealed, err := getOptional(ctx, txn, []byte(fmt.Sprintf("prototype/g%d/sealed/%s", p.Generation, phase)))
		if err != nil {
			return err
		}
		if sealed != nil {
			return fmt.Errorf("phase %s is sealed", phase)
		}
		old, err := getOptional(ctx, txn, key)
		if err != nil {
			return err
		}
		if in.Status == "PENDING" && old != nil {
			return fmt.Errorf("intent cannot be resent")
		}
		if in.Status == "APPLIED" {
			var previous intent
			if err = json.Unmarshal(old, &previous); err != nil {
				return err
			}
			if previous.Status != "PENDING" || previous.Digest != in.Digest || previous.Kind != in.Kind ||
				previous.ImportTS != in.ImportTS || previous.Count != in.Count {
				return fmt.Errorf("receipt identity mismatch")
			}
		}
		// Serializes intent/receipt with phase sealing and quarantine.
		if err = txn.Set(stateKey(p), state); err != nil {
			return err
		}
		return putJSON(txn, key, in)
	})
}

func allowedKey(p splitPlan, kind string, key []byte) bool {
	pid, idx, record, err := tablecodec.DecodeKeyHead(kv.Key(key))
	if err != nil {
		return false
	}
	_, target := p.Parents[pid]
	switch kind {
	case "record":
		return target && record
	case "local":
		return target && !record && (idx == 1 || idx == 2)
	case "global":
		ids := indexes(p, true)
		return pid == logicalID && !record && (idx == ids[0].ID || idx == ids[1].ID)
	}
	return false
}

func (im *importer) sorted(ctx context.Context, p splitPlan, kind string, input []pair) ([]pair, error) {
	storage, err := objstore.NewLocalStorage(im.dir)
	if err != nil {
		return nil, err
	}
	type result struct {
		files []string
		err   error
	}
	results := make(chan result, im.workers)
	var wg sync.WaitGroup
	for w := range im.workers {
		wg.Go(func() {
			var files []string
			writer := simplesst.NewWriterBuilder().SetMemorySizeLimit(64<<10).SetBlockSize(64<<10).
				SetOnDup(engineapi.OnDuplicateKeyError).
				SetOnCloseFunc(func(s *simplesst.WriterSummary) {
					for _, stat := range s.MultipleFilesStats {
						for _, f := range stat.Filenames {
							files = append(files, f[0])
						}
					}
				}).Build(storage, filepath.Join(fmt.Sprintf("generation-%d", p.Generation), kind), fmt.Sprint(w))
			var writeErr error
			// Reverse traversal makes input deliberately unsorted even within a source.
			for i := len(input) - 1; i >= 0; i-- {
				if i%im.workers != w {
					continue
				}
				if !allowedKey(p, kind, input[i].Key) {
					writeErr = fmt.Errorf("forbidden %s key %x", kind, input[i].Key)
					break
				}
				if writeErr = writer.WriteRow(ctx, input[i].Key, input[i].Value, nil); writeErr != nil {
					break
				}
			}
			closeErr := writer.Close(ctx)
			results <- result{files, errors.Join(writeErr, closeErr)}
		})
	}
	wg.Wait()
	close(results)
	var files []string
	for r := range results {
		if r.err != nil {
			return nil, r.err
		}
		files = append(files, r.files...)
	}
	im.files += len(files)
	if len(files) == 0 {
		if len(input) != 0 {
			return nil, fmt.Errorf("nonempty input has no output files")
		}
		return nil, nil
	}
	iter, err := simplesst.NewMergeKVIter(ctx, files, make([]uint64, len(files)), storage, 64<<10, false, 1)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	out := make([]pair, 0, len(input))
	for iter.Next() {
		key := bytes.Clone(iter.Key())
		if !allowedKey(p, kind, key) {
			return nil, fmt.Errorf("merged key outside namespace")
		}
		if len(out) > 0 && bytes.Compare(out[len(out)-1].Key, key) >= 0 {
			return nil, fmt.Errorf("duplicate/nonascending merged key %x", key)
		}
		out = append(out, pair{key, bytes.Clone(iter.Value())})
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}
	if len(out) != len(input) {
		return nil, fmt.Errorf("sort count mismatch %d/%d", len(out), len(input))
	}
	// Input and merged bytes must agree exactly, not just count.
	expected := slices.Clone(input)
	slices.SortFunc(expected, func(a, b pair) int { return bytes.Compare(a.Key, b.Key) })
	for i := range out {
		if !bytes.Equal(expected[i].Key, out[i].Key) || !bytes.Equal(expected[i].Value, out[i].Value) {
			return nil, fmt.Errorf("sort changed KV bytes")
		}
	}
	return out, nil
}

func (im *importer) quarantine(ctx context.Context, p splitPlan) error {
	return transact(ctx, im.client, func(txn *transaction.KVTxn) error {
		state, err := getOptional(ctx, txn, stateKey(p))
		if err != nil {
			return err
		}
		if bytes.Equal(state, []byte("PUBLISHED")) {
			return fmt.Errorf("cannot quarantine published generation")
		}
		if err = txn.Set(gateKey(p), []byte("closed")); err != nil {
			return err
		}
		return txn.Set(stateKey(p), []byte("QUARANTINED"))
	})
}

func (im *importer) bulk(ctx context.Context, p splitPlan, kind string, input []pair, loseReceipt bool) (err error) {

	// Reserve the phase once, before any mutable output. A restarted actor cannot
	// interpret an unfinished phase as permission to resend its RPCs.
	err = transact(ctx, im.client, func(txn *transaction.KVTxn) error {
		state, e := getOptional(ctx, txn, stateKey(p))
		if e != nil {
			return e
		}
		if string(state) != "BUILDING" {
			return fmt.Errorf("phase on inactive generation")
		}
		key := []byte(fmt.Sprintf("prototype/g%d/started/%s", p.Generation, kind))
		old, e := getOptional(ctx, txn, key)
		if e != nil {
			return e
		}
		if old != nil {
			return fmt.Errorf("phase %s cannot be repeated", kind)
		}
		if e = txn.Set(stateKey(p), state); e != nil {
			return e
		}
		return putJSON(txn, key, map[string]any{"kind": kind, "count": len(input)})
	})
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, im.quarantine(ctx, p))
		}
	}()
	sorted, err := im.sorted(ctx, p, kind, input)
	if err != nil {
		return err
	}
	ts, err := im.client.GetTimestamp(ctx)
	if err != nil {
		return err
	}
	for len(sorted) > 0 {
		region, e := im.pd.GetRegion(ctx, codec.EncodeBytes(nil, sorted[0].Key))
		if e != nil {
			return e
		}
		if region == nil || region.Leader == nil || len(region.Meta.Peers) != 1 {
			return fmt.Errorf("prototype needs a healthy single-replica region")
		}
		n := 0
		for n < len(sorted) {
			encoded := codec.EncodeBytes(nil, sorted[n].Key)
			if len(region.Meta.EndKey) > 0 && bytes.Compare(encoded, region.Meta.EndKey) >= 0 {
				break
			}
			n++
		}
		if n == 0 {
			return fmt.Errorf("PD returned noncovering region")
		}
		batch := sorted[:n]
		sorted = sorted[n:]
		store, e := im.pd.GetStore(ctx, region.Leader.StoreId)
		if e != nil {
			return e
		}
		conn, e := grpc.NewClient(store.Address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
		if e != nil {
			return e
		}
		e = im.send(ctx, p, kind, ts, batch, region.Meta, region.Leader, sst.NewImportSSTClient(conn), loseReceipt)
		closeErr := conn.Close()
		if e != nil {
			return errors.Join(e, closeErr)
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return transact(ctx, im.client, func(txn *transaction.KVTxn) error {
		state, e := getOptional(ctx, txn, stateKey(p))
		if e != nil {
			return e
		}
		if string(state) != "BUILDING" {
			return fmt.Errorf("seal on inactive generation")
		}
		if e = txn.Set(stateKey(p), state); e != nil {
			return e
		}
		return txn.Set([]byte(fmt.Sprintf("prototype/g%d/sealed/%s", p.Generation, kind)), []byte("sealed"))
	})
}

// digestPairs binds immutable payload bytes with length delimiters.
func digestPairs(pairs []pair) string {
	h := sha256.New()
	for _, p := range pairs {
		h.Write(codec.EncodeBytes(nil, p.Key))
		h.Write(codec.EncodeBytes(nil, p.Value))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (im *importer) send(ctx context.Context, p splitPlan, kind string, ts uint64, batch []pair,
	region *metapb.Region, leader *metapb.Peer, cli sst.ImportSSTClient, loseReceipt bool) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	im.regions[region.Id] = true
	digest := digestPairs(batch)
	in := intent{Kind: kind + "/write", Digest: digest, ImportTS: ts, Count: len(batch), Status: "PENDING"}
	im.rpcs++
	seq := im.rpcs
	if err := im.journal(ctx, p, seq, in); err != nil {
		return err
	}
	stream, err := cli.Write(ctx)
	if err != nil {
		return err
	}
	id := uuid.New()
	meta := &sst.SSTMeta{Uuid: id[:], RegionId: region.Id, RegionEpoch: region.RegionEpoch,
		ApiVersion: kvrpcpb.APIVersion_V1, Range: &sst.Range{Start: codec.EncodeBytes(nil, batch[0].Key), End: codec.EncodeBytes(nil, batch[len(batch)-1].Key)}}
	if err = stream.Send(&sst.WriteRequest{Chunk: &sst.WriteRequest_Meta{Meta: meta}}); err != nil {
		return err
	}
	for start := 0; start < len(batch); start += 128 {
		end := min(start+128, len(batch))
		pairs := make([]*sst.Pair, 0, end-start)
		for _, p := range batch[start:end] {
			pairs = append(pairs, &sst.Pair{Key: p.Key, Value: p.Value})
		}
		if err = stream.Send(&sst.WriteRequest{Chunk: &sst.WriteRequest_Batch{Batch: &sst.WriteBatch{CommitTs: ts, Pairs: pairs}}}); err != nil {
			return err
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("write SST: %v", resp.Error)
	}
	if len(resp.Metas) == 0 {
		return fmt.Errorf("nonempty write returned no SSTs")
	}
	if im.cfs == nil {
		im.cfs = make(map[string]int)
	}
	for _, meta := range resp.Metas {
		im.cfs[meta.CfName]++
	}

	in.Status = "APPLIED"
	if err = im.journal(ctx, p, seq, in); err != nil {
		return err
	}
	// Every returned CF participates in exactly one MultiIngest request. There is
	// no background retry loop, including after an ambiguous network error.
	metaBytes, err := json.Marshal(resp.Metas)
	if err != nil {
		return err
	}
	metaDigest := sha256.Sum256(metaBytes)
	in = intent{Kind: kind + "/ingest", Digest: hex.EncodeToString(metaDigest[:]), ImportTS: ts, Count: len(resp.Metas), Status: "PENDING"}
	im.rpcs++
	seq = im.rpcs
	if err = im.journal(ctx, p, seq, in); err != nil {
		return err
	}
	response, err := cli.MultiIngest(ctx, &sst.MultiIngestRequest{Context: &kvrpcpb.Context{
		RegionId: region.Id, RegionEpoch: region.RegionEpoch, Peer: leader}, Ssts: resp.Metas})
	if err != nil {
		return err
	}
	if response.Error != nil {
		return fmt.Errorf("ingest SST: %v", response.Error)
	}
	if loseReceipt {
		return errLostReceipt
	}
	in.Status = "APPLIED"
	return im.journal(ctx, p, seq, in)
}
