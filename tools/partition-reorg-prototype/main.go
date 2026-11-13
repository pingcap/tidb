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
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	tikverr "github.com/tikv/client-go/v2/error"
	"github.com/tikv/client-go/v2/txnkv"
	"github.com/tikv/client-go/v2/txnkv/transaction"
	pd "github.com/tikv/pd/client"
	"github.com/tikv/pd/client/pkg/caller"
)

// Larger seed ranges would overlap the reserved scenario handle namespace.
const maxRowsPerParent = 100000

type runConfig struct {
	rows, workers, mutations int
	splitRegions             bool
}

type report struct {
	FinalRowDigest     string         `json:"final_row_digest"`
	Regions            int            `json:"split_regions"`
	IngestRegions      int            `json:"ingested_regions"`
	UnknownAppliedRows int            `json:"quarantined_applied_rows"`
	FinalRows          int            `json:"final_rows"`
	OnlineCommits      int            `json:"background_index_commits"`
	IndexConflicts     int            `json:"index_merge_conflicts"`
	PhaseConflicts     int            `json:"index_phase_conflicts"`
	Status             string         `json:"status"`
	Rows               int            `json:"rows"`
	Workers            int            `json:"workers"`
	Checks             []string       `json:"checks"`
	SortedFiles        int            `json:"sorted_files"`
	MutatingRPCs       int            `json:"mutating_rpcs"`
	SSTCFs             map[string]int `json:"sst_cfs"`
	Duration           string         `json:"duration"`
	Error              string         `json:"error,omitempty"`
}

func main() {
	addr := flag.String("pd", "", "owned loopback PD endpoint")
	clusterID := flag.Uint64("cluster-id", 0, "exact ID of the fresh disposable cluster")
	dir := flag.String("work-dir", "", "new directory for sorted runs and result.json")
	rows := flag.Int("rows-per-parent", 1000, "rows in each of two source partitions (minimum 16)")
	workers := flag.Int("workers", 4, "parallel sorted-run writers")
	mutations := flag.Int("dml-iterations", 64, "successful background updates per index group")
	split := flag.Bool("split-regions", false, "pre-split owned cluster by physical table namespace")
	timeout := flag.Duration("timeout", 5*time.Minute, "overall experiment deadline")
	flag.Parse()
	start := time.Now()
	result := report{Status: "FAIL", Workers: *workers}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	err := run(ctx, *addr, *clusterID, *dir, runConfig{rows: *rows, workers: *workers, mutations: *mutations, splitRegions: *split}, &result)
	result.Duration = time.Since(start).String()
	if err == nil {
		result.Status = "PASS"
	} else {
		result.Error = err.Error()
	}
	b, marshalErr := json.MarshalIndent(result, "", "  ")
	if marshalErr != nil {
		fmt.Fprintln(os.Stderr, marshalErr)
		os.Exit(1)
	}
	fmt.Println(string(b))
	if *dir != "" {
		if writeErr := os.WriteFile(filepath.Join(*dir, "result.json"), b, 0o600); writeErr != nil {
			fmt.Fprintln(os.Stderr, writeErr)
			os.Exit(1)
		}
	}
	if err != nil {
		os.Exit(1)
	}
}

func scan(ctx context.Context, c *txnkv.Client, ts uint64, prefix []byte) ([]pair, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	it, err := c.GetSnapshot(ts).Iter(prefix, kv.Key(prefix).PrefixNext())
	if err != nil {
		return nil, err
	}
	defer it.Close()
	var out []pair
	for it.Valid() {
		out = append(out, pair{bytes.Clone(it.Key()), bytes.Clone(it.Value())})
		if err = it.Next(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func records(ctx context.Context, c *txnkv.Client, ts uint64, pids []int64) ([]pair, error) {
	var out []pair
	for _, pid := range pids {
		rows, err := scan(ctx, c, ts, tablecodec.GenTableRecordPrefix(pid))
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

func project(input []pair, p splitPlan) ([]pair, error) {
	out := make([]pair, 0, len(input))
	for _, v := range input {
		pid, h, err := tablecodec.DecodeRecordKey(v.Key)
		if err != nil {
			return nil, err
		}
		r, err := decodeRow(v.Value)
		if err != nil {
			return nil, err
		}
		q, err := p.target(r.Route)
		if err != nil {
			return nil, err
		}
		if p.Parents[q] != pid {
			return nil, fmt.Errorf("source row routed outside its parent")
		}
		out = append(out, pair{recordKey(q, h.IntValue()), bytes.Clone(v.Value)})
	}
	return out, nil
}

func equalPairs(a, b []pair) error {
	less := func(a, b pair) int { return bytes.Compare(a.Key, b.Key) }
	slices.SortFunc(a, less)
	slices.SortFunc(b, less)
	if len(a) != len(b) {
		return fmt.Errorf("KV counts differ: %d != %d", len(a), len(b))
	}
	for i := range a {
		if i > 0 && bytes.Equal(a[i-1].Key, a[i].Key) {
			return fmt.Errorf("duplicate expected key %x", a[i].Key)
		}
		if !bytes.Equal(a[i].Key, b[i].Key) || !bytes.Equal(a[i].Value, b[i].Value) {
			return fmt.Errorf("KV mismatch at %x / %x", a[i].Key, b[i].Key)
		}
	}
	return nil
}

func activate(ctx context.Context, c *txnkv.Client, p splitPlan) error {
	return transact(ctx, c, func(txn *transaction.KVTxn) error {
		b, err := getOptional(ctx, txn, stateKey(p))
		if err != nil {
			return err
		}
		if b != nil {
			return fmt.Errorf("generation already exists")
		}
		if err = putJSON(txn, []byte(fmt.Sprintf("prototype/g%d/plan", p.Generation)), p); err != nil {
			return err
		}
		if err = txn.Set(stateKey(p), []byte("BUILDING")); err != nil {
			return err
		}

		for _, global := range []bool{false, true} {
			if err = txn.Set(indexStateKey(p, global), []byte(indexInactive)); err != nil {
				return err
			}
		}
		return txn.Set(gateKey(p), []byte("open"))
	})
}

func verifyRows(ctx context.Context, c *txnkv.Client, p splitPlan) ([]pair, error) {
	ts, err := c.GetTimestamp(ctx)
	if err != nil {
		return nil, err
	}
	source, err := records(ctx, c, ts, p.Sources)
	if err != nil {
		return nil, err
	}
	expected, err := project(source, p)
	if err != nil {
		return nil, err
	}
	actual, err := records(ctx, c, ts, p.Targets)
	if err != nil {
		return nil, err
	}

	if err = equalPairs(expected, actual); err != nil {
		return nil, err
	}
	// Foreground is quiescent at this certificate in this storage experiment.
	err = transact(ctx, c, func(txn *transaction.KVTxn) error {
		state, e := getOptional(ctx, txn, stateKey(p))
		if e != nil {
			return e
		}
		if string(state) == "QUARANTINED" {
			return nil
		} // observation only, no publication certificate
		if string(state) != "BUILDING" {
			return fmt.Errorf("verification on inactive generation")
		}
		if e = txn.Set(stateKey(p), state); e != nil {
			return e
		}
		return putJSON(txn, []byte(fmt.Sprintf("prototype/g%d/rows-verified", p.Generation)), map[string]any{"ts": ts, "count": len(actual), "digest": digestPairs(actual)})
	})
	return actual, err
}

func buildIndexes(ctx context.Context, im *importer, p splitPlan, exercise bool, out *report) error {
	for _, global := range []bool{false, true} {
		if err := buildOnlineIndexGroup(ctx, im, p, global, exercise, out); err != nil {
			return fmt.Errorf("online index group global=%t: %w", global, err)
		}
	}
	// Check local again after building global: local must have tracked all later DML.
	for _, global := range []bool{false, true} {
		if err := verifyIndexGroup(ctx, im.client, p, global); err != nil {
			return err
		}
	}
	return nil
}

func verifyIndexGroup(ctx context.Context, c *txnkv.Client, p splitPlan, global bool) error {
	kind := "local"
	if global {
		kind = "global"
	}
	ts, err := c.GetTimestamp(ctx)
	if err != nil {
		return err
	}
	rows, err := records(ctx, c, ts, p.Targets)
	if err != nil {
		return err
	}
	var input []pair
	for _, v := range rows {
		pid, h, err := tablecodec.DecodeRecordKey(v.Key)
		if err != nil {
			return err
		}
		r, err := decodeRow(v.Value)
		if err != nil {
			return err
		}
		for _, idx := range indexes(p, global) {
			item, err := indexPair(p, pid, h.IntValue(), r, idx)
			if err != nil {
				return err
			}
			input = append(input, item)
		}
	}
	var actual []pair
	pids := p.Targets
	if global {
		pids = []int64{logicalID}
	}
	for _, pid := range pids {
		for _, idx := range indexes(p, global) {
			prefix := tablecodec.EncodeIndexSeekKey(pid, idx.ID, nil)
			found, err := scan(ctx, c, ts, prefix)
			if err != nil {
				return err
			}

			// Decode stored index entries and perform the actual handle/PID
			// lookup independently of the encoder used for expected bytes.
			for start := 0; start < len(found); start += 128 {
				batch := found[start:min(start+128, len(found))]
				keys := make([][]byte, 0, len(batch))
				for _, entry := range batch {
					handle, err := tablecodec.DecodeIndexHandle(entry.Key, entry.Value, 1)
					if err != nil {
						return err
					}
					rowPID := pid
					if global {
						ph, ok := handle.(kv.PartitionHandle)
						if !ok {
							return fmt.Errorf("global entry lacks partition handle")
						}
						rowPID = ph.PartitionID
						handle = ph.Handle
					}
					if _, ok := p.Parents[rowPID]; !ok {
						return fmt.Errorf("index points outside final layout")
					}
					keys = append(keys, recordKey(rowPID, handle.IntValue()))
				}
				lookedUp, err := c.GetSnapshot(ts).BatchGet(ctx, keys)
				if err != nil {
					return err
				}
				for i, entry := range batch {
					record, ok := lookedUp[string(keys[i])]
					if !ok {
						return fmt.Errorf("index points to missing row")
					}
					r, e := decodeRow(record.Value)
					if e != nil {
						return e
					}
					_, d, e := codec.DecodeOne(entry.Key[len(prefix):])
					if e != nil {
						return e
					}
					if r.Null != d.IsNull() {
						return fmt.Errorf("index NULL/row mismatch")
					}
					expected := r.Value
					if idx.Unique {
						expected = r.Identity
					}
					if !r.Null && d.GetInt64() != expected {
						return fmt.Errorf("index value/row mismatch")
					}
				}

			}
			actual = append(actual, found...)
		}
	}
	if err = equalPairs(input, actual); err != nil {
		return fmt.Errorf("%s indexes: %w", kind, err)
	}
	return nil
}

func publish(ctx context.Context, c *txnkv.Client, p splitPlan) error {
	return transact(ctx, c, func(txn *transaction.KVTxn) error {
		state, err := getOptional(ctx, txn, stateKey(p))
		if err != nil {
			return err
		}
		if string(state) != "BUILDING" {
			return fmt.Errorf("publish rejected: generation %s", state)
		}
		gate, err := getOptional(ctx, txn, gateKey(p))
		if err != nil {
			return err
		}
		if string(gate) != "closed" {
			return fmt.Errorf("publish rejected: repair not closed")
		}
		checked, err := getOptional(ctx, txn, []byte(fmt.Sprintf("prototype/g%d/rows-verified", p.Generation)))
		if err != nil {
			return err
		}
		if checked == nil {
			return fmt.Errorf("publish rejected: no row completion certificate")
		}

		for _, kind := range []string{"record", "local", "global"} {
			v, err := getOptional(ctx, txn, []byte(fmt.Sprintf("prototype/g%d/sealed/%s", p.Generation, kind)))
			if err != nil {
				return err
			}
			if string(v) != "sealed" {
				return fmt.Errorf("publish rejected: unsealed %s", kind)
			}
		}

		for _, global := range []bool{false, true} {
			phase, err := getOptional(ctx, txn, indexStateKey(p, global))
			if err != nil {
				return err
			}
			if string(phase) != indexDirect {
				return fmt.Errorf("publish rejected: incomplete index merge")
			}
		}
		prefix := []byte(fmt.Sprintf("prototype/g%d/rpc/", p.Generation))
		it, err := txn.Iter(prefix, kv.Key(prefix).PrefixNext())
		if err != nil {
			return err
		}
		defer it.Close()
		for it.Valid() {
			var in intent
			if err = json.Unmarshal(it.Value(), &in); err != nil {
				return err
			}
			if in.Status != "APPLIED" {
				return fmt.Errorf("publish rejected: pending RPC")
			}
			if err = it.Next(); err != nil {
				return err
			}
		}
		if err = putJSON(txn, layoutKey, p.Targets); err != nil {
			return err
		}
		return txn.Set(stateKey(p), []byte("PUBLISHED"))
	})
}

// readLayout binds the harness's MVCC metadata and rows to one timestamp. This
// exercises storage primitives, not the SQL planner's historical schema cache.
func readLayout(ctx context.Context, c *txnkv.Client, ts uint64) ([]pair, error) {
	v, err := c.GetSnapshot(ts).Get(ctx, layoutKey)
	if err != nil {
		return nil, err
	}
	var pids []int64
	if err = json.Unmarshal(v.Value, &pids); err != nil {
		return nil, err
	}
	return records(ctx, c, ts, pids)
}

func closeGate(ctx context.Context, c *txnkv.Client, p splitPlan) error {
	return transact(ctx, c, func(txn *transaction.KVTxn) error { return txn.Set(gateKey(p), []byte("closed")) })
}

func run(ctx context.Context, addr string, expectedCluster uint64, dir string, config runConfig, out *report) error {
	n, workers := config.rows, config.workers
	host, _, err := net.SplitHostPort(addr)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() || expectedCluster == 0 {
		return fmt.Errorf("require loopback PD and exact nonzero disposable cluster ID")
	}
	if dir == "" || n < 16 || n > maxRowsPerParent || workers < 1 || workers > 16 || config.mutations < 1 || config.mutations > 10000 {
		return fmt.Errorf("require work-dir, 16..100000 rows, 1..16 workers and 1..10000 mutations")
	}
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	pdClient, err := pd.NewClient(caller.Component("partition-reorg-prototype"), []string{addr}, pd.SecurityOption{})
	if err != nil {
		return err
	}
	defer pdClient.Close()
	if actual := pdClient.GetClusterID(ctx); actual != expectedCluster {
		return fmt.Errorf("cluster ID mismatch: %d != %d", actual, expectedCluster)
	}
	c, err := txnkv.NewClient([]string{addr})
	if err != nil {
		return err
	}
	defer c.Close()
	im := &importer{client: c, pd: pdClient, dir: dir, workers: workers, mutations: config.mutations, regions: make(map[uint64]bool)}
	defer func() {
		out.SortedFiles = im.files
		out.MutatingRPCs = im.rpcs
		out.SSTCFs = im.cfs
		out.IngestRegions = len(im.regions)
	}()
	p := newPlan(1)
	if config.splitRegions {
		var keys [][]byte
		for _, id := range append([]int64{logicalID}, p.Sources...) {
			keys = append(keys, tablecodec.EncodeTablePrefix(id))
		}
		for g := 1; g <= 3; g++ {
			for _, id := range newPlan(g).Targets {
				keys = append(keys, tablecodec.EncodeTablePrefix(id))
			}
		}
		ids, err := c.SplitRegions(ctx, keys, false, nil)
		if err != nil {
			return err
		}
		out.Regions = len(ids)
		for _, key := range keys {
			for {
				region, err := pdClient.GetRegion(ctx, codec.EncodeBytes(nil, key))
				if err != nil {
					return err
				}
				if region != nil && region.Leader != nil && region.Leader.StoreId != 0 {
					break
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(20 * time.Millisecond):
				}
			}
		}
	}

	if err = transact(ctx, c, func(txn *transaction.KVTxn) error {
		value, e := getOptional(ctx, txn, layoutKey)
		if e != nil {
			return e
		}
		if value != nil {
			return fmt.Errorf("cluster is not fresh: prototype layout exists")
		}
		return putJSON(txn, layoutKey, p.Sources)
	}); err != nil {
		return err
	}
	for parent, pid := range p.Sources {
		for start := 1; start <= n; start += 100 {
			if err = transact(ctx, c, func(txn *transaction.KVTxn) error {
				for h := start; h <= min(start+99, n); h++ {
					r := row{Route: int64(parent*100 + h%100), Value: int64(h % 11), Identity: int64(parent*n + h), Null: h%7 == 0}
					b, e := encodeRow(r)
					if e != nil {
						return e
					}
					if e = txn.Set(recordKey(pid, int64(h)), b); e != nil {
						return e
					}
					if !r.Null {
						if e = txn.Set(uniqueKey(r.Identity), codec.EncodeInt(codec.EncodeInt(nil, pid), int64(h))); e != nil {
							return e
						}
					}

				}
				return nil
			}); err != nil {
				return err
			}
		}
	}
	out.Rows = 2 * n
	if err = activate(ctx, c, p); err != nil {
		return err
	}
	s, err := c.GetTimestamp(ctx)
	if err != nil {
		return err
	}
	baseline, err := records(ctx, c, s, p.Sources)
	if err != nil {
		return err
	}
	if len(baseline) != 2*n {
		return fmt.Errorf("fixture incomplete")
	}
	projected, err := project(baseline, p)
	if err != nil {
		return err
	}
	out.Checks = append(out.Checks, "pure split preserves duplicate handles across two old parents")
	// These are successful commits after S, before the baseline overwrites shadows.
	// Running-only mutations capture keys but do not write shadow records.
	mutations := []struct {
		pid, h int64
		next   *row
	}{
		{1000, 1, nil},
		{1000, 2, &row{Route: 76, Value: 123, Identity: 2}},
		{1000, int64(n - 1), &row{Route: 123, Value: 456, Identity: int64(n - 1)}},
		{0, int64(n + 1), &row{Route: 180, Value: 789, Identity: int64(2*n + 1)}},
	}
	for _, m := range mutations {
		if err = mutate(ctx, c, p, m.pid, m.h, m.next, false); err != nil {
			return err
		}
	}
	// Roll back source and marker together; neither may become visible.
	rolled, err := c.Begin()
	if err != nil {
		return err
	}
	if err = rolled.Delete(recordKey(1000, 4)); err != nil {
		_ = rolled.Rollback()
		return err
	}
	if err = rolled.Set(markerKey(p, p.Targets[0], 4), []byte("rollback")); err != nil {
		_ = rolled.Rollback()
		return err
	}
	if err = rolled.Rollback(); err != nil {
		return err
	}
	if err = transact(ctx, c, func(txn *transaction.KVTxn) error {
		b, e := getOptional(ctx, txn, recordKey(1000, 4))
		if e != nil {
			return e
		}
		if b == nil {
			return fmt.Errorf("rollback deleted source")
		}
		b, e = getOptional(ctx, txn, markerKey(p, p.Targets[0], 4))
		if e != nil {
			return e
		}
		if b != nil {
			return fmt.Errorf("rollback leaked marker")
		}
		return nil
	}); err != nil {
		return err
	}
	out.Checks = append(out.Checks, "post-snapshot delete/update/move/insert and atomic rollback")
	if err = im.bulk(ctx, p, "record", projected, false); err != nil {
		return err
	}
	// A real commit races a repair that already read marker and source. The marker
	// write conflict must abort stale source replay, rather than accepting it.
	q := p.Targets[1]
	stale, err := prepareRepair(ctx, c, p, markerKey(p, q, 2))
	if err != nil {
		return err
	}
	next := row{Route: 76, Value: 999, Identity: 2}
	if err = mutate(ctx, c, p, 1000, 2, &next, true); err != nil {
		_ = stale.Rollback()
		return err
	}
	err = stale.Commit(ctx)
	if !tikverr.IsErrWriteConflict(err) {
		return fmt.Errorf("expected repair/DML write conflict, got %v", err)
	}
	out.Checks = append(out.Checks, "real TiKV repair/DML conflict prevents stale replay")
	if _, err = repair(ctx, c, p); err != nil {
		return err
	}
	// Prepare a second repair, close its range, then release it. A persistent
	// Closed gate is an actual conflicting write, not an in-memory cancellation.
	if err = mutate(ctx, c, p, 1000, 2, &next, true); err != nil {
		return err
	}
	late, err := prepareRepair(ctx, c, p, markerKey(p, q, 2))
	if err != nil {
		return err
	}
	if err = closeGate(ctx, c, p); err != nil {
		_ = late.Rollback()
		return err
	}
	err = late.Commit(ctx)
	if !tikverr.IsErrWriteConflict(err) {
		return fmt.Errorf("expected closed-gate write conflict, got %v", err)
	}
	if txn, e := prepareRepair(ctx, c, p, markerKey(p, q, 2)); e == nil {
		_ = txn.Rollback()
		return fmt.Errorf("new worker reopened closed gate")
	}
	out.Checks = append(out.Checks, "closed gate rejects both stale commit and new repair")
	finalRows, err := verifyRows(ctx, c, p)
	if err != nil {
		return err
	}
	out.Checks = append(out.Checks, "exact record KV equality, including deleted and moved endpoints")
	if err = buildIndexes(ctx, im, p, true, out); err != nil {
		return err
	}
	out.Checks = append(out.Checks, "local/global unique/nonunique NULL index KV equality and decoded handle/PID row lookup")
	finalRows, err = verifyRows(ctx, c, p)
	if err != nil {
		return err
	}
	out.FinalRows = len(finalRows)
	if err = verifyLogicalIdentities(finalRows, n, p); err != nil {
		return err
	}
	out.FinalRowDigest = digestPairs(finalRows)

	out.Checks = append(out.Checks, "independent logical identity oracle preserves every intended row",
		"online rollback leaves source, shadow, constraints and all normal/temp index KVs unchanged")

	out.Checks = append(out.Checks, "online b/m temp merge with key reuse, NULL transitions and concurrent writes",
		"Inactive/Running DML and Merging workers conflict with phase transitions",
		"completed local indexes remain current throughout global backfill")

	oldTS, err := c.GetTimestamp(ctx)
	if err != nil {
		return err
	}
	oldExpected, err := readLayout(ctx, c, oldTS)
	if err != nil {
		return err
	}
	if err = publish(ctx, c, p); err != nil {
		return err
	}
	oldRead, err := readLayout(ctx, c, oldTS)
	if err != nil {
		return err
	}
	if err = equalPairs(oldExpected, oldRead); err != nil {
		return fmt.Errorf("historical binding: %w", err)
	}
	// Reading fresh target PIDs with the original S must fail equality; retaining
	// this negative control makes the old-snapshot defect observable in real MVCC.
	wrong, err := records(ctx, c, s, p.Targets)
	if err != nil {
		return err
	}
	if len(wrong) != 0 {
		return fmt.Errorf("negative control unexpectedly had targets at S")
	}
	now, err := c.GetTimestamp(ctx)
	if err != nil {
		return err
	}
	current, err := readLayout(ctx, c, now)
	if err != nil {
		return err
	}
	if err = equalPairs(finalRows, current); err != nil {
		return err
	}
	out.Checks = append(out.Checks, "same-TS MVCC layout/rows after publication; old-TS/new-PID negative control")
	// Storage actually applies the failed generation, but the controller loses its
	// receipt. A pending Intent must remain unpublishable even after restart reads.
	bad := newPlan(2)
	if err = activate(ctx, c, bad); err != nil {
		return err
	}
	currentSource, err := records(ctx, c, now, bad.Sources)
	if err != nil {
		return err
	}
	badRows, err := project(currentSource, bad)
	if err != nil {
		return err
	}
	if err = im.bulk(ctx, bad, "record", badRows, true); !errors.Is(err, errLostReceipt) {
		return fmt.Errorf("lost receipt injection: %v", err)
	}
	if err = publish(ctx, c, bad); err == nil {
		return fmt.Errorf("uncertain generation published")
	}
	// Verify that the storage write really happened; otherwise this would test
	// only the trivial before-send failure case.
	appliedTS, err := c.GetTimestamp(ctx)
	if err != nil {
		return err
	}
	applied, err := records(ctx, c, appliedTS, bad.Targets)
	if err != nil {
		return err
	}
	if err = verifyAppliedSubset(badRows, applied); err != nil {
		return fmt.Errorf("fault was not a valid after-apply subset: %w", err)
	}
	out.UnknownAppliedRows = len(applied)
	if config.splitRegions {
		if len(applied) >= len(badRows) {
			return fmt.Errorf("multi-Region fault did not exercise partial import")
		}
		out.Checks = append(out.Checks, "multi-Region lost receipt applies a strict subset and still cannot publish")
	}

	ts, err := c.GetTimestamp(ctx)
	if err != nil {
		return err
	}
	journal, err := scan(ctx, c, ts, []byte("prototype/g2/rpc/"))
	if err != nil {
		return err
	}
	pending := 0
	for _, v := range journal {
		var in intent
		if err = json.Unmarshal(v.Value, &in); err != nil {
			return err
		}
		if in.Status == "PENDING" {
			pending++
		}
	}
	if pending == 0 {
		return fmt.Errorf("lost receipt left no pending intent")
	}
	out.Checks = append(out.Checks, "after-apply lost receipt persists pending Intent and quarantines generation")
	fresh := newPlan(3)
	if err = activate(ctx, c, fresh); err != nil {
		return err
	}
	freshTS, err := c.GetTimestamp(ctx)
	if err != nil {
		return err
	}
	source, err := records(ctx, c, freshTS, fresh.Sources)
	if err != nil {
		return err
	}
	rebuilt, err := project(source, fresh)
	if err != nil {
		return err
	}
	if err = im.bulk(ctx, fresh, "record", rebuilt, false); err != nil {
		return err
	}
	if _, err = repair(ctx, c, fresh); err != nil {
		return err
	}
	if err = closeGate(ctx, c, fresh); err != nil {
		return err
	}
	rebuilt, err = verifyRows(ctx, c, fresh)
	if err != nil {
		return err
	}
	if err = buildIndexes(ctx, im, fresh, false, out); err != nil {
		return err
	}
	if err = publish(ctx, c, fresh); err != nil {
		return err
	}
	for _, pid := range fresh.Targets {
		if _, ok := bad.Parents[pid]; ok {
			return fmt.Errorf("fresh generation reused PID")
		}
	}
	// An old published layout remains readable after a second publication.
	historical, err := readLayout(ctx, c, now)
	if err != nil {
		return err
	}
	if err = equalPairs(finalRows, historical); err != nil {
		return err
	}

	out.Checks = append(out.Checks, "fresh PID/global-ID rebuild publishes; previous published snapshot stays readable")
	before := im.rpcs
	if err = im.bulk(ctx, fresh, "record", rebuilt, false); err == nil {
		return fmt.Errorf("published phase repeated")
	}
	if im.rpcs != before {
		return fmt.Errorf("rejected phase sent an RPC")
	}
	currentTS, e := c.GetTimestamp(ctx)
	if e != nil {
		return e
	}
	stillPublished, e := c.GetSnapshot(currentTS).Get(ctx, stateKey(fresh))
	if e != nil {
		return e
	}
	if string(stillPublished.Value) != "PUBLISHED" {
		return fmt.Errorf("rejected retry damaged published generation")
	}
	if im.cfs["default"] == 0 || im.cfs["write"] == 0 {
		return fmt.Errorf("did not exercise both MVCC SST CFs: %v", im.cfs)
	}
	out.Checks = append(out.Checks, "published phase rejects replay without RPC; both default/write CFs were ingested")
	return nil
}

// Include encoded marker suffixes in namespace tests without parsing strings.
func markerCoordinates(key []byte, p splitPlan) (int64, int64, error) {
	if !bytes.HasPrefix(key, markerPrefix(p)) {
		return 0, 0, fmt.Errorf("wrong marker namespace")
	}
	rest, q, err := codec.DecodeInt(key[len(markerPrefix(p)):])
	if err != nil {
		return 0, 0, err
	}
	rest, h, err := codec.DecodeInt(rest)
	if err != nil {
		return 0, 0, err
	}
	if len(rest) != 0 {
		return 0, 0, fmt.Errorf("trailing marker bytes")
	}
	return q, h, nil
}

// verifyLogicalIdentities is independent of source-to-shadow KV comparison.
// Otherwise an accidental source overwrite in the test workload can make both
// source and shadow agree while silently losing an intended logical row.
func verifyLogicalIdentities(rows []pair, n int, p splitPlan) error {
	expected := make(map[int64]bool)
	for i := 1; i <= 2*n; i++ {
		expected[int64(i)] = true
	}
	delete(expected, 1)
	expected[int64(2*n+1)] = true
	for step := range 2 {
		base := int64(1000000 + p.Generation*1000 + step*100)
		for offset := range 3 {
			expected[base+int64(offset)] = true
		}
	}
	for _, entry := range rows {
		r, err := decodeRow(entry.Value)
		if err != nil {
			return err
		}
		if !expected[r.Identity] {
			return fmt.Errorf("logical row set mismatch: unexpected/repeated identity %d", r.Identity)
		}
		delete(expected, r.Identity)
	}
	if len(expected) > 0 {
		for identity := range expected {
			return fmt.Errorf("logical row set mismatch: missing identity %d", identity)
		}
	}
	return nil
}

// A lost first-region receipt need not imply the remaining regions were sent.
// Validate every applied byte without treating partial completion as publishable.
func verifyAppliedSubset(expected, actual []pair) error {
	if len(actual) == 0 {
		return fmt.Errorf("no applied rows")
	}
	values := make(map[string][]byte, len(expected))
	for _, entry := range expected {
		if _, ok := values[string(entry.Key)]; ok {
			return fmt.Errorf("duplicate expected key")
		}
		values[string(entry.Key)] = entry.Value
	}
	seen := make(map[string]bool, len(actual))
	for _, entry := range actual {
		key := string(entry.Key)
		value, ok := values[key]
		if !ok || !bytes.Equal(value, entry.Value) || seen[key] {
			return fmt.Errorf("unexpected, corrupt or repeated applied row")
		}
		seen[key] = true
	}
	return nil
}
