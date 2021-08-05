// Copyright 2019 PingCAP, Inc.
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

// Test backup with key locked errors.
//
// This file is copied from pingcap/schrodinger-test#428 https://git.io/Je1md

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/br/pkg/httputil"
	"github.com/pingcap/tidb/br/pkg/task"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/driver"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	"github.com/tikv/client-go/v2/tikvrpc"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
)

var (
	ca             = flag.String("ca", "", "CA certificate path for TLS connection")
	cert           = flag.String("cert", "", "certificate path for TLS connection")
	key            = flag.String("key", "", "private key path for TLS connection")
	tidbStatusAddr = flag.String("tidb", "", "TiDB status address")
	pdAddr         = flag.String("pd", "", "PD address")
	dbName         = flag.String("db", "", "Database name")
	tableName      = flag.String("table", "", "Table name")
	tableSize      = flag.Int64("table-size", 10000, "Table size, row count")
	timeout        = flag.Duration("run-timeout", time.Second*10, "The total time it executes")
	lockTTL        = flag.Duration("lock-ttl", time.Second*10, "The TTL of locks")
)

func main() {
	flag.Parse()
	if *tidbStatusAddr == "" {
		log.Panic("tidb status address is empty")
	}
	if *pdAddr == "" {
		log.Panic("pd address is empty")
	}
	if *dbName == "" {
		log.Panic("database name is empty")
	}
	if *tableName == "" {
		log.Panic("table name is empty")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	http.DefaultClient.Timeout = *timeout

	tableID, err := getTableID(ctx, *tidbStatusAddr, *dbName, *tableName)
	if err != nil {
		log.Panic("get table id failed", zap.Error(err))
	}

	pdclient, err := pd.NewClient([]string{*pdAddr}, pd.SecurityOption{
		CAPath:   *ca,
		CertPath: *cert,
		KeyPath:  *key,
	})
	if err != nil {
		log.Panic("create pd client failed", zap.Error(err))
	}
	pdcli := &codecPDClient{Client: pdclient}

	if len(*ca) != 0 {
		tidbCfg := config.NewConfig()
		tidbCfg.Security.ClusterSSLCA = *ca
		tidbCfg.Security.ClusterSSLCert = *cert
		tidbCfg.Security.ClusterSSLKey = *key
		config.StoreGlobalConfig(tidbCfg)
	}
	driver := driver.TiKVDriver{}
	store, err := driver.Open(fmt.Sprintf("tikv://%s?disableGC=true", *pdAddr))
	if err != nil {
		log.Panic("create tikv client failed", zap.Error(err))
	}

	locker := Locker{
		tableID:   tableID,
		tableSize: *tableSize,
		lockTTL:   *lockTTL,
		pdcli:     pdcli,
		kv:        store.(tikv.Storage),
	}
	err = locker.generateLocks(ctx)
	if err != nil {
		log.Panic("generate locks failed", zap.Error(err))
	}
}

func newHTTPClient() *http.Client {
	if len(*ca) != 0 {
		tlsCfg := &task.TLSConfig{
			CA:   *ca,
			Cert: *cert,
			Key:  *key,
		}
		cfg, err := tlsCfg.ToTLSConfig()
		if err != nil {
			log.Panic("fail to parse TLS config", zap.Error(err))
		}
		return httputil.NewClient(cfg)
	}
	return http.DefaultClient
}

// getTableID of the table with specified table name.
func getTableID(ctx context.Context, dbAddr, dbName, table string) (int64, error) {
	dbHost, _, err := net.SplitHostPort(dbAddr)
	if err != nil {
		return 0, errors.Trace(err)
	}
	dbStatusAddr := net.JoinHostPort(dbHost, "10080")
	url := fmt.Sprintf("https://%s/schema/%s/%s", dbStatusAddr, dbName, table)

	client := newHTTPClient()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, errors.Trace(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, errors.Trace(err)
	}

	if resp.StatusCode != 200 {
		return 0, errors.Errorf("HTTP request to TiDB status reporter returns %v. Body: %v", resp.StatusCode, string(body))
	}

	var data model.TableInfo
	err = json.Unmarshal(body, &data)
	if err != nil {
		return 0, errors.Trace(err)
	}
	return data.ID, nil
}

// Locker leaves locks on a table.
type Locker struct {
	tableID   int64
	tableSize int64
	lockTTL   time.Duration

	pdcli pd.Client
	kv    tikv.Storage
}

// generateLocks sends Prewrite requests to TiKV to generate locks, without committing and rolling back.
func (c *Locker) generateLocks(pctx context.Context) error {
	log.Info("genLock started")

	const maxTxnSize = 1000

	// How many keys should be in the next transaction.
	nextTxnSize := rand.Intn(maxTxnSize) + 1 // 0 is not allowed.

	// How many keys has been scanned since last time sending request.
	scannedKeys := 0
	var batch []int64

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for rowID := int64(0); ; rowID = (rowID + 1) % c.tableSize {
		select {
		case <-pctx.Done():
			log.Info("genLock done")
			return nil
		default:
		}

		scannedKeys++

		// Randomly decide whether to lock current key.
		lockThis := rand.Intn(2) == 0

		if lockThis {
			batch = append(batch, rowID)

			if len(batch) >= nextTxnSize {
				// The batch is large enough to start the transaction
				err := c.lockKeys(ctx, batch)
				if err != nil {
					return errors.Annotate(err, "lock keys failed")
				}

				// Start the next loop
				batch = batch[:0]
				scannedKeys = 0
				nextTxnSize = rand.Intn(maxTxnSize) + 1
			}
		}
	}
}

func (c *Locker) lockKeys(ctx context.Context, rowIDs []int64) error {
	keys := make([][]byte, 0, len(rowIDs))

	keyPrefix := tablecodec.GenTableRecordPrefix(c.tableID)
	for _, rowID := range rowIDs {
		key := tablecodec.EncodeRecordKey(keyPrefix, kv.IntHandle(rowID))
		keys = append(keys, key)
	}

	primary := keys[0]

	for len(keys) > 0 {
		lockedKeys, err := c.lockBatch(ctx, keys, primary)
		if err != nil {
			return errors.Trace(err)
		}
		keys = keys[lockedKeys:]
	}
	return nil
}

func (c *Locker) lockBatch(ctx context.Context, keys [][]byte, primary []byte) (int, error) {
	const maxBatchSize = 16 * 1024

	// TiKV client doesn't expose Prewrite interface directly. We need to manually locate the region and send the
	// Prewrite requests.

	bo := tikv.NewBackoffer(ctx, 20000)
	for {
		loc, err := c.kv.GetRegionCache().LocateKey(bo, keys[0])
		if err != nil {
			return 0, errors.Trace(err)
		}

		// Get a timestamp to use as the startTs
		physical, logical, err := c.pdcli.GetTS(ctx)
		if err != nil {
			return 0, errors.Trace(err)
		}
		startTS := oracle.ComposeTS(physical, logical)

		// Pick a batch of keys and make up the mutations
		var mutations []*kvrpcpb.Mutation
		batchSize := 0

		for _, key := range keys {
			if len(loc.EndKey) > 0 && bytes.Compare(key, loc.EndKey) >= 0 {
				break
			}
			if bytes.Compare(key, loc.StartKey) < 0 {
				break
			}

			value := randStr()
			mutations = append(mutations, &kvrpcpb.Mutation{
				Op:    kvrpcpb.Op_Put,
				Key:   key,
				Value: []byte(value),
			})
			batchSize += len(key) + len(value)

			if batchSize >= maxBatchSize {
				break
			}
		}

		lockedKeys := len(mutations)
		if lockedKeys == 0 {
			return 0, nil
		}

		prewrite := &kvrpcpb.PrewriteRequest{
			Mutations:    mutations,
			PrimaryLock:  primary,
			StartVersion: startTS,
			LockTtl:      uint64(c.lockTTL.Milliseconds()),
		}
		req := tikvrpc.NewRequest(tikvrpc.CmdPrewrite, prewrite)

		// Send the requests
		resp, err := c.kv.SendReq(bo, req, loc.Region, time.Second*20)
		if err != nil {
			return 0, errors.Annotatef(
				err,
				"send request failed. region: %+v [%+q, %+q), keys: %+q",
				loc.Region, loc.StartKey, loc.EndKey, keys[0:lockedKeys])
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return 0, errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss(), errors.New(regionErr.String()))
			if err != nil {
				return 0, errors.Trace(err)
			}
			continue
		}

		prewriteResp := resp.Resp
		if prewriteResp == nil {
			return 0, errors.Errorf("response body missing")
		}

		// Ignore key errors since we never commit the transaction and we don't need to keep consistency here.
		return lockedKeys, nil
	}
}

func randStr() string {
	length := rand.Intn(128)
	res := ""
	for i := 0; i < length; i++ {
		res += fmt.Sprint(rand.Intn(10))
	}
	return res
}
