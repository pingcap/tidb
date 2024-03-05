package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"hash/crc64"
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/txnkv"
	"go.uber.org/zap"
)

var (
	ca          = flag.String("ca", "", "CA certificate path for TLS connection")
	cert        = flag.String("cert", "", "certificate path for TLS connection")
	key         = flag.String("key", "", "private key path for TLS connection")
	pdAddr      = flag.String("pd", "127.0.0.1:2379", "Address of PD")
	runMode     = flag.String("mode", "", "Mode. One of 'rand-gen', 'checksum', 'scan', 'delete'")
	startKeyStr = flag.String("start-key", "", "Start key in hex")
	endKeyStr   = flag.String("end-key", "", "End key in hex")
	keyMaxLen   = flag.Int("key-max-len", 32, "Max length of keys for rand-gen mode")
	concurrency = flag.Int("concurrency", 32, "Concurrency to run rand-gen")
	duration    = flag.Int("duration", 10, "duration(second) of rand-gen")
)

func createClient(addr string) (*txnkv.Client, error) {
	if *ca != "" {
		conf := config.GetGlobalConfig()
		conf.Security.ClusterSSLCA = *ca
		conf.Security.ClusterSSLCert = *cert
		conf.Security.ClusterSSLKey = *key
		config.StoreGlobalConfig(conf)
	}

	cli, err := txnkv.NewClient([]string{addr})
	return cli, errors.Trace(err)
}

func main() {
	flag.Parse()

	startKey := []byte(*startKeyStr)
	endKey := []byte(*endKeyStr)
	if len(endKey) == 0 {
		log.Panic("Empty endKey is not supported yet")
	}

	if *runMode == "test-rand-key" {
		testRandKey(startKey, endKey, *keyMaxLen)
		return
	}

	client, err := createClient(*pdAddr)
	if err != nil {
		log.Panic("Failed to create client", zap.String("pd", *pdAddr), zap.Error(err))
	}

	switch *runMode {
	case "rand-gen":
		err = randGenWithDuration(client, startKey, endKey, *keyMaxLen, *concurrency, *duration)
	case "checksum":
		err = checksum(client, startKey, endKey)
	case "delete":
		err = deleteRange(client, startKey, endKey)
	}

	if err != nil {
		log.Panic("Error", zap.Error(err))
	}
}

func randGenWithDuration(client *txnkv.Client, startKey, endKey []byte,
	maxLen int, concurrency int, duration int) error {
	var err error
	ok := make(chan struct{})
	go func() {
		err = randGen(client, startKey, endKey, maxLen, concurrency)
		ok <- struct{}{}
	}()
	select {
	case <-time.After(time.Second * time.Duration(duration)):
	case <-ok:
	}
	return errors.Trace(err)
}

func randGen(client *txnkv.Client, startKey, endKey []byte, maxLen int, concurrency int) error {
	log.Info("Start rand-gen", zap.Int("maxlen", maxLen),
		zap.String("startkey", hex.EncodeToString(startKey)), zap.String("endkey", hex.EncodeToString(endKey)))
	log.Info("Rand-gen will keep running. Please Ctrl+C to stop manually.")

	// Cannot generate shorter key than commonPrefix
	commonPrefixLen := 0
	for ; commonPrefixLen < len(startKey) && commonPrefixLen < len(endKey) &&
		startKey[commonPrefixLen] == endKey[commonPrefixLen]; commonPrefixLen++ {
		continue
	}

	if maxLen < commonPrefixLen {
		return errors.Errorf("maxLen (%v) < commonPrefixLen (%v)", maxLen, commonPrefixLen)
	}

	const batchSize = 32

	errCh := make(chan error, concurrency)
	for i := maxLen; i <= maxLen+concurrency; i++ {
		go func(i int) {
			for {
				txn, err := client.Begin()
				if err != nil {
					errCh <- errors.Trace(err)
				}
				for j := 0; j < batchSize; j++ {
					key := randKey(startKey, endKey, i)
					// append index to avoid write conflict
					key = appendIndex(key, i)
					value := randValue()
					err = txn.Set(key, value)
					if err != nil {
						errCh <- errors.Trace(err)
					}
				}
				err = txn.Commit(context.TODO())
				if err != nil {
					errCh <- errors.Trace(err)
				}
			}
		}(i)
	}

	err := <-errCh
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func testRandKey(startKey, endKey []byte, maxLen int) {
	for {
		k := randKey(startKey, endKey, maxLen)
		if bytes.Compare(k, startKey) < 0 || bytes.Compare(k, endKey) >= 0 {
			panic(hex.EncodeToString(k))
		}
	}
}

//nolint:gosec
func randKey(startKey, endKey []byte, maxLen int) []byte {
Retry:
	for { // Regenerate on fail
		result := make([]byte, 0, maxLen)

		upperUnbounded := false
		lowerUnbounded := false

		for i := 0; i < maxLen; i++ {
			upperBound := 256
			if !upperUnbounded {
				if i >= len(endKey) {
					// The generated key is the same as endKey which is invalid. Regenerate it.
					continue Retry
				}
				upperBound = int(endKey[i]) + 1
			}

			lowerBound := 0
			if !lowerUnbounded {
				if i >= len(startKey) {
					lowerUnbounded = true
				} else {
					lowerBound = int(startKey[i])
				}
			}

			if lowerUnbounded {
				if rand.Intn(257) == 0 {
					return result
				}
			}

			value := rand.Intn(upperBound - lowerBound)
			value += lowerBound

			if value < upperBound-1 {
				upperUnbounded = true
			}
			if value > lowerBound {
				lowerUnbounded = true
			}

			result = append(result, uint8(value))
		}

		return result
	}
}

//nolint:gosec
func appendIndex(key []byte, i int) []byte {
	return append(key, uint8(i))
}

//nolint:gosec
func randValue() []byte {
	result := make([]byte, 0, 512)
	for i := 0; i < 512; i++ {
		value := rand.Intn(257)
		if value == 256 {
			if i > 0 {
				return result
			}
			value--
		}
		result = append(result, uint8(value))
	}
	return result
}

func checksum(client *txnkv.Client, startKey, endKey []byte) error {
	log.Info("Start checkcum on range",
		zap.String("startkey", hex.EncodeToString(startKey)), zap.String("endkey", hex.EncodeToString(endKey)))

	txn, err := client.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	iter, err := txn.Iter(startKey, endKey)
	if err != nil {
		return errors.Trace(err)
	}

	digest := crc64.New(crc64.MakeTable(crc64.ECMA))

	var res uint64

	for iter.Valid() {
		err := iter.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if len(iter.Key()) == 0 {
			break
		}
		_, _ = digest.Write(iter.Key())
		_, _ = digest.Write(iter.Value())
		res ^= digest.Sum64()
	}
	_ = txn.Commit(context.TODO())

	log.Info("Checksum result", zap.Uint64("checksum", res))
	fmt.Printf("Checksum result: %016x\n", res)
	return nil
}

func deleteRange(client *txnkv.Client, startKey, endKey []byte) error {
	log.Info("Start delete data in range",
		zap.String("startkey", hex.EncodeToString(startKey)), zap.String("endkey", hex.EncodeToString(endKey)))
	_, err := client.DeleteRange(context.TODO(), startKey, endKey, *concurrency)
	return err
}
