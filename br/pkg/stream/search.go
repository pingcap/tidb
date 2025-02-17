// Copyright 2022 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"sort"
	"strings"
	"sync"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Comparator is used for comparing the relationship of src and dst
type Comparator interface {
	Compare(src, dst []byte) bool
}

// startWithComparator is used for comparing whether src starts with dst
type startWithComparator struct{}

// NewStartWithComparator create a comparator to compare whether src starts with dst
func NewStartWithComparator() Comparator {
	return new(startWithComparator)
}

// Compare whether src starts with dst
func (ec *startWithComparator) Compare(src, dst []byte) bool {
	return bytes.HasPrefix(src, dst)
}

// StreamKVInfo stores kv info searched from log data files
type StreamKVInfo struct {
	Key        string `json:"key"`
	EncodedKey string `json:"-"`
	WriteType  byte   `json:"write-type"`
	StartTs    uint64 `json:"start-ts"`
	CommitTs   uint64 `json:"commit-ts"`
	CFName     string `json:"cf-name"`
	Value      string `json:"value,omitempty"`
	ShortValue string `json:"short-value,omitempty"`
}

// StreamBackupSearch is used for searching key from log data files
type StreamBackupSearch struct {
	storage    storage.ExternalStorage
	comparator Comparator
	searchKey  []byte // hex string
	startTs    uint64
	endTs      uint64
}

// NewStreamBackupSearch creates an instance of StreamBackupSearch
func NewStreamBackupSearch(
	storage storage.ExternalStorage,
	comparator Comparator,
	searchKey []byte,
) *StreamBackupSearch {
	bs := &StreamBackupSearch{
		storage:    storage,
		comparator: comparator,
	}

	bs.searchKey = codec.EncodeBytes([]byte{}, searchKey)
	return bs
}

// SetStartTS set start timestamp searched from
func (s *StreamBackupSearch) SetStartTS(startTs uint64) {
	s.startTs = startTs
}

// SetEndTs set end timestamp searched to
func (s *StreamBackupSearch) SetEndTs(endTs uint64) {
	s.endTs = endTs
}

func (s *StreamBackupSearch) readDataFiles(ctx context.Context, ch chan<- *backuppb.DataFileInfo) error {
	opt := &storage.WalkOption{SubDir: GetStreamBackupMetaPrefix()}
	pool := util.NewWorkerPool(64, "read backup meta")
	eg, egCtx := errgroup.WithContext(ctx)
	err := s.storage.WalkDir(egCtx, opt, func(path string, size int64) error {
		if !strings.Contains(path, GetStreamBackupMetaPrefix()) {
			return nil
		}

		pool.ApplyOnErrorGroup(eg, func() error {
			m := &backuppb.Metadata{}
			b, err := s.storage.ReadFile(egCtx, path)
			if err != nil {
				return errors.Trace(err)
			}
			err = m.Unmarshal(b)
			if err != nil {
				return errors.Trace(err)
			}

			s.resolveMetaData(egCtx, m, ch)
			log.Debug("read backup meta file", zap.String("path", path))
			return nil
		})

		return nil
	})

	if err != nil {
		return errors.Trace(err)
	}

	return eg.Wait()
}

func (s *StreamBackupSearch) resolveMetaData(
	ctx context.Context,
	metaData *backuppb.Metadata,
	ch chan<- *backuppb.DataFileInfo,
) {
	for _, file := range metaData.Files {
		if file.IsMeta {
			continue
		}

		// TODO dynamically configure filter policy
		if bytes.Compare(s.searchKey, file.StartKey) < 0 {
			continue
		}
		if bytes.Compare(s.searchKey, file.EndKey) > 0 {
			continue
		}

		if s.startTs > 0 {
			if file.MaxTs < s.startTs {
				continue
			}
		}
		if s.endTs > 0 {
			if file.MinTs > s.endTs {
				continue
			}
		}

		ch <- file
	}
}

// Search kv entries from log data files
func (s *StreamBackupSearch) Search(ctx context.Context) ([]*StreamKVInfo, error) {
	dataFilesCh := make(chan *backuppb.DataFileInfo, 32)
	entriesCh, errCh := make(chan *StreamKVInfo, 64), make(chan error, 8)
	go func() {
		defer close(dataFilesCh)
		if err := s.readDataFiles(ctx, dataFilesCh); err != nil {
			errCh <- err
		}
	}()

	pool := util.NewWorkerPool(16, "search key")
	var wg sync.WaitGroup

	for dataFile := range dataFilesCh {
		wg.Add(1)
		file := dataFile
		pool.Apply(func() {
			defer wg.Done()
			if err := s.searchFromDataFile(ctx, file, entriesCh); err != nil {
				errCh <- err
			}
		})
	}

	go func() {
		wg.Wait()
		close(entriesCh)
		close(errCh)
	}()

	for err := range errCh {
		return nil, errors.Trace(err)
	}

	defaultCFEntries := make(map[string]*StreamKVInfo, 64)
	writeCFEntries := make(map[string]*StreamKVInfo, 64)

	for entry := range entriesCh {
		if entry.CFName == WriteCF {
			writeCFEntries[entry.EncodedKey] = entry
		} else if entry.CFName == DefaultCF {
			defaultCFEntries[entry.EncodedKey] = entry
		}
	}

	entries := s.mergeCFEntries(defaultCFEntries, writeCFEntries)
	return entries, nil
}

func (s *StreamBackupSearch) searchFromDataFile(
	ctx context.Context,
	dataFile *backuppb.DataFileInfo,
	ch chan<- *StreamKVInfo,
) error {
	buff, err := s.storage.ReadFile(ctx, dataFile.Path)
	if err != nil {
		return errors.Annotatef(err, "read data file error, file: %s", dataFile.Path)
	}

	if checksum := sha256.Sum256(buff); !bytes.Equal(checksum[:], dataFile.GetSha256()) {
		return errors.Annotatef(err, "validate checksum failed, file: %s", dataFile.Path)
	}

	iter := NewEventIterator(buff)
	for iter.Valid() {
		iter.Next()
		if err := iter.GetError(); err != nil {
			return errors.Trace(err)
		}

		k, v := iter.Key(), iter.Value()
		if !s.comparator.Compare(k, s.searchKey) {
			continue
		}

		_, ts, err := codec.DecodeUintDesc(k[len(k)-8:])
		if err != nil {
			return errors.Annotatef(err, "decode ts from key error, file: %s", dataFile.Path)
		}

		k = k[:len(k)-8]
		_, rawKey, err := codec.DecodeBytes(k, nil)
		if err != nil {
			return errors.Annotatef(err, "decode raw key error, file: %s", dataFile.Path)
		}

		if dataFile.Cf == WriteCF {
			rawWriteCFValue := new(RawWriteCFValue)
			if err := rawWriteCFValue.ParseFrom(v); err != nil {
				return errors.Annotatef(err, "parse raw write cf value error, file: %s", dataFile.Path)
			}

			valueStr := ""
			if rawWriteCFValue.HasShortValue() {
				valueStr = base64.StdEncoding.EncodeToString(rawWriteCFValue.GetShortValue())
			}

			kvInfo := &StreamKVInfo{
				WriteType:  rawWriteCFValue.GetWriteType(),
				CFName:     dataFile.Cf,
				CommitTs:   ts,
				StartTs:    rawWriteCFValue.GetStartTs(),
				Key:        strings.ToUpper(hex.EncodeToString(rawKey)),
				EncodedKey: hex.EncodeToString(iter.Key()),
				ShortValue: valueStr,
			}
			ch <- kvInfo
		} else if dataFile.Cf == DefaultCF {
			kvInfo := &StreamKVInfo{
				CFName:     dataFile.Cf,
				StartTs:    ts,
				Key:        strings.ToUpper(hex.EncodeToString(rawKey)),
				EncodedKey: hex.EncodeToString(iter.Key()),
				Value:      base64.StdEncoding.EncodeToString(v),
			}
			ch <- kvInfo
		}
	}

	log.Info("finish search data file", zap.String("file", dataFile.Path))
	return nil
}

func (s *StreamBackupSearch) mergeCFEntries(defaultCFEntries, writeCFEntries map[string]*StreamKVInfo) []*StreamKVInfo {
	entries := make([]*StreamKVInfo, 0, len(defaultCFEntries)+len(writeCFEntries))
	mergedDefaultCFKeys := make(map[string]struct{}, 16)
	for _, entry := range writeCFEntries {
		entries = append(entries, entry)
		if entry.ShortValue != "" {
			continue
		}

		keyBytes, err := hex.DecodeString(entry.Key)
		if err != nil {
			log.Warn("hex decode key failed",
				zap.String("key", entry.Key), zap.String("encode-key", entry.EncodedKey), zap.Error(err))
			continue
		}

		encodedKey := codec.EncodeBytes([]byte{}, keyBytes)
		defaultCFKey := hex.EncodeToString(codec.EncodeUintDesc(encodedKey, entry.StartTs))
		defaultCFEntry, ok := defaultCFEntries[defaultCFKey]
		if !ok {
			continue
		}

		entry.Value = defaultCFEntry.Value
		mergedDefaultCFKeys[defaultCFKey] = struct{}{}
	}

	for key, entry := range defaultCFEntries {
		if _, ok := mergedDefaultCFKeys[key]; ok {
			continue
		}

		entries = append(entries, entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].CommitTs < entries[j].CommitTs
	})

	return entries
}
