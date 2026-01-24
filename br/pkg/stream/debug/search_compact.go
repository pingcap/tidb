// Copyright 2025 PingCAP, Inc. Licensed under Apache-2.0.

package debug

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	logclient "github.com/pingcap/tidb/br/pkg/restore/log_client"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/utils/consts"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/codec"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type StreamBackupCompactSearch struct {
	storage   storage.ExternalStorage
	manager   *logclient.LogFileManager
	searchKey []byte // encoded search key
	startTs   uint64
	endTs     uint64
}

func GetMigrations(ctx context.Context, storage storage.ExternalStorage) (*logclient.LockedMigrations, error) {
	ext := stream.MigrationExtension(storage)
	migs, err := ext.Load(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ms := migs.ListAll()
	readLock, err := ext.GetReadLock(ctx, "restore stream")
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &logclient.LockedMigrations{
		Migs:     ms,
		ReadLock: readLock,
	}, nil
}

func NewStreamBackupCompactSearch(
	ctx context.Context,
	storage storage.ExternalStorage,
	migs []*backuppb.Migration,
	searchKey []byte,
	startTs, endTs uint64,
) (*StreamBackupCompactSearch, error) {
	init := logclient.LogFileManagerInit{
		StartTS:                   startTs,
		RestoreTS:                 endTs,
		Storage:                   storage,
		MigrationsBuilder:         logclient.NewWithMigrationsBuilder(startTs, endTs),
		MetadataDownloadBatchSize: 128,
	}
	manager, err := logclient.CreateLogFileManager(ctx, init)
	if err != nil {
		return nil, errors.Trace(err)
	}
	manager.BuildMigrations(migs)
	return &StreamBackupCompactSearch{
		storage,
		manager,
		searchKey,
		startTs,
		endTs,
	}, nil
}

func (s *StreamBackupCompactSearch) fileRangeContainsSearchKey(file *logclient.LogDataFileInfo) bool {
	if file.IsMeta {
		return false
	}

	// file.StartKey and file.EndKey are encoded.
	if bytes.Compare(s.searchKey, file.StartKey) < 0 {
		return false
	}
	if bytes.Compare(s.searchKey, file.EndKey) > 0 {
		return false
	}

	log.Info("filter in file key",
		zap.String("cf", file.Cf),
		zap.String("file name", file.Path),
		zap.String("range", fmt.Sprintf("[%d:%d]", file.RangeOffset, file.RangeOffset+file.RangeLength)),
	)
	return true
}

func (s *StreamBackupCompactSearch) searchByFileIter(ctx context.Context, iter logclient.LogIter) ([]*stream.StreamKVInfo, error) {
	var lk sync.Mutex
	defaultCFEntries := make(map[string]*stream.StreamKVInfo, 64)
	writeCFEntries := make(map[string]*stream.StreamKVInfo, 64)
	eg, ectx := errgroup.WithContext(ctx)
	pool := util.NewWorkerPool(9, "search key")
	pool.ApplyOnErrorGroup(eg, func() error {
		for r := iter.TryNext(ectx); !r.Finished; r = iter.TryNext(ectx) {
			if r.Err != nil {
				return r.Err
			}

			dataFile := r.Item
			if s.fileRangeContainsSearchKey(dataFile) {
				pool.ApplyOnErrorGroup(eg, func() error {
					start := int64(dataFile.RangeOffset)
					end := int64(dataFile.RangeOffset + dataFile.RangeLength)
					reader, err := s.storage.Open(ctx, dataFile.Path, &storage.ReaderOption{
						StartOffset: &start,
						EndOffset:   &end,
					})
					if err != nil {
						return errors.Trace(err)
					}
					defer reader.Close()
					buff, err := io.ReadAll(reader)
					if err != nil {
						return errors.Trace(err)
					}
					decoder, _ := zstd.NewReader(nil)
					buff, err = decoder.DecodeAll(buff, nil)
					if err != nil {
						return errors.Trace(err)
					}
					if checksum := sha256.Sum256(buff); !bytes.Equal(checksum[:], dataFile.GetSha256()) {
						return errors.Annotatef(err, "validate checksum failed, file: %s", dataFile.Path)
					}

					iter := stream.NewEventIterator(buff)
					for iter.Valid() {
						iter.Next()
						if err := iter.GetError(); err != nil {
							return errors.Trace(err)
						}

						enck, v := iter.Key(), iter.Value()
						if !bytes.HasPrefix(enck, s.searchKey) {
							continue
						}
						_, ts, err := codec.DecodeUintDesc(enck[len(enck)-8:])
						if err != nil {
							return errors.Annotatef(err, "decode ts from key error, file: %s", dataFile.Path)
						}

						enck = enck[:len(enck)-8]
						_, rawKey, err := codec.DecodeBytes(enck, nil)
						if err != nil {
							return errors.Annotatef(err, "decode raw key error, file: %s", dataFile.Path)
						}

						switch dataFile.Cf {
						case consts.WriteCF:
							rawWriteCFValue := new(stream.RawWriteCFValue)
							if err := rawWriteCFValue.ParseFrom(v); err != nil {
								return errors.Annotatef(err, "parse raw write cf value error, file: %s", dataFile.Path)
							}

							valueStr := ""
							if rawWriteCFValue.HasShortValue() {
								valueStr = base64.StdEncoding.EncodeToString(rawWriteCFValue.GetShortValue())
							}

							kvInfo := &stream.StreamKVInfo{
								WriteType:  rawWriteCFValue.GetWriteType(),
								CFName:     dataFile.Cf,
								CommitTs:   ts,
								StartTs:    rawWriteCFValue.GetStartTs(),
								Key:        strings.ToUpper(hex.EncodeToString(rawKey)),
								EncodedKey: hex.EncodeToString(iter.Key()),
								ShortValue: valueStr,
							}

							lk.Lock()
							writeCFEntries[kvInfo.EncodedKey] = kvInfo
							lk.Unlock()
						case consts.DefaultCF:
							kvInfo := &stream.StreamKVInfo{
								CFName:     dataFile.Cf,
								StartTs:    ts,
								Key:        strings.ToUpper(hex.EncodeToString(rawKey)),
								EncodedKey: hex.EncodeToString(iter.Key()),
								Value:      base64.StdEncoding.EncodeToString(v),
							}

							lk.Lock()
							defaultCFEntries[kvInfo.EncodedKey] = kvInfo
							lk.Unlock()
						}
					}
					log.Info("finish search data file", zap.String("file", dataFile.Path))
					return nil
				})
			}
		}
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}
	return stream.MergeCFEntries(defaultCFEntries, writeCFEntries), nil
}

func (s *StreamBackupCompactSearch) Search(ctx context.Context) ([]*stream.StreamKVInfo, error) {
	iter, err := s.manager.LoadDMLFiles(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return s.searchByFileIter(ctx, iter)
}
