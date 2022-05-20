package restore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"sync"

	"github.com/pingcap/tidb/util/codec"

	"github.com/pingcap/tidb/br/pkg/utils"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"go.uber.org/zap"
)

const (
	encodeMarker   = byte(0xFF)
	encodePadding  = byte(0x0)
	encodeGroupLen = 8
)

type Comparator interface {
	Compare(src, dst []byte) bool
}

type startWithComparator struct{}

func NewStartWithComparator() Comparator {
	return new(startWithComparator)
}

func (ec *startWithComparator) Compare(src, dst []byte) bool {
	return bytes.HasPrefix(src, dst)
}

type StreamKVInfo struct {
	Key       string `json:"key"`
	WriteType byte   `json:"write_type"`
	StartTs   uint64 `json:"start_ts"`
	CommitTs  uint64 `json:"commit_ts"`
	CFName    string `json:"cf_name"`
	Value     string `json:"value"`
}

type StreamBackupSearch struct {
	storage    storage.ExternalStorage
	comparator Comparator
	searchKey  []byte
	startTs    uint64
	endTs      uint64
}

func NewStreamBackupSearch(storage storage.ExternalStorage, comparator Comparator, searchKey []byte) *StreamBackupSearch {
	bs := &StreamBackupSearch{
		storage:    storage,
		comparator: comparator,
	}

	bs.searchKey = codec.EncodeBytes([]byte{}, searchKey)
	return bs
}

func (s *StreamBackupSearch) SetStartTS(startTs uint64) {
	s.startTs = startTs
}

func (s *StreamBackupSearch) SetEndTs(endTs uint64) {
	s.endTs = endTs
}

func (s *StreamBackupSearch) decodeToRawKey(key []byte) ([]byte, error) {
	decodedKey := make([]byte, 0, len(key))
	for len(key) > encodeGroupLen {
		group := key[:encodeGroupLen+1]
		key = key[encodeGroupLen+1:]
		paddingNum := int(encodeMarker - group[encodeGroupLen])
		if paddingNum < 0 || paddingNum >= encodeGroupLen {
			return nil, errors.New("invalid padding number")
		}

		decodedKey = append(decodedKey, group[:encodeGroupLen-paddingNum]...)
		if paddingNum > 0 {
			break
		}
	}

	decodedKey = append(decodedKey, key...)
	return decodedKey, nil
}

func (s *StreamBackupSearch) encodeRawKey(key []byte) []byte {
	encodedKey := make([]byte, 0, (len(key)/8+1)*9)
	for len(key) > encodeGroupLen {
		group := key[:encodeGroupLen]
		key = key[encodeGroupLen:]
		encodedKey = append(encodedKey, group...)
		encodedKey = append(encodedKey, encodeMarker)
	}

	paddingNum := encodeGroupLen - len(key)
	encodedKey = append(encodedKey, key...)
	for i := 0; i < paddingNum; i++ {
		encodedKey = append(encodedKey, encodePadding)
	}
	encodedKey = append(encodedKey, encodeMarker-byte(paddingNum))
	return encodedKey
}

func (s *StreamBackupSearch) readDataFiles(ctx context.Context, ch chan<- *backuppb.DataFileInfo) error {
	opt := &storage.WalkOption{SubDir: GetStreamBackupMetaPrefix()}
	return s.storage.WalkDir(ctx, opt, func(path string, size int64) error {
		if strings.Contains(path, streamBackupMetaPrefix) {
			m := &backuppb.Metadata{}
			b, err := s.storage.ReadFile(ctx, path)
			if err != nil {
				return errors.Trace(err)
			}
			err = m.Unmarshal(b)
			if err != nil {
				return errors.Trace(err)
			}
			// TODO find a way to filter some unnecessary meta files.
			log.Debug("backup stream collect meta file", zap.String("file", path))

			s.resolveMetaData(ctx, m, ch)
		}
		return nil
	})
}

func (s *StreamBackupSearch) resolveMetaData(ctx context.Context, metaData *backuppb.Metadata, ch chan<- *backuppb.DataFileInfo) {
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

func (s *StreamBackupSearch) Search(ctx context.Context) ([]*StreamKVInfo, error) {
	dataFilesCh := make(chan *backuppb.DataFileInfo, 32)
	go func() {
		defer close(dataFilesCh)
		s.readDataFiles(ctx, dataFilesCh) // TODO deal with error
	}()

	pool := utils.NewWorkerPool(16, "search key")
	var wg, errWg sync.WaitGroup
	entriesCh, errCh := make(chan *StreamKVInfo, 64), make(chan error, 8)

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errCh {
			log.Warn("error happened when search data file", zap.Error(err))
		}
	}()

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

	entries := make([]*StreamKVInfo, 0, 128)
	for entry := range entriesCh {
		entries = append(entries, entry)
	}

	errWg.Wait()

	return entries, nil
}

func (s *StreamBackupSearch) searchFromDataFile(ctx context.Context, dataFile *backuppb.DataFileInfo, ch chan<- *StreamKVInfo) error {
	buff, err := s.storage.ReadFile(ctx, dataFile.Path)
	if err != nil {
		return errors.Annotatef(err, "read data file error, file: %s", dataFile.Path)
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

		k, v := iter.Key(), iter.Value()

		if !s.comparator.Compare(k, s.searchKey) {
			continue
		}

		_, ts, err := codec.DecodeUintDesc(k[len(k)-8:])
		if err != nil {
			return errors.Trace(err)
		}

		k = k[:len(k)-8]
		_, rawKey, err := codec.DecodeBytes(k, nil)
		if err != nil {
			return errors.Trace(err)
		}

		if dataFile.Cf == writeCFName {
			rawWriteCFValue := new(stream.RawWriteCFValue)
			rawWriteCFValue.HasShortValue()
			if err := rawWriteCFValue.ParseFrom(v); err != nil {
				return errors.Trace(err)
			}

			valueStr := ""
			if rawWriteCFValue.HasShortValue() {
				valueStr = base64.StdEncoding.EncodeToString(rawWriteCFValue.GetShortValue())
			}

			kvInfo := &StreamKVInfo{
				WriteType: rawWriteCFValue.GetWriteType(),
				CFName:    dataFile.Cf,
				CommitTs:  ts,
				StartTs:   rawWriteCFValue.GetStartTs(),
				Key:       strings.ToUpper(hex.EncodeToString(rawKey)),
				Value:     valueStr,
			}
			ch <- kvInfo
		} else if dataFile.Cf == defaultCFName {
			kvInfo := &StreamKVInfo{
				CFName:  dataFile.Cf,
				StartTs: ts,
				Key:     strings.ToUpper(hex.EncodeToString(rawKey)),
				Value:   base64.StdEncoding.EncodeToString(v),
			}
			ch <- kvInfo
		}
	}

	log.Info("finish search data file", zap.String("file", dataFile.Path))
	return nil
}
