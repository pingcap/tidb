package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
	"go.uber.org/zap"
)

const (
	CheckpointMetaPath = "checkpoint.meta"
	CheckpointDir      = "/checkpoints"

	CheckpointDataDir     = CheckpointDir + "/data"
	CheckpointChecksumDir = CheckpointDir + "/checksum"
)

const tickDuration = 30 * time.Second

type CheckpointMessage struct {
	// start-key of the origin range
	GroupKey string

	Group *rtree.Range
}

type RangeGroups struct {
	GroupKey string
	Groups   []*rtree.Range `json:"groups"`
}

type RangeGroupData struct {
	RangeGroupsEncriptedData []byte
	Checksum                 []byte
	CipherIv                 []byte

	Size int
}

type CheckpointData struct {
	RangeGroupMetas []*RangeGroupData `json:"range-group-metas"`
}

type CheckpointRunner struct {
	meta map[string]*RangeGroups

	storage storage.ExternalStorage
	cipher  *backuppb.CipherInfo

	appendCh chan *CheckpointMessage
	metaCh   chan map[string]*RangeGroups
	errCh    chan error

	wg sync.WaitGroup
}

func StartCheckpointRunnerForTest(ctx context.Context, storage storage.ExternalStorage, cipher *backuppb.CipherInfo, tick time.Duration) *CheckpointRunner {
	runner := &CheckpointRunner{
		meta: make(map[string]*RangeGroups),

		storage: storage,
		cipher:  cipher,

		appendCh: make(chan *CheckpointMessage),
		metaCh:   make(chan map[string]*RangeGroups),
		errCh:    make(chan error),
	}

	runner.startCheckpointLoop(ctx, tick)
	return runner
}

func StartCheckpointRunner(ctx context.Context, storage storage.ExternalStorage, cipher *backuppb.CipherInfo) *CheckpointRunner {
	runner := &CheckpointRunner{
		meta: make(map[string]*RangeGroups),

		storage: storage,
		cipher:  cipher,

		appendCh: make(chan *CheckpointMessage),
		metaCh:   make(chan map[string]*RangeGroups),
		errCh:    make(chan error),
	}

	runner.startCheckpointLoop(ctx, tickDuration)
	return runner
}

func (r *CheckpointRunner) FlushChecksum(ctx context.Context, tableID int64, crc64xor uint64, totalKvs uint64, totalBytes uint64) error {
	data, err := json.Marshal(&ChecksumItem{
		TableID:    tableID,
		Crc64xor:   crc64xor,
		TotalKvs:   totalKvs,
		TotalBytes: totalBytes,
	})
	if err != nil {
		return errors.Trace(err)
	}
	fname := fmt.Sprintf("%s/t%d", CheckpointChecksumDir, tableID)
	return r.storage.WriteFile(ctx, fname, data)
}

func (r *CheckpointRunner) Append(
	ctx context.Context,
	groupKey string,
	startKey []byte,
	endKey []byte,
	files []*backuppb.File,
) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-r.errCh:
		return err
	case r.appendCh <- &CheckpointMessage{
		GroupKey: groupKey,
		Group: &rtree.Range{
			StartKey: startKey,
			EndKey:   endKey,
			Files:    files,
		},
	}:
		return nil
	}
}

// Cannot be parallel with `Append` function
func (r *CheckpointRunner) Finish(ctx context.Context) (err error) {
	// can not append anymore
	close(r.appendCh)
	r.wg.Wait()
	return nil
}

func (r *CheckpointRunner) flushMeta(ctx context.Context, errCh chan error) error {
	meta := r.meta
	r.meta = make(map[string]*RangeGroups)
	// do flush
	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	case r.metaCh <- meta:
	}
	return nil
}

func (r *CheckpointRunner) startCheckpointRunner(ctx context.Context, wg *sync.WaitGroup) chan error {
	errCh := make(chan error)
	wg.Add(1)
	flushWorker := func(ctx context.Context, errCh chan error) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():

			case meta, ok := <-r.metaCh:
				if !ok {
					log.Info("stop checkpoint flush worker")
					return
				}
				if err := r.doFlush(ctx, meta); err != nil {
					errCh <- err
					return
				}
			}
		}
	}

	go flushWorker(ctx, errCh)
	return errCh
}

func (r *CheckpointRunner) startCheckpointLoop(ctx context.Context, tickDuration time.Duration) {
	r.wg.Add(1)
	checkpointLoop := func(ctx context.Context) {
		defer r.wg.Done()
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		errCh := r.startCheckpointRunner(cctx, &wg)
		ticker := time.NewTicker(tickDuration)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := r.flushMeta(ctx, errCh); err != nil {
					r.errCh <- err
					return
				}
			case msg, ok := <-r.appendCh:
				if !ok {
					log.Info("stop checkpoint runner")
					if err := r.flushMeta(ctx, errCh); err != nil {
						r.errCh <- err
					}
					// close the channel to flush worker
					// and wait it to consumes all the metas
					close(r.metaCh)
					wg.Wait()
					return
				}
				groups, exist := r.meta[msg.GroupKey]
				if !exist {
					groups = &RangeGroups{
						GroupKey: msg.GroupKey,
						Groups:   make([]*rtree.Range, 0),
					}
					r.meta[msg.GroupKey] = groups
				}
				groups.Groups = append(groups.Groups, msg.Group)
			case err := <-errCh:
				// pass flush worker's error back
				r.errCh <- err
				return
			}
		}
	}

	go checkpointLoop(ctx)
}

func (r *CheckpointRunner) doFlush(ctx context.Context, meta map[string]*RangeGroups) error {
	if len(meta) == 0 {
		return nil
	}

	checkpointData := &CheckpointData{
		RangeGroupMetas: make([]*RangeGroupData, 0, len(meta)),
	}

	var fname string

	for _, group := range meta {
		if len(group.Groups) == 0 {
			continue
		}

		// use the first item's group-key and sub-range-key as the filename
		if len(fname) == 0 {
			fname = fmt.Sprintf("%s..%s", group.GroupKey, group.Groups[0].StartKey)
		}

		// Flush the metaFile to storage
		content, err := json.Marshal(group)
		if err != nil {
			return errors.Trace(err)
		}

		encryptBuff, iv, err := metautil.Encrypt(content, r.cipher)
		if err != nil {
			return errors.Trace(err)
		}

		checksum := sha256.Sum256(content)

		checkpointData.RangeGroupMetas = append(checkpointData.RangeGroupMetas, &RangeGroupData{
			RangeGroupsEncriptedData: encryptBuff,
			Checksum:                 checksum[:],
			Size:                     len(content),
			CipherIv:                 iv,
		})
	}

	if len(checkpointData.RangeGroupMetas) > 0 {
		data, err := json.Marshal(checkpointData)
		if err != nil {
			return errors.Trace(err)
		}

		checksum := sha256.Sum256([]byte(fname))
		checksumEncoded := base64.URLEncoding.EncodeToString(checksum[:])
		path := fmt.Sprintf("%s/%s_%d.cpt", CheckpointDataDir, checksumEncoded, rand.Uint64())
		log.Info("path", zap.String("path", path))
		if err := r.storage.WriteFile(ctx, path, data); err != nil {
			log.Info("error", zap.Error(err))
			return errors.Trace(err)
		}
	}
	return nil
}

func WalkCheckpointFile(ctx context.Context, s storage.ExternalStorage, cipher *backuppb.CipherInfo, fn func(groupKey string, rg *rtree.Range)) error {
	err := s.WalkDir(ctx, &storage.WalkOption{SubDir: CheckpointDataDir}, func(path string, size int64) error {
		if strings.HasSuffix(path, ".cpt") {
			content, err := s.ReadFile(ctx, path)
			if err != nil {
				return errors.Trace(err)
			}

			checkpointData := &CheckpointData{}
			if err = json.Unmarshal(content, checkpointData); err != nil {
				return errors.Trace(err)
			}

			for _, meta := range checkpointData.RangeGroupMetas {
				decryptContent, err := metautil.Decrypt(meta.RangeGroupsEncriptedData, cipher, meta.CipherIv)
				if err != nil {
					return errors.Trace(err)
				}

				checksum := sha256.Sum256(decryptContent)
				if !bytes.Equal(meta.Checksum, checksum[:]) {
					return errors.Annotatef(berrors.ErrInvalidMetaFile,
						"checksum mismatch expect %x, got %x", meta.Checksum, checksum[:])
				}

				group := &RangeGroups{}
				if err = json.Unmarshal(decryptContent, meta); err != nil {
					return errors.Trace(err)
				}

				for _, g := range group.Groups {
					fn(group.GroupKey, g)
				}
			}
		}
		return nil
	})

	return errors.Trace(err)
}

type ChecksumItem struct {
	TableID    int64  `json:"table-id"`
	Crc64xor   uint64 `json:"crc64-xor"`
	TotalKvs   uint64 `json:"total-kvs"`
	TotalBytes uint64 `json:"total-bytes"`
}

type CheckpointMetadata struct {
	ConfigHash []byte        `json:"config-hash"`
	BackupTS   uint64        `json:"backup-ts"`
	Ranges     []rtree.Range `json:"ranges"`

	CheckpointChecksum map[int64]*ChecksumItem    `json:"-"`
	CheckpointDataMap  map[string]rtree.RangeTree `json:"-"`
}

func LoadCheckpointMetadata(ctx context.Context, s storage.ExternalStorage) (*CheckpointMetadata, error) {
	data, err := s.ReadFile(ctx, CheckpointMetaPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m := &CheckpointMetadata{}
	err = json.Unmarshal(data, m)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m.CheckpointChecksum, err = loadCheckpointChecksum(ctx, s)
	return m, errors.Trace(err)
}

func loadCheckpointChecksum(ctx context.Context, s storage.ExternalStorage) (map[int64]*ChecksumItem, error) {
	checkpointChecksum := make(map[int64]*ChecksumItem)

	err := s.WalkDir(ctx, &storage.WalkOption{SubDir: CheckpointChecksumDir}, func(path string, size int64) error {
		data, err := s.ReadFile(ctx, path)
		if err != nil {
			return errors.Trace(err)
		}
		c := &ChecksumItem{}
		err = json.Unmarshal(data, c)
		if err != nil {
			return errors.Trace(err)
		}
		checkpointChecksum[c.TableID] = c
		return nil
	})
	return checkpointChecksum, errors.Trace(err)
}

func SaveCheckpointMetadata(ctx context.Context, s storage.ExternalStorage, meta *CheckpointMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	err = s.WriteFile(ctx, CheckpointMetaPath, data)
	return errors.Trace(err)
}
