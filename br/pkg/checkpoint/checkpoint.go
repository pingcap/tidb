package checkpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/storage"
)

const (
	CheckpointMetaPath = "checkpoint.meta"
	CheckpointDir      = "/checkpoints"

	CheckpointDirFormat      = CheckpointDir + "/%s"
	CheckpointIndexDirFormat = CheckpointDirFormat + "/index"

	CheckpointFilesPathFormat = CheckpointDirFormat + "/filegroups.%s.cpt"
	CheckpointIndexPathFormat = CheckpointIndexDirFormat + "/file.%s.cpt"
)

const tickDuration = 30 * time.Second

type CheckpointMessage struct {
	GroupKey string

	Group *rtree.Range
}

type RangeGroups struct {
	Groups []*rtree.Range `json:"groups"`
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
						Groups: make([]*rtree.Range, 0),
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

	for groupKey, group := range meta {
		if len(group.Groups) == 0 {
			continue
		}
		idenKey := redact.Key(group.Groups[0].StartKey)
		fname := fmt.Sprintf(CheckpointFilesPathFormat, groupKey, idenKey)

		// Flush the metaFile to storage
		content, err := json.Marshal(group)
		if err != nil {
			return errors.Trace(err)
		}

		encryptBuff, iv, err := metautil.Encrypt(content, r.cipher)
		if err != nil {
			return errors.Trace(err)
		}

		err = r.storage.WriteFile(ctx, fname, encryptBuff)

		if err != nil {
			return errors.Trace(err)
		}

		checksum := sha256.Sum256(content)

		// Flush the indexFile (to metaFile) to storage
		file := &backuppb.File{
			Name:     fname,
			Sha256:   checksum[:],
			Size_:    uint64(len(content)),
			CipherIv: iv,
		}

		fname = fmt.Sprintf(CheckpointIndexPathFormat, groupKey, idenKey)

		content, err = file.Marshal()
		if err != nil {
			return errors.Trace(err)
		}

		err = r.storage.WriteFile(ctx, fname, content)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func WalkCheckpointFileWithSpecificKey(ctx context.Context, s storage.ExternalStorage, groupKey string, cipher *backuppb.CipherInfo, fn func(*rtree.Range)) error {
	subDir := fmt.Sprintf(CheckpointIndexDirFormat, groupKey)
	err := s.WalkDir(ctx, &storage.WalkOption{SubDir: subDir}, func(path string, _ int64) error {
		if strings.HasSuffix(path, ".cpt") {
			content, err := s.ReadFile(ctx, path)
			if err != nil {
				return err
			}

			metaIndex := &backuppb.File{}
			if err = metaIndex.Unmarshal(content); err != nil {
				return errors.Trace(err)
			}

			content, err = s.ReadFile(ctx, metaIndex.Name)
			if err != nil {
				return err
			}

			decryptContent, err := metautil.Decrypt(content, cipher, metaIndex.CipherIv)
			if err != nil {
				return errors.Trace(err)
			}

			checksum := sha256.Sum256(decryptContent)
			if !bytes.Equal(metaIndex.Sha256, checksum[:]) {
				return errors.Annotatef(berrors.ErrInvalidMetaFile,
					"checksum mismatch expect %x, got %x", metaIndex.Sha256, checksum[:])
			}

			meta := &RangeGroups{}
			if err = json.Unmarshal(decryptContent, meta); err != nil {
				return errors.Trace(err)
			}

			for _, g := range meta.Groups {
				fn(g)
			}
		}
		return nil
	})

	return errors.Trace(err)
}

type CheckpointMetadata struct {
	ConfigHash []byte `json:"config-hash"`
	BackupTS   uint64 `json:"backup-ts"`

	Ranges []rtree.Range `json:"ranges"`
}

func LoadCheckpointMetadata(ctx context.Context, s storage.ExternalStorage) (*CheckpointMetadata, error) {
	data, err := s.ReadFile(ctx, CheckpointMetaPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	m := &CheckpointMetadata{}
	err = json.Unmarshal(data, m)
	return m, errors.Trace(err)
}

func SaveCheckpointMetadata(ctx context.Context, s storage.ExternalStorage, meta *CheckpointMetadata) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	err = s.WriteFile(ctx, CheckpointMetaPath, data)
	return errors.Trace(err)
}
