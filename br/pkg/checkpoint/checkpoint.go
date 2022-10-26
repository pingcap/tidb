package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/tidb/br/pkg/metautil"
	"github.com/pingcap/tidb/br/pkg/storage"
)

const (
	CheckpointMetaPath = "checkpoint.meta"
	CheckpointDir      = "/checkpoints"
	CheckpointIndexDir = CheckpointDir + "/index"

	CheckpointFilesPathFormat = CheckpointDir + "/metafile.%09d.cpt"
	CheckpointIndexPathFormat = CheckpointIndexDir + "/file.%09d.cpt"
)

type dataFileLocker struct {
	// light lock for the dataFiles
	mutex     sync.Mutex
	dataFiles []*backuppb.File
}

func (d *dataFileLocker) append(files []*backuppb.File) {
	d.mutex.Lock()
	d.dataFiles = append(d.dataFiles, files...)
	d.mutex.Unlock()
}

func (d *dataFileLocker) drain() *backuppb.MetaFile {
	new := make([]*backuppb.File, 0)
	meta := &backuppb.MetaFile{}
	// do light lock
	d.mutex.Lock()
	meta.DataFiles = d.dataFiles
	d.dataFiles = new
	d.mutex.Unlock()

	return meta
}

type RangeGroups struct {
	Groups []*backuppb.BackupResponse `json:"groups"`
}

type CheckpointRunner struct {
	meta       *RangeGroups
	fileSeqNum int

	storage storage.ExternalStorage
	cipher  *backuppb.CipherInfo

	finishCh chan struct{}
	appendCh chan *backuppb.BackupResponse
	metaCh   chan *RangeGroups
	errCh    chan error

	wg sync.WaitGroup
}

func NewCheckpointRunner(storage storage.ExternalStorage, cipher *backuppb.CipherInfo, seqNum int) *CheckpointRunner {
	return &CheckpointRunner{
		meta: &RangeGroups{
			Groups: make([]*backuppb.BackupResponse, 0),
		},
		fileSeqNum: seqNum,

		storage: storage,
		cipher:  cipher,

		finishCh: make(chan struct{}),
		appendCh: make(chan *backuppb.BackupResponse),
		metaCh:   make(chan *RangeGroups),
		errCh:    make(chan error),
	}
}

func (r *CheckpointRunner) Append(ctx context.Context, resp *backuppb.BackupResponse) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-r.errCh:
		return err
	case r.appendCh <- resp:
		return nil
	}
}

// Cannot be parallel with `Append` function
func (r *CheckpointRunner) Finish(ctx context.Context) (err error) {
	// can not append anymore
	close(r.appendCh)
	select {
	case <-ctx.Done():
		return nil
	case e := <-r.errCh:
		return e
	case r.finishCh <- struct{}{}:
	}
	r.wg.Wait()
	return nil
}

func (r *CheckpointRunner) flushMeta(ctx context.Context, errCh chan error) error {
	meta := r.meta
	r.meta = &RangeGroups{
		Groups: make([]*backuppb.BackupResponse, 0),
	}
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

func (r *CheckpointRunner) StartCheckpointLoop(ctx context.Context) {
	r.wg.Add(1)
	checkpointLoop := func(ctx context.Context) {
		defer r.wg.Done()
		cctx, cancel := context.WithCancel(ctx)
		defer cancel()
		var wg sync.WaitGroup
		errCh := r.startCheckpointRunner(cctx, &wg)
		for {
			timer := time.NewTimer(30 * time.Second)
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				if err := r.flushMeta(ctx, errCh); err != nil {
					r.errCh <- err
					return
				}
			case <-r.finishCh:
				// appendCh has been closed
				for resp := range r.appendCh {
					r.meta.Groups = append(r.meta.Groups, resp)
				}
				if err := r.flushMeta(ctx, errCh); err != nil {
					r.errCh <- err
				}
				// close the channel to flush worker
				// and wait it to consumes all the metas
				close(r.metaCh)
				wg.Wait()
				return
			case resp := <-r.appendCh:
				r.meta.Groups = append(r.meta.Groups, resp)
			case err := <-errCh:
				// pass flush worker's error back
				r.errCh <- err
				return
			}
		}
	}

	go checkpointLoop(ctx)
}

func (r *CheckpointRunner) doFlush(ctx context.Context, meta *RangeGroups) error {
	if len(meta.Groups) == 0 {
		return nil
	}
	r.fileSeqNum++
	fname := fmt.Sprintf(CheckpointFilesPathFormat, r.fileSeqNum)

	// Flush the metaFile to storage
	content, err := json.Marshal(meta)
	if err != nil {
		return errors.Trace(err)
	}

	encryptBuff, iv, err := metautil.Encrypt(content, r.cipher)
	if err != nil {
		return errors.Trace(err)
	}

	err = r.storage.WriteFile(ctx, fname, append(iv, encryptBuff...))

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

	fname = fmt.Sprintf(CheckpointIndexPathFormat, r.fileSeqNum)

	content, err = file.Marshal()
	if err != nil {
		return errors.Trace(err)
	}

	err = r.storage.WriteFile(ctx, fname, append(iv, content...))

	return errors.Trace(err)
}

type CheckpointInfo struct {
	FlushedItemNum    uint64
	TotalDatafileSize uint64
	// used to generate MetaFile name.
	BackupMetafileSeqNum int

	IncrBackupMeta backuppb.BackupMeta
}

type CheckpointMetadata struct {
	ConfigHash []byte `json:"config-hash"`
	BackupTS   uint64 `json:"backup-ts"`

	CheckpointInfo   CheckpointInfo    `json:"-"`
	CheckpointRunner *CheckpointRunner `json:"-"`
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
