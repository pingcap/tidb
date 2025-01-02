package snapclient

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/stream"
	"github.com/pingcap/tidb/br/pkg/streamhelper"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/pkg/metrics"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	pd "github.com/tikv/pd/client"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type writerCall struct {
	cx context.Context
	cb func(error)
}

type writerRoutine struct {
	hnd chan<- writerCall
}

func (w writerRoutine) close() {
	close(w.hnd)
}

func (w writerRoutine) write(ctx context.Context) error {
	ch := make(chan error)
	w.hnd <- writerCall{
		cx: ctx,
		cb: func(err error) {
			select {
			case ch <- err:
			default:
			}
		},
	}
	return <-ch
}

func (c *pitrCollector) goWriter() {
	hnd := make(chan writerCall, 2048)
	exhaust := func(f func(writerCall)) {
	collect:
		for {
			select {
			case cb, ok := <-hnd:
				if !ok {
					log.Warn("Early channel close. Should not happen.")
					return
				}
				f(cb)
			default:
				break collect
			}
		}
	}

	go func() {
		for newCall := range hnd {
			cs := []writerCall{newCall}
			exhaust(func(newCall writerCall) {
				cs = append(cs, newCall)
			})

			err := c.doPersistExtraBackupMeta(cs[0].cx)

			for _, c := range cs {
				c.cb(err)
			}
		}
	}()

	c.writerRoutine = writerRoutine{
		hnd: hnd,
	}
}

type pitrCollector struct {
	// Immutable state.
	taskStorage    storage.ExternalStorage
	restoreStorage storage.ExternalStorage
	name           string
	enabled        bool
	restoreUUID    uuid.UUID

	// Mutable state.
	extraBackupMeta     extraBackupMeta
	extraBackupMetaLock sync.Mutex
	putMigOnce          sync.Once

	writerRoutine writerRoutine

	// Delegates.
	tso            func(ctx context.Context) (uint64, error)
	restoreSuccess func() bool
}

type extraBackupMeta struct {
	msg      pb.ExtraFullBackup
	rewrites map[int64]int64
}

func (c *extraBackupMeta) genMsg() *pb.ExtraFullBackup {
	msg := util.ProtoV1Clone(&c.msg)
	for old, new := range c.rewrites {
		msg.RewrittenTables = append(msg.RewrittenTables, &pb.RewrittenTableID{UpstreamOfUpstream: old, Upstream: new})
	}
	return msg
}

func (c *pitrCollector) close() error {
	if !c.enabled {
		return nil
	}

	cx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if !c.restoreSuccess() {
		log.Warn("Backup not success, put a half-finished metadata to the log backup.",
			zap.Stringer("uuid", c.restoreUUID))
		return errors.Annotatef(c.persistExtraBackupMeta(cx), "failed to persist the meta")
	}

	commitTS, err := c.commit(cx)
	if err != nil {
		return errors.Annotate(err, "failed to commit pitrCollector")
	}
	log.Info("Log backup SSTs are committed.",
		zap.Uint64("commitTS", commitTS), zap.String("committedTo", c.outputPath()))

	c.writerRoutine.close()
	return nil
}

func (c *pitrCollector) onBatch(ctx context.Context, fileSets restore.BatchBackupFileSet) (func() error, error) {
	if !c.enabled {
		return nil, nil
	}

	if err := c.prepareMigIfNeeded(ctx); err != nil {
		return nil, err
	}

	begin := time.Now()
	eg, ectx := errgroup.WithContext(ctx)
	fileCount := 0
	for _, fileSet := range fileSets {
		for _, file := range fileSet.SSTFiles {
			file := file
			fileCount += 1
			eg.Go(func() error {
				if err := c.putSST(ectx, file); err != nil {
					return errors.Annotatef(err, "failed to put sst %s", file.GetName())
				}
				return nil
			})
		}
		for _, hint := range fileSet.RewriteRules.TableIDRemapHint {
			hint := hint
			eg.Go(func() error {
				if err := c.putRewriteRule(ectx, hint.Origin, hint.Rewritten); err != nil {
					return errors.Annotatef(err, "failed to put rewrite rule of %v", fileSet.RewriteRules)
				}
				return nil
			})
		}
	}

	waitDone := func() error {
		err := eg.Wait()
		if err != nil {
			logutil.CL(ctx).Warn("Failed to upload SSTs for future PiTR.", logutil.ShortError(err))
			return err
		}

		logutil.CL(ctx).Info("Uploaded a batch of SSTs for future PiTR.",
			zap.Duration("take", time.Since(begin)), zap.Int("file-count", fileCount))

		err = c.persistExtraBackupMeta(ctx)
		if err != nil {
			return errors.Annotatef(err, "failed to persist backup meta when finishing batch")
		}
		return nil
	}
	return waitDone, nil
}

func (c *pitrCollector) doWithExtraBackupMetaLock(f func()) {
	c.extraBackupMetaLock.Lock()
	f()
	c.extraBackupMetaLock.Unlock()
}

// outputPath constructs the path by a relative path for outputting.
func (c *pitrCollector) outputPath(segs ...string) string {
	return filepath.Join(append([]string{"v1", "ext_backups", c.name}, segs...)...)
}

func (c *pitrCollector) metaPath() string {
	return c.outputPath("extbackupmeta")
}

func (c *pitrCollector) sstPath(name string) string {
	return c.outputPath("sst_files", name)
}

// putSST records an SST file.
func (c *pitrCollector) putSST(ctx context.Context, f *pb.File) error {
	if !c.enabled {
		return nil
	}

	begin := time.Now()

	f = util.ProtoV1Clone(f)
	out := c.sstPath(f.Name)

	copier, ok := c.taskStorage.(storage.Copier)
	if !ok {
		return errors.Annotatef(berrors.ErrInvalidArgument, "storage %T does not support copying", c.taskStorage)
	}
	spec := storage.CopySpec{
		From: f.GetName(),
		To:   out,
	}

	copyStart := time.Now()
	if err := copier.CopyFrom(ctx, c.restoreStorage, spec); err != nil {
		return err
	}
	log.Info("Copy SST to log backup storage success.", zap.String("file", f.Name), zap.Stringer("takes", time.Since(copyStart)))

	f.Name = out
	c.doWithExtraBackupMetaLock(func() { c.extraBackupMeta.msg.Files = append(c.extraBackupMeta.msg.Files, f) })

	metrics.RestoreUploadSSTForPiTRSeconds.Observe(time.Since(begin).Seconds())
	return nil
}

// putRewriteRule records a rewrite rule.
func (c *pitrCollector) putRewriteRule(_ context.Context, oldID int64, newID int64) error {
	if !c.enabled {
		return nil
	}
	var err error
	c.doWithExtraBackupMetaLock(func() {
		if oldVal, ok := c.extraBackupMeta.rewrites[oldID]; ok && oldVal != newID {
			err = errors.Annotatef(
				berrors.ErrInvalidArgument,
				"pitr coll rewrite rule conflict: we had %v -> %v, but you want rewrite to %v",
				oldID,
				oldVal,
				newID,
			)
			return
		}
		c.extraBackupMeta.rewrites[oldID] = newID
	})
	return err
}

func (c *pitrCollector) doPersistExtraBackupMeta(ctx context.Context) (err error) {
	c.extraBackupMetaLock.Lock()
	defer c.extraBackupMetaLock.Unlock()

	begin := time.Now()
	logutil.CL(ctx).Info("Persisting extra backup meta.",
		zap.Stringer("uuid", c.restoreUUID), zap.String("path", c.metaPath()), zap.Stringer("takes", time.Since(begin)))
	msg := c.extraBackupMeta.genMsg()
	bs, err := msg.Marshal()
	if err != nil {
		return errors.Annotate(err, "failed to marsal the committing message")
	}
	err = c.taskStorage.WriteFile(ctx, c.metaPath(), bs)
	if err != nil {
		return errors.Annotatef(err, "failed to put content to meta to %s", c.metaPath())
	}
	return nil
}

func (c *pitrCollector) persistExtraBackupMeta(ctx context.Context) (err error) {
	return c.writerRoutine.write(ctx)
}

// Commit commits the collected SSTs to a migration.
func (c *pitrCollector) prepareMig(ctx context.Context) error {
	if !c.enabled {
		return nil
	}

	est := stream.MigrationExtension(c.taskStorage)

	m := stream.NewMigration()
	m.ExtraFullBackupPaths = append(m.ExtraFullBackupPaths, c.metaPath())

	_, err := est.AppendMigration(ctx, m)
	if err != nil {
		return errors.Annotatef(err, "failed to add the extra backup at path %s", c.metaPath())
	}

	c.doWithExtraBackupMetaLock(func() {
		c.resetCommitting()
	})
	// Persist the metadata in case of SSTs were uploaded but the meta wasn't,
	// which leads to a leakage.
	return c.persistExtraBackupMeta(ctx)
}

func (c *pitrCollector) prepareMigIfNeeded(ctx context.Context) (err error) {
	c.putMigOnce.Do(func() {
		err = c.prepareMig(ctx)
	})
	return
}

func (c *pitrCollector) commit(ctx context.Context) (uint64, error) {
	c.extraBackupMeta.msg.Finished = true
	ts, err := c.tso(ctx)
	if err != nil {
		return 0, err
	}
	c.extraBackupMeta.msg.AsIfTs = ts
	return ts, c.persistExtraBackupMeta(ctx)
}

func (c *pitrCollector) resetCommitting() {
	c.extraBackupMeta = extraBackupMeta{
		rewrites: map[int64]int64{},
	}
	c.extraBackupMeta.msg.FilesPrefixHint = c.sstPath("")
	c.extraBackupMeta.msg.Finished = false
	c.extraBackupMeta.msg.BackupUuid = c.restoreUUID[:]
}

// PiTRCollDep is the dependencies of a PiTR collector.
type PiTRCollDep struct {
	PDCli   pd.Client
	EtcdCli *clientv3.Client
	Storage *pb.StorageBackend
}

// newPiTRColl creates a new PiTR collector.
func newPiTRColl(ctx context.Context, deps PiTRCollDep) (*pitrCollector, error) {
	mcli := streamhelper.NewMetaDataClient(deps.EtcdCli)
	ts, err := mcli.GetAllTasks(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if len(ts) > 1 {
		return nil, errors.Annotatef(berrors.ErrInvalidArgument, "more than one task found, pitr collector doesn't support that")
	}
	if len(ts) == 0 {
		return &pitrCollector{}, nil
	}

	coll := &pitrCollector{
		enabled: true,
	}

	strg, err := storage.Create(ctx, ts[0].Info.Storage, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	coll.taskStorage = strg

	tso := func(ctx context.Context) (uint64, error) {
		l, o, err := deps.PDCli.GetTS(ctx)
		return oracle.ComposeTS(l, o), err
	}
	coll.tso = tso

	t, err := tso(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	coll.name = fmt.Sprintf("backup-%016X", t)

	restoreStrg, err := storage.Create(ctx, deps.Storage, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	coll.restoreStorage = restoreStrg
	coll.restoreSuccess = summary.Succeed
	coll.goWriter()
	coll.resetCommitting()
	return coll, nil
}
