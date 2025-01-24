// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash/crc64"
	"maps"
	"math"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/docker/go-units"
	"github.com/fatih/color"
	"github.com/google/uuid"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	pb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"github.com/pingcap/tidb/pkg/util/versioninfo"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	baseMigrationSN   = 0
	baseMigrationName = "BASE"
	baseTmp           = "BASE_TMP"
	metaSuffix        = ".meta"
	migrationPrefix   = "v1/migrations"
	lockPrefix        = "v1/LOCK"

	SupportedMigVersion = pb.MigrationVersion_M2
)

func NewMigration() *pb.Migration {
	return &pb.Migration{
		Version: pb.MigrationVersion_M2,
		Creator: fmt.Sprintf("br;commit=%s;branch=%s", versioninfo.TiDBGitHash, versioninfo.TiDBGitBranch),
	}
}

type StreamMetadataSet struct {
	// if set true, the metadata and datafile won't be removed
	DryRun bool

	// keeps the meta-information of metadata as little as possible
	// to save the memory
	metadataInfos             map[string]*MetadataInfo
	MetadataDownloadBatchSize uint

	// a parser of metadata
	Helper *MetadataHelper

	// for test
	BeforeDoWriteBack func(path string, replaced *pb.Metadata) (skip bool)
}

// keep these meta-information for statistics and filtering
type FileGroupInfo struct {
	MaxTS   uint64
	Length  uint64
	KVCount int64
}

// keep these meta-information for statistics and filtering
type MetadataInfo struct {
	MinTS          uint64
	FileGroupInfos []*FileGroupInfo
}

func (ms *StreamMetadataSet) TEST_GetMetadataInfos() map[string]*MetadataInfo {
	return ms.metadataInfos
}

// LoadUntilAndCalculateShiftTS loads the metadata until the specified timestamp and calculate
// the shift-until-ts by the way. This would record all metadata files that *may* contain data
// from transaction committed before that TS.
func (ms *StreamMetadataSet) LoadUntilAndCalculateShiftTS(
	ctx context.Context,
	s storage.ExternalStorage,
	until uint64,
) (uint64, error) {
	metadataMap := struct {
		sync.Mutex
		metas        map[string]*MetadataInfo
		shiftUntilTS uint64
	}{}
	metadataMap.metas = make(map[string]*MetadataInfo)
	// `shiftUntilTS` must be less than `until`
	metadataMap.shiftUntilTS = until
	err := FastUnmarshalMetaData(ctx, s, ms.MetadataDownloadBatchSize, func(path string, raw []byte) error {
		m, err := ms.Helper.ParseToMetadataHard(raw)
		if err != nil {
			return err
		}
		// If the meta file contains only files with ts grater than `until`, when the file is from
		// `Default`: it should be kept, because its corresponding `write` must has commit ts grater
		//            than it, which should not be considered.
		// `Write`: it should trivially not be considered.
		if m.MinTs <= until {
			// record these meta-information for statistics and filtering
			fileGroupInfos := make([]*FileGroupInfo, 0, len(m.FileGroups))
			for _, group := range m.FileGroups {
				var kvCount int64 = 0
				for _, file := range group.DataFilesInfo {
					kvCount += file.NumberOfEntries
				}
				fileGroupInfos = append(fileGroupInfos, &FileGroupInfo{
					MaxTS:   group.MaxTs,
					Length:  group.Length,
					KVCount: kvCount,
				})
			}
			metadataMap.Lock()
			metadataMap.metas[path] = &MetadataInfo{
				MinTS:          m.MinTs,
				FileGroupInfos: fileGroupInfos,
			}
			metadataMap.Unlock()
		}
		// filter out the metadatas whose ts-range is overlap with [until, +inf)
		// and calculate their minimum begin-default-ts
		ts, ok := UpdateShiftTS(m, until, mathutil.MaxUint)
		if ok {
			metadataMap.Lock()
			if ts < metadataMap.shiftUntilTS {
				metadataMap.shiftUntilTS = ts
			}
			metadataMap.Unlock()
		}
		return nil
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	ms.metadataInfos = metadataMap.metas
	if metadataMap.shiftUntilTS != until {
		log.Warn("calculate shift-ts", zap.Uint64("start-ts", until), zap.Uint64("shift-ts", metadataMap.shiftUntilTS))
	}
	return metadataMap.shiftUntilTS, nil
}

// LoadFrom loads data from an external storage into the stream metadata set. (Now only for test)
func (ms *StreamMetadataSet) LoadFrom(ctx context.Context, s storage.ExternalStorage) error {
	_, err := ms.LoadUntilAndCalculateShiftTS(ctx, s, math.MaxUint64)
	return err
}

func (ms *StreamMetadataSet) iterateDataFiles(f func(d *FileGroupInfo) (shouldBreak bool)) {
	for _, m := range ms.metadataInfos {
		for _, d := range m.FileGroupInfos {
			if f(d) {
				return
			}
		}
	}
}

// IterateFilesFullyBefore runs the function over all files contain data before the timestamp only.
//
//	0                                          before
//	|------------------------------------------|
//	 |-file1---------------| <- File contains records in this TS range would be found.
//	                               |-file2--------------| <- File contains any record out of this won't be found.
//
// This function would call the `f` over file1 only.
func (ms *StreamMetadataSet) IterateFilesFullyBefore(before uint64, f func(d *FileGroupInfo) (shouldBreak bool)) {
	ms.iterateDataFiles(func(d *FileGroupInfo) (shouldBreak bool) {
		if d.MaxTS >= before {
			return false
		}
		return f(d)
	})
}

type updateFnHook struct {
	NoHooks
	updateFn func(num int64)
}

func (hook updateFnHook) DeletedAFileForTruncating(count int) {
	hook.updateFn(int64(count))
}

// RemoveDataFilesAndUpdateMetadataInBatch concurrently remove datafilegroups and update metadata.
// Only one metadata is processed in each thread, including deleting its datafilegroup and updating it.
// Returns the not deleted datafilegroups.
func (ms *StreamMetadataSet) RemoveDataFilesAndUpdateMetadataInBatch(
	ctx context.Context,
	from uint64,
	st storage.ExternalStorage,
	// num = deleted files
	updateFn func(num int64),
) ([]string, error) {
	hst := ms.hook(st)
	est := MigrationExtension(hst)
	est.Hooks = updateFnHook{updateFn: updateFn}
	res := MigratedTo{NewBase: NewMigration()}
	est.doTruncateLogs(ctx, ms, from, &res)

	if bst, ok := hst.ExternalStorage.(*storage.Batched); ok {
		effs, err := storage.SaveJSONEffectsToTmp(bst.ReadOnlyEffects())
		if err != nil {
			log.Warn("failed to save effects", logutil.ShortError(err))
		} else {
			log.Info("effects are saved, you may check them then.", zap.String("path", effs))
		}
	}

	notDeleted := []string{}
	for _, me := range res.NewBase.EditMeta {
		notDeleted = append(notDeleted, me.DeletePhysicalFiles...)
	}

	// Hacking: if we started to delete some files, we must enter the `cleanUp` phase,
	// then, all warnings should be `cannot delete file`.
	if len(res.Warnings) > 0 && len(notDeleted) == 0 {
		return nil, multierr.Combine(res.Warnings...)
	}

	return notDeleted, nil
}

func truncateAndWrite(ctx context.Context, s storage.ExternalStorage, path string, data []byte) error {
	// Performance hack: the `Write` implementation would truncate the file if it exists.
	if err := s.WriteFile(ctx, path, data); err != nil {
		return errors.Annotatef(err, "failed to save the file %s to %s", path, s.URI())
	}
	return nil
}

const (
	// TruncateSafePointFileName is the filename that the ts(the log have been truncated) is saved into.
	TruncateSafePointFileName = "v1_stream_trancate_safepoint.txt"
)

// GetTSFromFile gets the current truncate safepoint.
// truncate safepoint is the TS used for last truncating:
// which means logs before this TS would probably be deleted or incomplete.
func GetTSFromFile(
	ctx context.Context,
	s storage.ExternalStorage,
	filename string,
) (uint64, error) {
	exists, err := s.FileExists(ctx, filename)
	if err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	data, err := s.ReadFile(ctx, filename)
	if err != nil {
		return 0, err
	}
	value, err := strconv.ParseUint(string(data), 10, 64)
	if err != nil {
		return 0, berrors.ErrInvalidMetaFile.GenWithStackByArgs("failed to parse the truncate safepoint")
	}
	return value, nil
}

// SetTSToFile overrides the current truncate safepoint.
// truncate safepoint is the TS used for last truncating:
// which means logs before this TS would probably be deleted or incomplete.
func SetTSToFile(
	ctx context.Context,
	s storage.ExternalStorage,
	safepoint uint64,
	filename string,
) error {
	content := strconv.FormatUint(safepoint, 10)
	return truncateAndWrite(ctx, s, filename, []byte(content))
}

func UpdateShiftTS(m *pb.Metadata, startTS uint64, restoreTS uint64) (uint64, bool) {
	var (
		minBeginTS uint64
		isExist    bool
	)
	if len(m.FileGroups) == 0 || m.MinTs > restoreTS || m.MaxTs < startTS {
		return 0, false
	}

	for _, ds := range m.FileGroups {
		for _, d := range ds.DataFilesInfo {
			if d.Cf == DefaultCF || d.MinBeginTsInDefaultCf == 0 {
				continue
			}
			if d.MinTs > restoreTS || d.MaxTs < startTS {
				continue
			}
			if d.MinBeginTsInDefaultCf < minBeginTS || !isExist {
				isExist = true
				minBeginTS = d.MinBeginTsInDefaultCf
			}
		}
	}
	return minBeginTS, isExist
}

func updateMetadataInternalStat(meta *pb.Metadata) {
	if len(meta.FileGroups) == 0 {
		meta.MinTs = 0
		meta.MaxTs = 0
		meta.ResolvedTs = 0
		return
	}

	meta.MinTs = meta.FileGroups[0].MinTs
	meta.MaxTs = meta.FileGroups[0].MaxTs
	meta.ResolvedTs = meta.FileGroups[0].MinResolvedTs
	for _, group := range meta.FileGroups {
		if group.MinTs < meta.MinTs {
			meta.MinTs = group.MinTs
		}
		if group.MaxTs > meta.MaxTs {
			meta.MaxTs = group.MaxTs
		}
		if group.MinResolvedTs < meta.ResolvedTs {
			meta.ResolvedTs = group.MinResolvedTs
		}
	}
}

// replace the filegroups and update the ts of the replaced metadata
func ReplaceMetadata(meta *pb.Metadata, filegroups []*pb.DataFileGroup) {
	// replace the origin metadata
	meta.FileGroups = filegroups

	updateMetadataInternalStat(meta)
}

type marshalMigrationContext struct {
	context.Context
	est MigrationExt

	output   *glue.Table
	keyspace []string
}

func (m *marshalMigrationContext) emit(key, value string) {
	bold := color.New(color.Bold).Sprintf
	ks := new(strings.Builder)
	for _, k := range m.keyspace {
		ks.WriteString(k)
		ks.WriteString("/")
	}
	ks.WriteString(key)

	finalValue := bold(value)
	m.output.Add(ks.String(), finalValue)
}

func (m *marshalMigrationContext) keyspaced(key []string, f func()) {
	m.keyspace = append(m.keyspace, key...)
	defer func() {
		m.keyspace = m.keyspace[:len(m.keyspace)-len(key)]
	}()

	f()
}

func (m *marshalMigrationContext) addCompaction(c *pb.LogFileCompaction) {
	m.emit("name", c.Name)
	m.emit("time", fmt.Sprintf("%d ~ %d", c.CompactionFromTs, c.CompactionUntilTs))
	m.emit("file", fmt.Sprintf("[%q, %q]", c.Artifacts, c.GeneratedFiles))
}

func (m *marshalMigrationContext) addMetaEdits(em []*pb.MetaEdit) {
	if len(em) == 0 {
		return
	}

	totalDeletePhyFile := 0
	totalDeleteLgcFile := 0
	for _, edit := range em {
		totalDeletePhyFile += len(edit.DeletePhysicalFiles)
		for _, dl := range edit.DeleteLogicalFiles {
			totalDeleteLgcFile += len(dl.Spans)
		}
	}
	m.emit("edit_meta_files", strconv.Itoa(len(em)))
	m.emit("delete_physical_file", strconv.Itoa(totalDeletePhyFile))
	m.emit("delete_logical_file", strconv.Itoa(totalDeleteLgcFile))
}

func (m *marshalMigrationContext) addTruncatedTo(tso uint64) {
	if tso == 0 {
		return
	}
	m.emit("truncated_to", strconv.FormatUint(tso, 10))
	t := oracle.GetTimeFromTS(tso)
	m.emit("truncated_to_in_rfc3339", t.Format(time.RFC3339))
}

func (m *marshalMigrationContext) addMigration(mig *pb.Migration) {
	m.addTruncatedTo(mig.TruncatedTo)
	for i, c := range mig.Compactions {
		m.keyspaced([]string{"compactions", strconv.Itoa(i)}, func() {
			m.addCompaction(c)
		})
	}
	m.keyspaced([]string{"meta_edit"}, func() {
		m.addMetaEdits(mig.EditMeta)
	})
	for i, d := range mig.DestructPrefix {
		m.keyspaced([]string{"destruct_prefix", strconv.Itoa(i)}, func() {
			m.emit("value", d)
		})
	}
	for i, p := range mig.IngestedSstPaths {
		m.keyspaced([]string{"extra_full_backup", strconv.Itoa(i)}, func() {
			m.addIngestedSSTss(p)
		})
	}
}

func (m *marshalMigrationContext) addIngestedSSTss(path string) {
	fullbk, err := readIngestedSSTs(m.Context, path, m.est.s)
	if err != nil {
		m.emit("err_during_reading", err.Error())
		m.emit("meta_path", path)
		return
	}

	m.emit("as_if_ts", strconv.FormatUint(fullbk.AsIfTs, 10))
	m.emit("backup_uuid", hex.EncodeToString(fullbk.GetBackupUuid()))
	m.emit("files_count", strconv.Itoa(len(fullbk.Files)))
	m.emit("files_position", fullbk.FilesPrefixHint)
}

func (m MigrationExt) AddMigrationToTable(ctx context.Context, mig *pb.Migration, table *glue.Table) {
	cx := marshalMigrationContext{
		Context: ctx,
		est:     m,
		output:  table,
	}

	cx.addMigration(mig)
}

// MigrationExt is an extension to the `ExternalStorage` type.
// This added some support methods for the "migration" system of log backup.
//
// Migrations are idempontent batch modifications (adding a compaction, delete a file, etc..) to the backup files.
// You may check the protocol buffer message `Migration` for more details.
// Idempontence is important for migrations, as they may be executed multi times due to retry or racing.
//
// The encoded migrations will be put in a folder in the external storage,
// they are ordered by a series number.
//
// Not all migrations can be applied to the storage then removed from the migration.
// Small "additions" will be inlined into the migration, for example, a `Compaction`.
// Small "deletions" will also be delayed, for example, deleting a span in a file.
// Such operations will be save to a special migration, the first migration, named "BASE".
//
// A simple list of migrations (loaded by `Load()`):
/*
base = [ compaction, deleteSpan, ... ],
layers = {
  { sn = 1, content = [ compaction, ... ] },
  { sn = 2, content = [ compaction, deleteFiles, ... ] },
*/
type MigrationExt struct {
	s      storage.ExternalStorage
	prefix string
	// The hooks used for tracking the execution.
	// See the `Hooks` type for more details.
	Hooks Hooks
}

type Hooks interface {
	StartLoadingMetaForTruncating()
	EndLoadingMetaForTruncating()

	StartDeletingForTruncating(meta *StreamMetadataSet, shiftTS uint64)
	DeletedAFileForTruncating(count int)
	DeletedAllFilesForTruncating()

	StartHandlingMetaEdits([]*pb.MetaEdit)
	HandledAMetaEdit(*pb.MetaEdit)
	HandingMetaEditDone()
}

func NewProgressBarHooks(console glue.ConsoleOperations) *ProgressBarHooks {
	return &ProgressBarHooks{console: console}
}

type ProgressBarHooks struct {
	console glue.ConsoleOperations

	readMetaDone            func()
	deletingForTruncateProg glue.ProgressWaiter
	handlingMetaEditProg    glue.ProgressWaiter
}

func (p *ProgressBarHooks) StartLoadingMetaForTruncating() {
	log.Info("Start reading metadata.")
	p.readMetaDone = p.console.ShowTask("Reading Metadata... ", glue.WithTimeCost())
}

func (p *ProgressBarHooks) EndLoadingMetaForTruncating() {
	if p.readMetaDone != nil {
		p.readMetaDone()
	}
}

func (p *ProgressBarHooks) StartDeletingForTruncating(meta *StreamMetadataSet, shiftTS uint64) {
	var (
		fileCount int    = 0
		kvCount   int64  = 0
		totalSize uint64 = 0
	)

	meta.IterateFilesFullyBefore(shiftTS, func(d *FileGroupInfo) (shouldBreak bool) {
		fileCount++
		totalSize += d.Length
		kvCount += d.KVCount
		return
	})

	p.deletingForTruncateProg = p.console.StartProgressBar(
		"Clearing Data Files and Metadata", fileCount,
		glue.WithTimeCost(),
		glue.WithConstExtraField("kv-count", kvCount),
		glue.WithConstExtraField("kv-size", fmt.Sprintf("%d(%s)", totalSize, units.HumanSize(float64(totalSize)))),
	)
}

func (p *ProgressBarHooks) DeletedAFileForTruncating(count int) {
	if p.deletingForTruncateProg != nil {
		p.deletingForTruncateProg.IncBy(int64(count))
	}
}

func (p *ProgressBarHooks) DeletedAllFilesForTruncating() {
	if p.deletingForTruncateProg != nil {
		p.deletingForTruncateProg.Close()
	}
}

func (p *ProgressBarHooks) StartHandlingMetaEdits(edits []*pb.MetaEdit) {
	p.handlingMetaEditProg = p.console.StartProgressBar(
		"Applying Meta Edits", len(edits),
		glue.WithTimeCost(),
		glue.WithConstExtraField("meta-edits", len(edits)),
	)
}

func (p *ProgressBarHooks) HandledAMetaEdit(edit *pb.MetaEdit) {
	if p.handlingMetaEditProg != nil {
		p.handlingMetaEditProg.Inc()
	}
}

func (p *ProgressBarHooks) HandingMetaEditDone() {
	if p.handlingMetaEditProg != nil {
		p.handlingMetaEditProg.Close()
	}
}

// NoHooks is used for non-interactive secnarios.
type NoHooks struct{}

func (NoHooks) StartLoadingMetaForTruncating()                                     {}
func (NoHooks) EndLoadingMetaForTruncating()                                       {}
func (NoHooks) StartDeletingForTruncating(meta *StreamMetadataSet, shiftTS uint64) {}
func (NoHooks) DeletedAFileForTruncating(count int)                                {}
func (NoHooks) DeletedAllFilesForTruncating()                                      {}
func (NoHooks) StartHandlingMetaEdits([]*pb.MetaEdit)                              {}
func (NoHooks) HandledAMetaEdit(*pb.MetaEdit)                                      {}
func (NoHooks) HandingMetaEditDone()                                               {}

// MigrateionExtnsion installs the extension methods to an `ExternalStorage`.
func MigrationExtension(s storage.ExternalStorage) MigrationExt {
	return MigrationExt{
		s:      s,
		prefix: migrationPrefix,
		Hooks:  NoHooks{},
	}
}

// Merge merges two migrations.
// The merged migration contains all operations from the two arguments.
func MergeMigrations(m1 *pb.Migration, m2 *pb.Migration) *pb.Migration {
	out := NewMigration()
	out.EditMeta = mergeMetaEdits(m1.GetEditMeta(), m2.GetEditMeta())
	out.Compactions = append(out.Compactions, m1.GetCompactions()...)
	out.Compactions = append(out.Compactions, m2.GetCompactions()...)
	out.TruncatedTo = max(m1.GetTruncatedTo(), m2.GetTruncatedTo())
	out.DestructPrefix = append(out.DestructPrefix, m1.GetDestructPrefix()...)
	out.DestructPrefix = append(out.DestructPrefix, m2.GetDestructPrefix()...)
	out.IngestedSstPaths = append(out.IngestedSstPaths, m1.GetIngestedSstPaths()...)
	return out
}

// MergeAndMigratedTo is the result of a call to `MergeAndMigrateTo`.
type MergeAndMigratedTo struct {
	MigratedTo
	// The BASE migration of this "migrate to" operation.
	Base *pb.Migration
	// The migrations have been merged to the BASE migration.
	Source []*OrderedMigration
}

// MigratedTo is the result of trying to "migrate to" a migration.
//
// The term "migrate to" means, try to performance all possible operations
// from a migration to the storage.
type MigratedTo struct {
	// Non-fatal errors happen during executing the migration.
	Warnings []error
	// The new BASE migration after the operation.
	NewBase *pb.Migration
}

func (m *MigratedTo) Warn(err error) {
	m.Warnings = append(m.Warnings, err)
}

// Migrations represents living migrations from the storage.
type Migrations struct {
	// The BASE migration.
	Base *pb.Migration `json:"base"`
	// Appended migrations.
	// They are sorted by their sequence numbers.
	Layers []*OrderedMigration `json:"layers"`
}

// GetReadLock locks the storage and make sure there won't be other one modify this backup.
func (m *MigrationExt) GetReadLock(ctx context.Context, hint string) (storage.RemoteLock, error) {
	return storage.LockWith(ctx, storage.TryLockRemoteRead, m.s, lockPrefix, hint)
}

// OrderedMigration is a migration with its path and sequence number.
type OrderedMigration struct {
	SeqNum  int          `json:"seq_num"`
	Path    string       `json:"path"`
	Content pb.Migration `json:"content"`
}

func (o *OrderedMigration) unmarshalContent(b []byte) error {
	err := o.Content.Unmarshal(b)
	if err != nil {
		return err
	}
	if o.Content.Version > SupportedMigVersion {
		return errors.Annotatef(
			berrors.ErrMigrationVersionNotSupported,
			"the migration at %s has version %s(%d), the max version we support is %s(%d)",
			o.Path,
			o.Content.Version, o.Content.Version,
			SupportedMigVersion, SupportedMigVersion,
		)
	}

	return nil
}

type LoadOptions func(*loadConfig)

type loadConfig struct {
	notFoundIsErr bool
}

func MLNotFoundIsErr() LoadOptions {
	return func(c *loadConfig) {
		c.notFoundIsErr = true
	}
}

// Load loads the current living migrations from the storage.
func (m MigrationExt) Load(ctx context.Context, opts ...LoadOptions) (Migrations, error) {
	cfg := loadConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	opt := &storage.WalkOption{
		SubDir: m.prefix,
	}
	items := storage.UnmarshalDir(ctx, opt, m.s, func(t *OrderedMigration, name string, b []byte) error {
		t.Path = name
		var err error
		t.SeqNum, err = migIdOf(path.Base(name))
		if err != nil {
			return errors.Trace(err)
		}
		err = t.unmarshalContent(b)
		if err != nil {
			return err
		}

		if t.SeqNum == baseMigrationSN {
			// NOTE: the legacy truncating isn't implemented by appending a migration.
			// We load their checkpoint here to be compatible with them.
			// Then we can know a truncation happens so we are safe to remove stale compactions.
			truncatedTs, err := GetTSFromFile(ctx, m.s, TruncateSafePointFileName)
			if err != nil {
				return errors.Annotate(err, "failed to get the truncate safepoint for base migration")
			}
			t.Content.TruncatedTo = max(truncatedTs, t.Content.TruncatedTo)
		}
		return nil
	})
	collected := iter.CollectAll(ctx, items)
	if collected.Err != nil {
		return Migrations{}, collected.Err
	}
	if len(collected.Item) == 0 && cfg.notFoundIsErr {
		return Migrations{}, errors.Annotatef(berrors.ErrMigrationNotFound, "in the storage %s", m.s.URI())
	}
	sort.Slice(collected.Item, func(i, j int) bool {
		return collected.Item[i].SeqNum < collected.Item[j].SeqNum
	})

	var result Migrations
	if len(collected.Item) > 0 && collected.Item[0].SeqNum == baseMigrationSN {
		result = Migrations{
			Base:   &collected.Item[0].Content,
			Layers: collected.Item[1:],
		}
	} else {
		// The BASE migration isn't persisted.
		// This happens when `migrate-to` wasn't run ever.
		result = Migrations{
			Base:   NewMigration(),
			Layers: collected.Item,
		}
	}
	return result, nil
}

func (m MigrationExt) DryRun(f func(MigrationExt)) []storage.Effect {
	batchSelf := MigrationExt{
		s:      storage.Batch(m.s),
		prefix: m.prefix,
		Hooks:  m.Hooks,
	}
	f(batchSelf)
	return batchSelf.s.(*storage.Batched).ReadOnlyEffects()
}

func (m MigrationExt) AppendMigration(ctx context.Context, mig *pb.Migration) (int, error) {
	lock, err := storage.LockWith(ctx, storage.TryLockRemoteWrite, m.s, lockPrefix, "AppendMigration")
	if err != nil {
		return 0, err
	}
	defer lock.UnlockOnCleanUp(ctx)

	migs, err := m.Load(ctx)
	if err != nil {
		return 0, err
	}
	newSN := 1
	if len(migs.Layers) > 0 {
		newSN = migs.Layers[len(migs.Layers)-1].SeqNum + 1
	}
	name := path.Join(migrationPrefix, nameOf(mig, newSN))
	data, err := mig.Marshal()
	if err != nil {
		return 0, errors.Annotatef(err, "failed to encode the migration %s", mig)
	}
	return newSN, m.s.WriteFile(ctx, name, data)
}

// MergeTo merges migrations from the BASE in the live migrations until the specified sequence number.
func (migs Migrations) MergeTo(seq int) *pb.Migration {
	return migs.MergeToBy(seq, MergeMigrations)
}

func (migs Migrations) MergeToBy(seq int, merge func(m1, m2 *pb.Migration) *pb.Migration) *pb.Migration {
	newBase := migs.Base
	for _, mig := range migs.Layers {
		if mig.SeqNum > seq {
			return newBase
		}
		newBase = merge(newBase, &mig.Content)
	}
	return newBase
}

// ListAll returns a slice of all migrations in protobuf format.
// This includes the base migration and any additional layers.
func (migs Migrations) ListAll() []*pb.Migration {
	pbMigs := make([]*pb.Migration, 0, len(migs.Layers)+1)
	pbMigs = append(pbMigs, migs.Base)
	for _, m := range migs.Layers {
		pbMigs = append(pbMigs, &m.Content)
	}
	return pbMigs
}

type mergeAndMigrateToConfig struct {
	interactiveCheck       func(context.Context, *pb.Migration) bool
	alwaysRunTruncate      bool
	appendPhantomMigration []pb.Migration

	skipLockingInTest bool
}

type MergeAndMigrateToOpt func(*mergeAndMigrateToConfig)

func MMOptInteractiveCheck(f func(context.Context, *pb.Migration) bool) MergeAndMigrateToOpt {
	return func(c *mergeAndMigrateToConfig) {
		c.interactiveCheck = f
	}
}

func MMOptSkipLockingInTest() MergeAndMigrateToOpt {
	return func(c *mergeAndMigrateToConfig) {
		c.skipLockingInTest = true
	}
}

// MMOptAlwaysRunTruncate forces the merge and migrate to always run the truncating.
// If not set, when the `truncated-to` wasn'd modified, truncating will be skipped.
// This is necessary because truncating, even a no-op, requires a full scan over metadatas for now.
// This will be useful for retrying failed truncations.
func MMOptAlwaysRunTruncate() MergeAndMigrateToOpt {
	return func(c *mergeAndMigrateToConfig) {
		c.alwaysRunTruncate = true
	}
}

// MMOptAppendPhantomMigration appends a phantom migration to the merge and migrate to operation.
// The phantom migration will be persised to BASE during executing.
// We call it a "phantom" because it wasn't persisted.
// When the target version isn't the latest version, the order of migrating may be broken.
// Carefully use this in that case.
func MMOptAppendPhantomMigration(migs ...pb.Migration) MergeAndMigrateToOpt {
	return func(c *mergeAndMigrateToConfig) {
		c.appendPhantomMigration = append(c.appendPhantomMigration, migs...)
	}
}

// MergeAndMigrateTo will merge the migrations from BASE until the specified SN, then migrate to it.
// Finally it writes the new BASE and remove stale migrations from the storage.
func (m MigrationExt) MergeAndMigrateTo(
	ctx context.Context,
	targetSpec int,
	opts ...MergeAndMigrateToOpt,
) (result MergeAndMigratedTo) {
	config := mergeAndMigrateToConfig{}
	for _, o := range opts {
		o(&config)
	}

	if !config.skipLockingInTest {
		lock, err := storage.LockWith(ctx, storage.TryLockRemoteWrite, m.s, lockPrefix, "AppendMigration")
		if err != nil {
			result.MigratedTo = MigratedTo{
				Warnings: []error{
					errors.Annotate(err, "failed to get the lock, nothing will happen"),
				}}
			return
		}
		defer lock.UnlockOnCleanUp(ctx)
	}

	migs, err := m.Load(ctx)
	if err != nil {
		result.MigratedTo = MigratedTo{
			Warnings: []error{
				errors.Annotate(err, "failed to load migrations, nothing will happen"),
			}}
		return
	}
	result.Base = migs.Base
	for _, mig := range migs.Layers {
		if mig.SeqNum > targetSpec {
			break
		}
		result.Source = append(result.Source, mig)
	}
	for _, mig := range config.appendPhantomMigration {
		result.Source = append(result.Source, &OrderedMigration{
			SeqNum:  math.MaxInt,
			Path:    "",
			Content: mig,
		})
	}

	newBase := result.Base
	canSkipTruncate := true
	for _, mig := range result.Source {
		if mig.Content.TruncatedTo > newBase.TruncatedTo {
			canSkipTruncate = false
		}
		newBase = MergeMigrations(newBase, &mig.Content)
	}

	if config.interactiveCheck != nil && !config.interactiveCheck(ctx, newBase) {
		result.Warnings = append(result.Warnings, errors.New("User aborted, nothing will happen"))
		return
	}

	migTo := &result.MigratedTo

	// Put the generated BASE before we do any operation.
	// Or a user who reads a subset of migrations may get a broken storage.
	//
	// An example here, if we put the new BASE *after* executing:
	// | BASE => ∅
	// | [1]  => Del(foo.log)
	// Assume we are running `MergeAndMigrateTo(1)`, then we deleted `foo.log`,
	// but BR crashed and new BASE not written. A user want to read the BASE version,
	// the user cannot found the `foo.log` here.
	err = m.writeBase(ctx, newBase)
	if err != nil {
		result.Warnings = append(
			result.Warnings,
			errors.Annotate(err, "failed to save the merged new base"),
		)
		// Put the new BASE here anyway. The caller may want this.
		result.NewBase = newBase
		return
	}

	for _, mig := range result.Source {
		// Perhaps a phanom migration.
		if len(mig.Path) > 0 {
			err = m.s.DeleteFile(ctx, mig.Path)
			if err != nil {
				migTo.Warnings = append(
					migTo.Warnings,
					errors.Annotatef(err, "failed to delete the merged migration %s", migs.Layers[0].Path),
				)
			}
		}
	}
	result.MigratedTo = m.migrateTo(ctx, newBase, MTMaybeSkipTruncateLog(!config.alwaysRunTruncate && canSkipTruncate))

	// Put the final BASE.
	err = m.writeBase(ctx, result.NewBase)
	if err != nil {
		result.Warnings = append(result.Warnings, errors.Annotatef(err, "failed to save the new base"))
	}
	return
}

type migrateToOpt func(*migToOpt)

type migToOpt struct {
	skipTruncateLog bool
}

func MTSkipTruncateLog(o *migToOpt) {
	o.skipTruncateLog = true
}

func MTMaybeSkipTruncateLog(cond bool) migrateToOpt {
	if cond {
		return MTSkipTruncateLog
	}
	return func(*migToOpt) {}
}

// migrateTo migrates to a migration.
// If encountered some error during executing some operation, the operation will be put
// to the new BASE, which can be retryed then.
func (m MigrationExt) migrateTo(ctx context.Context, mig *pb.Migration, opts ...migrateToOpt) MigratedTo {
	opt := migToOpt{}
	for _, o := range opts {
		o(&opt)
	}

	result := MigratedTo{
		NewBase: NewMigration(),
	}
	m.processCompactions(ctx, mig, &result)
	m.processDestroyPrefixes(ctx, mig, &result)
	m.processExtFullBackup(ctx, mig, &result)
	if !opt.skipTruncateLog {
		m.processTruncatedTo(ctx, mig, &result)
	}

	// We do skip truncate log first, so metas removed by truncating can be removed in this execution.
	// Fills: EditMeta for new Base.
	m.processMetaEdits(ctx, mig, &result)

	return result
}

func (m MigrationExt) writeBase(ctx context.Context, mig *pb.Migration) error {
	content, err := mig.Marshal()
	if err != nil {
		return errors.Trace(err)
	}
	err = m.s.WriteFile(ctx, path.Join(m.prefix, baseTmp), content)
	if err != nil {
		return errors.Trace(err)
	}
	return m.s.Rename(ctx, path.Join(m.prefix, baseTmp), path.Join(m.prefix, baseMigrationName))
}

// processMetaEdits applies the modification to the meta files in the storage.
func (m MigrationExt) processMetaEdits(ctx context.Context, mig *pb.Migration, out *MigratedTo) {
	m.Hooks.StartHandlingMetaEdits(mig.EditMeta)

	handleAMetaEdit := func(medit *pb.MetaEdit) {
		if isEmptyEdition(medit) {
			return
		}

		// Sometimes, the meta file will be deleted by truncating.
		// We clean up those meta edits.
		// NOTE: can we unify the deletion of truncating and meta editing?
		// Say, add a "normalize" phase that load all files to be deleted to the migration.
		// The problem here is a huge migration may be created in memory then leading to OOM.
		exists, errChkExist := m.s.FileExists(ctx, medit.Path)
		if errChkExist == nil && !exists {
			log.Warn("The meta file doesn't exist, skipping the edit", zap.String("path", medit.Path))
			return
		}

		// Firstly delete data so they won't leak when BR crashes.
		m.cleanUpFor(ctx, medit, out)
		err := m.applyMetaEdit(ctx, medit)
		if err != nil {
			out.NewBase.EditMeta = append(out.NewBase.EditMeta, medit)
			out.Warnings = append(out.Warnings, errors.Annotatef(err, "failed to apply meta edit %s to meta file", medit.Path))
			return
		}
	}

	defer m.Hooks.HandingMetaEditDone()
	for _, medit := range mig.EditMeta {
		handleAMetaEdit(medit)
		m.Hooks.HandledAMetaEdit(medit)
	}
}

// cleanUpFor modifies the real storage, remove the log files
// removed in the meta file, as if the meta edition has been applied.
func (m MigrationExt) cleanUpFor(ctx context.Context, medit *pb.MetaEdit, out *MigratedTo) {
	var err error
	newMetaEdit := &pb.MetaEdit{
		Path: medit.Path,
	}

	if len(medit.DeletePhysicalFiles) > 0 {
		err = m.s.DeleteFiles(ctx, medit.DeletePhysicalFiles)
		if err != nil {
			out.Warnings = append(out.Warnings, errors.Annotate(err, "failed to delete file"))
			newMetaEdit.DeletePhysicalFiles = slices.Clone(medit.DeletePhysicalFiles)
		}
	}

	physicalFilesToDelete := []string{}
	for _, spans := range medit.DeleteLogicalFiles {
		if physicalFileCanBeDeleted(spans) {
			physicalFilesToDelete = append(physicalFilesToDelete, spans.Path)
		} else {
			newMetaEdit.DeleteLogicalFiles = append(newMetaEdit.DeleteLogicalFiles, spans)
		}
	}
	if len(physicalFilesToDelete) > 0 {
		err = m.s.DeleteFiles(ctx, physicalFilesToDelete)
		if err != nil {
			out.Warnings = append(out.Warnings, errors.Annotate(err, "failed to delete file"))
			newMetaEdit.DeletePhysicalFiles = append(newMetaEdit.DeletePhysicalFiles, physicalFilesToDelete...)
		}
	}

	if len(out.Warnings) > 0 {
		log.Warn(
			"Failed to clean up for meta edit.",
			zap.String("meta-edit", medit.Path),
			zap.Errors("warnings", out.Warnings),
		)
	}
	if !isEmptyEdition(newMetaEdit) {
		out.NewBase.EditMeta = append(out.NewBase.EditMeta, newMetaEdit)
	}
}

// applyMetaEdit applies the modifications in the `MetaEdit` to the real meta file.
// But this won't really clean up the real log files.
func (m MigrationExt) applyMetaEdit(ctx context.Context, medit *pb.MetaEdit) (err error) {
	if medit.DestructSelf {
		return m.s.DeleteFile(ctx, medit.Path)
	}

	mContent, err := m.s.ReadFile(ctx, medit.Path)
	if err != nil {
		return err
	}
	var metadata pb.Metadata
	err = metadata.Unmarshal(mContent)
	if err != nil {
		return err
	}

	return m.applyMetaEditTo(ctx, medit, &metadata)
}

func (m MigrationExt) applyMetaEditTo(ctx context.Context, medit *pb.MetaEdit, metadata *pb.Metadata) error {
	if isEmptyEdition(medit) {
		// Fast path: skip write back for empty meta edits.
		return nil
	}

	metadata.Files = slices.DeleteFunc(metadata.Files, func(dfi *pb.DataFileInfo) bool {
		// Here, `DeletePhysicalFiles` is usually tiny.
		// Use a hashmap to filter out if this gets slow in the future.
		return slices.Contains(medit.DeletePhysicalFiles, dfi.Path)
	})
	metadata.FileGroups = slices.DeleteFunc(metadata.FileGroups, func(dfg *pb.DataFileGroup) bool {
		del := slices.Contains(medit.DeletePhysicalFiles, dfg.Path)
		return del
	})
	for _, group := range metadata.FileGroups {
		idx := slices.IndexFunc(medit.DeleteLogicalFiles, func(dsof *pb.DeleteSpansOfFile) bool {
			return dsof.Path == group.Path
		})
		if idx >= 0 {
			sort.Slice(medit.DeleteLogicalFiles[idx].Spans, func(i, j int) bool {
				return medit.DeleteLogicalFiles[idx].Spans[i].Offset < medit.DeleteLogicalFiles[idx].Spans[j].Offset
			})
			var err error
			group.DataFilesInfo = slices.DeleteFunc(group.DataFilesInfo, func(dfi *pb.DataFileInfo) bool {
				received, ok := slices.BinarySearchFunc(
					medit.DeleteLogicalFiles[idx].Spans,
					dfi.RangeOffset,
					func(s *pb.Span, u uint64) int {
						return int(s.Offset - u)
					})
				if ok && medit.DeleteLogicalFiles[idx].Spans[received].Length != dfi.RangeLength {
					err = errors.Annotatef(
						berrors.ErrPiTRMalformedMetadata,
						"trying to delete a span that mismatches with metadata: to delete is %v, found %v",
						medit.DeleteLogicalFiles[idx].Spans[received],
						dfi,
					)
				}
				return ok
			})
			if err != nil {
				return err
			}
		}
	}
	metadata.FileGroups = slices.DeleteFunc(metadata.FileGroups, func(dfg *pb.DataFileGroup) bool {
		// As all spans in the physical data file has been deleted, it will be soonly removed.
		return len(dfg.DataFilesInfo) == 0
	})

	if isEmptyMetadata(metadata) {
		// As it is empty, even no hint to destruct self, we can safely delete it.
		return m.s.DeleteFile(ctx, medit.Path)
	}

	updateMetadataInternalStat(metadata)
	newContent, err := metadata.Marshal()
	if err != nil {
		return err
	}
	return truncateAndWrite(ctx, m.s, medit.Path, newContent)
}

func (m MigrationExt) tryRemovePrefix(ctx context.Context, pfx string, out *MigratedTo) {
	enumerateAndDelete := func(prefix string) error {
		if isInsane(prefix) {
			return errors.Annotatef(
				berrors.ErrPiTRMalformedMetadata,
				"trying to delete a prefix %q that is too wide, skipping deleting",
				prefix,
			)
		}
		files, err := m.loadFilesOfPrefix(ctx, prefix)
		if err != nil {
			return err
		}
		return m.s.DeleteFiles(ctx, files)
	}
	if err := enumerateAndDelete(pfx); err != nil {
		out.Warnings = append(out.Warnings, errors.Annotatef(err, "failed to delete prefix %s", pfx))
		out.NewBase.DestructPrefix = append(out.NewBase.DestructPrefix, pfx)
	}
}

// processTruncatedTo tries to remove outdated compaction, filling the not-yet removed compactions to the new migration.
func (m MigrationExt) processTruncatedTo(ctx context.Context, mig *pb.Migration, result *MigratedTo) {
	result.NewBase.TruncatedTo = mig.TruncatedTo
	m.Hooks.StartLoadingMetaForTruncating()
	mdSet := new(StreamMetadataSet)
	mdSet.MetadataDownloadBatchSize = 128
	shiftTS, err := mdSet.LoadUntilAndCalculateShiftTS(ctx, m.s, mig.TruncatedTo)
	if err != nil {
		result.Warnings = append(result.Warnings, errors.Annotatef(err, "failed to open meta storage"))
		return
	}
	m.Hooks.EndLoadingMetaForTruncating()

	m.doTruncateLogs(ctx, mdSet, shiftTS, result)
}

func (m MigrationExt) processDestroyPrefixes(ctx context.Context, mig *pb.Migration, result *MigratedTo) {
	for _, pfx := range mig.DestructPrefix {
		m.tryRemovePrefix(ctx, pfx, result)
	}
}

func (m MigrationExt) processCompactions(ctx context.Context, mig *pb.Migration, result *MigratedTo) {
	// NOTE: Execution of truncation wasn't implemented here.
	// If we are going to truncate some files, for now we still need to use `br log truncate`.
	for _, compaction := range mig.Compactions {
		// We can only clean up a compaction when we are sure all its inputs
		// are no more used.
		if compaction.InputMaxTs > mig.TruncatedTo {
			result.NewBase.Compactions = append(result.NewBase.Compactions, compaction)
		} else {
			m.tryRemovePrefix(ctx, compaction.Artifacts, result)
			m.tryRemovePrefix(ctx, compaction.GeneratedFiles, result)
		}
	}
}

func (m MigrationExt) processExtFullBackup(ctx context.Context, mig *pb.Migration, result *MigratedTo) {
	groups := LoadIngestedSSTs(ctx, m.s, mig.IngestedSstPaths)
	processGroup := func(outErr error, e IngestedSSTsGroup) (copyToNewMig bool, err error) {
		if outErr != nil {
			return true, outErr
		}

		if !e.GroupFinished() {
			return true, nil
		}

		if e.GroupTS() >= mig.TruncatedTo {
			return true, nil
		}

		for _, b := range e {
			m.tryRemovePrefix(ctx, b.FilesPrefixHint, result)
		}
		return false, nil
	}
	for err, item := range iter.AsSeq(ctx, groups) {
		copyToNewMig, err := processGroup(err, item)
		if err != nil {
			result.Warn(err)
		}
		if copyToNewMig {
			for _, exb := range item {
				result.NewBase.IngestedSstPaths = append(result.NewBase.IngestedSstPaths, exb.path)
			}
		}
	}
}

type PathedIngestedSSTs struct {
	*pb.IngestedSSTs
	path string
}

type IngestedSSTsGroup []PathedIngestedSSTs

func (ebs IngestedSSTsGroup) GroupFinished() bool {
	for _, b := range ebs {
		if b.Finished {
			return true
		}
	}
	return false
}

func (ebs IngestedSSTsGroup) GroupTS() uint64 {
	for _, b := range ebs {
		if b.Finished {
			return b.AsIfTs
		}
	}
	return math.MaxUint64
}

func LoadIngestedSSTs(
	ctx context.Context,
	s storage.ExternalStorage,
	paths []string,
) iter.TryNextor[IngestedSSTsGroup] {
	fullBackupDirIter := iter.FromSlice(paths)
	backups := iter.TryMap(fullBackupDirIter, func(name string) (PathedIngestedSSTs, error) {
		// name is the absolute path in external storage.
		bkup, err := readIngestedSSTs(ctx, name, s)
		failpoint.InjectCall("load-ingested-ssts-err", &err)
		if err != nil {
			return PathedIngestedSSTs{}, errors.Annotatef(err, "failed to read backup at %s", name)
		}
		return PathedIngestedSSTs{IngestedSSTs: bkup, path: name}, nil
	})
	extBackups, err := groupExtraBackups(ctx, backups)
	if err != nil {
		return iter.Fail[IngestedSSTsGroup](err)
	}
	return iter.FromSlice(extBackups)
}

func groupExtraBackups(ctx context.Context, i iter.TryNextor[PathedIngestedSSTs]) ([]IngestedSSTsGroup, error) {
	var (
		collected = map[uuid.UUID]IngestedSSTsGroup{}
		finished  = map[uuid.UUID]struct{}{}
	)

	for err, fbk := range iter.AsSeq(ctx, i) {
		if err != nil {
			return nil, err
		}

		if len(fbk.BackupUuid) != len(uuid.UUID{}) {
			return nil, errors.Annotatef(berrors.ErrInvalidArgument,
				"the full backup UUID has bad length(%d)", len(fbk.BackupUuid))
		}
		uid := uuid.UUID(fbk.BackupUuid)
		log.Info("Collecting extra full backup",
			zap.Stringer("UUID", uid), zap.String("path", fbk.FilesPrefixHint), zap.Bool("finished", fbk.Finished))

		if _, ok := finished[uid]; ok {
			log.Warn("Encountered a finished full backup.", zap.Stringer("UUID", uid), zap.String("path", fbk.FilesPrefixHint))
			return nil, errors.Annotatef(
				berrors.ErrInvalidArgument,
				"the extra full backup group %s at %s encounters an extra full backup meta after a finished one",
				uid, fbk.FilesPrefixHint,
			)
		}

		collected[uid] = append(collected[uid], fbk)
		if fbk.Finished {
			finished[uid] = struct{}{}
		}
	}

	res := make([]IngestedSSTsGroup, 0, len(collected))
	for v := range maps.Values(collected) {
		res = append(res, v)
	}
	return res, nil
}

func readIngestedSSTs(ctx context.Context, name string, s storage.ExternalStorage) (*pb.IngestedSSTs, error) {
	reader, err := s.ReadFile(ctx, name)
	if err != nil {
		return nil, err
	}

	var backup pb.IngestedSSTs
	if err := backup.Unmarshal(reader); err != nil {
		return nil, err
	}
	return &backup, nil
}

func (m MigrationExt) loadFilesOfPrefix(ctx context.Context, prefix string) (out []string, err error) {
	err = m.s.WalkDir(ctx, &storage.WalkOption{SubDir: prefix}, func(path string, size int64) error {
		out = append(out, path)
		return nil
	})
	return
}

// doTruncateLogs truncates the logs until the specified TS.
// This might be slow.
func (m MigrationExt) doTruncateLogs(
	ctx context.Context,
	metadataInfos *StreamMetadataSet,
	from uint64,
	out *MigratedTo,
) {
	mu := new(sync.Mutex)
	updateResult := func(f func(r *MigratedTo)) {
		mu.Lock()
		defer mu.Unlock()

		f(out)
	}
	emitErr := func(err error) {
		updateResult(func(r *MigratedTo) {
			r.Warnings = append(r.Warnings, err)
		})
	}

	worker := util.NewWorkerPool(metadataInfos.MetadataDownloadBatchSize, "delete files")
	eg, cx := errgroup.WithContext(ctx)

	m.Hooks.StartDeletingForTruncating(metadataInfos, from)
	defer m.Hooks.DeletedAllFilesForTruncating()
	for path, metaInfo := range metadataInfos.metadataInfos {
		if metaInfo.MinTS >= from {
			continue
		}
		worker.ApplyOnErrorGroup(eg, func() error {
			data, err := m.s.ReadFile(cx, path)
			if err != nil {
				return errors.Annotatef(err, "failed to open meta %s", path)
			}

			// Note: maybe make this a static method or just a normal function...
			meta, err := (*MetadataHelper).ParseToMetadataHard(nil, data)
			if err != nil {
				return errors.Annotatef(err, "failed to parse meta %s", path)
			}

			me := new(pb.MetaEdit)
			me.Path = path
			for _, ds := range meta.FileGroups {
				if ds.MaxTs < from {
					me.DeletePhysicalFiles = append(me.DeletePhysicalFiles, ds.Path)
				}
			}

			// Firstly clean up the data file so they won't leak (meta have been deleted,
			// but data not deleted. Then data cannot be found anymore.).
			//
			// We have already written `truncated-to` to the storage hence
			// we don't need to worry that the user access files already deleted.
			aOut := new(MigratedTo)
			aOut.NewBase = new(pb.Migration)
			m.cleanUpFor(ctx, me, aOut)
			updateResult(func(r *MigratedTo) {
				r.Warnings = append(r.Warnings, aOut.Warnings...)
				r.NewBase = MergeMigrations(r.NewBase, aOut.NewBase)
			})

			err = m.applyMetaEditTo(ctx, me, meta)
			if err != nil {
				updateResult(func(r *MigratedTo) {
					r.Warnings = append(r.Warnings, errors.Annotatef(err, "during handling %s", me.Path))
					r.NewBase.EditMeta = append(r.NewBase.EditMeta, me)
				})
			}
			m.Hooks.DeletedAFileForTruncating(len(me.DeletePhysicalFiles))
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		emitErr(err)
	}
}

// hookedStorage is a wrapper over the external storage,
// which triggers the `BeforeDoWriteBack` hook when putting a metadata.
type hookedStorage struct {
	storage.ExternalStorage
	metaSet *StreamMetadataSet
}

func (h hookedStorage) WriteFile(ctx context.Context, name string, data []byte) error {
	if strings.HasSuffix(name, metaSuffix) && h.metaSet.BeforeDoWriteBack != nil {
		meta, err := h.metaSet.Helper.ParseToMetadataHard(data)
		if err != nil {
			// Note: will this be too strict? But for now it seems this check won't fail.
			// We can remove this in the future if needed.
			return errors.Annotatef(err, "Writing non-meta during write back (to = %s)", name)
		}
		if h.metaSet.BeforeDoWriteBack(name, meta) {
			log.Info("Skipped writeback meta by the hook.", zap.String("meta", name))
			return nil
		}
	}

	return h.ExternalStorage.WriteFile(ctx, name, data)
}

func (h hookedStorage) DeleteFile(ctx context.Context, name string) error {
	if strings.HasSuffix(name, metaSuffix) && h.metaSet.BeforeDoWriteBack != nil {
		h.metaSet.BeforeDoWriteBack(name, new(pb.Metadata))
	}
	return h.ExternalStorage.DeleteFile(ctx, name)
}

func (ms *StreamMetadataSet) hook(s storage.ExternalStorage) hookedStorage {
	hooked := hookedStorage{
		ExternalStorage: s,
		metaSet:         ms,
	}
	if ms.DryRun {
		hooked.ExternalStorage = storage.Batch(hooked.ExternalStorage)
	}
	return hooked
}

func physicalFileCanBeDeleted(fs *pb.DeleteSpansOfFile) bool {
	sort.Slice(fs.Spans, func(i, j int) bool {
		return fs.Spans[i].Offset < fs.Spans[j].Offset
	})
	lastOffset := uint64(0)
	for _, s := range fs.Spans {
		if s.Offset != lastOffset {
			return false
		}
		lastOffset += s.Length
	}
	return lastOffset == fs.WholeFileLength
}

// mergeMetaEdits merges two meta edits.
//
// If the spans in the `DeleteLogicalFiles` consist a physical file,
// they will be transformed to `DeletePhysicalFiles`.
func mergeMetaEdits(s1, s2 []*pb.MetaEdit) []*pb.MetaEdit {
	edits := map[string]*pb.MetaEdit{}
	for _, edit := range s1 {
		edits[edit.GetPath()] = &pb.MetaEdit{
			Path:                edit.Path,
			DeletePhysicalFiles: edit.DeletePhysicalFiles[:len(edit.DeletePhysicalFiles):len(edit.DeletePhysicalFiles)],
			DeleteLogicalFiles:  edit.DeleteLogicalFiles[:len(edit.DeleteLogicalFiles):len(edit.DeleteLogicalFiles)],
		}
	}
	for _, edit := range s2 {
		target, ok := edits[edit.GetPath()]
		if !ok {
			edits[edit.GetPath()] = edit
		} else {
			target.DeletePhysicalFiles = append(target.DeletePhysicalFiles, edit.GetDeletePhysicalFiles()...)
			target.DeleteLogicalFiles = mergeDeleteLogicalFiles(target.GetDeleteLogicalFiles(), edit.GetDeleteLogicalFiles())
		}
	}

	val := make([]*pb.MetaEdit, 0, len(edits))
	for _, v := range edits {
		val = append(val, v)
	}
	return val
}

// mergeDeleteLogicalFiles merges two `DeleteSpansOfFile`.
func mergeDeleteLogicalFiles(s1, s2 []*pb.DeleteSpansOfFile) []*pb.DeleteSpansOfFile {
	files := map[string]*pb.DeleteSpansOfFile{}
	for _, file := range s1 {
		files[file.GetPath()] = &pb.DeleteSpansOfFile{
			Path:            file.GetPath(),
			Spans:           file.GetSpans()[:len(file.GetSpans()):len(file.GetSpans())],
			WholeFileLength: file.GetWholeFileLength(),
		}
	}
	for _, file := range s2 {
		target, ok := files[file.GetPath()]
		if !ok {
			files[file.GetPath()] = file
		} else {
			target.Spans = append(target.Spans, file.GetSpans()...)
		}
	}

	val := make([]*pb.DeleteSpansOfFile, 0, len(files))
	for _, v := range files {
		val = append(val, v)
	}
	return val
}

func isEmptyEdition(medit *pb.MetaEdit) bool {
	return len(medit.DeletePhysicalFiles) == 0 && len(medit.DeleteLogicalFiles) == 0 && !medit.DestructSelf
}

func migIdOf(s string) (int, error) {
	const (
		migrationPrefixLen = 8
	)
	if s == baseMigrationName {
		return baseMigrationSN, nil
	}
	if len(s) < 8 {
		return 0, errors.Annotatef(berrors.ErrUnknown,
			"migration name %s is too short, perhaps `migrations` dir corrupted", s)
	}
	toParse := s[:migrationPrefixLen]
	result, err := strconv.Atoi(toParse)
	if err != nil {
		return 0, errors.Annotatef(err,
			"migration name %s is not a valid number, perhaps `migrations` dir corrupted", s)
	}
	return result, nil
}

// isInsane checks whether deleting a prefix is insane: say, going to destroy the whole backup storage.
//
// This would be useful when a compaction's output dir is absent or modified.
func isInsane(pfx string) bool {
	normalized := path.Clean(pfx)
	switch normalized {
	case "", ".", "/", "/v1", "v1":
		return true
	default:
	}

	return strings.HasPrefix(pfx, "..")
}

func isEmptyMetadata(md *pb.Metadata) bool {
	return len(md.FileGroups) == 0 && len(md.Files) == 0
}

/* Below are hash algorithms for hashing a component of the migration.
 * Sadly there isn't a document describes the behavior of the algorithms.
 * Perhaps we can standardlize them in the future.
 * Maybe by defining a ordering-insensitive object hash algorithm for protocol buffer.
 *
 * Note: For now, the canon of the hash algorithm for a message should follow the following rules:
 * - If a hash algorithm for a message exists both in TiKV and BR and conflicting, we
 *   follow the implementation at where the message firstly creates (say, for compactions,
 *   TiKV will be the canonical implementation. while for extra full backups, BR is canonical.).
 * - For commonly used fields, follow the implementation in BR.
 *
 * Another note: nowadays, the hash of a migration is mainly used for detecting duplicated works,
 * so the difference between hash algorithms won't result in something too bad...
 */

func hashMigration(m *pb.Migration) uint64 {
	var crc64Res uint64 = 0
	for _, compaction := range m.Compactions {
		crc64Res ^= compaction.ArtifactsHash
	}
	for _, metaEdit := range m.EditMeta {
		crc64Res ^= hashMetaEdit(metaEdit)
	}
	for _, extBkup := range m.IngestedSstPaths {
		crc64Res ^= crc64.Checksum([]byte(extBkup), crc64.MakeTable(crc64.ISO))
	}
	return crc64Res ^ m.TruncatedTo
}

func hashMetaEdit(metaEdit *pb.MetaEdit) uint64 {
	var res uint64 = 0
	for _, df := range metaEdit.DeletePhysicalFiles {
		digest := crc64.New(crc64.MakeTable(crc64.ISO))
		digest.Write([]byte(df))
		res ^= digest.Sum64()
	}
	for _, spans := range metaEdit.DeleteLogicalFiles {
		for _, span := range spans.GetSpans() {
			crc := crc64.New(crc64.MakeTable(crc64.ISO))
			crc.Write([]byte(spans.GetPath()))
			crc.Write(binary.LittleEndian.AppendUint64(nil, span.GetOffset()))
			crc.Write(binary.LittleEndian.AppendUint64(nil, span.GetLength()))
			res ^= crc.Sum64()
		}
	}
	crc := crc64.New(crc64.MakeTable(crc64.ISO))
	if metaEdit.DestructSelf {
		crc.Write([]byte{1})
	} else {
		crc.Write([]byte{0})
	}
	return res ^ crc.Sum64()
}

func nameOf(mig *pb.Migration, sn int) string {
	return fmt.Sprintf("%08d_%016X.mgrt", sn, hashMigration(mig))
}

func isEmptyMigration(mig *pb.Migration) bool {
	return len(mig.Compactions) == 0 &&
		len(mig.EditMeta) == 0 &&
		len(mig.IngestedSstPaths) == 0 &&
		len(mig.DestructPrefix) == 0 &&
		mig.TruncatedTo == 0
}
