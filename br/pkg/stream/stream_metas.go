// Copyright 2024 PingCAP, Inc. Licensed under Apache-2.0.

package stream

import (
	"context"
	"fmt"
	"math"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/fatih/color"
	"github.com/pingcap/errors"
	pb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/br/pkg/utils/iter"
	"github.com/pingcap/tidb/pkg/util"
	"github.com/pingcap/tidb/pkg/util/mathutil"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	notDeletedBecameFatalThreshold = 128

	baseMigrationSN   = 0
	baseMigrationName = "BASE"
	baseTmp           = "BASE_TMP"
	migrationPrefix   = "v1/migrations"
)

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

// RemoveDataFilesAndUpdateMetadataInBatch concurrently remove datafilegroups and update metadata.
// Only one metadata is processed in each thread, including deleting its datafilegroup and updating it.
// Returns the not deleted datafilegroups.
func (ms *StreamMetadataSet) RemoveDataFilesAndUpdateMetadataInBatch(
	ctx context.Context,
	from uint64,
	storage storage.ExternalStorage,
	updateFn func(num int64),
) ([]string, error) {
	var notDeleted struct {
		item []string
		sync.Mutex
	}
	worker := util.NewWorkerPool(ms.MetadataDownloadBatchSize, "delete files")
	eg, cx := errgroup.WithContext(ctx)
	for path, metaInfo := range ms.metadataInfos {
		path := path
		minTS := metaInfo.MinTS
		// It's safety to remove the item within a range loop
		delete(ms.metadataInfos, path)
		if minTS >= from {
			// That means all the datafiles wouldn't be removed,
			// so that the metadata is skipped.
			continue
		}
		worker.ApplyOnErrorGroup(eg, func() error {
			if cx.Err() != nil {
				return cx.Err()
			}

			data, err := storage.ReadFile(ctx, path)
			if err != nil {
				return err
			}

			meta, err := ms.Helper.ParseToMetadataHard(data)
			if err != nil {
				return err
			}

			num, notDeletedItems, err := ms.removeDataFilesAndUpdateMetadata(ctx, storage, from, meta, path)
			if err != nil {
				return err
			}

			updateFn(num)

			notDeleted.Lock()
			notDeleted.item = append(notDeleted.item, notDeletedItems...)
			notDeleted.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, errors.Trace(err)
	}

	return notDeleted.item, nil
}

// removeDataFilesAndUpdateMetadata removes some datafilegroups of the metadata, if their max-ts is less than `from`
func (ms *StreamMetadataSet) removeDataFilesAndUpdateMetadata(
	ctx context.Context,
	st storage.ExternalStorage,
	from uint64,
	meta *pb.Metadata,
	metaPath string,
) (num int64, notDeleted []string, err error) {
	removed := make([]*pb.DataFileGroup, 0)
	remainedDataFiles := make([]*pb.DataFileGroup, 0)
	notDeleted = make([]string, 0)
	// can we assume those files are sorted to avoid traversing here? (by what?)
	for _, ds := range meta.FileGroups {
		if ds.MaxTs < from {
			removed = append(removed, ds)
		} else {
			// That means some kvs in the datafilegroup shouldn't be removed,
			// so it will be kept out being removed.
			remainedDataFiles = append(remainedDataFiles, ds)
		}
	}

	num = int64(len(removed))

	if ms.DryRun {
		st = storage.Batch(st)
	}

	// remove data file groups
	for _, f := range removed {
		log.Info("Deleting file", zap.String("path", f.Path))
		if err := st.DeleteFile(ctx, f.Path); err != nil {
			log.Warn("File not deleted.", zap.String("path", f.Path), logutil.ShortError(err))
			notDeleted = append(notDeleted, f.Path)
			if len(notDeleted) > notDeletedBecameFatalThreshold {
				return num, notDeleted, errors.Annotatef(berrors.ErrPiTRMalformedMetadata, "too many failure when truncating")
			}
		}
	}

	// update metadata
	if len(remainedDataFiles) != len(meta.FileGroups) {
		// rewrite metadata
		log.Info("Updating metadata.", zap.String("file", metaPath),
			zap.Int("data-file-before", len(meta.FileGroups)),
			zap.Int("data-file-after", len(remainedDataFiles)))

		// replace the filegroups and update the ts of the replaced metadata
		ReplaceMetadata(meta, remainedDataFiles)

		if ms.BeforeDoWriteBack != nil && ms.BeforeDoWriteBack(metaPath, meta) {
			log.Info("Skipped writeback meta by the hook.", zap.String("meta", metaPath))
			return num, notDeleted, nil
		}

		if err := ms.doWriteBackForFile(ctx, st, metaPath, meta); err != nil {
			// NOTE: Maybe we'd better roll back all writebacks? (What will happen if roll back fails too?)
			return num, notDeleted, errors.Annotatef(err, "failed to write back file %s", metaPath)
		}
	}

	if bst, ok := st.(*storage.Batched); ok {
		effs, err := storage.SaveJSONEffectsToTmp(bst.ReadOnlyEffects())
		if err != nil {
			log.Warn("failed to save effects", logutil.ShortError(err))
		} else {
			log.Info("effects are saved, you may check them then.", zap.String("path", effs))
		}
	}

	return num, notDeleted, nil
}

func (ms *StreamMetadataSet) doWriteBackForFile(
	ctx context.Context,
	s storage.ExternalStorage,
	path string,
	meta *pb.Metadata,
) error {
	// If the metadata file contains no data file, remove it due to it is meanless.
	if len(meta.FileGroups) == 0 {
		if err := s.DeleteFile(ctx, path); err != nil {
			return errors.Annotatef(err, "failed to remove the empty meta %s", path)
		}
		return nil
	}

	bs, err := ms.Helper.Marshal(meta)
	if err != nil {
		return errors.Annotatef(err, "failed to marshal the file %s", path)
	}
	return truncateAndWrite(ctx, s, path, bs)
}

func truncateAndWrite(ctx context.Context, s storage.ExternalStorage, path string, data []byte) error {
	// Performance hack: the `Write` implemention would truncate the file if it exists.
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

func AddMigrationToTable(m *pb.Migration, table *glue.Table) {
	rd := color.New(color.FgHiRed).Sprint
	if m.TruncatedTo > 0 {
		table.Add("truncated-to", strconv.FormatUint(m.TruncatedTo, 10))
	}
	for i, c := range m.Compactions {
		addCompactionToTable(c, table, i)
	}

	if len(m.EditMeta) > 0 {
		totalDeletePhyFile := 0
		totalDeleteLgcFile := 0
		for _, edit := range m.EditMeta {
			totalDeletePhyFile += len(edit.DeletePhysicalFiles)
			for _, dl := range edit.DeleteLogicalFiles {
				totalDeleteLgcFile += len(dl.Spans)
			}
		}
		table.Add(
			"edit-meta-files",
			fmt.Sprintf("%s meta files will be edited.", rd(len(m.EditMeta))),
		)
		table.Add(
			"delete-physical-file",
			fmt.Sprintf("%s physical files will be deleted.", rd(totalDeletePhyFile)),
		)
		table.Add(
			"delete-logical-file",
			fmt.Sprintf("%s logical segments may be deleted, if possible.", rd(totalDeleteLgcFile)),
		)
	}
	for i, c := range m.DestructPrefix {
		table.Add(fmt.Sprintf("destruct-prefix[%02d]", i), rd(c))
	}
}

func addCompactionToTable(m *pb.LogFileCompaction, table *glue.Table, idx int) {
	withIdx := func(s string) string { return fmt.Sprintf("compactions[%d].%s", idx, s) }
	table.Add(withIdx("name"), m.Name)
	table.Add(withIdx("time"), fmt.Sprintf("%d ~ %d", m.CompactionFromTs, m.CompactionUntilTs))
	table.Add(withIdx("file"), fmt.Sprintf("[%q, %q]", m.Artifacts, m.GeneratedFiles))
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
}

// MigrateionExtnsion installs the extension methods to an `ExternalStorage`.
func MigerationExtension(s storage.ExternalStorage) MigrationExt {
	return MigrationExt{
		s:      s,
		prefix: migrationPrefix,
	}
}

// Merge merges two migrations.
// The merged migration contains all operations from the two arguments.
func mergeMigrations(m1 *pb.Migration, m2 *pb.Migration) *pb.Migration {
	out := new(pb.Migration)
	out.EditMeta = mergeMetaEdits(m1.EditMeta, m2.EditMeta)
	out.Compactions = append(out.Compactions, m1.Compactions...)
	out.Compactions = append(out.Compactions, m2.Compactions...)
	out.TruncatedTo = mathutil.Max(m1.TruncatedTo, m2.TruncatedTo)
	out.DestructPrefix = append(out.DestructPrefix, m1.DestructPrefix...)
	out.DestructPrefix = append(out.DestructPrefix, m2.DestructPrefix...)
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
	// Errors happen during executing the migration.
	Warnings []error
	// The new BASE migration after the operation.
	NewBase *pb.Migration
}

// Migrations represents living migrations from the storage.
type Migrations struct {
	// The BASE migration.
	Base *pb.Migration
	// Appended migrations.
	// They are sorted by their sequence numbers.
	Layers []*OrderedMigration
}

// OrderedMigration is a migration with its path and sequence number.
type OrderedMigration struct {
	SeqNum  int          `json:"seq_num"`
	Path    string       `json:"path"`
	Content pb.Migration `json:"content"`
}

// Load loads the current living migrations from the storage.
func (m MigrationExt) Load(ctx context.Context) (Migrations, error) {
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
		if t.SeqNum == baseMigrationSN {
			// NOTE: the legacy truncating isn't implemented by appending a migration.
			// We load their checkpoint here to be compatible with them.
			// Then we can know a truncation happens so we are safe to remove stale compactions.
			truncatedTs, err := GetTSFromFile(ctx, m.s, TruncateSafePointFileName)
			if err != nil {
				return errors.Annotate(err, "failed to get the truncate safepoint for base migration")
			}
			t.Content.TruncatedTo = mathutil.Max(truncatedTs, t.Content.TruncatedTo)
		}
		return t.Content.Unmarshal(b)
	})
	collected := iter.CollectAll(ctx, items)
	if collected.Err != nil {
		return Migrations{}, collected.Err
	}
	sort.Slice(collected.Item, func(i, j int) bool {
		return collected.Item[i].SeqNum < collected.Item[j].SeqNum
	})

	var result Migrations
	if collected.Item[0].SeqNum == baseMigrationSN {
		result = Migrations{
			Base:   &collected.Item[0].Content,
			Layers: collected.Item[1:],
		}
	} else {
		// The BASE migration isn't persisted.
		// This happens when `migrate-to` wasn't run ever.
		result = Migrations{
			Base:   new(pb.Migration),
			Layers: collected.Item,
		}
	}
	return result, nil
}

// EstimateEffectFor estimiates the possible modifications (or, effects) to the storage after
// migrate to the specified migration.
func (m MigrationExt) EstimateEffectFor(ctx context.Context, mig *pb.Migration) (MigratedTo, []storage.Effect) {
	batchSelf := MigrationExt{
		s:      storage.Batch(m.s),
		prefix: m.prefix,
	}
	res := batchSelf.MigrateTo(ctx, mig)
	return res, batchSelf.s.(*storage.Batched).ReadOnlyEffects()
}

func (migs Migrations) AllPbMigrations() []*pb.Migration {
	pbMigs := make([]*pb.Migration, 0, len(migs.Layers)+1)
	pbMigs = append(pbMigs, migs.Base)
	for _, m := range migs.Layers {
		pbMigs = append(pbMigs, &m.Content)
	}
	return pbMigs
}

// MergeTo merges migrations from the BASE in the live migrations until the specified sequence number.
func (migs Migrations) MergeTo(seq int) *pb.Migration {
	newBase := migs.Base
	for _, mig := range migs.Layers {
		if mig.SeqNum > seq {
			return newBase
		}
		newBase = mergeMigrations(newBase, &mig.Content)
	}
	return newBase
}

// MergeAndMigrateTo will merge the migrations from BASE until the specified SN, then migrate to it.
// Finally it writes the new BASE and remove stale migrations from the storage.
func (m MigrationExt) MergeAndMigrateTo(ctx context.Context, seq int) (result MergeAndMigratedTo) {
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
		if mig.SeqNum > seq {
			break
		}
		result.Source = append(result.Source, mig)
	}

	newBase := result.Base
	for _, mig := range result.Source {
		newBase = mergeMigrations(newBase, &mig.Content)
	}

	migTo := m.MigrateTo(ctx, newBase)
	result.MigratedTo = migTo

	err = m.writeBase(ctx, migTo.NewBase)
	if err != nil {
		result.MigratedTo.Warnings = append(result.MigratedTo.Warnings, errors.Annotatef(err, "failed to save the new base"))
		return
	}
	for _, mig := range result.Source {
		err = m.s.DeleteFile(ctx, mig.Path)
		if err != nil {
			migTo.Warnings = append(
				migTo.Warnings,
				errors.Annotatef(err, "failed to delete the merged migration %s", migs.Layers[0].Path),
			)
		}
	}
	return
}

// MigrateTo migrates to a migration.
// If encountered some error during executing some operation, the operation will be put
// to the new BASE, which can be retryed then.
func (m MigrationExt) MigrateTo(ctx context.Context, mig *pb.Migration) MigratedTo {
	result := MigratedTo{
		NewBase: new(pb.Migration),
	}
	// Fills: EditMeta for new Base.
	m.doMetaEdits(ctx, mig, &result)
	// Fills: TruncatedTo, Compactions, DesctructPrefix.
	m.doTruncating(ctx, mig, &result)
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

// doMetaEdits applies the modification to the meta files in the storage.
func (m MigrationExt) doMetaEdits(ctx context.Context, mig *pb.Migration, out *MigratedTo) {
	for _, medit := range mig.EditMeta {
		if isEmptyEdition(medit) {
			continue
		}
		err := m.applyMetaEditToMeta(ctx, medit)
		if err != nil {
			out.NewBase.EditMeta = append(out.NewBase.EditMeta, medit)
			out.Warnings = append(out.Warnings, errors.Annotatef(err, "failed to apply meta edit %s to meta file", medit.Path))
			continue
		}

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

		if !isEmptyEdition(newMetaEdit) {
			out.NewBase.EditMeta = append(out.NewBase.EditMeta, newMetaEdit)
		}
	}
}

// applyMetaEditToMeta applies the modifications in the `MetaEdit` to the real meta file.
func (m MigrationExt) applyMetaEditToMeta(ctx context.Context, medit *pb.MetaEdit) (err error) {
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
	metadata.Files = slices.DeleteFunc(metadata.Files, func(dfi *pb.DataFileInfo) bool {
		// Here, `DeletePhysicalFiles` is usually tiny.
		// Use a hashmap to filter out if this gets slow in the future.
		return slices.Contains(medit.DeletePhysicalFiles, dfi.Path)
	})
	metadata.FileGroups = slices.DeleteFunc(metadata.FileGroups, func(dfg *pb.DataFileGroup) bool {
		return slices.Contains(medit.DeletePhysicalFiles, dfg.Path)
	})
	for _, group := range metadata.FileGroups {
		idx := slices.IndexFunc(medit.DeleteLogicalFiles, func(dsof *pb.DeleteSpansOfFile) bool {
			return dsof.Path == group.Path
		})
		if idx >= 0 {
			sort.Slice(medit.DeleteLogicalFiles[idx].Spans, func(i, j int) bool {
				return medit.DeleteLogicalFiles[idx].Spans[i].Offset < medit.DeleteLogicalFiles[idx].Spans[j].Offset
			})
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
				return
			}
		}
	}
	metadata.FileGroups = slices.DeleteFunc(metadata.FileGroups, func(dfg *pb.DataFileGroup) bool {
		// As all spans in the physical data file has been deleted, it will be soonly removed.
		return len(dfg.DataFilesInfo) == 0
	})

	if isEmptyMetadata(&metadata) {
		// As it is empty, even no hint to destruct self, we can safely delete it.
		return m.s.DeleteFile(ctx, medit.Path)
	}

	updateMetadataInternalStat(&metadata)
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

// doTruncating tries to remove outdated compaction, filling the not-yet removed compactions to the new migration.
func (m MigrationExt) doTruncating(ctx context.Context, mig *pb.Migration, result *MigratedTo) {
	// NOTE: Execution of truncation wasn't implemented here.
	// If we are going to truncate some files, for now we still need to use `br log truncate`.
	for _, compaction := range mig.Compactions {
		// Can we also remove the compaction when `until-ts` is equal to `truncated-to`...?
		if compaction.CompactionUntilTs > mig.TruncatedTo {
			result.NewBase.Compactions = append(result.NewBase.Compactions, compaction)
		} else {
			m.tryRemovePrefix(ctx, compaction.Artifacts, result)
			m.tryRemovePrefix(ctx, compaction.GeneratedFiles, result)
		}
	}
	for _, pfx := range mig.DestructPrefix {
		m.tryRemovePrefix(ctx, pfx, result)
	}
	result.NewBase.TruncatedTo = mig.TruncatedTo
}

func (m MigrationExt) loadFilesOfPrefix(ctx context.Context, prefix string) (out []string, err error) {
	err = m.s.WalkDir(ctx, &storage.WalkOption{SubDir: prefix}, func(path string, size int64) error {
		out = append(out, path)
		return nil
	})
	return
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
