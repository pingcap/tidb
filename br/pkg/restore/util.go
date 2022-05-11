// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/redact"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	quoteRegexp = regexp.MustCompile("`(?:[^`]|``)*`")
)

// AppliedFile has two types for now.
// 1. SST file used by full backup/restore.
// 2. KV file used by pitr restore.
type AppliedFile interface {
	GetStartKey() []byte
	GetEndKey() []byte
}

// getTableIDMap creates a map maping old tableID to new tableID.
func getTableIDMap(newTable, oldTable *model.TableInfo) map[int64]int64 {
	tableIDMap := make(map[int64]int64)

	if oldTable.Partition != nil && newTable.Partition != nil {
		nameMapID := make(map[string]int64)

		for _, old := range oldTable.Partition.Definitions {
			nameMapID[old.Name.L] = old.ID
		}
		for _, new := range newTable.Partition.Definitions {
			if oldID, exist := nameMapID[new.Name.L]; exist {
				tableIDMap[oldID] = new.ID
			}
		}
	}

	tableIDMap[oldTable.ID] = newTable.ID
	return tableIDMap
}

// getIndexIDMap creates a map maping old indexID to new indexID.
func getIndexIDMap(newTable, oldTable *model.TableInfo) map[int64]int64 {
	indexIDMap := make(map[int64]int64)
	for _, srcIndex := range oldTable.Indices {
		for _, destIndex := range newTable.Indices {
			if srcIndex.Name == destIndex.Name {
				indexIDMap[srcIndex.ID] = destIndex.ID
			}
		}
	}

	return indexIDMap
}

// GetRewriteRules returns the rewrite rule of the new table and the old table.
// getDetailRule is used for normal backup & restore.
// if set to true, means we collect the rules like tXXX_r, tYYY_i.
// if set to false, means we only collect the rules contain table_id, tXXX, tYYY.
func GetRewriteRules(
	newTable, oldTable *model.TableInfo, newTimeStamp uint64, getDetailRule bool,
) *RewriteRules {
	tableIDs := getTableIDMap(newTable, oldTable)
	indexIDs := getIndexIDMap(newTable, oldTable)

	dataRules := make([]*import_sstpb.RewriteRule, 0)
	for oldTableID, newTableID := range tableIDs {
		if getDetailRule {
			dataRules = append(dataRules, &import_sstpb.RewriteRule{
				OldKeyPrefix: tablecodec.GenTableRecordPrefix(oldTableID),
				NewKeyPrefix: tablecodec.GenTableRecordPrefix(newTableID),
				NewTimestamp: newTimeStamp,
			})
			for oldIndexID, newIndexID := range indexIDs {
				dataRules = append(dataRules, &import_sstpb.RewriteRule{
					OldKeyPrefix: tablecodec.EncodeTableIndexPrefix(oldTableID, oldIndexID),
					NewKeyPrefix: tablecodec.EncodeTableIndexPrefix(newTableID, newIndexID),
					NewTimestamp: newTimeStamp,
				})
			}
		} else {
			dataRules = append(dataRules, &import_sstpb.RewriteRule{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(newTableID),
				NewTimestamp: newTimeStamp,
			})
		}
	}

	return &RewriteRules{
		Data: dataRules,
	}
}

func GetRewriteRulesMap(
	newTable, oldTable *model.TableInfo, newTimeStamp uint64, getDetailRule bool,
) map[int64]*RewriteRules {
	rules := make(map[int64]*RewriteRules)

	tableIDs := getTableIDMap(newTable, oldTable)
	indexIDs := getIndexIDMap(newTable, oldTable)

	for oldTableID, newTableID := range tableIDs {
		dataRules := make([]*import_sstpb.RewriteRule, 0)
		if getDetailRule {
			dataRules = append(dataRules, &import_sstpb.RewriteRule{
				OldKeyPrefix: tablecodec.GenTableRecordPrefix(oldTableID),
				NewKeyPrefix: tablecodec.GenTableRecordPrefix(newTableID),
				NewTimestamp: newTimeStamp,
			})
			for oldIndexID, newIndexID := range indexIDs {
				dataRules = append(dataRules, &import_sstpb.RewriteRule{
					OldKeyPrefix: tablecodec.EncodeTableIndexPrefix(oldTableID, oldIndexID),
					NewKeyPrefix: tablecodec.EncodeTableIndexPrefix(newTableID, newIndexID),
					NewTimestamp: newTimeStamp,
				})
			}
		} else {
			dataRules = append(dataRules, &import_sstpb.RewriteRule{
				OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
				NewKeyPrefix: tablecodec.EncodeTablePrefix(newTableID),
				NewTimestamp: newTimeStamp,
			})
		}

		rules[oldTableID] = &RewriteRules{
			Data: dataRules,
		}
	}

	return rules
}

// GetRewriteRuleOfTable returns a rewrite rule from t_{oldID} to t_{newID}.
func GetRewriteRuleOfTable(
	oldTableID, newTableID int64,
	newTimeStamp uint64,
	indexIDs map[int64]int64,
	getDetailRule bool,
) *RewriteRules {
	dataRules := make([]*import_sstpb.RewriteRule, 0)

	if getDetailRule {
		dataRules = append(dataRules, &import_sstpb.RewriteRule{
			OldKeyPrefix: tablecodec.GenTableRecordPrefix(oldTableID),
			NewKeyPrefix: tablecodec.GenTableRecordPrefix(newTableID),
			NewTimestamp: newTimeStamp,
		})
		for oldIndexID, newIndexID := range indexIDs {
			dataRules = append(dataRules, &import_sstpb.RewriteRule{
				OldKeyPrefix: tablecodec.EncodeTableIndexPrefix(oldTableID, oldIndexID),
				NewKeyPrefix: tablecodec.EncodeTableIndexPrefix(newTableID, newIndexID),
				NewTimestamp: newTimeStamp,
			})
		}
	} else {
		dataRules = append(dataRules, &import_sstpb.RewriteRule{
			OldKeyPrefix: tablecodec.EncodeTablePrefix(oldTableID),
			NewKeyPrefix: tablecodec.EncodeTablePrefix(newTableID),
			NewTimestamp: newTimeStamp,
		})
	}

	return &RewriteRules{Data: dataRules}
}

// GetSSTMetaFromFile compares the keys in file, region and rewrite rules, then returns a sst conn.
// The range of the returned sst meta is [regionRule.NewKeyPrefix, append(regionRule.NewKeyPrefix, 0xff)].
func GetSSTMetaFromFile(
	id []byte,
	file *backuppb.File,
	region *metapb.Region,
	regionRule *import_sstpb.RewriteRule,
) import_sstpb.SSTMeta {
	// Get the column family of the file by the file name.
	var cfName string
	if strings.Contains(file.GetName(), defaultCFName) {
		cfName = defaultCFName
	} else if strings.Contains(file.GetName(), writeCFName) {
		cfName = writeCFName
	}
	// Find the overlapped part between the file and the region.
	// Here we rewrites the keys to compare with the keys of the region.
	rangeStart := regionRule.GetNewKeyPrefix()
	//  rangeStart = max(rangeStart, region.StartKey)
	if bytes.Compare(rangeStart, region.GetStartKey()) < 0 {
		rangeStart = region.GetStartKey()
	}

	// Append 10 * 0xff to make sure rangeEnd cover all file key
	// If choose to regionRule.NewKeyPrefix + 1, it may cause WrongPrefix here
	// https://github.com/tikv/tikv/blob/970a9bf2a9ea782a455ae579ad237aaf6cb1daec/
	// components/sst_importer/src/sst_importer.rs#L221
	suffix := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	rangeEnd := append(append([]byte{}, regionRule.GetNewKeyPrefix()...), suffix...)
	// rangeEnd = min(rangeEnd, region.EndKey)
	if len(region.GetEndKey()) > 0 && bytes.Compare(rangeEnd, region.GetEndKey()) > 0 {
		rangeEnd = region.GetEndKey()
	}

	if bytes.Compare(rangeStart, rangeEnd) > 0 {
		log.Panic("range start exceed range end",
			logutil.File(file),
			logutil.Key("startKey", rangeStart),
			logutil.Key("endKey", rangeEnd))
	}

	log.Debug("get sstMeta",
		logutil.File(file),
		logutil.Key("startKey", rangeStart),
		logutil.Key("endKey", rangeEnd))

	return import_sstpb.SSTMeta{
		Uuid:   id,
		CfName: cfName,
		Range: &import_sstpb.Range{
			Start: rangeStart,
			End:   rangeEnd,
		},
		Length:      file.GetSize_(),
		RegionId:    region.GetId(),
		RegionEpoch: region.GetRegionEpoch(),
		CipherIv:    file.GetCipherIv(),
	}
}

// makeDBPool makes a session pool with specficated size by sessionFactory.
func makeDBPool(size uint, dbFactory func() (*DB, error)) ([]*DB, error) {
	dbPool := make([]*DB, 0, size)
	for i := uint(0); i < size; i++ {
		db, e := dbFactory()
		if e != nil {
			return dbPool, e
		}
		dbPool = append(dbPool, db)
	}
	return dbPool, nil
}

// EstimateRangeSize estimates the total range count by file.
func EstimateRangeSize(files []*backuppb.File) int {
	result := 0
	for _, f := range files {
		if strings.HasSuffix(f.GetName(), "_write.sst") {
			result++
		}
	}
	return result
}

// MapTableToFiles makes a map that mapping table ID to its backup files.
// aware that one file can and only can hold one table.
func MapTableToFiles(files []*backuppb.File) map[int64][]*backuppb.File {
	result := map[int64][]*backuppb.File{}
	for _, file := range files {
		tableID := tablecodec.DecodeTableID(file.GetStartKey())
		tableEndID := tablecodec.DecodeTableID(file.GetEndKey())
		if tableID != tableEndID {
			log.Panic("key range spread between many files.",
				zap.String("file name", file.Name),
				logutil.Key("startKey", file.StartKey),
				logutil.Key("endKey", file.EndKey))
		}
		if tableID == 0 {
			log.Panic("invalid table key of file",
				zap.String("file name", file.Name),
				logutil.Key("startKey", file.StartKey),
				logutil.Key("endKey", file.EndKey))
		}
		result[tableID] = append(result[tableID], file)
	}
	return result
}

// GoValidateFileRanges validate files by a stream of tables and yields
// tables with range.
func GoValidateFileRanges(
	ctx context.Context,
	tableStream <-chan CreatedTable,
	fileOfTable map[int64][]*backuppb.File,
	splitSizeBytes, splitKeyCount uint64,
	errCh chan<- error,
) <-chan TableWithRange {
	// Could we have a smaller outCh size?
	outCh := make(chan TableWithRange, len(fileOfTable))
	go func() {
		defer close(outCh)
		defer log.Info("all range generated")
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case t, ok := <-tableStream:
				if !ok {
					return
				}
				files := fileOfTable[t.OldTable.Info.ID]
				if partitions := t.OldTable.Info.Partition; partitions != nil {
					log.Debug("table partition",
						zap.Stringer("database", t.OldTable.DB.Name),
						zap.Stringer("table", t.Table.Name),
						zap.Any("partition info", partitions),
					)
					for _, partition := range partitions.Definitions {
						files = append(files, fileOfTable[partition.ID]...)
					}
				}
				for _, file := range files {
					err := ValidateFileRewriteRule(file, t.RewriteRule)
					if err != nil {
						errCh <- err
						return
					}
				}
				// Merge small ranges to reduce split and scatter regions.
				ranges, stat, err := MergeFileRanges(
					files, splitSizeBytes, splitKeyCount)
				if err != nil {
					errCh <- err
					return
				}
				log.Info("merge and validate file",
					zap.Stringer("database", t.OldTable.DB.Name),
					zap.Stringer("table", t.Table.Name),
					zap.Int("Files(total)", stat.TotalFiles),
					zap.Int("File(write)", stat.TotalWriteCFFile),
					zap.Int("File(default)", stat.TotalDefaultCFFile),
					zap.Int("Region(total)", stat.TotalRegions),
					zap.Int("Regoin(keys avg)", stat.RegionKeysAvg),
					zap.Int("Region(bytes avg)", stat.RegionBytesAvg),
					zap.Int("Merged(regions)", stat.MergedRegions),
					zap.Int("Merged(keys avg)", stat.MergedRegionKeysAvg),
					zap.Int("Merged(bytes avg)", stat.MergedRegionBytesAvg))

				tableWithRange := TableWithRange{
					CreatedTable: t,
					Range:        ranges,
				}
				log.Debug("sending range info",
					zap.Stringer("table", t.Table.Name),
					zap.Int("files", len(files)),
					zap.Int("range size", len(ranges)),
					zap.Int("output channel size", len(outCh)))
				outCh <- tableWithRange
			}
		}
	}()
	return outCh
}

// ValidateFileRewriteRule uses rewrite rules to validate the ranges of a file.
func ValidateFileRewriteRule(file *backuppb.File, rewriteRules *RewriteRules) error {
	// Check if the start key has a matched rewrite key
	_, startRule := rewriteRawKey(file.GetStartKey(), rewriteRules)
	if rewriteRules != nil && startRule == nil {
		tableID := tablecodec.DecodeTableID(file.GetStartKey())
		log.Error(
			"cannot find rewrite rule for file start key",
			zap.Int64("tableID", tableID),
			logutil.File(file),
		)
		return errors.Annotate(berrors.ErrRestoreInvalidRewrite, "cannot find rewrite rule")
	}
	// Check if the end key has a matched rewrite key
	_, endRule := rewriteRawKey(file.GetEndKey(), rewriteRules)
	if rewriteRules != nil && endRule == nil {
		tableID := tablecodec.DecodeTableID(file.GetEndKey())
		log.Error(
			"cannot find rewrite rule for file end key",
			zap.Int64("tableID", tableID),
			logutil.File(file),
		)
		return errors.Annotate(berrors.ErrRestoreInvalidRewrite, "cannot find rewrite rule")
	}
	// the rewrite rule of the start key and the end key should be equaled.
	// i.e. there should only one rewrite rule for one file, a file should only be imported into one region.
	if !bytes.Equal(startRule.GetNewKeyPrefix(), endRule.GetNewKeyPrefix()) {
		startTableID := tablecodec.DecodeTableID(file.GetStartKey())
		endTableID := tablecodec.DecodeTableID(file.GetEndKey())
		log.Error(
			"unexpected rewrite rules",
			zap.Int64("startTableID", startTableID),
			zap.Int64("endTableID", endTableID),
			zap.Stringer("startRule", startRule),
			zap.Stringer("endRule", endRule),
			logutil.File(file),
		)
		return errors.Annotatef(berrors.ErrRestoreInvalidRewrite,
			"rewrite rule mismatch, the backup data may be dirty or from incompatible versions of BR, startKey rule: %X => %X, endKey rule: %X => %X",
			startRule.OldKeyPrefix, startRule.NewKeyPrefix, endRule.OldKeyPrefix, endRule.NewKeyPrefix,
		)
	}
	return nil
}

// Rewrites an encoded key and returns a encoded key.
func rewriteEncodedKey(key []byte, rewriteRules *RewriteRules) ([]byte, *import_sstpb.RewriteRule) {
	if rewriteRules == nil {
		return key, nil
	}
	if len(key) > 0 {
		_, rawKey, _ := codec.DecodeBytes(key, nil)
		return rewriteRawKey(rawKey, rewriteRules)
	}
	return nil, nil
}

// Rewrites a raw key with raw key rewrite rule and returns an encoded key.
func rewriteRawKey(key []byte, rewriteRules *RewriteRules) ([]byte, *import_sstpb.RewriteRule) {
	if rewriteRules == nil {
		return codec.EncodeBytes([]byte{}, key), nil
	}
	if len(key) > 0 {
		rule := matchOldPrefix(key, rewriteRules)
		ret := bytes.Replace(key, rule.GetOldKeyPrefix(), rule.GetNewKeyPrefix(), 1)
		return codec.EncodeBytes([]byte{}, ret), rule
	}
	return nil, nil
}

func matchOldPrefix(key []byte, rewriteRules *RewriteRules) *import_sstpb.RewriteRule {
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(key, rule.GetOldKeyPrefix()) {
			return rule
		}
	}
	return nil
}

func GetKeyTS(key []byte) (uint64, error) {
	if len(key) < 8 {
		return 0, errors.Annotatef(berrors.ErrInvalidArgument,
			"the length of key is smaller than 8, key:%s", redact.Key(key))
	}

	_, ts, err := codec.DecodeUintDesc(key[len(key)-8:])
	return ts, err
}

func TruncateTS(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	if len(key) < 8 {
		return key
	}
	return key[:len(key)-8]
}

// SplitRanges splits region by
// 1. data range after rewrite.
// 2. rewrite rules.
func SplitRanges(
	ctx context.Context,
	client *Client,
	ranges []rtree.Range,
	rewriteRules *RewriteRules,
	updateCh glue.Progress,
	isRawKv bool,
) error {
	splitter := NewRegionSplitter(NewSplitClient(client.GetPDClient(), client.GetTLSConfig(), isRawKv))

	return splitter.Split(ctx, ranges, rewriteRules, isRawKv, func(keys [][]byte) {
		for range keys {
			updateCh.Inc()
		}
	})
}

func findMatchedRewriteRule(file AppliedFile, rules *RewriteRules) *import_sstpb.RewriteRule {
	startID := tablecodec.DecodeTableID(file.GetStartKey())
	endID := tablecodec.DecodeTableID(file.GetEndKey())
	if startID != endID {
		return nil
	}
	_, rule := rewriteRawKey(file.GetStartKey(), rules)
	if rule == nil {
		// fall back to encoded key
		_, rule = rewriteEncodedKey(file.GetStartKey(), rules)
	}
	return rule
}

// RewriteFileKeys tries to choose and apply the rewrite rules to the file.
// This method would try to detach whether the file key is encoded.
// Note: Maybe add something like `GetEncodedStartKey` Or `GetRawStartkey` for `AppliedFile` for making it more determinable?
func RewriteFileKeys(file AppliedFile, rewriteRules *RewriteRules) (startKey, endKey []byte, err error) {
	startID := tablecodec.DecodeTableID(file.GetStartKey())
	endID := tablecodec.DecodeTableID(file.GetEndKey())
	var rule *import_sstpb.RewriteRule
	if startID == endID {
		startKey, rule = rewriteRawKey(file.GetStartKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			// fall back to encoded key
			log.Debug("cannot find rewrite rule with raw key format",
				logutil.Key("startKey", file.GetStartKey()),
				zap.Reflect("rewrite data", rewriteRules.Data))
			startKey, rule = rewriteEncodedKey(file.GetStartKey(), rewriteRules)
			if rule == nil {
				err = errors.Annotate(berrors.ErrRestoreInvalidRewrite, "cannot find rewrite rule for start key")
				return
			}
		}
		endKey, rule = rewriteRawKey(file.GetEndKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			endKey, rule = rewriteEncodedKey(file.GetEndKey(), rewriteRules)
			if rewriteRules != nil && rule == nil {
				err = errors.Annotate(berrors.ErrRestoreInvalidRewrite, "cannot find rewrite rule for end key")
				return
			}
		}
	} else {
		log.Error("table ids dont matched",
			zap.Int64("startID", startID),
			zap.Int64("endID", endID),
			logutil.Key("startKey", startKey),
			logutil.Key("endKey", endKey))
		err = errors.Annotate(berrors.ErrRestoreInvalidRewrite, "invalid table id")
	}
	return
}

func encodeKeyPrefix(key []byte) []byte {
	encodedPrefix := make([]byte, 0)
	ungroupedLen := len(key) % 8
	encodedPrefix = append(encodedPrefix, codec.EncodeBytes([]byte{}, key[:len(key)-ungroupedLen])...)
	return append(encodedPrefix[:len(encodedPrefix)-9], key[len(key)-ungroupedLen:]...)
}

// ZapTables make zap field of table for debuging, including table names.
func ZapTables(tables []CreatedTable) zapcore.Field {
	return logutil.AbbreviatedArray("tables", tables, func(input interface{}) []string {
		tables := input.([]CreatedTable)
		names := make([]string, 0, len(tables))
		for _, t := range tables {
			names = append(names, fmt.Sprintf("%s.%s",
				utils.EncloseName(t.OldTable.DB.Name.String()),
				utils.EncloseName(t.OldTable.Info.Name.String())))
		}
		return names
	})
}

// ParseQuoteName parse the quote `db`.`table` name, and split it.
func ParseQuoteName(name string) (db, table string) {
	names := quoteRegexp.FindAllStringSubmatch(name, -1)
	if len(names) != 2 {
		log.Panic("failed to parse schema name",
			zap.String("origin name", name),
			zap.Any("parsed names", names))
	}
	db = names[0][0]
	table = names[1][0]
	db = strings.ReplaceAll(unQuoteName(db), "``", "`")
	table = strings.ReplaceAll(unQuoteName(table), "``", "`")
	return db, table
}

func unQuoteName(name string) string {
	name = strings.TrimPrefix(name, "`")
	return strings.TrimSuffix(name, "`")
}
