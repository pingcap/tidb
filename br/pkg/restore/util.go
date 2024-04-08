// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/emirpasic/gods/maps/treemap"
	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/restore/split"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/redact"
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

// getPartitionIDMap creates a map maping old physical ID to new physical ID.
func getPartitionIDMap(newTable, oldTable *model.TableInfo) map[int64]int64 {
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

	return tableIDMap
}

// getTableIDMap creates a map maping old tableID to new tableID.
func getTableIDMap(newTable, oldTable *model.TableInfo) map[int64]int64 {
	tableIDMap := getPartitionIDMap(newTable, oldTable)
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
	rewriteMode RewriteMode,
) (meta *import_sstpb.SSTMeta, err error) {
	r := *region
	// If the rewrite mode is for keyspace, then the region bound should be decoded.
	if rewriteMode == RewriteModeKeyspace {
		if len(region.GetStartKey()) > 0 {
			_, r.StartKey, err = codec.DecodeBytes(region.GetStartKey(), nil)
			if err != nil {
				return
			}
		}
		if len(region.GetEndKey()) > 0 {
			_, r.EndKey, err = codec.DecodeBytes(region.GetEndKey(), nil)
			if err != nil {
				return
			}
		}
	}

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
	if bytes.Compare(rangeStart, r.GetStartKey()) < 0 {
		rangeStart = r.GetStartKey()
	}

	// Append 10 * 0xff to make sure rangeEnd cover all file key
	// If choose to regionRule.NewKeyPrefix + 1, it may cause WrongPrefix here
	// https://github.com/tikv/tikv/blob/970a9bf2a9ea782a455ae579ad237aaf6cb1daec/
	// components/sst_importer/src/sst_importer.rs#L221
	suffix := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	rangeEnd := append(append([]byte{}, regionRule.GetNewKeyPrefix()...), suffix...)
	// rangeEnd = min(rangeEnd, region.EndKey)
	if len(r.GetEndKey()) > 0 && bytes.Compare(rangeEnd, r.GetEndKey()) > 0 {
		rangeEnd = r.GetEndKey()
	}

	if bytes.Compare(rangeStart, rangeEnd) > 0 {
		log.Panic("range start exceed range end",
			logutil.File(file),
			logutil.Key("startKey", rangeStart),
			logutil.Key("endKey", rangeEnd))
	}

	log.Debug("get sstMeta",
		logutil.Region(region),
		logutil.File(file),
		logutil.Key("startKey", rangeStart),
		logutil.Key("endKey", rangeEnd))

	return &import_sstpb.SSTMeta{
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
	}, nil
}

// makeDBPool makes a session pool with specficated size by sessionFactory.
func makeDBPool(size uint, dbFactory func() (*DB, error)) ([]*DB, error) {
	dbPool := make([]*DB, 0, size)
	for i := uint(0); i < size; i++ {
		db, e := dbFactory()
		if e != nil {
			return dbPool, e
		}
		if db != nil {
			dbPool = append(dbPool, db)
		}
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
				ranges, stat, err := MergeAndRewriteFileRanges(
					files, t.RewriteRule, splitSizeBytes, splitKeyCount)
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
	updateCh glue.Progress,
	isRawKv bool,
) error {
	splitter := NewRegionSplitter(split.NewSplitClient(
		client.GetPDClient(),
		client.pdHTTPClient,
		client.GetTLSConfig(),
		isRawKv,
		maxSplitKeysOnce,
	))

	return splitter.ExecuteSplit(ctx, ranges, client.GetStoreCount(), isRawKv, func(keys [][]byte) {
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

// GetRewriteRawKeys rewrites rules to the raw key.
func GetRewriteRawKeys(file AppliedFile, rewriteRules *RewriteRules) (startKey, endKey []byte, err error) {
	startID := tablecodec.DecodeTableID(file.GetStartKey())
	endID := tablecodec.DecodeTableID(file.GetEndKey())
	var rule *import_sstpb.RewriteRule
	if startID == endID {
		startKey, rule = rewriteRawKey(file.GetStartKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			err = errors.Annotatef(berrors.ErrRestoreInvalidRewrite, "cannot find raw rewrite rule for start key, startKey: %s", redact.Key(file.GetStartKey()))
			return
		}
		endKey, rule = rewriteRawKey(file.GetEndKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			err = errors.Annotatef(berrors.ErrRestoreInvalidRewrite, "cannot find raw rewrite rule for end key, endKey: %s", redact.Key(file.GetEndKey()))
			return
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

// GetRewriteRawKeys rewrites rules to the encoded key
func GetRewriteEncodedKeys(file AppliedFile, rewriteRules *RewriteRules) (startKey, endKey []byte, err error) {
	startID := tablecodec.DecodeTableID(file.GetStartKey())
	endID := tablecodec.DecodeTableID(file.GetEndKey())
	var rule *import_sstpb.RewriteRule
	if startID == endID {
		startKey, rule = rewriteEncodedKey(file.GetStartKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			err = errors.Annotatef(berrors.ErrRestoreInvalidRewrite, "cannot find encode rewrite rule for start key, startKey: %s", redact.Key(file.GetStartKey()))
			return
		}
		endKey, rule = rewriteEncodedKey(file.GetEndKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			err = errors.Annotatef(berrors.ErrRestoreInvalidRewrite, "cannot find encode rewrite rule for end key, endKey: %s", redact.Key(file.GetEndKey()))
			return
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
	return logutil.AbbreviatedArray("tables", tables, func(input any) []string {
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

func PrefixStartKey(key []byte) []byte {
	var sk = make([]byte, 0, len(key)+1)
	sk = append(sk, 'z')
	sk = append(sk, key...)
	return sk
}

func PrefixEndKey(key []byte) []byte {
	if len(key) == 0 {
		return []byte{'z' + 1}
	}
	return PrefixStartKey(key)
}

func keyEq(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func keyCmp(a, b []byte) int {
	var length int
	var chosen int
	if len(a) < len(b) {
		length = len(a)
		chosen = -1
	} else if len(a) == len(b) {
		length = len(a)
		chosen = 0
	} else {
		length = len(b)
		chosen = 1
	}
	for i := 0; i < length; i++ {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}
	return chosen
}

func keyCmpInterface(a, b any) int {
	return keyCmp(a.([]byte), b.([]byte))
}

type RecoverRegionInfo struct {
	RegionId      uint64
	RegionVersion uint64
	StartKey      []byte
	EndKey        []byte
	TombStone     bool
}

func SortRecoverRegions(regions map[uint64][]*RecoverRegion) []*RecoverRegionInfo {
	// last log term -> last index -> commit index
	cmps := []func(a, b *RecoverRegion) int{
		func(a, b *RecoverRegion) int {
			return int(a.GetLastLogTerm() - b.GetLastLogTerm())
		},
		func(a, b *RecoverRegion) int {
			return int(a.GetLastIndex() - b.GetLastIndex())
		},
		func(a, b *RecoverRegion) int {
			return int(a.GetCommitIndex() - b.GetCommitIndex())
		},
	}

	// Sort region peer by last log term -> last index -> commit index, and collect all regions' version.
	var regionInfos = make([]*RecoverRegionInfo, 0, len(regions))
	for regionId, peers := range regions {
		sort.Slice(peers, func(i, j int) bool {
			for _, cmp := range cmps {
				if v := cmp(peers[i], peers[j]); v != 0 {
					return v > 0
				}
			}
			return false
		})
		v := peers[0].Version
		sk := PrefixStartKey(peers[0].StartKey)
		ek := PrefixEndKey(peers[0].EndKey)
		regionInfos = append(regionInfos, &RecoverRegionInfo{
			RegionId:      regionId,
			RegionVersion: v,
			StartKey:      sk,
			EndKey:        ek,
			TombStone:     peers[0].Tombstone,
		})
	}

	sort.Slice(regionInfos, func(i, j int) bool { return regionInfos[i].RegionVersion > regionInfos[j].RegionVersion })
	return regionInfos
}

func CheckConsistencyAndValidPeer(regionInfos []*RecoverRegionInfo) (map[uint64]struct{}, error) {
	// split and merge in progressing during the backup, there may some overlap region, we have to handle it
	// Resolve version conflicts.
	var treeMap = treemap.NewWith(keyCmpInterface)
	for _, p := range regionInfos {
		var fk, fv any
		fk, _ = treeMap.Ceiling(p.StartKey)
		// keyspace overlap sk within ceiling - fk
		if fk != nil && (keyEq(fk.([]byte), p.StartKey) || keyCmp(fk.([]byte), p.EndKey) < 0) {
			continue
		}

		// keyspace overlap sk within floor - fk.end_key
		fk, fv = treeMap.Floor(p.StartKey)
		if fk != nil && keyCmp(fv.(*RecoverRegionInfo).EndKey, p.StartKey) > 0 {
			continue
		}
		treeMap.Put(p.StartKey, p)
	}

	// After resolved, all validPeer regions shouldn't be tombstone.
	// do some sanity check
	var validPeers = make(map[uint64]struct{}, 0)
	var iter = treeMap.Iterator()
	var prevEndKey = PrefixStartKey([]byte{})
	var prevRegion uint64 = 0
	for iter.Next() {
		v := iter.Value().(*RecoverRegionInfo)
		if v.TombStone {
			log.Error("validPeer shouldn't be tombstone", zap.Uint64("region id", v.RegionId))
			// TODO, some enhancement may need, a PoC or test may need for decision
			return nil, errors.Annotatef(berrors.ErrRestoreInvalidPeer,
				"Peer shouldn't be tombstone")
		}
		if !keyEq(prevEndKey, iter.Key().([]byte)) {
			log.Error("regions are not adjacent", zap.Uint64("pre region", prevRegion), zap.Uint64("cur region", v.RegionId))
			// TODO, some enhancement may need, a PoC or test may need for decision
			return nil, errors.Annotatef(berrors.ErrInvalidRange,
				"invalid region range")
		}
		prevEndKey = v.EndKey
		prevRegion = v.RegionId
		validPeers[v.RegionId] = struct{}{}
	}
	return validPeers, nil
}

// in cloud, since iops and bandwidth limitation, write operator in raft is slow, so raft state (logterm, lastlog, commitlog...) are the same among the peers
// LeaderCandidates select all peers can be select as a leader during the restore
func LeaderCandidates(peers []*RecoverRegion) ([]*RecoverRegion, error) {
	if peers == nil {
		return nil, errors.Annotatef(berrors.ErrRestoreRegionWithoutPeer,
			"invalid region range")
	}
	candidates := make([]*RecoverRegion, 0, len(peers))
	// by default, the peers[0] to be assign as a leader, since peers already sorted by leader selection rule
	leader := peers[0]
	candidates = append(candidates, leader)
	for _, peer := range peers[1:] {
		// qualificated candidate is leader.logterm = candidate.logterm && leader.lastindex = candidate.lastindex && && leader.commitindex = candidate.commitindex
		if peer.LastLogTerm == leader.LastLogTerm && peer.LastIndex == leader.LastIndex && peer.CommitIndex == leader.CommitIndex {
			log.Debug("leader candidate", zap.Uint64("store id", peer.StoreId), zap.Uint64("region id", peer.RegionId), zap.Uint64("peer id", peer.PeerId))
			candidates = append(candidates, peer)
		}
	}
	return candidates, nil
}

// for region A, has candidate leader x, y, z
// peer x on store 1 with storeBalanceScore 3
// peer y on store 3 with storeBalanceScore 2
// peer z on store 4 with storeBalanceScore 1
// result: peer z will be select as leader on store 4
func SelectRegionLeader(storeBalanceScore map[uint64]int, peers []*RecoverRegion) *RecoverRegion {
	// by default, the peers[0] to be assign as a leader
	leader := peers[0]
	minLeaderStore := storeBalanceScore[leader.StoreId]
	for _, peer := range peers[1:] {
		log.Debug("leader candidate", zap.Int("score", storeBalanceScore[peer.StoreId]), zap.Int("min-score", minLeaderStore), zap.Uint64("store id", peer.StoreId), zap.Uint64("region id", peer.RegionId), zap.Uint64("peer id", peer.PeerId))
		if storeBalanceScore[peer.StoreId] < minLeaderStore {
			minLeaderStore = storeBalanceScore[peer.StoreId]
			leader = peer
		}
	}
	return leader
}

// each 64 items constitute a bitmap unit
type bitMap map[int]uint64

func newBitMap() bitMap {
	return make(map[int]uint64)
}

func (m bitMap) pos(off int) (blockIndex int, bitOffset uint64) {
	return off >> 6, uint64(1) << (off & 63)
}

func (m bitMap) Set(off int) {
	blockIndex, bitOffset := m.pos(off)
	m[blockIndex] |= bitOffset
}

func (m bitMap) Hit(off int) bool {
	blockIndex, bitOffset := m.pos(off)
	return (m[blockIndex] & bitOffset) > 0
}

type fileMap struct {
	// group index -> bitmap of kv files
	pos map[int]bitMap
}

func newFileMap() fileMap {
	return fileMap{
		pos: make(map[int]bitMap),
	}
}

type LogFilesSkipMap struct {
	// metadata group key -> group map
	skipMap map[string]fileMap
}

func NewLogFilesSkipMap() *LogFilesSkipMap {
	return &LogFilesSkipMap{
		skipMap: make(map[string]fileMap),
	}
}

func (m *LogFilesSkipMap) Insert(metaKey string, groupOff, fileOff int) {
	mp, exists := m.skipMap[metaKey]
	if !exists {
		mp = newFileMap()
		m.skipMap[metaKey] = mp
	}
	gp, exists := mp.pos[groupOff]
	if !exists {
		gp = newBitMap()
		mp.pos[groupOff] = gp
	}
	gp.Set(fileOff)
}

func (m *LogFilesSkipMap) NeedSkip(metaKey string, groupOff, fileOff int) bool {
	mp, exists := m.skipMap[metaKey]
	if !exists {
		return false
	}
	gp, exists := mp.pos[groupOff]
	if !exists {
		return false
	}
	return gp.Hit(fileOff)
}
