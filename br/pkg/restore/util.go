// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package restore

import (
	"bytes"
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql" // mysql driver
	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/glue"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/br/pkg/summary"
	"github.com/pingcap/tidb/br/pkg/utils"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/pingcap/tidb/util/codec"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	recordPrefixSep = []byte("_r")
	quoteRegexp     = regexp.MustCompile("`(?:[^`]|``)*`")
)

// GetRewriteRules returns the rewrite rule of the new table and the old table.
func GetRewriteRules(
	newTable, oldTable *model.TableInfo, newTimeStamp uint64,
) *RewriteRules {
	tableIDs := make(map[int64]int64)
	tableIDs[oldTable.ID] = newTable.ID
	if oldTable.Partition != nil {
		for _, srcPart := range oldTable.Partition.Definitions {
			for _, destPart := range newTable.Partition.Definitions {
				if srcPart.Name == destPart.Name {
					tableIDs[srcPart.ID] = destPart.ID
				}
			}
		}
	}
	indexIDs := make(map[int64]int64)
	for _, srcIndex := range oldTable.Indices {
		for _, destIndex := range newTable.Indices {
			if srcIndex.Name == destIndex.Name {
				indexIDs[srcIndex.ID] = destIndex.ID
			}
		}
	}

	dataRules := make([]*import_sstpb.RewriteRule, 0)
	for oldTableID, newTableID := range tableIDs {
		dataRules = append(dataRules, &import_sstpb.RewriteRule{
			OldKeyPrefix: append(tablecodec.EncodeTablePrefix(oldTableID), recordPrefixSep...),
			NewKeyPrefix: append(tablecodec.EncodeTablePrefix(newTableID), recordPrefixSep...),
			NewTimestamp: newTimeStamp,
		})
		for oldIndexID, newIndexID := range indexIDs {
			dataRules = append(dataRules, &import_sstpb.RewriteRule{
				OldKeyPrefix: tablecodec.EncodeTableIndexPrefix(oldTableID, oldIndexID),
				NewKeyPrefix: tablecodec.EncodeTableIndexPrefix(newTableID, newIndexID),
				NewTimestamp: newTimeStamp,
			})
		}
	}

	return &RewriteRules{
		Data: dataRules,
	}
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
	}
}

// MakeDBPool makes a session pool with specficated size by sessionFactory.
func MakeDBPool(size uint, dbFactory func() (*DB, error)) ([]*DB, error) {
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
	// the new prefix of the start rule must equal or less than the new prefix of the end rule
	if bytes.Compare(startRule.GetNewKeyPrefix(), endRule.GetNewKeyPrefix()) > 0 {
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
		return errors.Annotate(berrors.ErrRestoreInvalidRewrite, "unexpected rewrite rules")
	}

	startID := tablecodec.DecodeTableID(file.GetStartKey())
	endID := tablecodec.DecodeTableID(file.GetEndKey())
	if startID != endID {
		log.Error("table ids mismatch",
			zap.Int64("startID", startID),
			zap.Int64("endID", endID),
			logutil.File(file))
		return errors.Annotate(berrors.ErrRestoreTableIDMismatch, "file start_key end_key table ids mismatch")
	}
	return nil
}

// Rewrites a raw key and returns a encoded key.
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

func matchNewPrefix(key []byte, rewriteRules *RewriteRules) *import_sstpb.RewriteRule {
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(key, rule.GetNewKeyPrefix()) {
			return rule
		}
	}
	return nil
}

func truncateTS(key []byte) []byte {
	if len(key) == 0 {
		return nil
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
) error {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		summary.CollectDuration("split region", elapsed)
	}()
	splitter := NewRegionSplitter(NewSplitClient(client.GetPDClient(), client.GetTLSConfig()))

	return splitter.Split(ctx, ranges, rewriteRules, func(keys [][]byte) {
		for range keys {
			updateCh.Inc()
		}
	})
}

func rewriteFileKeys(file *backuppb.File, rewriteRules *RewriteRules) (startKey, endKey []byte, err error) {
	startID := tablecodec.DecodeTableID(file.GetStartKey())
	endID := tablecodec.DecodeTableID(file.GetEndKey())
	var rule *import_sstpb.RewriteRule
	if startID == endID {
		startKey, rule = rewriteRawKey(file.GetStartKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			log.Error("cannot find rewrite rule",
				logutil.Key("startKey", file.GetStartKey()),
				zap.Reflect("rewrite data", rewriteRules.Data))
			err = errors.Annotate(berrors.ErrRestoreInvalidRewrite, "cannot find rewrite rule for start key")
			return
		}
		endKey, rule = rewriteRawKey(file.GetEndKey(), rewriteRules)
		if rewriteRules != nil && rule == nil {
			err = errors.Annotate(berrors.ErrRestoreInvalidRewrite, "cannot find rewrite rule for end key")
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
