// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"bytes"

	"github.com/pingcap/errors"
	backuppb "github.com/pingcap/kvproto/pkg/brpb"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/log"
	berrors "github.com/pingcap/tidb/br/pkg/errors"
	"github.com/pingcap/tidb/br/pkg/logutil"
	"github.com/pingcap/tidb/br/pkg/rtree"
	"github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/tablecodec"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tidb/pkg/util/redact"
	"go.uber.org/zap"
)

// AppliedFile has two types for now.
// 1. SST file used by full backup/restore.
// 2. KV file used by pitr restore.
type AppliedFile interface {
	GetStartKey() []byte
	GetEndKey() []byte
}

// RewriteRules contains rules for rewriting keys of tables.
type RewriteRules struct {
	Data        []*import_sstpb.RewriteRule
	OldKeyspace []byte
	NewKeyspace []byte
}

// Append append its argument to this rewrite rules.
func (r *RewriteRules) Append(other RewriteRules) {
	r.Data = append(r.Data, other.Data...)
}

// EmptyRewriteRule make a map of new, empty rewrite rules.
func EmptyRewriteRulesMap() map[int64]*RewriteRules {
	return make(map[int64]*RewriteRules)
}

// EmptyRewriteRule make a new, empty rewrite rule.
func EmptyRewriteRule() *RewriteRules {
	return &RewriteRules{
		Data: []*import_sstpb.RewriteRule{},
	}
}

// GetRewriteRules returns the rewrite rule of the new table and the old table.
// getDetailRule is used for normal backup & restore.
// if set to true, means we collect the rules like tXXX_r, tYYY_i.
// if set to false, means we only collect the rules contain table_id, tXXX, tYYY.
func GetRewriteRules(
	newTable, oldTable *model.TableInfo, newTimeStamp uint64, getDetailRule bool,
) *RewriteRules {
	tableIDs := GetTableIDMap(newTable, oldTable)
	indexIDs := GetIndexIDMap(newTable, oldTable)

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

	tableIDs := GetTableIDMap(newTable, oldTable)
	indexIDs := GetIndexIDMap(newTable, oldTable)

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

// GetRewriteTableID gets rewrite table id by the rewrite rule and original table id
func GetRewriteTableID(tableID int64, rewriteRules *RewriteRules) int64 {
	tableKey := tablecodec.GenTableRecordPrefix(tableID)
	rule := matchOldPrefix(tableKey, rewriteRules)
	if rule == nil {
		return 0
	}

	return tablecodec.DecodeTableID(rule.GetNewKeyPrefix())
}

func FindMatchedRewriteRule(file AppliedFile, rules *RewriteRules) *import_sstpb.RewriteRule {
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

func replacePrefix(s []byte, rewriteRules *RewriteRules) ([]byte, *import_sstpb.RewriteRule) {
	// We should search the dataRules firstly.
	for _, rule := range rewriteRules.Data {
		if bytes.HasPrefix(s, rule.GetOldKeyPrefix()) {
			return append(append([]byte{}, rule.GetNewKeyPrefix()...), s[len(rule.GetOldKeyPrefix()):]...), rule
		}
	}

	return s, nil
}

func RewriteRange(rg *rtree.Range, rewriteRules *RewriteRules) (*rtree.Range, error) {
	if rewriteRules == nil {
		return rg, nil
	}
	startID := tablecodec.DecodeTableID(rg.StartKey)
	endID := tablecodec.DecodeTableID(rg.EndKey)
	var rule *import_sstpb.RewriteRule
	if startID != endID {
		log.Warn("table id does not match",
			logutil.Key("startKey", rg.StartKey),
			logutil.Key("endKey", rg.EndKey),
			zap.Int64("startID", startID),
			zap.Int64("endID", endID))
		return nil, errors.Annotate(berrors.ErrRestoreTableIDMismatch, "table id mismatch")
	}
	rg.StartKey, rule = replacePrefix(rg.StartKey, rewriteRules)
	if rule == nil {
		log.Warn("cannot find rewrite rule", logutil.Key("key", rg.StartKey))
	} else {
		log.Debug(
			"rewrite start key",
			logutil.Key("key", rg.StartKey), logutil.RewriteRule(rule))
	}
	oldKey := rg.EndKey
	rg.EndKey, rule = replacePrefix(rg.EndKey, rewriteRules)
	if rule == nil {
		log.Warn("cannot find rewrite rule", logutil.Key("key", rg.EndKey))
	} else {
		log.Debug(
			"rewrite end key",
			logutil.Key("origin-key", oldKey),
			logutil.Key("key", rg.EndKey),
			logutil.RewriteRule(rule))
	}
	return rg, nil
}
