// Copyright 2026 PingCAP, Inc.
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

package ddl

import (
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/infoschema"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/dbterror"
)

// On the classic kernel a FULLTEXT index is materialised in TiKV as a positional
// inverted index. To the DDL machinery it is an ordinary non-unique KV index
// over one column that carries IndexInfo.TiKVFullText, and the ordinary ADD
// INDEX job builds it; the marker is what makes the storage layer tokenize the
// column instead of indexing its value.
//
// The next-gen kernel never reaches this code: its preprocessor rewrites a
// FULLTEXT index into a columnar index, which is held by the columnar engine.

// checkTiKVFullTextClusterSupport refuses to create a FULLTEXT index in TiKV
// while a node in the cluster would not maintain it. The DDL version
// detection loop records whether every node is at least tikvFullTextFirstVer.
func checkTiKVFullTextClusterSupport() error {
	if model.GetJobVerInUse() < model.JobVersion2 {
		// The analyzer snapshot travels in typed job arguments, which the
		// untyped v1 layout cannot carry.
		return dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT index requires DDL job version 2, which the cluster does not use yet")
	}
	if !model.GetTiKVFullTextSupported() {
		return dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT index requires every TiDB node in the cluster to support it; upgrade the remaining nodes first")
	}
	return nil
}

// IsTiKVFullTextIndexOption reports whether an index option describes a
// FULLTEXT index built in TiKV. The preprocessor marks the option on the
// classic kernel; NormalizeTiKVFullTextIndexOption does the same for callers
// that build metadata without the preprocessor.
func IsTiKVFullTextIndexOption(option *ast.IndexOption) bool {
	return option != nil && option.Tp == ast.IndexTypeFulltext
}

// NormalizeTiKVFullTextIndexOption marks the option of a FULLTEXT index
// definition so that index building recognises it, returning an option that
// is never nil.
func NormalizeTiKVFullTextIndexOption(option *ast.IndexOption) *ast.IndexOption {
	if option == nil {
		option = &ast.IndexOption{}
	}
	option.Tp = ast.IndexTypeFulltext
	return option
}

// buildTiKVFullTextInfoWithCheck validates a FULLTEXT index definition and
// resolves the analyzer it is built with: the parser named by the definition,
// and the settings snapshot carried by the meta-build context.
func buildTiKVFullTextInfoWithCheck(
	ctx *metabuild.Context,
	indexPartSpecifications []*ast.IndexPartSpecification,
	indexOption *ast.IndexOption,
	tblInfo *model.TableInfo,
) (*model.TiKVFullTextIndexInfo, error) {
	// A positional inverted index tokenizes one column; the key holds a term
	// of that column, so there is no second column to add to it.
	if len(indexPartSpecifications) != 1 || indexPartSpecifications[0].Column == nil {
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT index must specify one column name")
	}
	idxPart := indexPartSpecifications[0]
	if idxPart.Length != types.UnspecifiedLength {
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT index does not support prefix length")
	}
	if idxPart.Desc {
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT index does not support DESC order")
	}
	if indexOption != nil && indexOption.Global {
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT index does not support GLOBAL")
	}
	if indexOption != nil && indexOption.Condition != nil {
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack("FULLTEXT index does not support a partial condition")
	}

	colInfo := findColumnByName(idxPart.Column.Name.L, tblInfo)
	if colInfo == nil {
		return nil, infoschema.ErrColumnNotExists.GenWithStackByArgs(idxPart.Column.Name, tblInfo.Name)
	}
	if err := checkTiKVFullTextColumn(colInfo); err != nil {
		return nil, err
	}

	parserType := model.FullTextParserTypeStandardV1
	if indexOption != nil && indexOption.ParserName.L != "" {
		parserType = model.GetFullTextParserTypeBySQLName(indexOption.ParserName.L)
		if parserType == model.FullTextParserTypeInvalid {
			return nil, dbterror.ErrUnsupportedIndexType.GenWithStack("Unsupported parser '%s'", indexOption.ParserName.O)
		}
	}

	info := metabuild.DefaultTiKVFullTextAnalyzer()
	if ctx != nil {
		var err error
		if info, err = ctx.GetTiKVFullTextAnalyzer(); err != nil {
			return nil, errors.Trace(err)
		}
	}
	info.ParserType = parserType
	if err := fulltext.ValidateTiKVFullTextIndex(&info); err != nil {
		return nil, dbterror.ErrUnsupportedIndexType.GenWithStack(fmt.Sprintf("FULLTEXT index with %s", err))
	}
	return &info, nil
}

// checkTiKVFullTextColumn checks that a column can be tokenized: it must hold
// text the analyzer can read.
func checkTiKVFullTextColumn(colInfo *model.ColumnInfo) error {
	if !types.IsString(colInfo.FieldType.GetType()) {
		return dbterror.ErrUnsupportedIndexType.GenWithStack(
			"FULLTEXT index requires a string column, but %s is %s", colInfo.Name, colInfo.FieldType.String())
	}
	// A binary column holds bytes, not text. The analyzer would still produce
	// tokens from it, splitting whatever byte sequences happen to look like
	// words, so the index would build and quietly contain nonsense.
	if types.IsBinaryStr(&colInfo.FieldType) {
		return dbterror.ErrUnsupportedIndexType.GenWithStack(
			"FULLTEXT index requires a non-binary string column, but %s is %s",
			colInfo.Name, colInfo.FieldType.String())
	}
	// The analyzer reads UTF-8. TiDB stores the other charsets it supports
	// in their own encoding, which would tokenize as garbage.
	switch colInfo.GetCharset() {
	case charset.CharsetUTF8MB4, charset.CharsetUTF8, charset.CharsetASCII, charset.CharsetLatin1:
	default:
		return dbterror.ErrUnsupportedIndexType.GenWithStack(
			"FULLTEXT index requires a utf8mb4, utf8, ascii or latin1 column, but %s is %s",
			colInfo.Name, colInfo.GetCharset())
	}
	return nil
}
