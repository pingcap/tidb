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
	"testing"

	"github.com/pingcap/tidb/pkg/config/deploymode"
	"github.com/pingcap/tidb/pkg/config/kerneltype"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/charset"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func fullTextPtr[T any](value T) *T {
	return &value
}

func fullTextStringFieldType(tp byte) types.FieldType {
	fieldType := types.NewFieldType(tp)
	fieldType.SetCharset(charset.CharsetUTF8MB4)
	fieldType.SetCollate(charset.CollationUTF8MB4)
	return *fieldType
}

func TestBuildFullTextParserOptions(t *testing.T) {
	setupStarterFTSTest(t)
	ngram, err := buildFullTextNgramOption(nil)
	require.NoError(t, err)
	require.Equal(t, ast.DefaultFullTextNgramMinGram, ngram.MinGram)
	require.Equal(t, ast.DefaultFullTextNgramMaxGram, ngram.MaxGram)
	require.Equal(t, ast.DefaultFullTextNgramGranularity, ngram.Granularity)
	require.Equal(t, ast.DefaultFullTextNgramLowerCase, ngram.LowerCase)

	ngram, err = buildFullTextNgramOption(&ast.FullTextNgramParserParams{
		MinGram:     fullTextPtr(uint64(3)),
		MaxGram:     fullTextPtr(uint64(5)),
		Granularity: fullTextPtr("char"),
		LowerCase:   fullTextPtr(false),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(3), ngram.MinGram)
	require.Equal(t, uint64(5), ngram.MaxGram)
	require.Equal(t, ast.FullTextNgramGranularityChar, ngram.Granularity)
	require.False(t, ngram.LowerCase)

	_, err = buildFullTextNgramOption(&ast.FullTextNgramParserParams{
		MaxGram: fullTextPtr(uint64(6)),
	})
	require.EqualError(t, err, "MAX_GRAM must be less than or equal to 5")

	pathHierarchy, err := buildFullTextPathHierarchyOption(nil)
	require.NoError(t, err)
	require.Equal(t, ast.DefaultFullTextPathHierarchyDelimiter, pathHierarchy.Delimiter)
	pathHierarchy, err = buildFullTextPathHierarchyOption(&ast.FullTextPathHierarchyParserParams{Delimiter: fullTextPtr(".")})
	require.NoError(t, err)
	require.Equal(t, ".", pathHierarchy.Delimiter)
}

func TestBuildFullTextInfoWithPerColumnParsers(t *testing.T) {
	setupStarterFTSTest(t)
	tblInfo := &model.TableInfo{
		Name: ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{ID: 20, Name: ast.NewCIStr("col1"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{ID: 10, Name: ast.NewCIStr("col2"), FieldType: fullTextStringFieldType(mysql.TypeBlob)},
		},
	}
	parts := []*ast.FullTextIndexPartSpecification{
		{
			Column: &ast.ColumnName{Name: ast.NewCIStr("col1")},
			ParserSpecs: []*ast.FullTextParserSpec{
				{ParserType: ast.FullTextParserTypeMultilingualV1},
				{
					ParserType: ast.FullTextParserTypeNgramV1,
					Ngram: &ast.FullTextNgramParserParams{
						MinGram: fullTextPtr(uint64(2)),
						MaxGram: fullTextPtr(uint64(4)),
					},
				},
			},
		},
		{
			Column: &ast.ColumnName{Name: ast.NewCIStr("col2")},
			ParserSpecs: []*ast.FullTextParserSpec{
				{ParserType: ast.FullTextParserTypeExactV1},
				{
					ParserType:    ast.FullTextParserTypePathHierarchyV1,
					PathHierarchy: &ast.FullTextPathHierarchyParserParams{Delimiter: fullTextPtr("/")},
				},
			},
		},
	}

	info, err := buildFullTextInfoWithCheck(parts, ast.FullTextIndexPartsToIndexParts(parts), &ast.IndexOption{}, tblInfo)
	require.NoError(t, err)
	require.Empty(t, info.ParserType)
	require.Len(t, info.IndexFields, 4)
	require.Equal(t, int64(20), info.IndexFields[0].ColumnID)
	require.Equal(t, ast.FullTextParserTypeMultilingualV1, info.IndexFields[0].IndexFieldOption.AnalyzerType)
	require.Equal(t, ast.FullTextParserTypeNgramV1, info.IndexFields[1].IndexFieldOption.AnalyzerType)
	require.Equal(t, uint64(2), info.IndexFields[1].IndexFieldOption.Ngram.MinGram)
	require.Equal(t, uint64(4), info.IndexFields[1].IndexFieldOption.Ngram.MaxGram)
	require.Equal(t, int64(10), info.IndexFields[2].ColumnID)
	require.Equal(t, ast.FullTextParserTypeExactV1, info.IndexFields[2].IndexFieldOption.AnalyzerType)
	require.Equal(t, ast.FullTextParserTypePathHierarchyV1, info.IndexFields[3].IndexFieldOption.AnalyzerType)
	require.Equal(t, "/", info.IndexFields[3].IndexFieldOption.PathHierarchy.Delimiter)
}

func TestBuildFullTextInfoLegacySharedParser(t *testing.T) {
	setupStarterFTSTest(t)
	tblInfo := &model.TableInfo{
		Name: ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("col1"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{ID: 2, Name: ast.NewCIStr("col2"), FieldType: fullTextStringFieldType(mysql.TypeBlob)},
		},
	}
	parts := []*ast.FullTextIndexPartSpecification{
		{Column: &ast.ColumnName{Name: ast.NewCIStr("col1")}},
		{Column: &ast.ColumnName{Name: ast.NewCIStr("col2")}},
	}
	indexOption := &ast.IndexOption{FullTextParser: &ast.FullTextParserSpec{
		ParserType: ast.FullTextParserTypeNgramV1,
		Ngram: &ast.FullTextNgramParserParams{
			MinGram: fullTextPtr(uint64(3)),
			MaxGram: fullTextPtr(uint64(5)),
		},
	}}

	info, err := buildFullTextInfoWithCheck(parts, ast.FullTextIndexPartsToIndexParts(parts), indexOption, tblInfo)
	require.NoError(t, err)
	require.Equal(t, ast.FullTextParserTypeNgramV1, info.ParserType)
	require.Len(t, info.IndexFields, 2)
	for _, field := range info.IndexFields {
		require.Equal(t, ast.FullTextParserTypeNgramV1, field.IndexFieldOption.AnalyzerType)
		require.Equal(t, uint64(3), field.IndexFieldOption.Ngram.MinGram)
		require.Equal(t, uint64(5), field.IndexFieldOption.Ngram.MaxGram)
	}

	legacyIndexOption := &ast.IndexOption{
		ParserName: ast.NewCIStr("MULTILINGUAL"),
	}
	info, err = buildFullTextInfoWithCheck(parts[:1], ast.FullTextIndexPartsToIndexParts(parts[:1]), legacyIndexOption, tblInfo)
	require.NoError(t, err)
	require.Equal(t, ast.FullTextParserTypeMultilingualV1, info.ParserType)
	require.Equal(t, ast.FullTextParserTypeMultilingualV1, info.IndexFields[0].IndexFieldOption.AnalyzerType)
	require.Nil(t, info.IndexFields[0].IndexFieldOption.Ngram)
}

func TestBuildFullTextInfoAllowsOverlappingIndexes(t *testing.T) {
	setupStarterFTSTest(t)
	tblInfo := &model.TableInfo{
		Name: ast.NewCIStr("docs"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("content_text"), FieldType: fullTextStringFieldType(mysql.TypeBlob)},
			{ID: 2, Name: ast.NewCIStr("description"), FieldType: fullTextStringFieldType(mysql.TypeBlob)},
		},
		Indices: []*model.IndexInfo{
			{
				ID:   1,
				Name: ast.NewCIStr("ft_old_content"),
				Columns: []*model.IndexColumn{
					{Name: ast.NewCIStr("content_text"), Offset: 0, Length: types.UnspecifiedLength},
				},
				FullTextInfo: &ast.FullTextIndexInfo{
					IndexFields: []ast.FullTextIndexFieldInfo{
						{
							ColumnID: 1,
							IndexFieldOption: ast.FullTextIndexFieldOption{
								AnalyzerType: ast.FullTextParserTypeMultilingualV1,
								EnableBM25:   true,
							},
						},
					},
				},
			},
		},
	}
	parts := []*ast.FullTextIndexPartSpecification{
		{
			Column: &ast.ColumnName{Name: ast.NewCIStr("content_text")},
			ParserSpecs: []*ast.FullTextParserSpec{
				{ParserType: ast.FullTextParserTypeMultilingualV1},
			},
		},
		{
			Column: &ast.ColumnName{Name: ast.NewCIStr("description")},
			ParserSpecs: []*ast.FullTextParserSpec{
				{ParserType: ast.FullTextParserTypeMultilingualV1},
			},
		},
	}

	info, err := buildFullTextInfoWithCheck(parts, ast.FullTextIndexPartsToIndexParts(parts), &ast.IndexOption{}, tblInfo)
	require.NoError(t, err)
	require.Len(t, info.IndexFields, 2)
	require.Equal(t, int64(1), info.IndexFields[0].ColumnID)
	require.Equal(t, ast.FullTextParserTypeMultilingualV1, info.IndexFields[0].IndexFieldOption.AnalyzerType)
	require.Equal(t, int64(2), info.IndexFields[1].ColumnID)
	require.Equal(t, ast.FullTextParserTypeMultilingualV1, info.IndexFields[1].IndexFieldOption.AnalyzerType)
}

func TestBuildFullTextInfoRejectsInvalidParserAssignments(t *testing.T) {
	setupStarterFTSTest(t)
	tblInfo := &model.TableInfo{
		Name: ast.NewCIStr("t"),
		Columns: []*model.ColumnInfo{
			{ID: 1, Name: ast.NewCIStr("col1"), FieldType: *types.NewFieldType(mysql.TypeVarchar)},
			{ID: 2, Name: ast.NewCIStr("col2"), FieldType: *types.NewFieldType(mysql.TypeBlob)},
		},
	}
	column := func(name string, parsers ...ast.FullTextParserType) *ast.FullTextIndexPartSpecification {
		part := &ast.FullTextIndexPartSpecification{Column: &ast.ColumnName{Name: ast.NewCIStr(name)}}
		for _, parserType := range parsers {
			part.ParserSpecs = append(part.ParserSpecs, &ast.FullTextParserSpec{ParserType: parserType})
		}
		return part
	}

	testCases := []struct {
		name        string
		parts       []*ast.FullTextIndexPartSpecification
		indexOption *ast.IndexOption
		errContains string
	}{
		{
			name:        "mixed index and column parser definitions",
			parts:       []*ast.FullTextIndexPartSpecification{column("col1", ast.FullTextParserTypeExactV1)},
			indexOption: &ast.IndexOption{FullTextParser: &ast.FullTextParserSpec{ParserType: ast.FullTextParserTypeStandardV1}},
			errContains: "cannot mix index-level and column-level parser definitions",
		},
		{
			name:        "missing parser on one column",
			parts:       []*ast.FullTextIndexPartSpecification{column("col1", ast.FullTextParserTypeExactV1), column("col2")},
			errContains: "every fulltext index column must specify at least one parser",
		},
		{
			name:        "duplicate column",
			parts:       []*ast.FullTextIndexPartSpecification{column("col1"), column("col1")},
			errContains: "duplicate fulltext index column col1",
		},
		{
			name:        "duplicate parser",
			parts:       []*ast.FullTextIndexPartSpecification{column("col1", ast.FullTextParserTypeExactV1, ast.FullTextParserTypeExactV1)},
			errContains: "duplicate fulltext parser EXACT for column col1",
		},
		{
			name: "parser-specific parameter mismatch",
			parts: []*ast.FullTextIndexPartSpecification{
				{
					Column: &ast.ColumnName{Name: ast.NewCIStr("col1")},
					ParserSpecs: []*ast.FullTextParserSpec{
						{ParserType: ast.FullTextParserTypeExactV1, Ngram: &ast.FullTextNgramParserParams{}},
					},
				},
			},
			errContains: "fulltext parser EXACT does not support parameters",
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := buildFullTextInfoWithCheck(
				testCase.parts,
				ast.FullTextIndexPartsToIndexParts(testCase.parts),
				testCase.indexOption,
				tblInfo,
			)
			require.ErrorContains(t, err, testCase.errContains)
		})
	}
}

func TestBuildFullTextInfoColumnTypeCompatibility(t *testing.T) {
	setupStarterFTSTest(t)
	type columnTypeCase struct {
		name      string
		tp        byte
		flag      uint
		character bool
		binary    bool
		elems     []string
	}
	newColumn := func(testCase columnTypeCase) *model.ColumnInfo {
		fieldType := types.NewFieldType(testCase.tp)
		fieldType.SetFlag(testCase.flag)
		fieldType.SetElems(testCase.elems)
		if testCase.character {
			fieldType.SetCharset(charset.CharsetUTF8MB4)
			fieldType.SetCollate(charset.CollationUTF8MB4)
		} else if testCase.binary {
			fieldType.SetCharset(charset.CharsetBin)
			fieldType.SetCollate(charset.CollationBin)
		}
		return &model.ColumnInfo{ID: 1, Name: ast.NewCIStr("value"), FieldType: *fieldType}
	}
	build := func(col *model.ColumnInfo, parsers ...ast.FullTextParserType) (*ast.FullTextIndexInfo, error) {
		part := &ast.FullTextIndexPartSpecification{Column: &ast.ColumnName{Name: col.Name}}
		for _, parserType := range parsers {
			part.ParserSpecs = append(part.ParserSpecs, &ast.FullTextParserSpec{ParserType: parserType})
		}
		parts := []*ast.FullTextIndexPartSpecification{part}
		return buildFullTextInfoWithCheck(
			parts,
			ast.FullTextIndexPartsToIndexParts(parts),
			&ast.IndexOption{},
			&model.TableInfo{Name: ast.NewCIStr("t"), Columns: []*model.ColumnInfo{col}},
		)
	}

	supportedExact := []columnTypeCase{
		{name: "tiny", tp: mysql.TypeTiny},
		{name: "boolean", tp: mysql.TypeTiny, flag: mysql.IsBooleanFlag},
		{name: "short", tp: mysql.TypeShort},
		{name: "int24", tp: mysql.TypeInt24},
		{name: "long", tp: mysql.TypeLong},
		{name: "longlong", tp: mysql.TypeLonglong},
		{name: "unsigned", tp: mysql.TypeLonglong, flag: mysql.UnsignedFlag},
		{name: "bit", tp: mysql.TypeBit},
		{name: "year", tp: mysql.TypeYear},
		{name: "float", tp: mysql.TypeFloat},
		{name: "double", tp: mysql.TypeDouble},
		{name: "date", tp: mysql.TypeDate},
		{name: "newdate", tp: mysql.TypeNewDate},
		{name: "datetime", tp: mysql.TypeDatetime},
		{name: "timestamp", tp: mysql.TypeTimestamp},
		{name: "duration", tp: mysql.TypeDuration},
		{name: "enum", tp: mysql.TypeEnum, elems: []string{"small", "medium", "large"}},
		{name: "varchar", tp: mysql.TypeVarchar, character: true},
		{name: "text", tp: mysql.TypeBlob, character: true},
	}
	for _, testCase := range supportedExact {
		t.Run("exact accepts "+testCase.name, func(t *testing.T) {
			info, err := build(newColumn(testCase), ast.FullTextParserTypeExactV1)
			require.NoError(t, err)
			require.Equal(t, ast.FullTextParserTypeExactV1, info.IndexFields[0].IndexFieldOption.AnalyzerType)
		})
	}

	t.Run("text analyzers accept character strings", func(t *testing.T) {
		info, err := build(
			newColumn(columnTypeCase{name: "text", tp: mysql.TypeBlob, character: true}),
			ast.FullTextParserTypeStandardV1,
			ast.FullTextParserTypeMultilingualV1,
			ast.FullTextParserTypeNgramV1,
			ast.FullTextParserTypePathHierarchyV1,
		)
		require.NoError(t, err)
		require.Len(t, info.IndexFields, 4)
	})

	for _, parserType := range []ast.FullTextParserType{
		ast.FullTextParserTypeStandardV1,
		ast.FullTextParserTypeMultilingualV1,
		ast.FullTextParserTypeNgramV1,
		ast.FullTextParserTypePathHierarchyV1,
	} {
		for _, testCase := range []columnTypeCase{
			{name: "signed", tp: mysql.TypeLonglong},
			{name: "enum", tp: mysql.TypeEnum, elems: []string{"small", "medium", "large"}},
		} {
			t.Run(parserType.SQLName()+" rejects "+testCase.name, func(t *testing.T) {
				col := newColumn(testCase)
				require.False(t, isFullTextStringColumnType(&col.FieldType))
				_, err := build(col, parserType)
				require.ErrorContains(t, err, "can only be used on a non-binary string-like column")
			})
		}
	}

	unsupportedExact := []columnTypeCase{
		{name: "decimal", tp: mysql.TypeNewDecimal},
		{name: "json", tp: mysql.TypeJSON},
		{name: "set", tp: mysql.TypeSet},
		{name: "geometry", tp: mysql.TypeGeometry},
		{name: "vector", tp: mysql.TypeTiDBVectorFloat32},
		{name: "binary varchar", tp: mysql.TypeVarchar, binary: true},
		{name: "blob", tp: mysql.TypeBlob},
	}
	for _, testCase := range unsupportedExact {
		t.Run("exact rejects "+testCase.name, func(t *testing.T) {
			_, err := build(newColumn(testCase), ast.FullTextParserTypeExactV1)
			require.ErrorContains(t, err, "fulltext parser EXACT does not support column value")
		})
	}

	t.Run("legacy shared exact parser accepts native values", func(t *testing.T) {
		col := newColumn(columnTypeCase{name: "signed", tp: mysql.TypeLonglong})
		parts := []*ast.FullTextIndexPartSpecification{{Column: &ast.ColumnName{Name: col.Name}}}
		info, err := buildFullTextInfoWithCheck(
			parts,
			ast.FullTextIndexPartsToIndexParts(parts),
			&ast.IndexOption{FullTextParser: &ast.FullTextParserSpec{ParserType: ast.FullTextParserTypeExactV1}},
			&model.TableInfo{Name: ast.NewCIStr("t"), Columns: []*model.ColumnInfo{col}},
		)
		require.NoError(t, err)
		require.Equal(t, ast.FullTextParserTypeExactV1, info.ParserType)
	})
}

func TestBuildIndexInfoWithTypedExactColumns(t *testing.T) {
	setupStarterFTSTest(t)
	column := func(id int64, name string, tp byte, flags uint) *model.ColumnInfo {
		fieldType := types.NewFieldType(tp)
		fieldType.SetFlag(flags)
		return &model.ColumnInfo{ID: id, Name: ast.NewCIStr(name), FieldType: *fieldType}
	}
	enumColumn := column(6, "enum_value", mysql.TypeEnum, 0)
	enumColumn.FieldType.SetElems([]string{"small", "medium", "large"})
	tblInfo := &model.TableInfo{
		Name: ast.NewCIStr("typed_exact_docs"),
		Columns: []*model.ColumnInfo{
			column(1, "signed_value", mysql.TypeLonglong, 0),
			column(2, "unsigned_value", mysql.TypeLonglong, mysql.UnsignedFlag),
			column(3, "bool_value", mysql.TypeTiny, mysql.IsBooleanFlag),
			column(4, "date_value", mysql.TypeDate, 0),
			column(5, "datetime_value", mysql.TypeDatetime, 0),
			enumColumn,
		},
	}
	parts := make([]*ast.FullTextIndexPartSpecification, 0, len(tblInfo.Columns))
	for _, col := range tblInfo.Columns {
		parts = append(parts, &ast.FullTextIndexPartSpecification{
			Column:      &ast.ColumnName{Name: col.Name},
			ParserSpecs: []*ast.FullTextParserSpec{{ParserType: ast.FullTextParserTypeExactV1}},
		})
	}

	indexInfo, err := BuildIndexInfo(
		nil,
		tblInfo,
		ast.NewCIStr("ft_typed"),
		false,
		false,
		model.ColumnarIndexTypeFulltext,
		ast.FullTextIndexPartsToIndexParts(parts),
		parts,
		&ast.IndexOption{},
		model.StatePublic,
	)
	require.NoError(t, err)
	require.Len(t, indexInfo.Columns, len(tblInfo.Columns))
	require.Len(t, indexInfo.FullTextInfo.IndexFields, len(tblInfo.Columns))
}

func TestCheckIndexInModifiableColumnsUsesFullTextAnalyzer(t *testing.T) {
	setupStarterFTSTest(t)
	indexInfo := &model.IndexInfo{
		Columns: []*model.IndexColumn{{Name: ast.NewCIStr("value"), Offset: 0}},
		FullTextInfo: &ast.FullTextIndexInfo{IndexFields: []ast.FullTextIndexFieldInfo{{
			ColumnID: 1,
			IndexFieldOption: ast.FullTextIndexFieldOption{
				AnalyzerType: ast.FullTextParserTypeStandardV1,
			},
		}}},
	}
	column := func(tp byte) []*model.ColumnInfo {
		return []*model.ColumnInfo{{ID: 1, Name: ast.NewCIStr("value"), FieldType: *types.NewFieldType(tp)}}
	}

	require.ErrorContains(
		t,
		checkIndexInModifiableColumns(column(mysql.TypeLonglong), indexInfo),
		"fulltext parser STANDARD can only be used on a non-binary string-like column",
	)
	indexInfo.FullTextInfo.IndexFields[0].IndexFieldOption.AnalyzerType = ast.FullTextParserTypeExactV1
	require.NoError(t, checkIndexInModifiableColumns(column(mysql.TypeLonglong), indexInfo))
	require.NoError(t, checkIndexInModifiableColumns(column(mysql.TypeEnum), indexInfo))
	require.ErrorContains(
		t,
		checkIndexInModifiableColumns(column(mysql.TypeNewDecimal), indexInfo),
		"fulltext parser EXACT does not support column value",
	)
}

func setupStarterFTSTest(t *testing.T) {
	t.Helper()
	if !kerneltype.IsNextGen() {
		t.Skip("starter deploy mode is nextgen-only")
	}
	previous := deploymode.Get()
	require.NoError(t, deploymode.Set(deploymode.Starter))
	t.Cleanup(func() { require.NoError(t, deploymode.Set(previous)) })
}
