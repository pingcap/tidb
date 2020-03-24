%{
// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package parser

import (
	"math"
	"strconv"

	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
)

%}

%union {
	ident   string
	number  uint64
	hint    *ast.TableOptimizerHint
	hints []*ast.TableOptimizerHint
	table 	ast.HintTable
}

%token	<number>

	/*yy:token "%d" */
	hintIntLit "a 64-bit unsigned integer"

%token	<ident>

	/*yy:token "%c" */
	hintIdentifier

	/*yy:token "@%c" */
	hintSingleAtIdentifier "identifier with single leading at"

	/*yy:token "'%c'" */
	hintStringLit

	/* MySQL 8.0 hint names */
	hintJoinFixedOrder      "JOIN_FIXED_ORDER"
	hintJoinOrder           "JOIN_ORDER"
	hintJoinPrefix          "JOIN_PREFIX"
	hintJoinSuffix          "JOIN_SUFFIX"
	hintBKA                 "BKA"
	hintNoBKA               "NO_BKA"
	hintBNL                 "BNL"
	hintNoBNL               "NO_BNL"
	hintHashJoin            "HASH_JOIN"
	hintNoHashJoin          "NO_HASH_JOIN"
	hintMerge               "MERGE"
	hintNoMerge             "NO_MERGE"
	hintIndexMerge          "INDEX_MERGE"
	hintNoIndexMerge        "NO_INDEX_MERGE"
	hintMRR                 "MRR"
	hintNoMRR               "NO_MRR"
	hintNoICP               "NO_ICP"
	hintNoRangeOptimization "NO_RANGE_OPTIMIZATION"
	hintSkipScan            "SKIP_SCAN"
	hintNoSkipScan          "NO_SKIP_SCAN"
	hintSemijoin            "SEMIJOIN"
	hintNoSemijoin          "NO_SEMIJOIN"
	hintMaxExecutionTime    "MAX_EXECUTION_TIME"
	hintSetVar              "SET_VAR"
	hintResourceGroup       "RESOURCE_GROUP"
	hintQBName              "QB_NAME"

	/* TiDB hint names */
	hintAggToCop              "AGG_TO_COP"
	hintIgnorePlanCache       "IGNORE_PLAN_CACHE"
	hintHashAgg               "HASH_AGG"
	hintIgnoreIndex           "IGNORE_INDEX"
	hintInlHashJoin           "INL_HASH_JOIN"
	hintInlJoin               "INL_JOIN"
	hintInlMergeJoin          "INL_MERGE_JOIN"
	hintMemoryQuota           "MEMORY_QUOTA"
	hintNoSwapJoinInputs      "NO_SWAP_JOIN_INPUTS"
	hintQueryType             "QUERY_TYPE"
	hintReadConsistentReplica "READ_CONSISTENT_REPLICA"
	hintReadFromStorage       "READ_FROM_STORAGE"
	hintSMJoin                "MERGE_JOIN"
	hintStreamAgg             "STREAM_AGG"
	hintSwapJoinInputs        "SWAP_JOIN_INPUTS"
	hintUseIndexMerge         "USE_INDEX_MERGE"
	hintUseIndex              "USE_INDEX"
	hintUsePlanCache          "USE_PLAN_CACHE"
	hintUseToja               "USE_TOJA"
	hintTimeRange             "TIME_RANGE"
	hintUseCascades           "USE_CASCADES"

	/* Other keywords */
	hintOLAP            "OLAP"
	hintOLTP            "OLTP"
	hintTiKV            "TIKV"
	hintTiFlash         "TIFLASH"
	hintFalse           "FALSE"
	hintTrue            "TRUE"
	hintMB              "MB"
	hintGB              "GB"
	hintDupsWeedOut     "DUPSWEEDOUT"
	hintFirstMatch      "FIRSTMATCH"
	hintLooseScan       "LOOSESCAN"
	hintMaterialization "MATERIALIZATION"

%type	<ident>
	Identifier                             "identifier (including keywords)"
	QueryBlockOpt                          "Query block identifier optional"
	JoinOrderOptimizerHintName
	UnsupportedTableLevelOptimizerHintName
	SupportedTableLevelOptimizerHintName
	UnsupportedIndexLevelOptimizerHintName
	SupportedIndexLevelOptimizerHintName
	SubqueryOptimizerHintName
	BooleanHintName                        "name of hints which take a boolean input"
	NullaryHintName                        "name of hints which take no input"
	SubqueryStrategy
	Value                                  "the value in the SET_VAR() hint"
	HintQueryType                          "query type in optimizer hint (OLAP or OLTP)"
	HintStorageType                        "storage type in optimizer hint (TiKV or TiFlash)"

%type	<number>
	UnitOfBytes "unit of bytes (MB or GB)"
	CommaOpt    "optional ','"

%type	<hints>
	OptimizerHintList           "optimizer hint list"
	StorageOptimizerHintOpt     "storage level optimizer hint"
	HintStorageTypeAndTableList "storage type and tables list in optimizer hint"

%type	<hint>
	TableOptimizerHintOpt   "optimizer hint"
	HintTableList           "table list in optimizer hint"
	HintTableListOpt        "optional table list in optimizer hint"
	HintIndexList           "table name with index list in optimizer hint"
	IndexNameList           "index list in optimizer hint"
	IndexNameListOpt        "optional index list in optimizer hint"
	SubqueryStrategies      "subquery strategies"
	SubqueryStrategiesOpt   "optional subquery strategies"
	HintTrueOrFalse         "true or false in optimizer hint"
	HintStorageTypeAndTable "storage type and tables in optimizer hint"

%type	<table>
	HintTable "Table in optimizer hint"


%start	Start

%%

Start:
	OptimizerHintList
	{
		parser.result = $1
	}

OptimizerHintList:
	TableOptimizerHintOpt
	{
		if $1 != nil {
			$$ = []*ast.TableOptimizerHint{$1}
		}
	}
|	OptimizerHintList CommaOpt TableOptimizerHintOpt
	{
		if $3 != nil {
			$$ = append($1, $3)
		} else {
			$$ = $1
		}
	}
|	StorageOptimizerHintOpt
	{
		$$ = $1
	}
|	OptimizerHintList CommaOpt StorageOptimizerHintOpt
	{
		$$ = append($1, $3...)
	}

TableOptimizerHintOpt:
	"JOIN_FIXED_ORDER" '(' QueryBlockOpt ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	JoinOrderOptimizerHintName '(' HintTableList ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	UnsupportedTableLevelOptimizerHintName '(' HintTableListOpt ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	SupportedTableLevelOptimizerHintName '(' HintTableListOpt ')'
	{
		h := $3
		h.HintName = model.NewCIStr($1)
		$$ = h
	}
|	UnsupportedIndexLevelOptimizerHintName '(' HintIndexList ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	SupportedIndexLevelOptimizerHintName '(' HintIndexList ')'
	{
		h := $3
		h.HintName = model.NewCIStr($1)
		$$ = h
	}
|	SubqueryOptimizerHintName '(' QueryBlockOpt SubqueryStrategiesOpt ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	"MAX_EXECUTION_TIME" '(' QueryBlockOpt hintIntLit ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: model.NewCIStr($1),
			QBName:   model.NewCIStr($3),
			HintData: $4,
		}
	}
|	"SET_VAR" '(' Identifier '=' Value ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	"RESOURCE_GROUP" '(' Identifier ')'
	{
		parser.warnUnsupportedHint($1)
		$$ = nil
	}
|	"QB_NAME" '(' Identifier ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: model.NewCIStr($1),
			QBName:   model.NewCIStr($3),
		}
	}
|	"MEMORY_QUOTA" '(' QueryBlockOpt hintIntLit UnitOfBytes ')'
	{
		maxValue := uint64(math.MaxInt64) / $5
		if $4 <= maxValue {
			$$ = &ast.TableOptimizerHint{
				HintName: model.NewCIStr($1),
				HintData: int64($4 * $5),
				QBName:   model.NewCIStr($3),
			}
		} else {
			yylex.AppendError(ErrWarnMemoryQuotaOverflow.GenWithStackByArgs(math.MaxInt64))
			parser.lastErrorAsWarn()
			$$ = nil
		}
	}
|	"TIME_RANGE" '(' hintStringLit CommaOpt hintStringLit ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: model.NewCIStr($1),
			HintData: ast.HintTimeRange{
				From: $3,
				To:   $5,
			},
		}
	}
|	BooleanHintName '(' QueryBlockOpt HintTrueOrFalse ')'
	{
		h := $4
		h.HintName = model.NewCIStr($1)
		h.QBName = model.NewCIStr($3)
		$$ = h
	}
|	NullaryHintName '(' QueryBlockOpt ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: model.NewCIStr($1),
			QBName:   model.NewCIStr($3),
		}
	}
|	"QUERY_TYPE" '(' QueryBlockOpt HintQueryType ')'
	{
		$$ = &ast.TableOptimizerHint{
			HintName: model.NewCIStr($1),
			QBName:   model.NewCIStr($3),
			HintData: model.NewCIStr($4),
		}
	}

StorageOptimizerHintOpt:
	"READ_FROM_STORAGE" '(' QueryBlockOpt HintStorageTypeAndTableList ')'
	{
		hs := $4
		name := model.NewCIStr($1)
		qb := model.NewCIStr($3)
		for _, h := range hs {
			h.HintName = name
			h.QBName = qb
		}
		$$ = hs
	}

HintStorageTypeAndTableList:
	HintStorageTypeAndTable
	{
		$$ = []*ast.TableOptimizerHint{$1}
	}
|	HintStorageTypeAndTableList ',' HintStorageTypeAndTable
	{
		$$ = append($1, $3)
	}

HintStorageTypeAndTable:
	HintStorageType '[' HintTableList ']'
	{
		h := $3
		h.HintData = model.NewCIStr($1)
		$$ = h
	}

QueryBlockOpt:
	/* empty */
	{
		$$ = ""
	}
|	hintSingleAtIdentifier

CommaOpt:
	/*empty*/
	{}
|	','
	{}

/**
 * HintTableListOpt:
 *
 *	[@query_block_name] [tbl_name [, tbl_name] ...]
 *	[tbl_name@query_block_name [, tbl_name@query_block_name] ...]
 *
 */
HintTableListOpt:
	HintTableList
|	QueryBlockOpt
	{
		$$ = &ast.TableOptimizerHint{
			QBName: model.NewCIStr($1),
		}
	}

HintTableList:
	QueryBlockOpt HintTable
	{
		$$ = &ast.TableOptimizerHint{
			Tables: []ast.HintTable{$2},
			QBName: model.NewCIStr($1),
		}
	}
|	HintTableList ',' HintTable
	{
		h := $1
		h.Tables = append(h.Tables, $3)
		$$ = h
	}

HintTable:
	Identifier QueryBlockOpt
	{
		$$ = ast.HintTable{
			TableName: model.NewCIStr($1),
			QBName:    model.NewCIStr($2),
		}
	}
|	Identifier '.' Identifier QueryBlockOpt
	{
		$$ = ast.HintTable{
			DBName:    model.NewCIStr($1),
			TableName: model.NewCIStr($3),
			QBName:    model.NewCIStr($4),
		}
	}

/**
 * HintIndexList:
 *
 *	[@query_block_name] tbl_name [index_name [, index_name] ...]
 *	tbl_name@query_block_name [index_name [, index_name] ...]
 */
HintIndexList:
	QueryBlockOpt HintTable CommaOpt IndexNameListOpt
	{
		h := $4
		h.Tables = []ast.HintTable{$2}
		h.QBName = model.NewCIStr($1)
		$$ = h
	}

IndexNameListOpt:
	/* empty */
	{
		$$ = &ast.TableOptimizerHint{}
	}
|	IndexNameList

IndexNameList:
	Identifier
	{
		$$ = &ast.TableOptimizerHint{
			Indexes: []model.CIStr{model.NewCIStr($1)},
		}
	}
|	IndexNameList ',' Identifier
	{
		h := $1
		h.Indexes = append(h.Indexes, model.NewCIStr($3))
		$$ = h
	}

/**
 * Miscellaneous rules
 */
SubqueryStrategiesOpt:
	/* empty */
	{}
|	SubqueryStrategies

SubqueryStrategies:
	SubqueryStrategy
	{}
|	SubqueryStrategies ',' SubqueryStrategy

Value:
	hintStringLit
|	Identifier
|	hintIntLit
	{
		$$ = strconv.FormatUint($1, 10)
	}

UnitOfBytes:
	"MB"
	{
		$$ = 1024 * 1024
	}
|	"GB"
	{
		$$ = 1024 * 1024 * 1024
	}

HintTrueOrFalse:
	"TRUE"
	{
		$$ = &ast.TableOptimizerHint{HintData: true}
	}
|	"FALSE"
	{
		$$ = &ast.TableOptimizerHint{HintData: false}
	}

JoinOrderOptimizerHintName:
	"JOIN_ORDER"
|	"JOIN_PREFIX"
|	"JOIN_SUFFIX"

UnsupportedTableLevelOptimizerHintName:
	"BKA"
|	"NO_BKA"
|	"BNL"
|	"NO_BNL"
/* HASH_JOIN is supported by TiDB */
|	"NO_HASH_JOIN"
|	"MERGE"
|	"NO_MERGE"

SupportedTableLevelOptimizerHintName:
	"MERGE_JOIN"
|	"INL_JOIN"
|	"INL_HASH_JOIN"
|	"SWAP_JOIN_INPUTS"
|	"NO_SWAP_JOIN_INPUTS"
|	"INL_MERGE_JOIN"
|	"HASH_JOIN"

UnsupportedIndexLevelOptimizerHintName:
	"INDEX_MERGE"
/* NO_INDEX_MERGE is currently a nullary hint in TiDB */
|	"MRR"
|	"NO_MRR"
|	"NO_ICP"
|	"NO_RANGE_OPTIMIZATION"
|	"SKIP_SCAN"
|	"NO_SKIP_SCAN"

SupportedIndexLevelOptimizerHintName:
	"USE_INDEX"
|	"IGNORE_INDEX"
|	"USE_INDEX_MERGE"

SubqueryOptimizerHintName:
	"SEMIJOIN"
|	"NO_SEMIJOIN"

SubqueryStrategy:
	"DUPSWEEDOUT"
|	"FIRSTMATCH"
|	"LOOSESCAN"
|	"MATERIALIZATION"

BooleanHintName:
	"USE_TOJA"
|	"USE_CASCADES"

NullaryHintName:
	"USE_PLAN_CACHE"
|	"HASH_AGG"
|	"STREAM_AGG"
|	"AGG_TO_COP"
|	"NO_INDEX_MERGE"
|	"READ_CONSISTENT_REPLICA"
|	"IGNORE_PLAN_CACHE"

HintQueryType:
	"OLAP"
|	"OLTP"

HintStorageType:
	"TIKV"
|	"TIFLASH"

Identifier:
	hintIdentifier
/* MySQL 8.0 hint names */
|	"JOIN_FIXED_ORDER"
|	"JOIN_ORDER"
|	"JOIN_PREFIX"
|	"JOIN_SUFFIX"
|	"BKA"
|	"NO_BKA"
|	"BNL"
|	"NO_BNL"
|	"HASH_JOIN"
|	"NO_HASH_JOIN"
|	"MERGE"
|	"NO_MERGE"
|	"INDEX_MERGE"
|	"NO_INDEX_MERGE"
|	"MRR"
|	"NO_MRR"
|	"NO_ICP"
|	"NO_RANGE_OPTIMIZATION"
|	"SKIP_SCAN"
|	"NO_SKIP_SCAN"
|	"SEMIJOIN"
|	"NO_SEMIJOIN"
|	"MAX_EXECUTION_TIME"
|	"SET_VAR"
|	"RESOURCE_GROUP"
|	"QB_NAME"
/* TiDB hint names */
|	"AGG_TO_COP"
|	"IGNORE_PLAN_CACHE"
|	"HASH_AGG"
|	"IGNORE_INDEX"
|	"INL_HASH_JOIN"
|	"INL_JOIN"
|	"INL_MERGE_JOIN"
|	"MEMORY_QUOTA"
|	"NO_SWAP_JOIN_INPUTS"
|	"QUERY_TYPE"
|	"READ_CONSISTENT_REPLICA"
|	"READ_FROM_STORAGE"
|	"MERGE_JOIN"
|	"STREAM_AGG"
|	"SWAP_JOIN_INPUTS"
|	"USE_INDEX_MERGE"
|	"USE_INDEX"
|	"USE_PLAN_CACHE"
|	"USE_TOJA"
|	"TIME_RANGE"
|	"USE_CASCADES"
/* other keywords */
|	"OLAP"
|	"OLTP"
|	"TIKV"
|	"TIFLASH"
|	"FALSE"
|	"TRUE"
|	"MB"
|	"GB"
|	"DUPSWEEDOUT"
|	"FIRSTMATCH"
|	"LOOSESCAN"
|	"MATERIALIZATION"
%%
