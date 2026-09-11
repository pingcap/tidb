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

package ddl

import (
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/expression/fulltext"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/sessionctx"
	"github.com/pingcap/tidb/pkg/util/intest"
)

// NewMetaBuildContextWithSctx creates a new MetaBuildContext with the given session context.
func NewMetaBuildContextWithSctx(sctx sessionctx.Context, otherOpts ...metabuild.Option) *metabuild.Context {
	intest.AssertNotNil(sctx)
	sessVars := sctx.GetSessionVars()
	intest.AssertNotNil(sessVars)
	opts := []metabuild.Option{
		metabuild.WithExprCtx(sctx.GetExprCtx()),
		metabuild.WithEnableAutoIncrementInGenerated(sessVars.EnableAutoIncrementInGenerated),
		metabuild.WithPrimaryKeyRequired(!sessVars.InRestrictedSQL && sessVars.PrimaryKeyRequired),
		metabuild.WithClusteredIndexDefMode(sessVars.EnableClusteredIndex),
		metabuild.WithShardRowIDBits(sessVars.ShardRowIDBits),
		metabuild.WithPreSplitRegions(sessVars.PreSplitRegions),
		metabuild.WithInfoSchema(sctx.GetDomainInfoSchema()),
	}

	// Resolve the fulltext analyzer settings here rather than during meta
	// building: a FULLTEXT index freezes them into the schema, so they must be
	// read once from the session issuing the statement.
	//
	// A failure is carried into the context rather than dropped. Skipping the
	// option would leave no analyzer behind, and a FULLTEXT index built from a
	// zero-valued configuration bounds tokens to length zero: it would be
	// created successfully and index nothing.
	ftsConfig, err := fulltext.AnalyzerConfigFromSessionVars(sessVars, model.FullTextParserTypeStandardV1)
	if err != nil {
		opts = append(opts, metabuild.WithFullTextAnalyzerError(errors.Annotate(err,
			"read the fulltext analyzer settings (innodb_ft_min_token_size, "+
				"innodb_ft_max_token_size, innodb_ft_enable_stopword, ngram_token_size) "+
				"needed to build a FULLTEXT index")))
	} else {
		opts = append(opts, metabuild.WithFullTextAnalyzer(ftsConfig))
	}

	if len(otherOpts) > 0 {
		opts = append(opts, otherOpts...)
	}

	return metabuild.NewContext(opts...)
}
