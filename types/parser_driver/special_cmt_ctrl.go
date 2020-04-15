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

package driver

import (
	"fmt"
	"regexp"

	"github.com/pingcap/parser"
)

// To add new features that needs to be downgrade-compatible,
// 1. Define a featureID below and make sure it is unique.
//    For example, `const FeatureIDMyFea = "my_fea"`.
// 2. Register the new featureID in init().
//    Only the registered parser can parse the comment annotated with `my_fea`.
//    Now, the parser treats `/*T![my_fea] what_ever */` and `what_ever` equivalent.
//    In other word, the parser in old-version TiDB will ignores these comments.
// 3. [optional] Add a pattern into FeatureIDPatterns.
//    This is only required if the new feature is contained in DDL,
//    and we want to comment out this part of SQL in binlog.
func init() {
	parser.SpecialCommentsController.Register(string(FeatureIDAutoRandom))
	parser.SpecialCommentsController.Register(string(FeatureIDAutoIDCache))
}

// SpecialCommentVersionPrefix is the prefix of TiDB executable comments.
const SpecialCommentVersionPrefix = `/*T!`

// BuildSpecialCommentPrefix returns the prefix of `featureID` special comment.
// For some special feature in TiDB, we will refine ddl query with special comment,
// which may be useful when
// A: the downstream is directly MySQL instance (treat it as comment for compatibility).
// B: the downstream is lower version TiDB (ignore the unknown feature comment).
// C: the downstream is same/higher version TiDB (parse the feature syntax out).
func BuildSpecialCommentPrefix(featureID featureID) string {
	return fmt.Sprintf("%s[%s]", SpecialCommentVersionPrefix, featureID)
}

type featureID string

const (
	// FeatureIDAutoRandom is the `auto_random` feature.
	FeatureIDAutoRandom featureID = "auto_rand"
	// FeatureIDAutoIDCache is the `auto_id_cache` feature.
	FeatureIDAutoIDCache featureID = "auto_id_cache"
)

// FeatureIDPatterns is used to record special comments patterns.
var FeatureIDPatterns = map[featureID]*regexp.Regexp{
	FeatureIDAutoRandom:  regexp.MustCompile(`(?i)AUTO_RANDOM\s*(\(\s*\d+\s*\))?\s*`),
	FeatureIDAutoIDCache: regexp.MustCompile(`(?i)AUTO_ID_CACHE\s*=?\s*\d+\s*`),
}
