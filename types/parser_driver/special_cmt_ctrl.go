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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"fmt"
	"regexp"

	"github.com/pingcap/tidb/parser/tidb"
)

// SpecialCommentVersionPrefix is the prefix of TiDB executable comments.
const SpecialCommentVersionPrefix = `/*T!`

// BuildSpecialCommentPrefix returns the prefix of `featureID` special comment.
// For some special feature in TiDB, we will refine ddl query with special comment,
// which may be useful when
// A: the downstream is directly MySQL instance (treat it as comment for compatibility).
// B: the downstream is lower version TiDB (ignore the unknown feature comment).
// C: the downstream is same/higher version TiDB (parse the feature syntax out).
func BuildSpecialCommentPrefix(featureID string) string {
	return fmt.Sprintf("%s[%s]", SpecialCommentVersionPrefix, featureID)
}

// FeatureIDPatterns is used to record special comments patterns.
var FeatureIDPatterns = map[string]*regexp.Regexp{
	tidb.FeatureIDAutoRandom:     regexp.MustCompile(`(?P<REPLACE>(?i)AUTO_RANDOM\b\s*(\s*\(\s*\d+\s*\)\s*)?)`),
	tidb.FeatureIDAutoIDCache:    regexp.MustCompile(`(?P<REPLACE>(?i)AUTO_ID_CACHE\s*=?\s*\d+\s*)`),
	tidb.FeatureIDAutoRandomBase: regexp.MustCompile(`(?P<REPLACE>(?i)AUTO_RANDOM_BASE\s*=?\s*\d+\s*)`),
	tidb.FeatureIDClusteredIndex: regexp.MustCompile(`(?i)(PRIMARY)?\s+KEY(\s*\(.*\))?\s+(?P<REPLACE>(NON)?CLUSTERED\b)`),
	tidb.FeatureIDForceAutoInc:   regexp.MustCompile(`(?P<REPLACE>(?i)FORCE)\b\s*AUTO_INCREMENT\s*`),
}
