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

func init() {
	parser.SpecialCommentsController.Register(FeatureIDAutoRandom)
}

// SpecialCommentVersionPrefix is the prefix of TiDB executable comments.
const SpecialCommentVersionPrefix = `/*T!`

// BuildSpecialCommentPrefix returns the prefix of `featureID` special comment.
func BuildSpecialCommentPrefix(featureID string) string {
	return fmt.Sprintf("%s[%s]", SpecialCommentVersionPrefix, featureID)
}

const (
	FeatureIDAutoRandom = "auto_rand"
)

// FeatureIDPatterns is used to record special comments patterns.
var FeatureIDPatterns = map[string]*regexp.Regexp{
	FeatureIDAutoRandom: regexp.MustCompile(`AUTO_RANDOM\s*\(\s*\d+\s*\)\s*`),
}
