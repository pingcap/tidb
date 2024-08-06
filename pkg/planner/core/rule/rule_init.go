package rule

import "github.com/pingcap/tidb/pkg/planner/core/rule/util"

// rule/pkg should rely on operator/pkg to do type check and dig in and out,
// rule/util doesn't have to rely on rule/pkg, but it can be put with rule
// handling logic, and be referenced by operator/pkg.
// the core usage only care and call about the rule/pkg and operator/pkg.

func init() {
	util.BuildKeyInfoPortal = buildKeyInfo
}
