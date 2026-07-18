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

use super::*;

/// `tests/integrationtest/t/planner/core/integration.test:1712` exercises
/// Go's UNDERSCORE_CHARSET production with hex literals.  The parser's
/// ValueExpr restore intentionally drops the introducer and emits canonical
/// `x'...'` literals before the explicit `COLLATE binary` suffix.
#[test]
fn explain_binary_hex_collation_restores_like_go() {
    let sql = "explain format='plan_tree' select * from t30094 where  concat(a,'1') = _binary 0xe59388e59388e59388 collate binary and concat(a,'1') = _binary 0xe598bfe598bfe598bf collate binary";
    assert_eq!(
        r(sql),
        "EXPLAIN FORMAT = 'plan_tree' SELECT * FROM `t30094` WHERE CONCAT(`a`, _UTF8MB4'1')=x'e59388e59388e59388' COLLATE binary AND CONCAT(`a`, _UTF8MB4'1')=x'e598bfe598bfe598bf' COLLATE binary"
    );
}
