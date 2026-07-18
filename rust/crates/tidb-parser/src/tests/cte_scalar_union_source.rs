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

//! Source-backed coverage for a scalar subquery whose body is a set operation.
//!
//! The exact row is `tests/integrationtest/t/cte.test:328`. It exercises the
//! Go parser's general `parseSubquery` path in a scalar comparison: the inner
//! query contains a top-level `UNION`, several nested scalar/`IN` subqueries,
//! and a parenthesized join. The typed `QueryStmt` envelope keeps that set-op
//! body intact instead of narrowing the scalar slot to one `SelectStmt`.

use super::*;

const SQL: &str = "WITH cte_0 AS (select distinct ref_0.wkey as c0, ref_0.pkey as c1, ref_0.c_xhsndb as c2 from t_dnmxh as ref_0 where (1 <= ( select ref_1.pkey not in ( select ref_5.wkey as c0 from t_dnmxh as ref_5 where (ref_5.wkey < ( select ref_6.pkey as c0 from t_cqmg3b as ref_6 where 88 between 96 and 76)) ) as c0 from (t_cqmg3b as ref_1 left outer join t_dnmxh as ref_2 on (ref_1.wkey = ref_2.wkey )) where ref_0.c_xhsndb is NULL union select 33 <= 91 as c0 from t_cqmg3b as ref_8 ))), cte_1 AS (select ref_9.wkey as c0, ref_9.pkey as c1, ref_9.c_anpf_c as c2, ref_9.c_b_fp_c as c3, ref_9.c_ndccfb as c4, ref_9.c_8rswc as c5 from t_cqmg3b as ref_9) select count(1) from cte_0 as ref_10 where case when 56 < 50 then case when 100 in ( select distinct ref_11.c4 as c0 from cte_1 as ref_11 where (ref_11.c4 > ( select ref_13.pkey as c0 from t_dnmxh as ref_13 where (ref_13.wkey > ( select distinct ref_11.c1 as c0 from cte_0 as ref_14)) )) or (1 = 1)) then null else null end else '7mxv6' end not like 'ki4%vc'";

const RESTORED: &str = "WITH `cte_0` AS (SELECT DISTINCT `ref_0`.`wkey` AS `c0`,`ref_0`.`pkey` AS `c1`,`ref_0`.`c_xhsndb` AS `c2` FROM `t_dnmxh` AS `ref_0` WHERE (1<=(SELECT `ref_1`.`pkey` NOT IN (SELECT `ref_5`.`wkey` AS `c0` FROM `t_dnmxh` AS `ref_5` WHERE (`ref_5`.`wkey`<(SELECT `ref_6`.`pkey` AS `c0` FROM `t_cqmg3b` AS `ref_6` WHERE 88 BETWEEN 96 AND 76))) AS `c0` FROM `t_cqmg3b` AS `ref_1` LEFT JOIN `t_dnmxh` AS `ref_2` ON (`ref_1`.`wkey`=`ref_2`.`wkey`) WHERE `ref_0`.`c_xhsndb` IS NULL UNION SELECT 33<=91 AS `c0` FROM `t_cqmg3b` AS `ref_8`))), `cte_1` AS (SELECT `ref_9`.`wkey` AS `c0`,`ref_9`.`pkey` AS `c1`,`ref_9`.`c_anpf_c` AS `c2`,`ref_9`.`c_b_fp_c` AS `c3`,`ref_9`.`c_ndccfb` AS `c4`,`ref_9`.`c_8rswc` AS `c5` FROM `t_cqmg3b` AS `ref_9`) SELECT COUNT(1) FROM `cte_0` AS `ref_10` WHERE CASE WHEN 56<50 THEN CASE WHEN 100 IN (SELECT DISTINCT `ref_11`.`c4` AS `c0` FROM `cte_1` AS `ref_11` WHERE (`ref_11`.`c4`>(SELECT `ref_13`.`pkey` AS `c0` FROM `t_dnmxh` AS `ref_13` WHERE (`ref_13`.`wkey`>(SELECT DISTINCT `ref_11`.`c1` AS `c0` FROM `cte_0` AS `ref_14`)))) OR (1=1)) THEN NULL ELSE NULL END ELSE _UTF8MB4'7mxv6' END NOT LIKE _UTF8MB4'ki4%vc'";

#[test]
fn cte_scalar_union_restores_like_go() {
    let statement = parse(SQL).expect("parse scalar UNION subquery");
    assert_eq!(statement.restore(), RESTORED);
}
