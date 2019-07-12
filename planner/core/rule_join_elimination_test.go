package core_test

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/tidb/util/testkit"
	"github.com/pingcap/tidb/util/testleak"
)

var _ = Suite(&testJoinElimination{})

type testJoinElimination struct {
}

// for issue 9536
func (s *testJoinElimination) TestOuterJoinElimination(c *C) {
	defer testleak.AfterTest(c)()
	store, _, err := newStoreWithBootstrap()
	c.Assert(err, IsNil)
	tk := testkit.NewTestKit(c, store)

	tk.MustExec(`use test`)
	tk.MustExec(`drop table if exists org_department, org_position, org_employee_position`)
	tk.MustExec(`CREATE TABLE org_department (
		id int(11) NOT NULL AUTO_INCREMENT,
		ctx int(11) DEFAULT '0' COMMENT 'organization id',
		name varchar(128) DEFAULT NULL,
		left_value int(11) DEFAULT NULL,
		right_value int(11) DEFAULT NULL,
		depth int(11) DEFAULT NULL,
		leader_id bigint(20) DEFAULT NULL,
		status int(11) DEFAULT '1000',
		created_on datetime DEFAULT NULL,
		updated_on datetime DEFAULT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY org_department_id_uindex (id),
		KEY org_department_leader_id_index (leader_id),
		KEY org_department_ctx_index (ctx)
	  ) `)

	tk.MustExec(`CREATE TABLE org_position (
		id int(11) NOT NULL AUTO_INCREMENT,
		ctx int(11) DEFAULT NULL,
		name varchar(128) DEFAULT NULL,
		left_value int(11) DEFAULT NULL,
		right_value int(11) DEFAULT NULL,
		depth int(11) DEFAULT NULL,
		department_id int(11) DEFAULT NULL,
		status int(2) DEFAULT NULL,
		created_on datetime DEFAULT NULL,
		updated_on datetime DEFAULT NULL,
		PRIMARY KEY (id),
		UNIQUE KEY org_position_id_uindex (id),
		KEY org_position_department_id_index (department_id)
	  ) ENGINE=InnoDB AUTO_INCREMENT=22 DEFAULT CHARSET=utf8`)

	tk.MustExec(`CREATE TABLE org_employee_position (
		hotel_id int(11) DEFAULT NULL,
		user_id bigint(20) DEFAULT NULL,
		position_id int(11) DEFAULT NULL,
		status int(11) DEFAULT NULL,
		created_on datetime DEFAULT NULL,
		updated_on datetime DEFAULT NULL,
		UNIQUE KEY org_employee_position_pk (hotel_id,user_id,position_id)
	  ) ENGINE=InnoDB DEFAULT CHARSET=utf8`)

	tk.MustQuery(`explain SELECT  d.id, d.ctx, d.name, d.left_value, d.right_value, d.depth, d.leader_id, d.status, d.created_on, d.updated_on 
		FROM org_department AS d 
		LEFT JOIN org_position AS p ON p.department_id = d.id AND p.status = 1000 
		LEFT JOIN org_employee_position AS ep ON ep.position_id = p.id AND ep.status = 1000 
		WHERE (d.ctx = 1 AND (ep.user_id = 62 OR d.id = 20 OR d.id = 20) AND d.status = 1000)
		GROUP BY d.id ORDER BY d.left_value`).Check(testkit.Rows("Sort_10 1.00 root test.d.left_value:asc]\n" +
		"[└─HashAgg_15 1.00 root group by:test.d.id, funcs:firstrow(test.d.id), firstrow(test.d.ctx), firstrow(test.d.name), firstrow(test.d.left_value), firstrow(test.d.right_value), firstrow(test.d.depth), firstrow(test.d.leader_id), firstrow(test.d.status), firstrow(test.d.created_on), firstrow(test.d.updated_on)]\n" +
		"[  └─Selection_22 0.01 root or(eq(test.ep.user_id, 62), or(eq(test.d.id, 20), eq(test.d.id, 20)))]\n" +
		"[    └─HashLeftJoin_23 0.02 root left outer join, inner:TableReader_58, equal:[eq(test.p.id, test.ep.position_id)]]\n" +
		"[      ├─IndexJoin_32 0.01 root left outer join, inner:IndexLookUp_31, outer key:test.d.id, inner key:test.p.department_id]\n" +
		"[      │ ├─IndexLookUp_48 0.01 root ]\n" +
		"[      │ │ ├─IndexScan_45 10.00 cop table:d, index:ctx, range:[1,1], keep order:false, stats:pseudo]\n" +
		"[      │ │ └─Selection_47 0.01 cop eq(test.d.status, 1000)]\n" +
		"[      │ │   └─TableScan_46 10.00 cop table:org_department, keep order:false, stats:pseudo]\n" +
		"[      │ └─IndexLookUp_31 0.01 root ]\n" +
		"[      │   ├─Selection_29 9.99 cop not(isnull(test.p.department_id))]\n" +
		"[      │   │ └─IndexScan_27 10.00 cop table:p, index:department_id, range: decided by [eq(test.p.department_id, test.d.id)], keep order:false, stats:pseudo]\n" +
		"[      │   └─Selection_30 0.01 cop eq(test.p.status, 1000)]\n" +
		"[      │     └─TableScan_28 9.99 cop table:org_position, keep order:false, stats:pseudo]\n" +
		"[      └─TableReader_58 9.99 root data:Selection_57]\n" +
		"[        └─Selection_57 9.99 cop eq(test.ep.status, 1000), not(isnull(test.ep.position_id))]\n" +
		"[          └─TableScan_56 10000.00 cop table:ep, range:[-inf,+inf], keep order:false, stats:pseudo"))
}
