// Copyright 2023-2023 PingCAP, Inc.

package executor_test

import (
	"strings"
	"testing"

	"github.com/pingcap/tidb/pkg/parser/auth"
	"github.com/pingcap/tidb/pkg/testkit"
	"github.com/pingcap/tidb/pkg/util/dbterror/exeerrors"
	"github.com/stretchr/testify/require"
)

func insertCustomerWithoutLabel(tk *testkit.TestKit) {
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit)  VALUES ( 1, 'SILVER', 'Harry', 'Hill', 'NORTH', 11000.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit)  VALUES ( 2, 'SILVER', 'Vic', 'Reeves', 'NORTH', 2000.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit)  VALUES ( 3, 'SILVER', 'Bob', 'Mortimer', 'WEST', 500.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES ( 4, 'SILVER', 'Paul', 'Whitehouse', 'SOUTH', 1000.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES ( 5, 'SILVER', 'Harry', 'Enfield', 'EAST', 20000.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES ( 6, 'GOLD', 'Jenifer', 'Lopez', 'WEST', 500.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES ( 7, 'GOLD', 'Kylie', 'Minogue', 'NORTH', 1000.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES ( 8, 'GOLD', 'Maria', 'Carey', 'WEST', 1000.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES ( 9, 'GOLD', 'Dani', 'Minogue', 'SOUTH', 20000.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES (10, 'GOLD', 'Whitney', 'Houston', 'EAST', 500.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES (11, 'PLATINUM', 'Robbie', 'Williams', 'SOUTH', 500.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES (12, 'PLATINUM', 'Thom', 'Yorke', 'NORTH', 2000.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES (13, 'PLATINUM', 'Gareth', 'Gates', 'WEST', 10000.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES (14, 'PLATINUM', 'Darius', 'Dinesh', 'EAST', 2000.00)")
	tk.MustExec("INSERT INTO nopolicy_customers (id, cust_type, first_name, last_name, region, credit) VALUES (15, 'PLATINUM', 'Will', 'Young', 'EAST', 100.00)")
}

func insertCustomerWithLabel(tk *testkit.TestKit) {
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label)  VALUES ( 1, 'SILVER', 'Harry', 'Hill', 'NORTH', 11000.00, "30:E:R20")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label)  VALUES ( 2, 'SILVER', 'Vic', 'Reeves', 'NORTH', 2000.00,"20:E:R20")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label)  VALUES ( 3, 'SILVER', 'Bob', 'Mortimer', 'WEST', 500.00,"10:E:R80")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES ( 4, 'SILVER', 'Paul', 'Whitehouse', 'SOUTH', 1000.00,"30:E:R40")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES ( 5, 'SILVER', 'Harry', 'Enfield', 'EAST', 20000.00,"30:E:R60")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES ( 6, 'GOLD', 'Jenifer', 'Lopez', 'WEST', 500.00, "10:E:R80")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES ( 7, 'GOLD', 'Kylie', 'Minogue', 'NORTH', 1000.00,"20:E:R20")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES ( 8, 'GOLD', 'Maria', 'Carey', 'WEST', 1000.00, "20:E:R80")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES ( 9, 'GOLD', 'Dani', 'Minogue', 'SOUTH', 20000.00, "10:E:R40")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES (10, 'GOLD', 'Whitney', 'Houston', 'EAST', 500.00,"30:E:R60")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES (11, 'PLATINUM', 'Robbie', 'Williams', 'SOUTH', 500.00, "10:M:R40")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES (12, 'PLATINUM', 'Thom', 'Yorke', 'NORTH', 2000.00,"20:M:R20")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES (13, 'PLATINUM', 'Gareth', 'Gates', 'WEST', 10000.00,"30:M:R80")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES (14, 'PLATINUM', 'Darius', 'Dinesh', 'EAST', 2000.00,"20:M:R60")`)
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES (15, 'PLATINUM', 'Will', 'Young', 'EAST', 100.00, "10:M:R60")`)
}

func createFiveUsers(tk *testkit.TestKit) {
	// create user sales_manager
	tk.MustExec("create user 'sales_manager'@'%'")
	tk.MustExec("grant SELECT ON linpin.customers TO 'sales_manager'@'%'")
	tk.MustExec("grant SELECT ON linpin.nopolicy_customers TO 'sales_manager'@'%'")

	// create user sales_north
	tk.MustExec("create user 'sales_north'@'%'")
	tk.MustExec("grant SELECT ON linpin.customers TO 'sales_north'@'%'")
	tk.MustExec("grant SELECT ON linpin.nopolicy_customers TO 'sales_north'@'%'")

	// create user sales_south
	tk.MustExec("create user 'sales_south'@'%'")
	tk.MustExec("grant SELECT ON linpin.customers TO 'sales_south'@'%'")
	tk.MustExec("grant SELECT ON linpin.nopolicy_customers TO 'sales_south'@'%'")

	// create user sales_east
	tk.MustExec("create user 'sales_east'@'%'")
	tk.MustExec("grant SELECT ON linpin.customers TO 'sales_east'@'%'")
	tk.MustExec("grant SELECT ON linpin.nopolicy_customers TO 'sales_east'@'%'")

	// create user sales_west
	tk.MustExec("create user 'sales_west'@'%'")
	tk.MustExec("grant SELECT ON linpin.customers TO 'sales_west'@'%'")
	tk.MustExec("grant SELECT ON linpin.nopolicy_customers TO 'sales_west'@'%'")
}

func containsSpecific(result *testkit.Result, col int, specific string) bool {
	rows := result.Rows()
	for i := range rows {
		if strings.Contains(rows[i][col].(string), specific) {
			return true
		}
	}
	return false
}

func notContainsSpecific(result *testkit.Result, specific string) bool {
	rows := result.Rows()
	for i := range rows {
		for j := range rows[i] {
			if strings.Contains(rows[i][j].(string), specific) {
				return false
			}
		}
	}
	return true
}

func checkLbacProcedure(tk *testkit.TestKit) bool {
	procedureNames := []string{"create_policy", "drop_policy", "create_level", "drop_level", "create_compartment",
		"drop_compartment", "create_group", "drop_group", "set_user_labels", "drop_user_labels", "apply_table_policy", "remove_table_policy"}
	res := tk.MustQuery("select routine_name from information_schema.routines")
	for _, name := range procedureNames {
		if !containsSpecific(res, 0, name) {
			return false
		}
	}

	return true
}

func applyPolicyToCustomer(tk *testkit.TestKit) {
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.create_policy("region_policy", "region_label")`)

	tk.MustExec(`CALL LABELSECURITY_SCHEMA.CREATE_LEVEL('region_policy',10,'10','Level 1')`)
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.CREATE_LEVEL('region_policy',20,'20','Level 2')`)
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.CREATE_LEVEL('region_policy',30,'30','Level 3')`)

	tk.MustExec(`CALL LABELSECURITY_SCHEMA.CREATE_COMPARTMENT('region_policy',100,'M','MANAGEMENT')`)
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.CREATE_COMPARTMENT('region_policy',120,'E','EMPLOYEE')`)

	tk.MustExec(`CALL LABELSECURITY_SCHEMA.CREATE_GROUP('region_policy',20,'R20','REGION NORTH')`)
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.CREATE_GROUP('region_policy',40,'R40','REGION SOUTH')`)
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.CREATE_GROUP('region_policy',60,'R60','REGION EAST')`)
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.CREATE_GROUP('region_policy',80,'R80','REGION WEST')`)

	tk.MustExec(`CALL LABELSECURITY_SCHEMA.SET_USER_LABELS('region_policy','sales_manager','30:M,E:R20,R40,R60,R80')`)
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.SET_USER_LABELS('region_policy','sales_north','30:E:R20,R40')`)
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.SET_USER_LABELS('region_policy','sales_south','30:E:R20,R40,R60,R80')`)
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.SET_USER_LABELS('region_policy','sales_east','30:E:R60')`)
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.SET_USER_LABELS('region_policy','sales_west','30:E:R80')`)

	tk.MustExec(`CALL LABELSECURITY_SCHEMA.APPLY_TABLE_POLICY('region_policy', 'linpin', 'CUSTOMERS', 'READ_CONTROL')`)
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.APPLY_TABLE_POLICY('region_policy', 'linpin', 't1', 'READ_CONTROL')`)
}

func TestSelectUsingLabelSecurity(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	tk.MustExec("create database if not exists linpin")
	tk.MustExec("use linpin")
	tk.MustExec("set global tidb_enable_label_security = true")
	tk.MustExec("drop table if exists customers, nopolicy_customers")
	tk.MustExec(`CREATE TABLE customers (
		id                       int NOT NULL,
		cust_type                VARCHAR(10),
		first_name               VARCHAR(30),
		last_name                VARCHAR(30),
		region                   VARCHAR(5),
		credit                   decimal(10,2),
		PRIMARY KEY (id)
	)`)
	tk.MustExec(`CREATE TABLE nopolicy_customers like customers`)
	createFiveUsers(tk)
	insertCustomerWithoutLabel(tk)

	// case1: table without policy, it doesn't use label.
	res := tk.MustQuery("explain SELECT * FROM customers")
	require.True(t, notContainsSpecific(res, "label_accessible"))

	// case2: table without policy, it doesn't use label, reads all rows.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "sales_manager", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	res = tk.MustQuery("select * from linpin.nopolicy_customers")
	require.Truef(t, len(res.Rows()) == 15, "real len: %d", len(res.Rows()))

	// case3: abnormal case, drop the system table mysql.tidb_ls_policies.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("drop table if exists mysql.tidb_ls_policies")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "sales_manager", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	res = tk.MustQuery("select * from linpin.nopolicy_customers")
	require.Truef(t, len(res.Rows()) == 15, "real len: %d", len(res.Rows()))

	// case4: abnormal case, drop the system table mysql.tidb_ls_tables
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec("drop table if exists mysql.tidb_ls_tables")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "sales_manager", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExecToErr("select * from linpin.nopolicy_customers", "[schema:1146]Table 'mysql.tidb_ls_tables' doesn't exist")

	// restore system table.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec(`CREATE TABLE IF NOT EXISTS mysql.tidb_ls_policies(
		policy_name varchar(64) NOT NULL,
		label_column varchar(64) NOT NULL,
		PRIMARY KEY(policy_name)
	)`)
	tk.MustExec("CREATE TABLE IF NOT EXISTS mysql.tidb_ls_tables(" +
		"policy_name varchar(64) NOT NULL, " +
		"schema_name varchar(64) NOT NULL COLLATE utf8mb4_general_ci, " +
		"table_name varchar(64) NOT NULL COLLATE utf8mb4_general_ci, " +
		"table_options enum('READ_CONTROL','WRITE_CONTROL'), " +
		"PRIMARY KEY(policy_name), " +
		"UNIQUE KEY(schema_name, table_name) " +
		") CHARSET=utf8mb4")

	// case5-1: executing "enbale label security" needs super priv.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "sales_manager", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustGetErrCode("admin lbac enable", 8121)
	// case5-2: enbale label security.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec(`set global tidb_enable_procedure = ON`)
	tk.MustExec("admin lbac enable")
	require.True(t, checkLbacProcedure(tk))

	// case6: make a label policy.
	applyPolicyToCustomer(tk)
	insertCustomerWithLabel(tk)
	res = tk.MustQuery("select * from linpin.customers")
	require.Truef(t, len(res.Rows()) == 15, "real len: %d", len(res.Rows()))
	res = tk.MustQuery("show columns from linpin.customers")
	res.CheckAt([]int{0}, [][]any{{"id"}, {"cust_type"}, {"first_name"}, {"last_name"}, {"region"}, {"credit"}, {"region_label"}})

	lbacSelectTestCases := map[string][]struct {
		sqlStr     string
		rowCount   int
		explainSQL string
		explainRes string
	}{
		"sales_manager": { // 30:M,E:R20,R40,R60,R80
			{
				// case 7-1: no filter
				"select * from linpin.customers",
				15,
				"explain select * from linpin.customers",
				"label_accessible",
			},
			{
				// case 7-2: one filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD'",
				5,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD'",
				"label_accessible",
			},
			{
				// case 7-3: and filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8",
				2,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8",
				"label_accessible",
			},
			{
				// case 7-4: or filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8 or cust_type = 'SILVER'",
				7,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8 or cust_type = 'SILVER'",
				"label_accessible",
			},
			{
				// case 7-5: table has alias.
				"SELECT * FROM linpin.customers x",
				15,
				"explain SELECT * FROM linpin.customers x",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 7-6: table has alias.
				"SELECT * FROM linpin.customers x where cust_type='SILVER'",
				5,
				"explain SELECT * FROM linpin.customers x",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 7-7: alias and filter.
				"SELECT * FROM linpin.customers x where x.cust_type='SILVER'",
				5,
				"explain SELECT * FROM linpin.customers x where x.cust_type='SILVER'",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 7-8: point-get.
				"SELECT * FROM linpin.customers WHERE id = 1",
				1,
				"explain SELECT * FROM linpin.customers WHERE id = 1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 7-9: point-get and alias.
				"SELECT * FROM linpin.customers x WHERE x.id = 1",
				1,
				"explain SELECT * FROM linpin.customers x WHERE x.id = 1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 7-10: join statement.
				"SELECT * FROM customers join nopolicy_customers on customers.id = nopolicy_customers.id where customers.cust_type = 'SILVER'",
				5,
				"explain SELECT * FROM customers join nopolicy_customers on customers.id = nopolicy_customers.id where customers.cust_type = 'SILVER'",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 7-11: join statement.
				"SELECT * FROM (SELECT * FROM linpin.customers) tt1",
				15,
				"explain SELECT * FROM (SELECT * FROM linpin.customers) tt1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 7-12: join statement.
				"SELECT * FROM nopolicy_customers where id in (SELECT id FROM customers)",
				15,
				"explain SELECT * FROM nopolicy_customers where id in (SELECT id FROM customers)",
				"label_accessible(linpin.customers.region_label",
			},
		},
		"sales_south": { // 30:E:R20,R40,R60,R80
			{
				// case 8-1: no filter
				"select * from linpin.customers",
				10,
				"explain select * from linpin.customers",
				"label_accessible",
			},
			{
				// case 8-2: one filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD'",
				5,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD'",
				"label_accessible",
			},
			{
				// case 8-3: and filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8",
				2,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8",
				"label_accessible",
			},
			{
				// case 8-4: or filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8 or cust_type = 'SILVER'",
				7,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8 or cust_type = 'SILVER'",
				"label_accessible",
			},
			{
				// case 8-5: table has alias.
				"SELECT * FROM linpin.customers x",
				10,
				"explain SELECT * FROM linpin.customers x",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 8-6: table has alias and filter.
				"SELECT * FROM linpin.customers x where cust_type='SILVER'",
				5,
				"explain SELECT * FROM linpin.customers x",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 8-7: alias and filter.
				"SELECT * FROM linpin.customers x where x.cust_type='SILVER'",
				5,
				"explain SELECT * FROM linpin.customers x where x.cust_type='SILVER'",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 8-8: point-get.
				"SELECT * FROM linpin.customers WHERE id = 1",
				1,
				"explain SELECT * FROM linpin.customers WHERE id = 1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 8-9: point-get.
				"SELECT * FROM linpin.customers WHERE id = 11",
				0,
				"explain SELECT * FROM linpin.customers WHERE id = 11",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 8-10: point-get and alias.
				"SELECT * FROM linpin.customers x WHERE x.id = 1",
				1,
				"explain SELECT * FROM linpin.customers x WHERE x.id = 1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 8-11: join statement.
				"SELECT * FROM customers join nopolicy_customers on customers.id = nopolicy_customers.id where customers.cust_type = 'SILVER'",
				5,
				"explain SELECT * FROM customers join nopolicy_customers on customers.id = nopolicy_customers.id where customers.cust_type = 'SILVER'",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 8-12: derived table statement.
				"SELECT * FROM (SELECT * FROM linpin.customers) tt1",
				10,
				"explain SELECT * FROM (SELECT * FROM linpin.customers) tt1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 8-13: where subquery statement.
				"SELECT * FROM nopolicy_customers where id in (SELECT id FROM customers)",
				10,
				"explain SELECT * FROM nopolicy_customers where id in (SELECT id FROM customers)",
				"label_accessible(linpin.customers.region_label",
			},
		},
		"sales_north": { // 30:E:R20,R40
			{
				// case 9-1: no filter
				"select * from linpin.customers",
				5,
				"explain select * from linpin.customers",
				"label_accessible",
			},
			{
				// case 9-2: one filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD'",
				2,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD'",
				"label_accessible",
			},
			{
				// case 9-3: and filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8",
				1,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8",
				"label_accessible",
			},
			{
				// case 9-4: or filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8 or cust_type = 'SILVER'",
				4,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8 or cust_type = 'SILVER'",
				"label_accessible",
			},
			{
				// case 9-5: table has alias.
				"SELECT * FROM linpin.customers x",
				5,
				"explain SELECT * FROM linpin.customers x",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 9-6: table has alias and filter.
				"SELECT * FROM linpin.customers x where cust_type='SILVER'",
				3,
				"explain SELECT * FROM linpin.customers x",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 9-7: alias and filter.
				"SELECT * FROM linpin.customers x where x.cust_type='SILVER'",
				3,
				"explain SELECT * FROM linpin.customers x where x.cust_type='SILVER'",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 9-8: point-get.
				"SELECT * FROM linpin.customers WHERE id = 1",
				1,
				"explain SELECT * FROM linpin.customers WHERE id = 1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 9-9: point-get.
				"SELECT * FROM linpin.customers WHERE id = 11",
				0,
				"explain SELECT * FROM linpin.customers WHERE id = 11",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 9-10: point-get and alias.
				"SELECT * FROM linpin.customers x WHERE x.id = 1",
				1,
				"explain SELECT * FROM linpin.customers x WHERE x.id = 1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 9-11: join statement.
				"SELECT * FROM customers join nopolicy_customers on customers.id = nopolicy_customers.id where customers.cust_type = 'SILVER'",
				3,
				"explain SELECT * FROM customers join nopolicy_customers on customers.id = nopolicy_customers.id where customers.cust_type = 'SILVER'",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 9-12: derived table statement.
				"SELECT * FROM (SELECT * FROM linpin.customers) tt1",
				5,
				"explain SELECT * FROM (SELECT * FROM linpin.customers) tt1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 9-13: where subquery statement.
				"SELECT * FROM nopolicy_customers where id in (SELECT id FROM customers)",
				5,
				"explain SELECT * FROM nopolicy_customers where id in (SELECT id FROM customers)",
				"label_accessible(linpin.customers.region_label",
			},
		},
		"sales_east": { // 30:E:R60
			{
				// case 10-1: no filter
				"select * from linpin.customers",
				2,
				"explain select * from linpin.customers",
				"label_accessible",
			},
			{
				// case 10-2: one filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD'",
				1,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD'",
				"label_accessible",
			},
			{
				// case 10-3: and filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8",
				1,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8",
				"label_accessible",
			},
			{
				// case 10-4: or filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8 or cust_type = 'SILVER'",
				2,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8 or cust_type = 'SILVER'",
				"label_accessible",
			},
			{
				// case 10-5: table has alias.
				"SELECT * FROM linpin.customers x",
				2,
				"explain SELECT * FROM linpin.customers x",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 10-6: table has alias and filter.
				"SELECT * FROM linpin.customers x where cust_type='SILVER'",
				1,
				"explain SELECT * FROM linpin.customers x",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 10-7: alias and filter.
				"SELECT * FROM linpin.customers x where x.cust_type='SILVER'",
				1,
				"explain SELECT * FROM linpin.customers x where x.cust_type='SILVER'",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 10-8: point-get.
				"SELECT * FROM linpin.customers WHERE id = 1",
				0,
				"explain SELECT * FROM linpin.customers WHERE id = 1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 10-9: point-get.
				"SELECT * FROM linpin.customers WHERE id = 11",
				0,
				"explain SELECT * FROM linpin.customers WHERE id = 11",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 10-10: point-get and alias.
				"SELECT * FROM linpin.customers x WHERE x.id = 5",
				1,
				"explain SELECT * FROM linpin.customers x WHERE x.id = 5",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 10-11: join statement.
				"SELECT * FROM customers join nopolicy_customers on customers.id = nopolicy_customers.id where customers.cust_type = 'SILVER'",
				1,
				"explain SELECT * FROM customers join nopolicy_customers on customers.id = nopolicy_customers.id where customers.cust_type = 'SILVER'",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 10-12: derived table statement.
				"SELECT * FROM (SELECT * FROM linpin.customers) tt1",
				2,
				"explain SELECT * FROM (SELECT * FROM linpin.customers) tt1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 10-13: where subquery statement.
				"SELECT * FROM nopolicy_customers where id in (SELECT id FROM customers)",
				2,
				"explain SELECT * FROM nopolicy_customers where id in (SELECT id FROM customers)",
				"label_accessible(linpin.customers.region_label",
			},
		},
		"sales_west": { // 30:E:R80
			{
				// case 11-1: no filter
				"select * from linpin.customers",
				3,
				"explain select * from linpin.customers",
				"label_accessible",
			},
			{
				// case 11-2: one filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD'",
				2,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD'",
				"label_accessible",
			},
			{
				// case 11-3: and filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8",
				0,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8",
				"label_accessible",
			},
			{
				// case 11-4: or filter.
				"SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8 or cust_type = 'SILVER'",
				1,
				"explain SELECT * FROM linpin.customers WHERE cust_type = 'GOLD' and id > 8 or cust_type = 'SILVER'",
				"label_accessible",
			},
			{
				// case 11-5: table has alias.
				"SELECT * FROM linpin.customers x",
				3,
				"explain SELECT * FROM linpin.customers x",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 11-6: table has alias and filter.
				"SELECT * FROM linpin.customers x where cust_type='SILVER'",
				1,
				"explain SELECT * FROM linpin.customers x",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 11-7: alias and filter.
				"SELECT * FROM linpin.customers x where x.cust_type='SILVER'",
				1,
				"explain SELECT * FROM linpin.customers x where x.cust_type='SILVER'",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 11-8: point-get.
				"SELECT * FROM linpin.customers WHERE id = 3",
				1,
				"explain SELECT * FROM linpin.customers WHERE id = 3",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 11-9: point-get.
				"SELECT * FROM linpin.customers WHERE id = 11",
				0,
				"explain SELECT * FROM linpin.customers WHERE id = 11",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 11-10: point-get and alias.
				"SELECT * FROM linpin.customers x WHERE x.id = 5",
				0,
				"explain SELECT * FROM linpin.customers x WHERE x.id = 5",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 11-11: join statement.
				"SELECT * FROM customers join nopolicy_customers on customers.id = nopolicy_customers.id where customers.cust_type = 'SILVER'",
				1,
				"explain SELECT * FROM customers join nopolicy_customers on customers.id = nopolicy_customers.id where customers.cust_type = 'SILVER'",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 11-12: derived table statement.
				"SELECT * FROM (SELECT * FROM linpin.customers) tt1",
				3,
				"explain SELECT * FROM (SELECT * FROM linpin.customers) tt1",
				"label_accessible(linpin.customers.region_label",
			},
			{
				// case 11-13: where subquery statement.
				"SELECT * FROM nopolicy_customers where id in (SELECT id FROM customers)",
				3,
				"explain SELECT * FROM nopolicy_customers where id in (SELECT id FROM customers)",
				"label_accessible(linpin.customers.region_label",
			},
		},
	}
	for userName, cases := range lbacSelectTestCases {
		require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: userName, Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
		for i, info := range cases {
			res = tk.MustQuery(info.sqlStr)
			require.Truef(t, len(res.Rows()) == info.rowCount, "userName: %s, case %d, real len: %d, expected len: %d", userName, i+1, len(res.Rows()), info.rowCount)
			res = tk.MustQuery(info.explainSQL)
			require.True(t, containsSpecific(res, 4, info.explainRes))
		}
	}

	// test insert process which uses label security.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec(`create table customers2 like customers`)
	tk.MustExec("grant insert ON linpin.customers TO 'sales_north'@'%'")
	tk.MustExec("grant insert ON linpin.nopolicy_customers TO 'sales_north'@'%'")
	tk.MustExec("grant update, delete ON linpin.customers TO 'sales_north'@'%'")
	tk.MustExec("grant select, insert on linpin.customers2 to 'sales_north'@'%'")
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "sales_north", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	// case 12-1: insert with accessible label.
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label)  VALUES ( 16, 'SILVER', 'Harry_north', 'Hill', 'NORTH', 11000.00, "30:E:R20")`)
	// case 12-2: insert with unaccessible compartment.
	tk.MustGetErrCode(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label) VALUES ( 17, 'SILVER', 'Harry_north', 'Hill', 'NORTH', 11000.00, "30:M:R20");`, 8800)
	// case 12-3: insert with unaccessible level.
	tk.MustGetErrCode(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label)  VALUES ( 17, 'SILVER', 'Harry_north', 'Hill', 'NORTH', 11000.00, "40:E:R20")`, 8800)
	// case 12-4: insert with unaccessible group.
	tk.MustGetErrCode(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label)  VALUES ( 17, 'SILVER', 'Harry_north', 'Hill', 'NORTH', 11000.00, "30:E:R60")`, 8800)
	// case 12-5: insert with NULL label.
	tk.MustGetErrCode(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label)  VALUES ( 17, 'SILVER', 'Harry_north2', 'Hill', 'NORTH', 11000.00, NULL)`, 8800)
	// case 12-6: insert with empty label.
	tk.MustExec(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label)  VALUES ( 17, 'SILVER', 'Harry_north2', 'Hill', 'NORTH', 11000.00, '')`)
	// case 12-7: INSERT ... SELECT
	tk.MustExec(`INSERT INTO nopolicy_customers SELECT id, cust_type, first_name, last_name, region, credit FROM customers WHERE id = 11`)
	// case 12-8: PRIMARY KEY CONFLICT
	tk.MustGetErrCode(`INSERT INTO customers (id, cust_type, first_name, last_name, region, credit, region_label)  VALUES ( 16, 'SILVER', 'Harry_north3', 'Hill', 'NORTH', 11000.00, "30:E:R20")`, 1062)
	// case 12-9: insert ignore , accessible label
	tk.MustExec(`INSERT IGNORE INTO customers (id, cust_type, first_name, last_name, region, credit, region_label)  VALUES ( 16, 'SILVER', 'Harry_north3', 'Hill', 'NORTH', 11000.00, "30:E:R20")`)
	// case 12-10: insert ignore , unaccessible label
	tk.MustGetErrCode(`INSERT IGNORE INTO customers (id, cust_type, first_name, last_name, region, credit, region_label)  VALUES ( 16, 'SILVER', 'Harry_north3', 'Hill', 'NORTH', 11000.00, "40:E:R20")`, 8800)
	// case 12-11: insert ... select , insert a unaccessible row.
	tk.MustExec(`insert into customers2 select * from customers`)
	res = tk.MustQuery("SELECT * FROM customers2")
	require.Truef(t, len(res.Rows()) == 7, "real len: %d", len(res.Rows()))
	tk.MustExec(`insert into customers2(id, cust_type, first_name, last_name, region, credit, region_label) values (18, 'PLATINUM','ERobbie', 'Williams', 'SOUTH', 603.10,'10:M:R40')`)
	tk.MustGetErrCode(`insert into customers select * from customers2 where id = 18`, 8800)

	// test replace into command.
	// we don't support lbac for replace into command.
	// the current rule is: it doesn't check the old label, only the new label.
	// case 13-1: a null label.
	tk.MustGetErrCode(`REPLACE into customers(id, cust_type) values (1, 'Harry01')`, 8800)
	// case 13-2: old and new label is accessible.
	tk.MustExec("REPLACE into customers(id, cust_type, first_name, region_label) values (1, 'SILVER','Harry01' ,'30:E:R20')")
	// case 13-3: old is unaccessible and the new is accessible
	tk.MustExec("REPLACE into customers(id, cust_type, first_name, region_label) values (11, 'SILVER','Robbie' ,'30:E:R20')")
	// case 13-4: old is accesible and the the new is unaccessible
	tk.MustGetErrCode(`REPLACE into customers(id, cust_type, first_name, last_name, region, credit, region_label) values (11, 'PLATINUM','Robbie', 'Williams', 'SOUTH', 603.10,'10:M:R40')`, 8800)

	// test update command. lbac is only used for updateing single table.
	// case 14-1: point-get
	tk.MustGetDBError(`update customers set credit = 621.1 where id = 12`, exeerrors.ErrRowLabelUnAccessible)
	// case 14-2: table-scan
	tk.MustGetDBError(`update customers set credit = 621.1 where cust_type = 'PLATINUM' and first_name = 'Thom'`, exeerrors.ErrRowLabelUnAccessible)
	// case 14-3: accessible row
	tk.MustExec(`update customers set credit = 621.1 where id = 1`)
	tk.MustExec(`update customers set credit = 621.1 where cust_type = 'SILVER' and first_name = 'Harry' and last_name = 'Hill'`)
	// the old is accessible, the new is not accessible, update successfully.
	tk.MustExec(`update customers set region_label = '40:E:R20' where id = 1`)
	tk.MustQuery(`select * from customers where id = 1`).Check(testkit.Rows())

	// test delete command.
	// case 15-1: point-get , delete successfully
	tk.MustExec("delete from linpin.customers where id = 4")
	tk.MustQuery(`select * from linpin.customers where id = 4`).Check(testkit.Rows())
	// case 15-2: point-get , delete unsuccessfully
	tk.MustExec(`delete from linpin.customers where id = 12`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustQuery(`select id from linpin.customers where id = 12`).Check(testkit.Rows("12"))
	// case 15-3: point-get , delete accessible rows.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "sales_north", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec(`delete from linpin.customers where id > 6`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustQuery(`select count(*) from linpin.customers`).Check(testkit.Rows("11"))

	// test labc procedure.
	// executing procedure causing panic
	/*
		// It can't drop label when it's using.
		tk.MustExec("CALL LABELSECURITY_SCHEMA.DROP_LEVEL('region_policy','10')")
		tk.MustExec("CALL LABELSECURITY_SCHEMA.DROP_COMPARTMENT('region_policy','M')")
		tk.MustExec("CALL LABELSECURITY_SCHEMA.DROP_GROUP('region_policy','R20')")
		tk.MustQuery("select count(*) from mysql.tidb_ls_elements where element_name = '10' and policy_name = 'region_policy' and element_type = 'level'").Check(testkit.Rows("1"))
		tk.MustQuery("select count(*) from mysql.tidb_ls_elements where element_name = 'M' and policy_name = 'region_policy' and element_type = 'compartment'").Check(testkit.Rows("1"))
		tk.MustQuery("select count(*) from mysql.tidb_ls_elements where element_name = 'R20' and policy_name = 'region_policy' and element_type = 'group'").Check(testkit.Rows("1"))
	*/
	// case 16-1: remove policy from table, label is invalid to this label.
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.REMOVE_TABLE_POLICY('region_policy', 'linpin', 'CUSTOMERS')`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "sales_north", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustQuery(`SELECT Count(*) FROM linpin.customers`).Check(testkit.Rows("11"))
	// case 16-2: apply policy to table, the label is null for all rows. All rows are unaccessible.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.APPLY_TABLE_POLICY('region_policy', 'linpin', 'CUSTOMERS', 'READ_CONTROL')`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "sales_north", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustQuery(`SELECT Count(*) FROM linpin.customers`).Check(testkit.Rows("0"))
	// case 16-3: drop policy, label is invaliad to this label.
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "root", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustExec(`CALL LABELSECURITY_SCHEMA.drop_policy('region_policy')`)
	require.NoError(t, tk.Session().Auth(&auth.UserIdentity{Username: "sales_north", Hostname: "127.0.0.1", AuthHostname: "%"}, nil, nil, nil))
	tk.MustQuery(`SELECT Count(*) FROM linpin.customers`).Check(testkit.Rows("11"))
}
