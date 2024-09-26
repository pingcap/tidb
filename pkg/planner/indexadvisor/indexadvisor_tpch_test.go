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

package indexadvisor_test

import (
	"context"
	"testing"

	"github.com/pingcap/tidb/pkg/planner/indexadvisor"
	"github.com/pingcap/tidb/pkg/testkit"
	s "github.com/pingcap/tidb/pkg/util/set"
	"github.com/stretchr/testify/require"
)

// we have to split this test into multiple cases to speed it up.
var (
	tpchQ1 = `
/*PLACEHOLDER*/ select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date_sub('1998-12-01', interval 108 day)
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
`
	tpchQ2 = `
/*PLACEHOLDER*/ select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	part,
	supplier,
	partsupp,
	nation,
	region
where
	p_partkey = ps_partkey
	and s_suppkey = ps_suppkey
	and p_size = 30
	and p_type like '%STEEL'
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'ASIA'
	and ps_supplycost = (
		select
			min(ps_supplycost)
		from
			partsupp,
			supplier,
			nation,
			region
		where
			p_partkey = ps_partkey
			and s_suppkey = ps_suppkey
			and s_nationkey = n_nationkey
			and n_regionkey = r_regionkey
			and r_name = 'ASIA'
	)
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;
`
	tpchQ3 = `
/*PLACEHOLDER*/ select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'AUTOMOBILE'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < '1995-03-13'
	and l_shipdate > '1995-03-13'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
limit 10;
`
	tpchQ4 = `
/*PLACEHOLDER*/ select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= '1995-01-01'
	and o_orderdate < date_add('1995-01-01', interval '3' month)
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority;
`
	tpchQ5 = `
/*PLACEHOLDER*/ select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = 'MIDDLE EAST'
	and o_orderdate >= '1994-01-01'
	and o_orderdate < date_add('1994-01-01', interval '1' year)
group by
	n_name
order by
	revenue desc;
`
	tpchQ6 = `
/*PLACEHOLDER*/ select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= '1994-01-01'
	and l_shipdate < date_add('1994-01-01', interval '1' year)
	and l_discount between 0.06 - 0.01 and 0.06 + 0.01
	and l_quantity < 24;
`
	tpchQ7 = `
/*PLACEHOLDER*/ select
	supp_nation,
	cust_nation,
	l_year,
	sum(volume) as revenue
from
	(
		select
			n1.n_name as supp_nation,
			n2.n_name as cust_nation,
			extract(year from l_shipdate) as l_year,
			l_extendedprice * (1 - l_discount) as volume
		from
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2
		where
			s_suppkey = l_suppkey
			and o_orderkey = l_orderkey
			and c_custkey = o_custkey
			and s_nationkey = n1.n_nationkey
			and c_nationkey = n2.n_nationkey
			and (
				(n1.n_name = 'JAPAN' and n2.n_name = 'INDIA')
				or (n1.n_name = 'INDIA' and n2.n_name = 'JAPAN')
			)
			and l_shipdate between '1995-01-01' and '1996-12-31'
	) as shipping
group by
	supp_nation,
	cust_nation,
	l_year
order by
	supp_nation,
	cust_nation,
	l_year;
`
	tpchQ8 = `
/*PLACEHOLDER*/ select
	o_year,
	sum(case
		when nation = 'INDIA' then volume
		else 0
	end) / sum(volume) as mkt_share
from
	(
		select
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) as volume,
			n2.n_name as nation
		from
			part,
			supplier,
			lineitem,
			orders,
			customer,
			nation n1,
			nation n2,
			region
		where
			p_partkey = l_partkey
			and s_suppkey = l_suppkey
			and l_orderkey = o_orderkey
			and o_custkey = c_custkey
			and c_nationkey = n1.n_nationkey
			and n1.n_regionkey = r_regionkey
			and r_name = 'ASIA'
			and s_nationkey = n2.n_nationkey
			and o_orderdate between '1995-01-01' and '1996-12-31'
			and p_type = 'SMALL PLATED COPPER'
	) as all_nations
group by
	o_year
order by
	o_year;
`
	tpchQ9 = `
/*PLACEHOLDER*/ select
	nation,
	o_year,
	sum(amount) as sum_profit
from
	(
		select
			n_name as nation,
			extract(year from o_orderdate) as o_year,
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
		from
			part,
			supplier,
			lineitem,
			partsupp,
			orders,
			nation
		where
			s_suppkey = l_suppkey
			and ps_suppkey = l_suppkey
			and ps_partkey = l_partkey
			and p_partkey = l_partkey
			and o_orderkey = l_orderkey
			and s_nationkey = n_nationkey
			and p_name like '%dim%'
	) as profit
group by
	nation,
	o_year
order by
	nation,
	o_year desc;
`
	tpchQ10 = `
/*PLACEHOLDER*/ select
	c_custkey,
	c_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	c_acctbal,
	n_name,
	c_address,
	c_phone,
	c_comment
from
	customer,
	orders,
	lineitem,
	nation
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate >= '1993-08-01'
	and o_orderdate < date_add('1993-08-01', interval '3' month)
	and l_returnflag = 'R'
	and c_nationkey = n_nationkey
group by
	c_custkey,
	c_name,
	c_acctbal,
	c_phone,
	n_name,
	c_address,
	c_comment
order by
	revenue desc
limit 20;
`
	tpchQ11 = `
/*PLACEHOLDER*/ select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = 'MOZAMBIQUE'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * 0.0001000000
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = 'MOZAMBIQUE'
		)
order by
	value desc;
`
	tpchQ12 = `
/*PLACEHOLDER*/ select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in ('RAIL', 'FOB')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= '1997-01-01'
	and l_receiptdate < date_add('1997-01-01', interval '1' year)
group by
	l_shipmode
order by
	l_shipmode;
`
	tpchQ13 = `
/*PLACEHOLDER*/ select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey,
			count(o_orderkey) as c_count
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%pending%deposits%'
		group by
			c_custkey
	) c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc;
`
	tpchQ14 = `
/*PLACEHOLDER*/ select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= '1996-12-01'
	and l_shipdate < date_add('1996-12-01', interval '1' month);
`
	tpchQ16 = `
/*PLACEHOLDER*/ select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> 'Brand#34'
	and p_type not like 'LARGE BRUSHED%'
	and p_size in (48, 19, 12, 4, 41, 7, 21, 39)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;
`
	tpchQ17 = `
/*PLACEHOLDER*/ select
	sum(l_extendedprice) / 7.0 as avg_yearly
from
	lineitem,
	part
where
	p_partkey = l_partkey
	and p_brand = 'Brand#44'
	and p_container = 'WRAP PKG'
	and l_quantity < (
		select
			0.2 * avg(l_quantity)
		from
			lineitem
		where
			l_partkey = p_partkey
	);
`
	tpchQ18 = `
/*PLACEHOLDER*/ select
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice,
	sum(l_quantity)
from
	customer,
	orders,
	lineitem
where
	o_orderkey in (
		select
			l_orderkey
		from
			lineitem
		group by
			l_orderkey having
				sum(l_quantity) > 314
	)
	and c_custkey = o_custkey
	and o_orderkey = l_orderkey
group by
	c_name,
	c_custkey,
	o_orderkey,
	o_orderdate,
	o_totalprice
order by
	o_totalprice desc,
	o_orderdate
limit 100;
`
	tpchQ19 = `
/*PLACEHOLDER*/ select
	sum(l_extendedprice* (1 - l_discount)) as revenue
from
	lineitem,
	part
where
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#52'
		and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
		and l_quantity >= 4 and l_quantity <= 4 + 10
		and p_size between 1 and 5
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#11'
		and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
		and l_quantity >= 18 and l_quantity <= 18 + 10
		and p_size between 1 and 10
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	)
	or
	(
		p_partkey = l_partkey
		and p_brand = 'Brand#51'
		and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
		and l_quantity >= 29 and l_quantity <= 29 + 10
		and p_size between 1 and 15
		and l_shipmode in ('AIR', 'AIR REG')
		and l_shipinstruct = 'DELIVER IN PERSON'
	);
`
	tpchQ20 = `
/*PLACEHOLDER*/ select
	s_name,
	s_address
from
	supplier,
	nation
where
	s_suppkey in (
		select
			ps_suppkey
		from
			partsupp
		where
			ps_partkey in (
				select
					p_partkey
				from
					part
				where
					p_name like 'green%'
			)
			and ps_availqty > (
				select
					0.5 * sum(l_quantity)
				from
					lineitem
				where
					l_partkey = ps_partkey
					and l_suppkey = ps_suppkey
					and l_shipdate >= '1993-01-01'
					and l_shipdate < date_add('1993-01-01', interval '1' year)
			)
	)
	and s_nationkey = n_nationkey
	and n_name = 'ALGERIA'
order by
	s_name;
`
	tpchQ21 = `
/*PLACEHOLDER*/ select
	s_name,
	count(*) as numwait
from
	supplier,
	lineitem l1,
	orders,
	nation
where
	s_suppkey = l1.l_suppkey
	and o_orderkey = l1.l_orderkey
	and o_orderstatus = 'F'
	and l1.l_receiptdate > l1.l_commitdate
	and exists (
		select
			*
		from
			lineitem l2
		where
			l2.l_orderkey = l1.l_orderkey
			and l2.l_suppkey <> l1.l_suppkey
	)
	and not exists (
		select
			*
		from
			lineitem l3
		where
			l3.l_orderkey = l1.l_orderkey
			and l3.l_suppkey <> l1.l_suppkey
			and l3.l_receiptdate > l3.l_commitdate
	)
	and s_nationkey = n_nationkey
	and n_name = 'EGYPT'
group by
	s_name
order by
	numwait desc,
	s_name
limit 100;
`
	tpchQ22 = `
/*PLACEHOLDER*/ select
	cntrycode,
	count(*) as numcust,
	sum(c_acctbal) as totacctbal
from
	(
		select
			substring(c_phone from 1 for 2) as cntrycode,
			c_acctbal
		from
			customer
		where
			substring(c_phone from 1 for 2) in
				('20', '40', '22', '30', '39', '42', '21')
			and c_acctbal > (
				select
					avg(c_acctbal)
				from
					customer
				where
					c_acctbal > 0.00
					and substring(c_phone from 1 for 2) in
						('20', '40', '22', '30', '39', '42', '21')
			)
			and not exists (
				select
					*
				from
					orders
				where
					o_custkey = c_custkey
			)
	) as custsale
group by
	cntrycode
order by
	cntrycode;
`
)

func createTPCHTables(tk *testkit.TestKit) {
	tk.MustExec(`use test`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS nation (
				N_NATIONKEY BIGINT NOT NULL,
				N_NAME CHAR(25) NOT NULL,
				N_REGIONKEY BIGINT NOT NULL,
				N_COMMENT VARCHAR(152),
				PRIMARY KEY (N_NATIONKEY))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS region (
				R_REGIONKEY BIGINT NOT NULL,
				R_NAME CHAR(25) NOT NULL,
				R_COMMENT VARCHAR(152),
				PRIMARY KEY (R_REGIONKEY))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS part (
	   P_PARTKEY BIGINT NOT NULL,
	   P_NAME VARCHAR(55) NOT NULL,
	   P_MFGR CHAR(25) NOT NULL,
	   P_BRAND CHAR(10) NOT NULL,
	   P_TYPE VARCHAR(25) NOT NULL,
	   P_SIZE BIGINT NOT NULL,
	   P_CONTAINER CHAR(10) NOT NULL,
	   P_RETAILPRICE DECIMAL(15, 2) NOT NULL,
	   P_COMMENT VARCHAR(23) NOT NULL,
	   PRIMARY KEY (P_PARTKEY)	)`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS supplier (
    S_SUPPKEY BIGINT NOT NULL,
    S_NAME CHAR(25) NOT NULL,
    S_ADDRESS VARCHAR(40) NOT NULL,
    S_NATIONKEY BIGINT NOT NULL,
    S_PHONE CHAR(15) NOT NULL,
    S_ACCTBAL DECIMAL(15, 2) NOT NULL,
    S_COMMENT VARCHAR(101) NOT NULL,
    PRIMARY KEY (S_SUPPKEY))`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS partsupp (
	  PS_PARTKEY BIGINT NOT NULL,
	  PS_SUPPKEY BIGINT NOT NULL,
	  PS_AVAILQTY BIGINT NOT NULL,
	  PS_SUPPLYCOST DECIMAL(15, 2) NOT NULL,
	  PS_COMMENT VARCHAR(199) NOT NULL,
	  PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)
	)`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS customer (
	  C_CUSTKEY BIGINT NOT NULL,
	  C_NAME VARCHAR(25) NOT NULL,
	  C_ADDRESS VARCHAR(40) NOT NULL,
	  C_NATIONKEY BIGINT NOT NULL,
	  C_PHONE CHAR(15) NOT NULL,
	  C_ACCTBAL DECIMAL(15, 2) NOT NULL,
	  C_MKTSEGMENT CHAR(10) NOT NULL,
	  C_COMMENT VARCHAR(117) NOT NULL,
	  PRIMARY KEY (C_CUSTKEY)
	)`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS orders (
	  O_ORDERKEY BIGINT NOT NULL,
	  O_CUSTKEY BIGINT NOT NULL,
	  O_ORDERSTATUS CHAR(1) NOT NULL,
	  O_TOTALPRICE DECIMAL(15, 2) NOT NULL,
	  O_ORDERDATE DATE NOT NULL,
	  O_ORDERPRIORITY CHAR(15) NOT NULL,
	  O_CLERK CHAR(15) NOT NULL,
	  O_SHIPPRIORITY BIGINT NOT NULL,
	  O_COMMENT VARCHAR(79) NOT NULL,
	  PRIMARY KEY (O_ORDERKEY)
	)`)
	tk.MustExec(`CREATE TABLE IF NOT EXISTS lineitem (
	  L_ORDERKEY BIGINT NOT NULL,
	  L_PARTKEY BIGINT NOT NULL,
	  L_SUPPKEY BIGINT NOT NULL,
	  L_LINENUMBER BIGINT NOT NULL,
	  L_QUANTITY DECIMAL(15, 2) NOT NULL,
	  L_EXTENDEDPRICE DECIMAL(15, 2) NOT NULL,
	  L_DISCOUNT DECIMAL(15, 2) NOT NULL,
	  L_TAX DECIMAL(15, 2) NOT NULL,
	  L_RETURNFLAG CHAR(1) NOT NULL,
	  L_LINESTATUS CHAR(1) NOT NULL,
	  L_SHIPDATE DATE NOT NULL,
	  L_COMMITDATE DATE NOT NULL,
	  L_RECEIPTDATE DATE NOT NULL,
	  L_SHIPINSTRUCT CHAR(25) NOT NULL,
	  L_SHIPMODE CHAR(10) NOT NULL,
	  L_COMMENT VARCHAR(44) NOT NULL,
	  PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)
	)`)
}

func TestIndexAdvisorTPCH1(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createTPCHTables(tk)

	querySet := s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ1, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ2, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ3, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ4, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ5, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ6, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ7, Frequency: 1})

	ctx := context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	r, err := indexadvisor.AdviseIndexes(ctx, tk.Session(), nil, nil)
	require.NoError(t, err) // no error and can get some recommendations
	require.True(t, len(r) > 0)
}

func TestIndexAdvisorTPCH2(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createTPCHTables(tk)

	querySet := s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ8, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ9, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ10, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ11, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ12, Frequency: 1})

	ctx := context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	r, err := indexadvisor.AdviseIndexes(ctx, tk.Session(), nil, nil)
	require.NoError(t, err) // no error and can get some recommendations
	require.True(t, len(r) > 0)
}

func TestIndexAdvisorTPCH3(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createTPCHTables(tk)

	querySet := s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ13, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ14, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ16, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ17, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ18, Frequency: 1})

	ctx := context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	r, err := indexadvisor.AdviseIndexes(ctx, tk.Session(), nil, nil)
	require.NoError(t, err) // no error and can get some recommendations
	require.True(t, len(r) > 0)
}

func TestIndexAdvisorTPCH4(t *testing.T) {
	store := testkit.CreateMockStore(t)
	tk := testkit.NewTestKit(t, store)
	createTPCHTables(tk)

	querySet := s.NewSet[indexadvisor.Query]()
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ19, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ20, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ21, Frequency: 1})
	querySet.Add(indexadvisor.Query{SchemaName: "test", Text: tpchQ22, Frequency: 1})

	ctx := context.WithValue(context.Background(), indexadvisor.TestKey("query_set"), querySet)
	r, err := indexadvisor.AdviseIndexes(ctx, tk.Session(), nil, nil)
	require.NoError(t, err) // no error and can get some recommendations
	require.True(t, len(r) > 0)
}
