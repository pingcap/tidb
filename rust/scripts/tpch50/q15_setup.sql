create or replace view revenue0 (supplier_no, total_revenue) as
select
	l_suppkey,
	sum(l_extendedprice * (1 - l_discount))
from
	lineitem
where
	l_shipdate >= '1997-07-01'
	and l_shipdate < date_add('1997-07-01', interval '3' month)
group by
	l_suppkey;
