# Proposal: SPM with parameters partition

- Author(s): [Zhuomin Liu](https://github.com/lzmhhh123) 
- Last updated:  Mar. 3, 2020
- Discussion at: 

## Abstract

The proposal aims to solve a series of problems of multiple SQL binding by partitioning one standardized SQL's bindings with its parameters.

## Background

Currently TiDB's SPM is a series of processing based on c SQL binding. Standardized SQL binding refers to replacing all predicate constants in a sentence with the wildcard "?" And adding various hints to this SQL. As the current SPM does not eliminate the bad binding mechanism, coupled with standardized predicates, there are many big differences in data reading, such as the following two SQL:

```sql
select * from t where t.a> = 1 and t.a <= 2;
select * from t where t.a> = -10 and t.a <= 10;
```

These two SQLs are the same after standardization, but the data in the table may be concentrated in the [1,10] interval, so the first SQL will be better returned to the table by index, and the second table will be more reasonable.

This is just a simple example. From this example, it can be seen that when there are many predicates, standardized SQL will have many different optimal plans according to the different predicates. Then after the plan evolution is started, it may produce There are a lot of baselines, which will cause the optimizer to build a variety of abstract syntax trees with hints when choosing the baseline to go through the optimization logic, which may become a bottleneck for the optimizer's overhead.On the other hand, too many baselines will make the choice of binding fall back to the choice of CBO, which is also not expected.

## Proposal

### Keys of the problem
    1. TiDB's current SPM has no elimination mechanism, and it is easy to generate a lot of baselines. It is possible that some baselines are rarely used as the data changes.
    2. Since we build bind based on standardized SQL, the pros and cons of the plan really depend on the specific value of the predicate.

### Design Overview

Refer to [Adaptive Cursors and SQL Plan Management in Oracle](oracle.com/technical-resources/articles/database/sql-11g-sqlplanmanagement.html), after discussion, we decide to partitioning the bindings by "single column rangeable" predicates.

#### "single column rangeable" predicates

TiDB optimizer uses [Conjunctive Normal Form](https://en.wikipedia.org/wiki/Conjunctive_normal_form) (hereinafter CNF) to divide `where` conditions. For example:

```sql
where t.a = 1 and 
      t.b >= 2 and 
      (t.c*2 == 1 or t.c is null)
```

Its CNF is $(`t.a=1`) \cap (`t.b>=2`) \cap (`t.c*2==1` \cup `t.c is null`)$. For each single condition, "single column rangeable" is like the format as **${column_name} ${compare_operator} constant**. In this example, the first and second conditions are "single column rangeable".

#### partition the bindings

At first, distinguish two kinds of bindings, *SQL binding* and *Parameter binding*. *SQL binding* has the same behaviour with the origin SQL bind. *Parameter binding* is especially used for SPM plan evolution. Here's a table for differences between them.

binding type   |   Priority    | way to create             | action scope
---------------|:------------- |:--------------------------|-------------------------------------------------
SQL bind       | first choice  | use SQL                   | one standardized SQL
Parameter bind | second choice | enable SPM plan evolution | one standardized SQL with some parameters' range

The parameters' range is the range of the value of the constant which in "single column rangeable" condition.

## Compatibility

Incompatible with older versions of **SQL Bind**.

## Implementation
- Create a new table for parameter bind bucket.
```sql
CREATE TABLE IF NOT EXISTS mysql.bind_info_parameter_bucket (
        original_sql text NOT NULL,
        bucket_id bigint(64) NOT NULL,
        count bigint(64) NOT NULL,
        lower_bound blob default NULL COMMENT "used to Parameter bind, similar with histogram bucket lower bound, composed of a series of constants",
        upper_bound blob NOT NULL COMMENT "used to Parameter bind, similar with histogram bucket upper bound, composed of a series of constants",
        unique key bucket(original_sql(1024), bucket_id)
)
```

- Add three columns to `mysql.bind_info`
    1. bind type (options are "SQL bind" and "Parameter bind")
    2. bucket_id (used to Parameter bind, which bucket belong to)
    3. execute_time (only updated by SPM plan evolution)

```sql
CREATE TABLE IF NOT EXISTS mysql.bind_info (
		original_sql text NOT NULL,
      	bind_sql text NOT NULL,
      	default_db text  NOT NULL,
		status text NOT NULL,
		create_time timestamp(3) NOT NULL,
		update_time timestamp(3) NOT NULL,
		charset text NOT NULL,
		collation text NOT NULL,
        bind_type tinyint NOT NULL COMMENT "the type of the bind, options are 0(SQL bind) and 1(Parameter bind)",
        bucket_id bigint(64) default NULL,
        execute_time float(64) default NULL,
		INDEX sql_index(original_sql(1024), default_db(1024)) COMMENT "accelerate the speed when add global binding query",
		INDEX time_index(update_time) COMMENT "accelerate the speed when querying with last update time"
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
```

- Add another parameter binding cache in TiDB

- Change the binding chosen logic
    1. Reserve the origin SQL bind chosen logic.
    2. If hits none SQL bind, the try to hit parameter bind.

- SPM plan evolution
    1. Only change the parameter bind, not affect SQL bind.
    2. Insert into the bucket like insert histogram bucket in statistics.
    3. Try to split the bucket if its count exceeds twice count of the smallest bucket.
    4. If the number of bind SQL for one bucket is more than 20 or another number, drop the binding with max execute_time.

## Open issues (if applicable)
