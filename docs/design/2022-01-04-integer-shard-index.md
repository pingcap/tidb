# Integer shard index

- Tracking Issue: https://github.com/pingcap/tidb/issues/31040

Here is a table definition as bellow. The service writes data monotonically on the field `id2`, so the index `hotIndex` becomes a hot index which hinders the scalability of a TiDB cluster. The service executes point SELECT and point UPDATE by the `hotIndex`.

```sql
CREATE TABLE test(id1 INT PRIMARY, id2 INT, id3 INT, UNIQUE KEY hotIndex(id2));

INSERT INTO test values(val1,val2,val3);
UPDATE test SET id3 = val3 WHERE id2 = val2;
SELECT * FROM test WHERE id2 = val2;
```

The Integer shard index realized by this design doc can scatter `hotIndex` and ensure the service sqls still use the unique key `hotIndex`.  Here is the new table definition including shard index as bellow.

```sql
CREATE TABLE test(id1 INT PRIMARY, id2 INT, id3 INT, UNIQUE KEY hotIndex((tidb_shard(id2)),id2));
```

Do we need to modify service sqls above? No, we don't. Optimizer can recognize the shard index and add `tidb_shard(id2) = shardVal` to the `WHERE` clause automatically. Here is the final form of service sqls processed by optimizer as bellow. So, we don't need to modify service sqls and theses sqls can still use the unique index `hotIndex`

```sql
UPDATE test SET id3 = val3 WHERE tidb_shard(id2) = 8 AND id2 = 100;
SELECT * FROM test WHERE tidb_shard(id2) = 8 AND id2 = 100;
```



## Summary

Hot index reduces the write scalability of the TiDB cluster when the written data is monotonically increasing. We find out a proposal to solve the problem that is using an expression index to scatter the hot index, the new expression index is called a shard index.

The followers is the scalability of hot index and shard index.

| threads | tidb | tikv | tidb CPU | tikv CPU | duration(95)                 | QPS(K) | scalability | remark |
| ------- | ---- | ---- | -------- | -------- | ---------------------------- | ------ | ----------- | ------ |
| 800     | 3    | 3    | 4700     | 3350     | insert:  60.5     update:110 | 49.8   |             |        |
| 800     | 3    | 3    | 4400     | 3000     | insert:  61     update:111   | 49     |             |        |
| 800     | 3    | 4    | 5000     | 3500     | insert:  57     update:103   | 53     | 19.30%      | round1 |
| 800     | 3    | 4    | 5100     | 3600     | insert:  56     update:100   | 54     | 30.60%      | round2 |

​																					hot index scalability



| threads | tidb | tikv | tidb CPU | tikv CPU | duration                    | QPS  | scalability | remark |
| ------- | ---- | ---- | -------- | -------- | --------------------------- | ---- | ----------- | ------ |
| 800     | 3    | 3    | 5200     | 3100     | insert: 60     update:110   | 47.5 |             |        |
| 800     | 3    | 3    | 5200     | 3000     | insert:  60     update:110  | 47.2 |             |        |
| 800     | 3    | 4    | 6200     | 3800     | insert:  56.5     update:88 | 57.5 | 63.20%      | round1 |
| 800     | 3    | 4    | 6250     | 3800     | insert:  56.5     update:88 | 58.5 | 71.40%      | round2 |

​																					shard index scalability



Computing formula of scalability is  as bellow: `qps1` is the qps before expansion, `tikv1` is the tikv node count before expansion; `qps2` is the qps after expansion, `tikv2` is the tikv node count afterexpansion.

<img alt="CNF 200" src=".\imgs\scalability-formula.png" width="200pt"/>



## Detailed design

We made a new built-in function "tidb_shard()" as the expression prefix of shard index. There is only one integer parameter for tidb_shard function. What index is a shard index? It must meet the follower rules.

- It is an unique index
- The first field of index is tidb_shard(`a`) , the column `a` is integer type and is the second field of the index.

When querying based on the original index field, the TiDB optimizer should automatically recognize the shard index and add the expression field `tidb_shard(a) = val` to the query condition.

How to solve the hot index problem by shard index? For example, the definition of a table is `test(id int primary key clustered, a int, b int, unique key idx_a(a))` . The index `idx_a` is a hot index because of monotonically increasing data. We modify the definition of the index `idx_a` as follows `unique key idx_a((tidb_shard(a)),a)`. The written data of index `idx_a` is not monotonically increasing now. But here is a question, how the optimizer of TiDB chooses the index for original queries that are designed for `idx_a(a)`? e.g. `SELECT * FROM test WHERE a = 100`, it can't use the unique index 'idx_a((tidb_shard(a)),a' any more. We add a new rule for TiDB optimizer, if the access condition of a query contains all the fields of shard index except the first `tidb_shard(a)` expression, The optimizer adds the expression`tidb_shard(a) = xxx` to the access condition and the new query statement is like `SELECT * FROM test WHERE tidb_shard(a) = 8 AND a = 100`, so that the query can use the shard index.

### tidb_shard function

#### dsciription

Only support an integer parameter. It transforms an integer number to a value in the range [0, 255]. e.g. `tidb_shard(100)` is the value `8` .

#### principle

It calculate the hash value of the input parameter, and get the mod value by 256.

```go
func (b *builtinTidbShardSig) evalInt(row chunk.Row) (int64, bool, error) {
	shardKeyInt, isNull, err := b.args[0].EvalInt(b.ctx, row)
	if isNull || err != nil {
		return 0, true, err
	}
	var hashed uint64
	if hashed, err = vitess.HashUint64(uint64(shardKeyInt)); err != nil {
		return 0, true, err
	}
	hashed = hashed % tidbShardBucketCount
	return int64(hashed), false, nil
}
```



### Add expression to access condition by optimizer

The optimizer should add the expresion `tidb_shard(xxx) = yyy` to the access condition, so that a the query can use shard index.

The follower are the functions used to  add the expresion `tidb_shard(xxx) = yyy` to the access condition. They are Introduced in order of function call.



#### (ds *DataSource) PredicatePushDown

The entry point to add the `tidb_shard` expression is the function as bellow. We must add it before access condition was pushed down. Maybe this work should be done after sql parse, we do it here just for simplify the work.

```go

func (ds *DataSource) PredicatePushDown(predicates []expression.Expression, opt *logicalOptimizeOp) ([]expression.Expression, LogicalPlan) {
	predicates = expression.PropagateConstant(ds.ctx, predicates)
	predicates = DeleteTrueExprs(ds, predicates)
	// Add tidb_shard() prefix to the condtion for shard index in some scenarios
	// TODO: remove it to the place building logical plan
	predicates = ds.AddPrefix4ShardIndexes(ds.ctx, predicates)
	ds.allConds = predicates
	ds.pushedDownConds, predicates = expression.PushDownExprs(ds.ctx.GetSessionVars().StmtCtx, predicates, ds.ctx.GetClient(), kv.UnSpecified)
	appendDataSourcePredicatePushDownTraceStep(ds, opt)
	return predicates, ds
}
```

#### (ds *DataSource) AddPrefix4ShardIndexes

`AddPrefix4ShardIndexes` is the interface to add expression prefix for shard index. Pay attention, just for shard index!

```go
// AddPrefix4ShardIndexes Add expression prefix for shard index. e.g. an index is test.uk(tidb_shard(a), a).
// It transforms the sql "SELECT * FROM test WHERE a = 10" to
// "SELECT * FROM test WHERE tidb_shard(a) = val AND a = 10", val is the value of tidb_shard(10).
// It also transforms the sql "SELECT * FROM test WHERE a IN (10, 20, 30)" to
// "SELECT * FROM test WHERE tidb_shard(a) = val1 AND a = 10 OR tidb_shard(a) = val2 AND a = 20"
// @param[in] conds            the original condtion of this datasource
// @retval - the new condition after adding expression prefix
func (ds *DataSource) AddPrefix4ShardIndexes(sc sessionctx.Context, conds []expression.Expression) []expression.Expression {
	if !ds.containExprPrefixUk {
		return conds
	}

	var err error
	newConds := make([]expression.Expression, 0, len(conds))
	newConds = append(newConds, conds...)

	for _, path := range ds.possibleAccessPaths {
		if path.IsTablePath() || !path.IsUkShardIndex() {
			continue
		}
		newConds, err = ds.addExprPrefixCond(sc, path, newConds)
		if err != nil {
			logutil.BgLogger().Error("Add tidb_shard expression failed", zap.Error(err))
			return conds
		}
	}

	return newConds
}
```

#### (ds *DataSource) addExprPrefixCond

`addExprPrefixCond` is the wrapper for  `addExprPrefix4ShardIndex`

```go
func (ds *DataSource) addExprPrefixCond(sc sessionctx.Context, path *util.AccessPath,
	conds []expression.Expression) ([]expression.Expression, error) {
	IdxCols, IdxColLens :=
		expression.IndexInfo2PrefixCols(ds.Columns, ds.schema.Columns, path.Index)
	if len(IdxCols) == 0 {
		return conds, nil
	}

	adder := &exprPrefixAdder{
		sctx:      sc,
		OrigConds: conds,
		cols:      IdxCols,
		lengths:   IdxColLens,
	}

	return adder.addExprPrefix4ShardIndex()
}
```

#### (adder *exprPrefixAdder) addExprPrefix4ShardIndex

`addExprPrefix4ShardIndex` is the wrapper for  `addExprPrefix4DNFCond` and `addExprPrefix4CNFCond`. `addExprPrefix4DNFCond` is used to process `OR` expression, `addExprPrefix4CNFCond` is used to process `AND` expression.

```Go
// if original condition is a LogicOr expression, such as `WHERE a = 1 OR a = 10`,
// call the function AddExprPrefix4DNFCond to add prefix expression tidb_shard(a) = xxx for shard index.
// Otherwise, if the condition is  `WHERE a = 1`, `WHERE a = 1 AND b = 10`, `WHERE a IN (1, 2, 3)`......,
// call the function AddExprPrefix4CNFCond to add prefix expression for shard index.
func (adder *exprPrefixAdder) addExprPrefix4ShardIndex() ([]expression.Expression, error) {
	if len(adder.OrigConds) == 1 {
		if sf, ok := adder.OrigConds[0].(*expression.ScalarFunction); ok && sf.FuncName.L == ast.LogicOr {
			return adder.addExprPrefix4DNFCond(sf)
		}
	}
	return adder.addExprPrefix4CNFCond(adder.OrigConds)
}
```



#### (adder *exprPrefixAdder) addExprPrefix4DNFCond

- function declaration


```go
// add the prefix expression for DNF condition, e.g. `WHERE a = 1 OR a = 10`, ......
// The condition returned is `WHERE (tidb_shard(a) = 214 AND a = 1) OR (tidb_shard(a) = 142 AND a = 10)`
// @param[in] condition    the original condtion of the datasoure. e.g. `WHERE a = 1 OR a = 10`.
//                          condtion is `a = 1 OR a = 10`
// @return 	 -          the new condition after adding expression prefix. It's still a LogicOr expression.
func (adder *exprPrefixAdder) addExprPrefix4DNFCond(condition *expression.ScalarFunction) ([]expression.Expression, error) 
```

- Function Process
  - Extract evrey binary expression from `condition` to a expression.Expression slice `dnfItems`.
  - Make a new expression.Expression slice `newAccessItems` to store new condtions  after processing tidb_shard prefix.
  - Traverse every `item` in slice `dnfItems`, if it's a `LogicAnd` expression, extracts every binary expression from it to a slice `cnfItems` and calls function `addExprPrefix4CNFCond(cnfItems)`  to add tidb_shard prefix if need; if it's a `ast.EQ` or a `ast.In` expression, calls function `addExprPrefix4CNFCond([]Expression{item})` to add tidb_shard prefix if need.
  - Make a new `LogicOR` condition by `newAccessItems`, return it.



#### (adder *exprPrefixAdder) addExprPrefix4CNFCond

Call the function `ranger.AddExpr4EqAndInCondition` to process `AND` expression.

```go
// add the prefix expression for CNF condition, e.g. `WHERE a = 1`, `WHERE a = 1 AND b = 10`, ......
// @param[in] conds        the original condtion of the datasoure. e.g. `WHERE t1.a = 1 AND t1.b = 10 AND t2.a = 20`.
//                         if current datasource is `t1`, conds is {t1.a = 1, t1.b = 10}. if current datasource is
//                         `t2`, conds is {t2.a = 20}
// @return  -     the new condition after adding expression prefix
func (adder *exprPrefixAdder) addExprPrefix4CNFCond(conds []expression.Expression) ([]expression.Expression, error) {

	newCondtionds, err := ranger.AddExpr4EqAndInCondition(adder.sctx,
		conds, adder.cols)

	return newCondtionds, err
}
```

##### AddExpr4EqAndInCondition

The core function that adds `tidb_shard(x) = xxx`  to the accessCond.

- function declaration

```go
// AddExpr4EqAndInCondition add the `tidb_shard(x) = xxx` prefix
// Add tidb_shard() for EQ and IN function. e.g. input condition is `WHERE a = 1`,
// output condition is `WHERE tidb_shard(a) = 214 AND a = 1`. e.g. input condition
// is `WHERE a IN (1, ,2 ,3)`, output condition is `WHERE (tidb_shard(a) = 214 AND a = 1)
// OR (tidb_shard(a) = 143 AND a = 2) OR (tidb_shard(a) = 156 AND a = 3)`
// @param[in] conditions  the original condition to be processed
// @param[in] cols        the columns of shard index, such as [tidb_shard(a), a, ...]
// @param[in] lengths     the length for every column of shard index
// @retval - the new condition after adding tidb_shard() prefix
func AddExpr4EqAndInCondition(sctx sessionctx.Context, conditions []expression.Expression,
	cols []*expression.Column) ([]expression.Expression, error) 
```

- Function Process
  - Traverse every `cond` in  slice `conditions`, place the `cond` to a new slice `accesses` according to the index definition field order.
  - Traverse every  `cond` in slice `accesses`, if the `cond` is not a `ast.EQ` or `ast.In` expression,  do nothing and return, otherwise extract the column value from `cond` and store it to slice `columnValues`.
  - Call the function `NeedAddGcColumn4ShardIndex` to judge if necessary to add `tidb_shard` prefix for `conditions`, if not, do nothing and return; otherwise call function `AddGcColumnCond` to add  `tidb_shard` prefix for `conditions`. 



##### NeedAddGcColumn4ShardIndex

Check whether to add `tidb_shard(x) = xxx` to the access condition.

- function declaration

```go
// NeedAddGcColumn4ShardIndex check whether to add `tidb_shard(x) = xxx`
// @param[in] cols          the columns of shard index, such as [tidb_shard(a), a, ...]
// @param[in] accessCond    the condtions relative to the index and arranged by the index column order.
//                          e.g. the index is uk(tidb_shard(a), a, b) and the where clause is
//                          `WHERE b = 1 AND a = 2 AND c = 3`, the param accessCond is {a = 2, b = 1} that is
//                          only relative to uk's columns.
// @param[in] columnValues  the values of index columns in param accessCond. if accessCond is {a = 2, b = 1},
//                          columnValues is {2, 1}. if accessCond the "IN" function like `a IN (1, 2)`, columnValues
//                          is empty.
// @retval -  return true if it needs to addr tidb_shard() prefix, ohterwise return false
func NeedAddGcColumn4ShardIndex(
	cols []*expression.Column,
	accessCond []expression.Expression,
	columnValues []*valueInfo) bool 
```

- Function Process
  - The columns of shard index shoude be more than 2, like `(tidb_shard(a),a,...)`.
  - The first column of index must be a generated column and the expression must be `tidb_shard` built-in function.
  - The argument of  `tidb_shard` must be a column other than expression or else, and it must be the second column of the index.



##### AddGcColumnCond

`AddGcColumnCond` is the wrapper function for `AddGcColumn4EqCond` and `AddGcColumn4InCond`. 

```go
// AddGcColumnCond add the `tidb_shard(x) = xxx` to the condition
// @param[in] cols          the columns of shard index, such as [tidb_shard(a), a, ...]
// @param[in] accessCond    the condtions relative to the index and arranged by the index column order.
//                          e.g. the index is uk(tidb_shard(a), a, b) and the where clause is
//                          `WHERE b = 1 AND a = 2 AND c = 3`, the param accessCond is {a = 2, b = 1} that is
//                          only relative to uk's columns.
// @param[in] columnValues  the values of index columns in param accessCond. if accessCond is {a = 2, b = 1},
//                          columnValues is {2, 1}. if accessCond the "IN" function like `a IN (1, 2)`, columnValues
//                          is empty.
// @retval -  []expression.Expression   the new condtions after adding `tidb_shard() = xxx` prefix
//            error                     if error gernerated, return error
func AddGcColumnCond(sctx sessionctx.Context,
	cols []*expression.Column,
	accessesCond []expression.Expression,
	columnValues []*valueInfo) ([]expression.Expression, error) {

	if cond := accessesCond[1]; cond != nil {
		if f, ok := cond.(*expression.ScalarFunction); ok {
			switch f.FuncName.L {
			case ast.EQ:
				return AddGcColumn4EqCond(sctx, cols, accessesCond, columnValues)
			case ast.In:
				return AddGcColumn4InCond(sctx, cols, accessesCond)
			}
		}
	}

	return accessesCond, nil
}
```



##### AddGcColumn4EqCond

Add the `tidb_shard(x) = xxx` prefix for equal access condition.

- function declaration

```go
// AddGcColumn4EqCond add the `tidb_shard(x) = xxx` prefix for equal condition
// For param explanation, please refer to the function `AddGcColumnCond`.
// @retval -  []expression.Expression   the new condtions after adding `tidb_shard() = xxx` prefix
//            []*valueInfo              the values of every columns in the returned new condtions
//            error                     if error gernerated, return error
func AddGcColumn4EqCond(sctx sessionctx.Context,
	cols []*expression.Column,
	accessesCond []expression.Expression,
	columnValues []*valueInfo) ([]expression.Expression, error) 
```

- Function Process
  - Traverse every `ColumnValue` in slice `columnValues`, calculate the value of `tidb_shard` by `ColumnValue` and store it to the variable`evaluated`.
  - Make a new expression `tidb_shard(columnName) = evaluated` and store it to `accessesCond[0]`



##### AddGcColumn4InCond

 Add the `tidb_shard(x) = xxx` for `IN` condition.

- function declaration

```go
// AddGcColumn4InCond add the `tidb_shard(x) = xxx` for `IN` condition
// For param explanation, please refer to the function `AddGcColumnCond`.
// @retval -  []expression.Expression   the new condtions after adding `tidb_shard() = xxx` prefix
//            error                     if error gernerated, return error
func AddGcColumn4InCond(sctx sessionctx.Context,
	cols []*expression.Column,
	accessesCond []expression.Expression) ([]expression.Expression, error)
```

- Function Process

Traverse every `argument` from `In` expression, calculate the `value` of `tidb_shard` with `argument` and make a new expression `tidb_shard(columnName) = value`, then make a `LogicAnd` like `tidb_shard(a) = 8 AND a = 100`. When process the second or above argument , add the `LogicAnd` to the `LogicOR` expression, like `tidb_shard(a) = 8 AND a = 100 OR tidb_shard(a) = 8 AND a = 100`.



## Drawbacks

Using the shard index to replace the hot index my decrease a little of the performance when the TiDB cluster is small and the pressure of workload is not high, But it can increase the scalability obviously.



## Alternatives

We can use expression index directly other than as the prefix of a index. e.g. replace `index idx(a)` to `indx idx(expr(a))` , it seems more simpler. But  `expr(a)` and `(a)`  don't correspond one to one, the PointGet plan becomes IndexRangeScan and the selection operation `WEHRE a = xxx` must be executed in the tidb-server or tikv. So the performance may be dropped a lot.



## Unresolved questions

### Shard Index Only Support Unique Secondary Index

The shard index is only valid for unique secondary index, not for primary key or non-unique secondary index, and it must meet the follower rules.

- It is an unique index
- The first field of index is tidb_shard(`a`) , the column `a` is integer type and is the second field of the index.

### Non-equivalent Condition Can't Use Index

The proposal only adds expression `tidb_shard(?) = ?` for equivalent condition in `WHERE` clause. e.g. If there is a unique index `uk(a)` on table `A` , optimizer chooses index scan by `uk(a)` for query  ` SELECT * FROM A WHERE a > 100 `.The index definition is changed to `uk(tidb_shard(a), a)` by this proposal, optimizer doesn't choose index for the query above any more.

### `AND` Expression Mixing With `OR` Expression Can't Use Index

The proposal only adds expression `tidb_shard(?) = ?` for all "AND" expression or all "OR" expression or none of them in `WHERE` clause.  If "and" expression mixing with "or" expression in `WHERE` clause, it can't use index. e.g. If there is a unique index `uk(a)` on table `A` , optimizer chooses index scan by `uk(a)` for query  ` SELECT * FROM A WHERE ((a=100 and b = 100) or a = 200) and b = 300 `. The index definition is changed to `uk(tidb_shard(a), a)` by this proposal, optimizer doesn't choose index for the query above any more.

### Group By Can't Use Index

The proposal only adds expression `tidb_shard(?) = ?` for equivalent condition in `WHERE` clause. It does nothings for `GROUP BY ` clause. e.g.  If there is a unique index `uk(a)` on table `A` , optimizer chooses index scan by `uk(a)` for query ` SELECT SUM(a) FROM A GROUP BY a `. The index definition is changed to `uk(tidb_shard(a), a)` by this proposal, optimizer doesn't choose index for the query above any more and it needs to be grouped in tidb-server.

### Order By Can't Use Index

The proposal only adds expression `tidb_shard(?) = ?` for equivalent condition in `WHERE` clause. It does nothing for `ORDER BY ` clause. e.g.  If there is a unique index `uk(a)` on table `A` , optimizer chooses index scan by `uk(a)` for query ` SELECT a FROM A ORDER BY a `. The index definition is changed to `uk(tidb_shard(a), a)` by this proposal, optimizer doesn't choose index for the query above any more and it needs to be orderedin tidb-server.

### Join Condition Can't Use Index

The proposal only adds expression `tidb_shard(?) = ?` for equivalent condition in `WHERE` clause. It does nothing for the  `ON` clause. e.g. If there is a unique index `uk(a)` on table `A` , optimizer chooses index scan by `uk(a)` for query ` SELECT * FROM B JOIN A ON B.a = A.a`. The index definition is changed to `uk(tidb_shard(a), a)` by this proposal, optimizer doesn't choose index for the query above any more.

### Where Subquery Can't Use Index

The proposal only adds expression `tidb_shard(?) = ?` for equivalent condition in `WHERE` clause.  It does nothing for the  sub-query clause. e.g. If there is a unique index `uk(a)` on table `A` , optimizer chooses index scan by `uk(a)` for query `SELECT * FROM A WHERE A.a IN (SELECT B.a FROM B WHERE B.a > 100) `. The index definition is changed to `uk(tidb_shard(a), a)` by this proposal, optimizer doesn't choose index for the query above any more.

### Joint Index May Be Invalid

The proposal only adds expression `tidb_shard(?) = ?` for equivalent condition in `WHERE` clause. If the access condition in `WHERE` clause only use part of a joint index, this proposal may leads to index useless. e.g. If there is a unique index `uk(a)` on table `A` , optimizer chooses index scan by `uk(a, b)` for query ` SELECT * FROM A WHERE A.a = 10`. The index definition is changed to `uk(tidb_shard(a), a, b)` by this proposal, optimizer doesn't choose index for the query above any more.

### FastPlan May Be Invalid

It can't adds expression `tidb_shard(?) = ?` to access condition in the FastPlan process. So, the FastPlan process is invalid for shard index.  e.g. If there is a unique index `uk(a)` on table `A` , optimizer makes a PointGet plan by FastPlan process for query ` SELECT * FROM A WHERE A.a = 100`. The index definition is changed to `uk(tidb_shard(a), a)` by this proposal, FastPlan can't recognize it is a shard index field in `WHERE` clause.

### Shard Index Can't Use Prepare Plan Cache

If there is a generated column on a table, the queries about this table can't use prepare plan cache. Shard index uses the expression `tidb_shard(a)` as the first field, and it makes a generated column. So the table with shard index can't use prepare plan cache.
