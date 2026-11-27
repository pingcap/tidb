# Proposal: Implement withRollup

- Author(s):     Mamiao
- Last updated:  2022-05-11

## Abstract

This proposal proposes to implement withRollup in TiDB, aimed to support the with rollup semantics 

## Background

Currently, TiDB did not support with rollup clause. The with rollup clause is commonly used to calculate the aggregates of hierarchical data, and it can improve the usability and performance in Business Intelligence(BI) scenarios which could avoid to run several aggregate queries to get another level aggregate result.

With rollup is an attribute of group by operator. Its implementation is closely combined with groupby calculation. According to the implementation framework of general operator, DB implementation of an operator is mainly composed of several parts: yy parser, cost, plan generation, optimizer, executor and  result set processing .

## Proposal

In a nutshell, we need to introduce the implement into several parts: 
introduce the withRollup semantics,
the implement of withRollup in mysql,
How to implement the withRollup in tidb

### Introduce withRollup Semantics
The WITH ROLLUP modifier adds extra rows to the resultset that represent super-aggregate summaries. The super-aggregated column is represented by a NULL value. Multiple aggregates over different columns will be added if there are multiple GROUP BY columns.

The following is an example of the aggregation function of rolup, which is used in the following application scenarios:

mysql> SELECT   SEX, TOWN, COUNT(*)
    -> FROM     EmployeeS
    -> GROUP BY SEX, TOWN WITH ROLLUP;
+-----+-----------+----------+
| SEX | TOWN      | COUNT(*) |
+-----+-----------+----------+
| F   | Eltham    |        2 |
| F   | Inglewood |        1 |
| F   | Midhurst  |        1 |
| F   | Plymouth  |        1 |
| F   | NULL      |        5 |
| M   | Douglas   |        1 |
| M   | Inglewood |        1 |
| M   | Stratford |        7 |
| M   | NULL      |        9 |
| NULL | NULL      |       14 |
+-----+-----------+----------+
10 rows in set (0.00 sec)

WithRollup uses groupby keys as the grouping set and implements hierarchical aggregation processing. In the above example, WithRollup semantics adds a distinct sex and town  group row, the column of TOWN is null, and finally implements the aggregation row of all months (SEX, TOWN is null)

### The implement of with Rollup in mysql
The first stage of MySQL optimizer operations of rollup in the optimization is to allocate the specific data structures(join:: optimize_rollup) required by rollup process. 

Provide rollup State which shows the status
According to the send_group_parts of groupby operator, to allocate rollup_null_result and  ref_pointer_arrays items corresponding to each groupkey parts.

The second stage is to initialize the data structure (join:: rollup_make_fields), including:
* rollup.field points to fields_list projection column
* build item_null_result according to group->item 
* Item_null_result->result_field points to item->tmp_table_field determine the result_field of item_null_result points to the temporary table. 

The aggregate output column for rollup points to the item (item_null_result) which is used to represent rollup aggregation. Copy the item corresponding to
the non-aggregate column, and mark the ref_pointer_arrays which is used to set replacement of join->ref_ptrs when sending query result.

fields_list----->Item_null_result    Item_null_result    Item_null_result    Item_sum   level 0
                   Item              Item_null_result    Item_null_result    Item_sum   level 1
                   Item                  Item            Item_null_result    Item_sum   level 2



The item (item::sum_func_item) of the aggregation function is also copied through sum_funcs and sum_funcs_end to determine which items need to be calculated
when reading a row data as belows:

                 Group             level2                 level 1           level 0
sum_funcs -----> Item_sum       new Item_sum            new Item_sum      new Item_sum    0*0
                                        |____________________|_________________   ||
                        ||===============|===================|================|====
                        ||                                   |                |
    sum_funcs_end ->sum_funcs_end[0] sum_funcs_end[1]  sum_funcs_end[2]  sum_funcs_end[3]

The index of sum_funcs_end indicates the level number of the rollup level which is used to merge the specify result to the final results according to the groupby
keys sequence. Such a structure design can facilitate the rollup level n the execution stage by the index of the sum_funcs_end. That is one function in different
rollup level.

After execution, the results of all levels rollup need to be returned (join:: rollup_send_data) or written to the temporary table (join:: rollup_write_data).

### The performance in mysql8.0 With Rollup implement
In MySQL 5.7, group by will sort the result set on the grouping expression. You can add each expression to ASC in "ascending order" if it is not specified.
Let us take an example in MySQL 5.7:
mysql-5.7> SELECT a, b, SUM(c) as SUM FROM t1 GROUP BY a,b WITH ROLLUP; 
+------+------+------+
| a    | b    | SUM  |
+------+------+------+
|  111 |   11 |   11 |
|  111 |   12 |   12 |
|  111 | NULL |   23 |
|  222 |   22 |   22 |
|  222 |   23 |   23 |
|  222 | NULL |   45 |
| NULL | NULL |   68 |
+------+------+------+
7 rows in set (0.01 sec)

mysql-5.7> SELECT a, b, SUM(c) as SUM FROM t1 GROUP BY a ASC , b DESC WITH ROLLUP;
+------+------+------+
| a    | b    | SUM  |
+------+------+------+
|  111 |   12 |   12 |
|  111 |   11 |   11 |
|  111 | NULL |   23 |
|  222 |   23 |   23 |
|  222 |   22 |   22 |
|  222 | NULL |   45 |
| NULL | NULL |   68 |
+------+------+------+
7 rows in set, 2 warnings (0.00 sec)
Rollup row does not participate in the calculation and arrangement of groupby after calculating the rows of goopby,
but 8.0 cancels the sorting semantics of group by. Therefore, when sorting is required, you need to specify the sorting
column and sorting order through orderby. At the same time, rollup aggregated rows participate in the sorting of orderby,
which will make the results more semantic, such as:
mysql> SELECT a, b, SUM(c) as SUM FROM t1 GROUP BY a, b WITH ROLLUP ORDER BY a,b;
+------+------+------+
| a    | b    | SUM  |
+------+------+------+
| NULL | NULL |   68 |
|  111 | NULL |   23 |
|  111 |   11 |   11 |
|  111 |   12 |   12 |
|  222 | NULL |   45 |
|  222 |   22 |   22 |
|  222 |   23 |   23 |
+------+------+------+
7 rows in set (0.00 sec)
 
mysql> SELECT a, b, SUM(c) as SUM FROM t1 GROUP BY a, b WITH ROLLUP ORDER BY a, b DESC;
+------+------+------+
| a    | b    | SUM  |
+------+------+------+
| NULL | NULL |   68 |
|  111 |   12 |   12 |
|  111 |   11 |   11 |
|  111 | NULL |   23 |
|  222 |   23 |   23 |
|  222 |   22 |   22 |
|  222 | NULL |   45 |
+------+------+------+
7 rows in set (0.01 sec)

Tidb is compatible with mysql5.7, so the design of tidb with rollup refers to mysql5 7 implementation.

### Implementation of tidb groupby operator
Tidb group by operator can be divided into two types of executors:
HashExecutor
StreamAggExecutor

During the executation process of hash aggregate, HashExecutor need to maintain a hash table. The key of hash
table is the group by column of aggregation calculation, and the value is the intermediate result such as sum and
count of aggregation function. 

The executation process of stream aggregate needs to ensure that the input data is in order according to the group by
column. In the calculation process, whenever the value of a new group is read or all data input is completed, So the
previous data can be returned to the previous stream immediately after the group is processed.

There are five modes depending on the execution mode:

|-----------------|--------------|--------------|
| AggFunctionMode | input        | output       |
|-----------------|--------------|--------------|
| CompleteMode  | origin data  | final result   |
| FinalMode          | partial data | final result|
| Partial1Mode     | origin data  | partial data|
| Partial2Mode     | partial data | partial data|
| DedupMode       | origin data  | origin data  |
 |----------------|-------------|-------------- |

For example:
select AVG (b) from t group by a 
By dividing the calculation stages, there can be a combination of many different calculation modes, such as:
1. The whole calculation process of AVG function has only one stage, orignalData-->completemode
2 The calculation process of AVG function is divided into two stages, orignalData-->partial1mode -- > finalmode
3 The calculation process of AVG function is divided into three stages, orignalData-->Partial1Mode --> Partial2Mode-->FinalMode
In addition, if the aggregation function needs to de duplicate the parameters orignalData-->DupMode-->Partial1Mode-->FinalMode

### The implement of tidb with rollup 
With rollup is an attribute of group by operator. Its implementation is closely combined with groupby calculation. According to the
implementation framework of general operator, DB implementation of an operator is mainly composed of several parts: yy parser, cost,
plan generation, optimizer, executor and  result set processing.

#### YACC parser
Tidb uses yyparse for lex parsing of SQL, the parser.y is the input, add withrollup case and assign ast.WithRollUp true.

#### COST
The factor of AGG execution process is determined by the predefine aggfuncfactor. As rollup is an aggfunc, it needs to be embedded in
the whole cost calculation framework. Since rollup processing is to perform aggregation processing at different levels according to the
data group and aggregation function, and the aggregation function processing of all groups is given in the last line, It should belong to
the additional cost addition_cost, which does not affect the original Aggfunc cost
in proportion to the coefficient, so aggfuncfactor [ast.Aggfuncrollup] = 1

addition_cost = GROUP_NDV * calculate_cost_constrants * AggFuncFactor
Notes: 
calculate_cost_constrants is the const parameter，which is the same as ROW_EVALUATE_COST in mysql.
For example:
SQL select c07, avg(c07) from t1 group by c07 with rollup
Assume calculate_cost_constrants is 0.2, 
addition_cost = NDV * calculate_cost_constrants * AggFuncFactor(AVG)

####Plan Generation
Tidb uses the cascades optimizer framework which is a  top down framework whose advantages are predictable intermediate process and limited
space exploration. For example, when solving the problem, repeated sub problems may be encountered, and the solution of the sub problems can
be recorded as to avoid repeated calculation.

####Optimizer
Rollup is an attribute of the group operator and is executed in the server layer. Firstly, Dooptimize makes the projection prune through prunecolumns,
generates the group set according to the logical plan according to the ideas of the cascades paper, and then converts according to the predefine
transformationrulebatches rules, to find the equivalent class according to memo group operators, and to execute the corresponding operator rules
OnTransForm.
When executing with rollup, the cost depends on the group NDV. Because there may be multiple group sets on different tikv, the secondary aggregation phase
on tidb server is required. The cost of rollup is not high in the overall calculation proportion. Therefore, it is planned to be implemented in the server layer,
so there is no need to modify/add transformation rules.
* Reuse pushaggdowngather process
In the cascades optimizer framework of tidb, planbuilder builds according to ast Stmt type build the plan of the corresponding statement:
PlanBuilder->build->BuildSelect -> buildAggregation->detectSelectAgg
1 detectSelectAgg:  To judge the ast.withrollup semantics, and add iswithrollup flag in PlanBuilder (see PlanBuilder. Optflag)
2. In build aggregation process if the isWithRollup flag is true, add withrollup aggfunc to the correlatedAggMapper function of PlanBuilder function, and use the args of groupby as the aggregation.AgggFuncDesc.Args of withrollup aggfunc args.

#### implementation process
FindBestPlan->onPhaseImplementation->implGroup->implGroupExpr->rule.OnImplement->(ImplHashAgg->OnImplement)->plannercore.PhysicalHashAgg->buildHashAgg
Generate with Rollup AggFunc
aggfuncs->build 
    Acoording to ast.WithRollUp to use buildWithRollUp() function to generate withRollup AggFunc
    1 Generate baseAggFunc
    2 According to args->GetType().EvalType() to generate the different data type agg process
    3 Since the WithRollup only calculate at the final stage, so just do not consider the aggFuncDesc.Mode.

#### WithRollup Execution
                            +------------
                            | Main Thread |
                            +------+------+
                                   ^
                                   |
                                   +
                              +-+-            +-+
                              | |    ......   | |  finalOutputCh
                              +++-            +-+
                               ^
                               |
                               +---------------+
                               |               |
                 +--------------+             +--------------+
                 | final worker |     ......  | final worker |
                 +------------+-+             +-+------------+
                              ^                 ^
                              |                 |
                             +-+  +-+  ......  +-+
                             | |  | |          | |
                             ...  ...          ...    partialOutputChs
                             | |  | |          | |
                             +++  +++          +++
                              ^    ^            ^
          +-+                 |    |            |
          | |        +--------o----+            |
 inputCh  +-+        |        +-----------------+---+
          | |        |                             
          ...    +---+------------+            +----+-----------+
          | |    | partial worker |   ......   | partial worker |
          +++    +--------------+-+            +-+--------------+/           |                     ^                ^
           |                     |                |
      +----v---------+          +++ +-+          +++
      | data fetcher | +------> | | | |  ......  | |   partialInputChs
      +--------------+          +-+ +-+          +-+
The mainly execution process :
1 To generate Executors: executorBuilder build plan to generate Executors
2 To open Executors as the plan sequence, if parallel, create partial workers and final workers according to the degree of parallelism, and create the data channels of partialworkers and finalworkers at the same time
3 To execute Executors using Next() function, it reads all data from source.
4 To save the results: during the processing of hashAgg, use partialResults to record the aggfunc calculation value of each groupkey


Refer to mysql8 0, combined with the existing tidb hashAgg framework, WithRollup can be embedded into Agg computing framework as aggfunc. Aggfunc processes each row of data by implementing the function of aggfunc interface as below:
type AggFunc interface {
    AllocPartialResult() (pr PartialResult, memDelta int64)
    ResetPartialResult(pr PartialResult)
    UpdatePartialResult(sctx sessionctx.Context, rowsInGroup []chunk.Row, pr PartialResult) (memDelta int64, err error)
    MergePartialResult(sctx sessionctx.Context, src, dst PartialResult) (memDelta int64, err error)
    AppendFinalResult2Chunk(sctx sessionctx.Context, pr PartialResult, chk *chunk.Chunk) error
}

If WithRollup is an aggfunc, the framework assigns a partialresult to each groupkey. For example, Groupby keys a, b, c
partialResult[0] stores Groupby a, b, c -> 1 row 
partialResult[1] stores Groupby a, b -> n row
partialResult[2] stores Groupby a -> n row
The result of n row should be recalculated by ResetPartialResult after each output merge

### EXPLAIN
TIDB group by explain:
mysql> explain select a from t group by a;
+--------------------------+---------+-----------+---------------+-------------------------------------------------------+
| id                       | estRows | task      | access object | operator info                                         |
+--------------------------+---------+-----------+---------------+-------------------------------------------------------+
| HashAgg_7                | 1.00    | root      |               | group by:test.t.a, funcs:firstrow(test.t.a)->test.t.a |
| └─TableReader_12         | 1.00    | root      |               | data:TableFullScan_11                                 |
|   └─TableFullScan_11     | 1.00    | cop[tikv] | table:t       | keep order:false, stats:pseudo                        |
+--------------------------+---------+-----------+---------------+-------------------------------------------------------+
3 rows in set (0.01 sec)
mysql8.0
mysql> explain select a from t1 group by a;
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+-----------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows | filtered | Extra           |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+-----------------+
|  1 | SIMPLE      | t1    | NULL       | ALL  | NULL          | NULL | NULL    | NULL |    1 |   100.00 | Using temporary |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+-----------------+
1 row in set, 1 warning (0.01 sec)

mysql> explain select a from t1 group by a with rollup;
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------+
| id | select_type | table | partitions | type | possible_keys | key  | key_len | ref  | rows | filtered | Extra          |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------+
|  1 | SIMPLE      | t1    | NULL       | ALL  | NULL          | NULL | NULL    | NULL |    1 |   100.00 | Using filesort |
+----+-------------+-------+------------+------+---------------+------+---------+------+------+----------+----------------+
1 row in set, 1 warning (0.00 sec)

It can be seen from the explain below that WithRollup in 8.0 is a flow aggregation after filesort. The groupby operator aggregates by temp table and sets explain flags during  optimization
explain_flags.set(ESC_BUFFER_RESULT, ESP_USING_TMPTABLE);
explain_flags.set(sort_order->src, ESP_USING_FILESORT);

The explain result of tidb is more informative. Rollup, as an aggfunc, will be displayed in the operator info column in the first row, and WithRollup has no corresponding projection column. Therefore, FuncS: rollup has no corresponding projection column after it:

HashAgg_ 7                | 1.00    | root      |               | group by:test. t.a, funcs:rollup(test.t.a), funcs:firstrow(test.t.a)->test. t.a

To implement this, we can add the correct WithRollup func in LogicalAggregation -> AggFuncs which will get the correct display in explaininfo.

###test set
Withrollup reuses mysql5.7 testsuit.


## Rationale

withRollup clause have been supported in a number of database systems. Without it, our users have to use some workarounds to get the same result, which is both inefficient and error-prone.


## Compatibility

This proposal has no effect on the compatibility.

## Open issues (if applicable)

https://github.com/pingcap/tidb/issues/4250
