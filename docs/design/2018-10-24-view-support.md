# Proposal: Implement View Feature

- Author(s):     AndrewDi
- Last updated:  2018-10-24
- Discussion at: https://github.com/pingcap/tidb/issues/7974

## Abstract
This proposal proposes to implement basic VIEW feature in TiDB, aimed to make SQL easier to write. VIEW's advanced feature would be considered later.

## Background
A database view is a searchable object in a database that is defined by a query. Though a view doesn’t store data, some refer to a VIEW as "virtual tables", and you can query a view like you can query a table. A view can combine data from two or more tables, using joins, and also just contain a subset of information. This makes them convenient to abstract, or hide, complicated queries.

Below is a visual depiction of a view:
  ![AnatomyOfAview](imgs/view.png)   
  reference from https://www.essentialsql.com/what-is-a-relational-database-view/
  
A view is created from a query using the "`CREATE OR REPLACE VIEW`" command. In the example below we are creating a PopularBooks view based on a query which selects all Books that have the IsPopular field checked. Following is the query:
```mysql
CREATE OR REPLACE 
  VIEW PopularBooks AS 
    SELECT ISBN, Title, Author,PublishDate FROM Books WHERE IsPopular=1
```  

Once a view is created, you can use it as you query any table in a `SELECT` statement. For example, to list all the popular book titles ordered by an author, you could write:  
```mysql
SELECT Author, Title FROM PopularBooks ORDER BY Author
```

In general you can use any of the SELECT clauses, such as GROUP BY, in a select statement containing a view.

## Proposal
This proposal is prepared to implement basic VIEW feature, which contains "CREATE OR REPLACE VIEW", "SELECT FROM VIEW", "DROP VIEW" and "SHOW TABLE STATUS". All other unimplemented feature will list as compatibility and discuss later.
and we mainly introduce new struct named `ViewInfo` to store view's metadata.

## Rationale
`VIEW` is just a `TableInfo` object store with a `ViewInfo` struct, and only supports limited DDL operations.  
Here is `ViewInfo`'s attributes and detail attribute explaination:
```
type ViewInfo struct {
	Algorithm   AlgorithmType    `json:"view_algorithm"`
	Definer     string           `json:"view_definer"`  
	Security    SecurityType     `json:"view_security""`
	SelectStmt  string           `json:"view_select"`
	CheckOption CheckOption      `json:"view_checkoption"`
	Cols        []string         `json:"view_cols"`
	IsUpdatable bool             `json:"view_isUpdatable"`
}
```
* AlgorithmType  ref https://dev.mysql.com/doc/refman/5.7/en/view-algorithms.html   
    The view SQL AlGORITHM characteristic. The value is one of UNDEFINED、MERGE OR TEMPTABLE, if no ALGORITHM clause is present, UNDEFINED is the default algorithm.
    We will implement Algorithm=UNDEFINED only now.
* Definer  ref https://dev.mysql.com/doc/refman/5.7/en/create-view.html  
    The account of the user who created the view, in 'user_name'@'host_name' format.
* SecurityType  ref https://dev.mysql.com/doc/refman/5.7/en/create-view.html  
    The view SQL SECURITY characteristic. The value is one of DEFINER or INVOKER.
* CheckOptionType  ref https://dev.mysql.com/doc/refman/5.7/en/view-check-option.html  
    The WITH CHECK OPTION clause can be given for an updatable view to prevent inserts to rows for which the WHERE clause in the select_statement is not true. It also prevents updates to rows for which the WHERE clause is true but the update would cause it to be not true (in other words, it prevents visible rows from being updated to nonvisible rows).  
    In a WITH CHECK OPTION clause for an updatable view, the LOCAL and CASCADED keywords determine the scope of check testing when the view is defined in terms of another view. When neither keyword is given, the default is CASCADED.
* IsUpdatable  ref https://dev.mysql.com/doc/refman/5.7/en/view-updatability.html  
    This parameter mark if this view is updatable.
* SelectStmt
    This string is the origin select sql statement.
* Cols
    This string array is the view's column alias names.
* TableInfo.Columns
    `TableInfo.Columns` only stores view's column origin names, if no alias name specific, it stores the same values as `ViewInfo.Cols`.

We add `ViewInfo` struct point which named `View` to `TableInfo`, if `TableInfo.ViewInfo` != nil, then this tableinfo is a base table, else this tableinfo is a view. 
 
Let me describe more details about the view DDL operation:
1. Create VIEW  
   This proposal only support following grammar to create view:
   ```
    CREATE
        [OR REPLACE]
        [ALGORITHM = {UNDEFINED | MERGE | TEMPTABLE}]
        [DEFINER = { user | CURRENT_USER }]
        [SQL SECURITY { DEFINER | INVOKER }]
        VIEW view_name [(column_list)]
        AS select_statement
        [WITH [CASCADED | LOCAL] CHECK OPTION]
    ```
    1. Parse the create view statement and build a logical plan for select cause part. If any grammar error occurs, return errors to parser.   
    2. Examine view definer's privileges. Definer should own both `CREATE_VIEW` and base table's `SELECT` privileges.  
    3. Examine create view statement, If ViewFieldList cause part is empty, then we should generate view column names from SelectStmt cause. Otherwise check len(ViewFieldList) == len(Columns from SelectStmt). And then we save column names to `TableInfo.Columns` .
2. Drop a view  
  Implement `DROP VIEW` grammar, and delete the existing view tableinfo object. This function should reuse `DROP TABLE` code logical
3. Select from a view  
  3.1 In function `func (b *PlanBuilder) buildDataSource(tn *ast.TableName) (LogicalPlan, error)`, if `tn *ast.TableName` is a view, then we build a select `LogicalPlan` from view's `SelectStmt` string. But this solution meet a problem,here is the example:  
    ```mysql
       create table t(a int,b int);
       create view v like select * from t;
       select * from t;
    ```
    Once we query from view `v`, database will rewrite view's `SelectStmt` from `select * from t` into <bold>`select a as a,b as b from t`</bold>
    
    ```mysql
       drop table t;
       create table t(c int,d int);
    ```
    If we rebuild table v from sql above and query view `v` again, database will rewrite view's `SelectStmt` from `select * from t` into <bold>`select c as c,d as d from t`</bold>
    So the problem is view's statement can be rewrite to different sql and generate different query set.
    In order to resolve the problem describe above, we build a `Projection` at the top of original select's `LogicalPlan`, just like we rewriter view's `SelectStmt` from `select * from t` into `select a as a,b as b from (select * from t)`.
    This is a temporary fix and we will implement TiDB to rewrite sql with replace all wildcard finally.
4. Show table status
  Modify `SHOW TABLE STATUS` function to support show view status, and we use this command to check if `CREATE VIEW` and `DROP VIEW` operation is successful. To reuse `SHOW TABLE STAUS` code logical is perferred.

## Compatibility
Add TiDB support basic view feature without affecting other existing functions, and makes TiDB more compatible with MySQL.

## Implementation
|Action  |Priority|Deadline|Notes|
| ------ | ------ | ------ |-----|
|`CREATE [OR REPLACE] VIEW view_name [(column_list)] AS select_statement`|P1|2019/01/15|This task must be done before any other tasks.|
|Add parser to parse ViewAlgorithm|P1|2019/01/15|--|
|`SHOW TABLE STATUS`|P1|2019/01/30|--|
|`DROP VIEW viewname`|P1|2019/01/30|--|
|`SELECT … FROM VIEW`|P1|2019/03/10|--|
|Add some test cases for CreateView and Select … From View(port from MySQL test)|P1|2019/03/30|--|
|UPDATE VIEW|P2| |Difficult|
|INSERT VIEW|P2| |Difficult, dependent on UPDATE VIEW)|
|Support CREATE_VIEW_PRIV check|P2| | |
|SHOW CREATE [VIEW &#124; TABLE]|P2| | |
|ALTER VIEW|P2| | |
|ALTER &#124; DROP TABLE Check if table is a View|P2| | |
|Add test cases for Update &#124; Insert View|P2| | |
|Add INFORMATION_SCHEMA.VIEWS view|P3| | |
|Parse ViewDefiner ViewSQLSecurity to CreateViewStmt|P3| | |
|CREATE [OR REPLACE] VIEW [DEFINER = { user &#124; CURRENT_USER }] [SQL SECURITY { DEFINER &#124; INVOKER }] AS select_statement|P3| | |
|CREATE [OR REPLACE] VIEW [ALGORITHM = {TEMPTABLE}] AS select_statement|P3| | |
|CREATE [OR REPLACE] VIEW AS select_statement [WITH [CASCADED &#124; LOCAL] CHECK OPTION]|P3| | |


## Open issues (if applicable)
https://github.com/pingcap/tidb/issues/7974
