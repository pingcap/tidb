# Proposal: Implement View Feature

- Author(s):     AndrewDi
- Last updated:  2018-10-24
- Discussion at: https://github.com/pingcap/tidb/issues/7974

## Abstract
This proposal proposes to implement VIEW feature in TiDB, aimed to make SQL easier to write.

## Background
A database view is a searchable object in a database that is defined by a query. Though a view doesn’t store data, some refer to a VIEW as "virtual tables", and you can query a view like you can query a table. A view can combine data from two or more tables, using joins, and also just contain a subset of information. This makes them convenient to abstract, or hide, complicated queries.

Below is a visual depiction of a view:
  ![AnatomyOfAview](imgs/view.png)
  
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
I implement the "`CREATE OR REPLACE`" command to create a view with the `SELECT` statement, implement "`DROP VIEW`" to drop a view, and "`SHOW FULL TABLES`" can also display the view list with different table types. The "`SHOW CREATE TABlE`" command can also regenerate a view create command.

## Rationale
`VIEW` is just a `TableInfo` object store with a `ViewInfo` struct, and only supports limited DDL operations.  
Let me describe more details about the view DDL operation:
1. Create a VIEW  
    1. Parse the create view statement and build a logical plan for select cause part. If any grammar error occurs, return errors to parser.   
    2. Examine view definer's privileges. Definer should own both `CREATE_VIEW` and base table's `SELECT` privileges.  
    3. Examine create view statement, If ViewFieldList cause part is empty, then we should generate view column names from SelectStmt cause. Otherwise check len(ViewFieldList) == len(Columns from SelectStmt). And then we save column names to `TableInfo.Columns`.
    4. Replace outermost layer wildcard with column name and column alias name in `SELECT` cause. For example:
       Origin SQL would like this:
       ```mysql
       SELECT * FROM 
         (
           SELECT t1.a,t2.b,sum(t1.b) FROM (SELECT * from t3 WHERE a=4) AS t1,t2
         ) AS TT
        ```  
       After SQL rewriter, it would be like following:
       ```mysql
       SELECT TT.a AS a,TT.b AS b2 ,TT.'sum(t1.b)' AS 'sum(t1.b)' FROM 
         (
           SELECT t1.a,t2.b,sum(t1.b) FROM (SELECT * from t3 WHERE a=4) AS t1,t2
         ) AS TT
       ``` 
    5. Test SELECT cause with following extra rules to see if this view is updatable. This part is difficult, following rules need more details.
        * There must be no duplicate view column names.
        * The view must contain all columns in the base table that do not have a default value.
        * The view columns must be simple column references. They must not be expressions or const expressions.  
    6. Store the view definition into following tables
        * `INFORMATION_SCHEMA.VIEWS` lists one row for each view, this need to be done later.
        * `INFORMATION_SCHEMA.VIEW_COLUMN_USAGE` lists one row for each column in a view including the base table of the column where possible   
        * `INFORMATION_SCHEMA.VIEW_TABLE_USAGE` lists one row for each table used in a view 
      
2. Drop a view  
  Implement DROP VIEW grammar, and delete the existing view tableinfo object.  
3. Select from a view  
  3.1 In function `func (b *PlanBuilder) buildDataSource(tn *ast.TableName) (LogicalPlan, error)`, if `tn *ast.TableName` is a view, then we build a select logicalplan from view's view_select string.   
4. Insert/Update into a view  
  This is view's advanced feature, we will discuss this later. 
5. Describe a view  
  Generate the view select logicalplan. If any error occurs, return the error to parser. Otherwise, return `SELECT` cause's column info.
6. Show Create Table
  Regenerate create view statement from `TableInfo`.

## Compatibility
It makes TiDB more compatible with MySQL.

## Implementation
We define a new struct named `ViewInfo`:
```go
type ViewInfo struct {
	Algorithm   AlgorithmType    `json:"view_algorithm"`
	Definer     string           `json:"view_definer"`  
	Security    SecurityType     `json:"view_security""`
	SelectStmt  string           `json:"view_select"`
	CheckOption CheckOptionType  `json:"view_checkoption"`
	isUpdatable bool             `json:"view_isUpdatable"`
}
```
Here is the detail explaination about viewinfo parameters:
* AlgorithmType  
    The view SQL AlGORITHM characteristic. The value is one of UNDEFINED、MERGE OR TEMPTABLE, if no ALGORITHM clause is present, UNDEFINED is the default algorithm.
    We will implement Algorithm=UNDEFINED only now.
* DEFINER  
    The account of the user who created the view, in 'user_name'@'host_name' format.
* SECURITYTYPE  
    The view SQL SECURITY characteristic. The value is one of DEFINER or INVOKER.
* CheckOptionType  
    The WITH CHECK OPTION clause can be given for an updatable view to prevent inserts to rows for which the WHERE clause in the select_statement is not true. It also prevents updates to rows for which the WHERE clause is true but the update would cause it to be not true (in other words, it prevents visible rows from being updated to nonvisible rows).  
    In a WITH CHECK OPTION clause for an updatable view, the LOCAL and CASCADED keywords determine the scope of check testing when the view is defined in terms of another view. When neither keyword is given, the default is CASCADED.
* isUpdatable  
    This parameter mark if this view is updatable.
* SelectStmt
    This string is the select sql statement after sql rewriter.
    
We add `ViewInfo` struct point which named `View` to `TableInfo`. If `&View` is nil, then this tableinfo is a base table, else this tableinfo is a view. 

Following is the main implementation details:  
1. ast/ddl.go  
I do check both the view name and the SELECT statement, viewname is a tablename object.
2. planner/core/planbuilder.go  
The code executes SELECT and returns `expression.columns` within `plan.Schema()`.
3. ddl_api.go  
We convert `expression.columns` into `table.Column` and reuse function `buildTableInfo` to build view
4. logical_plan_builder.go  
Every time we make a `SELECT` statement within a view, modify the `buildDataSource` function to rewrite the view table to select `LogicalPlan`.

## SubTask Schedule
|Action  |Priority|Deadline|Notes|
| ------ | ------ | ------ |-----|
|`CREATE [OR REPLACE] VIEW view_name [(column_list)] AS select_statement`|P1|2019/01/15|后续的所有工作都依赖这项|
|Parser 添加解析 ViewAlgorithm|P1|2019/01/15|--|
|`SHOW TABLE STATUS`|P1|2019/01/30|--|
|`DROP VIEW viewname`|P1|2019/01/30|--|
|`SELECT … FROM VIEW`|P1|2019/03/10|--|
|Add some test cases for CreateView and Select … From View(port from MySQL test)|P1|2019/03/30|--|
|UPDATE VIEW|P2| |Difficult|
|INSERT VIEW|P2| |Difficult, 部分依赖 UPDATE VIEW(如检查 isInsertable 依赖 isUpdatable)|
|SHOW CREATE [VIEW &#124; TABLE]|P2| | |
|ALTER VIEW|P2| | |
|ALTER &#124; DROP TABLE 检查表名是否是一个 View|P2| | |
|Add test cases for Update &#124; Insert View|P2| | |
|添加INFORMATION_SCHEMA.VIEWS视图|P3| | |
|Parser 添加提取 ViewDefiner ViewSQLSecurity 至CreateViewStmt|P3| | |
|CREATE [OR REPLACE] VIEW [DEFINER = { user &#124; CURRENT_USER }] [SQL SECURITY { DEFINER &#124; INVOKER }] AS select_statement|P3| | |
|CREATE [OR REPLACE] VIEW [ALGORITHM = {TEMPTABLE}] AS select_statement|P3| | |
|CREATE [OR REPLACE] VIEW AS select_statement [WITH [CASCADED &#124; LOCAL] CHECK OPTION]|P3| | |


## Open issues (if applicable)
https://github.com/pingcap/tidb/issues/7974
