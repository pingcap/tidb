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
`CREATE OR REPLACE VIEW PopularBooks AS SELECT ISBN, Title, Author,PublishDate FROM Books WHERE IsPopular=1`  

Once a view is created, you can use it as you query any table in a `SELECT` statement. For example, to list all the popular book titles ordered by an author, you could write:  
`SELECT Author, Title FROM PopularBooks ORDER BY Author`  

In general you can use any of the SELECT clauses, such as GROUP BY, in a select statement containing a view.

## Proposal
I implement the "`CREATE OR REPLACE`" command to create a view with the `SELECT` statement. Currently, I reuse "`DROP TABLE`" to drop a view, and "`SHOW FULL TABLES`" can also display the view list with different table types. The "`SHOW CREATE TABlE`" command can also regenerate a view create command.

## Rationale
`VIEW` is just a tableinfo object store with a `SELECT` statement, and only supports two types of DDL operations, which are "`CREATE OR REPLACE VIEW`" and "`DROP`". Currently, view can only be used within the `SELECT` statement.  
Let me describe more details about the view DDL operation:
1. Create a VIEW  
  1.1 Parse the create view statement and build a logical plan for select cause part. If any grammar error occurs, return errors to parser.   
  1.2 Examine view definer's privileges. Definer should own both `CREATE_VIEW` and base table's `SELECT` privileges.  
  1.3 If `ViewFieldList` cause is empty, then we should generate view column name from `SelectStmt` cause.  
  1.4 Replace any wildcard with column name in `SELECT` cause.  
  1.5 Test `SELECT` cause with some rules to see if this view is updateable.  
  1.6 Store the view definition into following tables
     * `INFORMATION_SCHEMA.VIEW_COLUMN_USAGE` lists one row for each column in a view including the base table of the column where possible 
     * `INFORMATION_SCHEMA.VIEW_TABLE_USAGE` lists one row for each table used in a view 
     * `INFORMATION_SCHEMA.VIEWS` lists one row for each view
      
2. Drop a view  
  Implement DROP VIEW grammar, and delete the existing view tableinfo object.  
3. Select from a view  
  3.1 In function `func (b *PlanBuilder) buildDataSource(tn *ast.TableName) (LogicalPlan, error)`, if `tn *ast.TableName` is a view, then we build a select logicalplan from view's view_select string.   
4. Insert/Update into a view  
  I do not have any idea now, and will implement later.   
5. Describe a view  
  Generate the view select logicalplan. If any error occurs, return the error to parser. Otherwise, return `SELECT` cause's column info.

## Compatibility
It makes TiDB more compatible with MySQL.

## Implementation
First, we need to introduce `TableType` for `TableInfo`. Currently, I define two kinds of tables, which are `BaseTable` and `View`. By default if we use the "`CREATE TABLE`" command to create a table, TiDB creates a BaseTable `TableInfo`; if we use the "`CREATE VIEW`" command to create a table, TiDB creates a View `TableInfo`.

In order to store the VIEW's select text, we also need to add attribute `ViewSelectStmt string json:"view_select_stmt"` store select statement.

Following is the main implementation details:  
1. ast/ddl.go  
I do check both the view name and the SELECT statement, viewname is a tablename object.
2. planner/core/planbuilder.go  
The code executes SELECT and returns `expression.columns` within `plan.Schema()`.
3. ddl_api.go  
We convert `expression.columns` into `table.Column` and reuse function `buildTableInfo` to build view
4. logical_plan_builder.go  
Every time we make a `SELECT` statement within a view, modify the `buildDataSource` function to rewrite the view table to select `LogicalPlan`.

## Open issues (if applicable)
https://github.com/pingcap/tidb/issues/7974
