## Filter  

filter provides a library to filter replicate on schema/table by given rules

### Rules
rules like replication rules in MySQL, ref document:
hhttps://dev.mysql.com/doc/refman/8.0/en/replication-rules-db-options.html
https://dev.mysql.com/doc/refman/8.0/en/replication-rules-table-options.html

####   Priority and Key Points
* DoDBs > IgnoreDBs
  * which rules to use?
    * If there are some DoDB rules, only use DoDB Rules
    * Otherwise if there are some IgnoreDB rules, use IgnoreDB rules
    * tables that are not filtered out or there are empty DoDBs/IgnoreDBs rules would go to filter on DoTables/IgnoreTables rules
* DoTables > IgnoreTables
    * if there are DoTable Rules, but no one is matched, we would ignore corresponding table
