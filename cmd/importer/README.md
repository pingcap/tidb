## importer

importer is a tool for generating and inserting datas to database which is compatible with MySQL protocol, like MySQL, TiDB.

## How to use

```
Usage of importer:
  -D string
      set the database name (default "test")
  -L string
      log level: debug, info, warn, error, fatal (default "info")
  -P int
      set the database host port (default 3306)
  -b int
      insert batch commit count (default 1)
  -c int
      parallel worker count (default 1)
  -config string
      Config file
  -h string
      set the database host ip (default "127.0.0.1")
  -i string
      create index sql
  -n int
      total job count (default 1)
  -p string
      set the database password
  -t string
      create table sql
  -u string
      set the database user (default "root")
```

## Example

```
./importer -t "create table t(a int primary key, b double, c varchar(10), d date unique, e time unique, f timestamp unique, g date unique, h datetime unique, i year unique);" -i "create unique index u_b on t(b);" -c 1 -n 10 -P 4000
```

Or use config file.

```
./importer --config=config.toml
```

## Rules
Moreover, we have some interesting rules for column value generating, like:

### range
```
./importer -t "create table t(a int comment '[[range=1,10]]');" -P 4000 -c 1 -n 10
```
Then the table rows will be like this:
```
mysql> select * from t;
+------+
| a    |
+------+
|    5 |
|    6 |
|    9 |
|    5 |
|    3 |
|    3 |
|   10 |
|    9 |
|    3 |
|   10 |
+------+
10 rows in set (0.00 sec)
```
Support Type: 

tinyint | smallint | int | bigint | float | double | decimal | char | varchar | date | time | datetime | timestamp.


### step
```
./importer -t "create table t(a int unique comment '[[step=2]]');" -P 4000 -c 1 -n 10
```
Then the table rows will be like this:
```
mysql> select * from t;
+------+
| a    |
+------+
|    0 |
|    2 |
|    4 |
|    6 |
|    8 |
|   10 |
|   12 |
|   14 |
|   16 |
|   18 |
+------+
10 rows in set (0.00 sec)
```

Support Type [can only be used in unique index]: 

tinyint | smallint | int | bigint | float | double | decimal | date | time | datetime | timestamp.


### set
```
./importer -t "create table t(a int comment '[[set=1,2,3]]');" -P 4000 -c 1 -n 10
```
Then the table rows will be like this:
```
mysql> select * from t;
+------+
| a    |
+------+
|    3 |
|    3 |
|    3 |
|    2 |
|    1 |
|    3 |
|    3 |
|    2 |
|    1 |
|    1 |
+------+
10 rows in set (0.00 sec)
```
Support Type [can only be used in none unique index]: 

tinyint | smallint | int | bigint | float | double | decimal.

## License
Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.
