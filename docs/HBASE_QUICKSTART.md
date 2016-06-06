# TiDB on HBase


**TiDB only supports HBase >= 0.98.5**

1) Build && Install pingcap/themis coprocessor to HBase:

1. git clone https://github.com/pingcap/themis.git
2. cd themis && mvn clean package -DskipTests
4. cp themis-coprocessor/target/themis-coprocessor-1.0-SNAPSHOT-jar-with-dependencies.jar $HBASE_ROOT/lib
5. Add configurations for themis coprocessor in hbase-site.xml:

```
<property>
    <name>hbase.coprocessor.user.region.classes</name>
    <value>org.apache.hadoop.hbase.themis.cp.ThemisEndpoint,org.apache.hadoop.hbase.themis.cp.ThemisScanObserver,org.apache.hadoop.hbase.regionserver.ThemisRegionObserver</value>
</property>
<property>
    <name>hbase.coprocessor.master.classes</name>
    <value>org.apache.hadoop.hbase.master.ThemisMasterObserver</value>
</property>
```

and then restart HBase.


2) Build TiDB:

```
git clone https://github.com/pingcap/tidb.git $GOPATH/src/github.com/pingcap/tidb
cd $GOPATH/src/github.com/pingcap/tidb
make
```

Run command line interpreter:

```
make interpreter
cd interpreter && ./interpreter -store hbase -dbpath localhost/tidb -dbname test
```

Enjoy it.

```
Welcome to the TiDB.
Version:
Git Commit Hash: f37bd11d16c79a3db1cdb068ef7a6c872f682cda
UTC Build Time:  2015-12-07 08:10:25

tidb> create table t1(id int, name text, key id(id));
Query OK, 0 row affected (2.12 sec)
tidb> insert into t1 values(1, "hello");
Query OK, 1 row affected (0.01 sec)
tidb> insert into t1 values(2, "world");
Query OK, 1 row affected (0.00 sec)
tidb> select * from t1;
+----+-------+
| id | name  |
+----+-------+
| 1  | hello |
| 2  | world |
+----+-------+
2 rows in set (0.00 sec)
tidb>
```

Run TiDB server:

```
make server
cd tidb-server
./tidb-server -store=hbase -path="zkaddrs/hbaseTbl?tso=tsoType" -P=4000
DSN parameters:
zkaddrs is the address of zookeeper.
hbaseTbl is the table in hbase to store TiDB data.
tsoaddr is the type of tso sever. Its value could be zk or local.
Here is an example of dsn:
./tidb-server -store=hbase -path="zk1,zk2/test?tso=zk" -P=5000
```
