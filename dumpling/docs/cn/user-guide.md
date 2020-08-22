# Dumpling 使用手册

[Dumpling](https://github.com/pingcap/dumpling) 是支持以 SQL 文本或者 CSV 格式将 MySQL/TiDB 数据导出的工具。

设计初衷是为了替代 [Mydumper](https://github.com/pingcap/mydumper), 所以基本用法可以参考 Mydumper,
当然在实现中没有完全照搬 Mydumper, 因此存在与 Mydumper 不同的用法。

下表罗列了一些主要参数

| 主要参数 |     |
| --------| --- |
| -B 或 --database | 导出指定数据库 |
| -T 或 --tables-list | 导出指定数据表 |
| -f 或 --filter | 导出能匹配模式的表，语法可参考 [table-filter](https://github.com/pingcap/tidb-tools/blob/master/pkg/table-filter/README.md)（只有英文版） |
| --case-sensitive | table-filter 是否大小写敏感，默认为 false 不敏感 |
| -h 或 --host| 链接节点地址(默认 "127.0.0.1")|
| -t 或 --threads | 备份并发线程数|
| -r 或 --rows |将 table 划分成 row 行数据，一般针对大表操作并发生成多个文件。|
| --loglevel | 日志级别 {debug,info,warn,error,dpanic,panic,fatal} (默认 "info") |
| -d 或 --no-data | 不导出数据, 适用于只导出 schema 场景 |
| --no-header | 导出 table csv 数据，不生成 header |
| -W 或 --no-views| 不导出 view, 默认 true |
| -m 或 --no-schemas | 不导出 schema , 只导出数据 |
| -s 或--statement-size | 控制 Insert Statement 的大小，单位 bytes |
| -F 或 --filesize | 将 table 数据划分出来的文件大小, 需指明单位 (如 `128B`, `64KiB`, `32MiB`, `1.5GiB`) |
| --filetype| 导出文件类型 csv/sql (默认 sql) |
| -o 或 --output | 设置导出文件路径 |
| --output-filename-template | 设置导出文件名模版，详情见下 |
| -S 或 --sql | 根据指定的 sql 导出数据，该指令不支持并发导出 |
| --consistency | flush: dump 前用 FTWRL <br> snapshot: 通过 tso 指定 dump 位置 <br> lock: 对需要 dump 的所有表执行 lock tables read <br> none: 不加锁 dump，无法保证一致性 <br> auto: MySQL flush, TiDB snapshot|
| --snapshot | snapshot tso, 只在 consistency=snapshot 下生效 |
| --where | 对备份的数据表通过 where 条件指定范围 |
| -p 或 --password | 链接密码 |
| -P 或 --port | 链接端口，默认 4000 |
| -u 或 --user | 默认 root |

更多具体用法可以使用 -h, --help 进行查看。

## Mydumper 相关参考

[Mydumper usage](https://github.com/maxbube/mydumper/blob/master/docs/mydumper_usage.rst)

[TiDB Mydumper 使用文档](https://pingcap.com/docs-cn/stable/reference/tools/mydumper/)

## Dumpling 下载链接

[nightly](https://download.pingcap.org/dumpling-nightly-linux-amd64.tar.gz)

## 导出文件名模版

`--output-filename-template` 参数指定了所有文件的命名方式（不含扩展名）。它使用 [Go 的 `text/template` 语法](https://golang.org/pkg/text/template/)。

模板可使用以下字段：

* `.DB` — 库名
* `.Table` — 表名、物件名称。
* `.Index` — 由 0 开始的序列号，代表当前导出的表中的哪一份文件

库和表名中可能包含 `/` 之类的特殊字符，而这些字符不能用在文件系统中。因此，Dumpling 提供了一个 `fn` 函数来对这些特殊字符进行百分号编码。它们是：

* U+0000 到 U+001F (控制字符)
* `/`、`\`、`<`、`>`、`:`、`"`、`*`、`?` (无效的 Windows 路径字符)
* `.` (库/表名分隔符)
* `-`，当出现在 `-schema` 字串里

例如，使用 `--output-filename-template '{{fn .Table}}.{{printf "%09d" .Index}}'` 后，Dumpling 会把表 `"db"."tbl:normal"` 导出到 `tbl%3Anormal.000000000.sql`、`tbl%3Anormal.000000001.sql` 等文件。

除数据文件外，Dumpling 还支持透过子模版自定义命名表结构文件的名称。默认的配置是：

| 模版名 | 默认内容 |
|------|---------|
| data | `{{fn .DB}}.{{fn .Table}}.{{.Index}}` |
| schema | `{{fn .DB}}-schema-create` |
| table | `{{fn .DB}}.{{fn .Table}}-schema` |
| event | `{{fn .DB}}.{{fn .Table}}-schema-post` |
| function | `{{fn .DB}}.{{fn .Table}}-schema-post` |
| procedure | `{{fn .DB}}.{{fn .Table}}-schema-post` |
| sequence | `{{fn .DB}}.{{fn .Table}}-schema-sequence` |
| trigger | `{{fn .DB}}.{{fn .Table}}-schema-triggers` |
| view | `{{fn .DB}}.{{fn .Table}}-schema-view` |

例如，使用 `--output-filename-template '{{define "table"}}{{fn .Table}}.$schema{{end}}{{define "data"}}{{fn .Table}}.{{printf "%09d" .Index}}{{end}}'`后，Dumpling 会把表 `"db"."tbl:normal"` 的结构写到 `tbl%3Anormal.$schema.sql`，以及把数据写到 `tbl%3Anormal.000000000.sql`。
