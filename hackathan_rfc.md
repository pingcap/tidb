+ 作者：胡海峰(huhaifeng@pingcap.com)/李淳竹(lichunzhu@pingcap.com)/曹闯(2546768090@qq.com)
+ 项目进展：正在写 [demo](git@github.com:hackathon2021index/tidb.git)

# 项目介绍
    用 lightning 的方式来实现 索引相关 ddl: 生成 sst 文件然后 ingest 到 tikv.

# 背景&动机
    在表数据量巨大的情况下，建立索引的 ddl 很慢耗时较长。而 TiDB 虽然支持 `在线 DDL`，但是还是会耗时很久。但在实际场景中，我们往往会在优化大表读取时才会加索引，因此这会为我们带来极大的不便。

    研究 TiDB 代码后，可以发现 TiDB 在加索引时的回填数据操作大量使用事务写并重试，耗时较大。

    而 lightning local backend 模式可以快速导入数据并绕过事务层直接插入 kv 对。所以，该项目使用 lighting 导入索引功能来实现 tidb 的 ddl 中索引插入部分，从而极大的节约加索引时间，提升数据库效率。

# 项目设计
## 架构设计

原来的 `index ddl` 基本流程：
+ 修改表 meta 数据
+ 修改索引数据
+ finish

现在，原来的没有变化，只需要把 `修改索引数据` 这里 修改为 `lightning` 来完成就可以了。  
`修改索引数据` 其实也有如下步骤：
+ 将索引的各列数据取出来
+ 将 索引列数据，构造为 kv 保存到本地，主键数据还会进行冲突检测
+ ddl 完成的时候，将 sst 文件 ingest 到 tikv

该功能，主要涉及到 `tidb/ddl` / `table/tables` 相关组件

## 测试

- 功能验证
    - admin check
    - 跟 ddl 速度对比
    - 是否正常走索引
- 完备性验证
    - 正常加索引
    - 读时 加索引
    - 写时 加索引
    - 读写时 加索引
