Last updated: 2022-05-24

## BR 的 Backup 和 Restore 流程梳理

本文主要梳理 Backup 和 Restore 的代码执行流程

### Backup 部分

![img](../resources/backup.png)

### Restore 部分

![img](../resources/restore.png)


其中 Restore 过程中的流水线传输过程如下：


![img](../resources/restore-pipeline.png)