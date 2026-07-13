# Input Audit

## 2026-07-11

User request (verbatim):

> 实现以上方案；不断用 e2e测试验证后迭代优化（只少10轮），每次可通过命令编译并部署环境 make  && tiup playground nightly --tiflash=0 --db.binpath=/Users/solotzg/Work/tidb/bin/tidb-server；

Interpretation:

- Start from the user-reset baseline commit `8be4bd0`.
- Replace the discarded digest/profile approach with execution-time feedback.
- Use repeated E2E evidence, with no fewer than ten rounds.
- Build the local TiDB binary and deploy it through the supplied TiUP command.
