# TiDB BR Lease Lock Context

This glossary defines project-specific language for BR lease-lock safety discussions.
It captures domain terms only, not implementation decisions.

## Language

**业务临界区安全**:
一个 holder 只有在能证明自己的 lease window 仍然有效时，才可以继续执行受该 lock 保护的业务写入或删除。若无法证明有效 lease window，holder 必须停止受保护的业务临界区。
_Avoid_: storage-protocol proof, zombie-free guarantee, availability guarantee

**物理 lock 实例**:
一次 acquire 创建并由一个 holder 持有的具体 lock 身份。不同 acquire 不复用同一个物理 lock 实例。
_Avoid_: logical lock family, stable lock path

**RemoteLock 终止流程**:
一个 `RemoteLock` 从持有状态结束到不再代表业务持锁状态的过程。正常释放和 lease lost 都是终止流程的不同结果。
_Avoid_: unlock-only path, lost cleanup side path

**终止结果**:
`RemoteLock` 在业务语义上的最终结果，表示它是正常释放还是已经丢锁。终止结果和 renewal goroutine 是否已经退出是不同概念。
_Avoid_: renewal goroutine state, stop signal

## Example Dialogue

Developer: "这次方案要不要保证对象存储里永远不会出现 delayed renewal zombie？"

Domain expert: "不用。我们的目标是业务临界区安全：一旦 holder 不能证明有效 lease window，它就不能继续写受保护数据。zombie 可以作为后续 cleanup 和可用性问题处理。"

Developer: "丢锁后主动删自己的 lock 会不会误删别人？"

Domain expert: "不会。删除的是 holder acquire 到的物理 lock 实例；后续 acquire 会使用不同的物理 lock 实例。"

Developer: "lost cleanup 要不要单独走一套流程？"

Domain expert: "不要。把正常释放和 lease lost 都纳入 RemoteLock 终止流程，用不同结果表达语义差异。"

Developer: "如果正在 Unlock，renewal 还能不能同时判定 lost？"

Domain expert: "终止流程采用 first terminal reason wins。正常释放先赢时，renewal 的取消只是 shutdown signal；lease lost 先赢时，后续 Unlock 只是观察到已经终止。"
