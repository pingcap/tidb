# 为什么要写设计文档（RFC）

设计文档展现了一个项目的实施方案，可以帮助阅读者更快了解项目的设计思路。

# 设计文档模板

- 作者：[@tiancaiamao](https://github.com/tiancaiamao) [@Hchenn](https://github.com/Hchenn/)
- 项目进展：
    - tidb https://github.com/pingcap/tidb/pull/31276
	- client-go https://github.com/tikv/client-go/compare/master...tiancaiamao:hackathon?expand=1
	- kvproto https://github.com/pingcap/kvproto/compare/master...tiancaiamao:hackathon?expand=1

# 项目介绍

字节跳动开源了他们的 RPC 框架 [KiteX](https://github.com/cloudwego/kitex)，可以支持 thrift 和 grpc。
将 TiDB 使用的标准 grpc 替换成 KiteX，期望获得性能提升。

# 背景&动机

TiDB 在网络这一层的性能可能并没有被完全挖掘出来。

在 2.2 升 2.3 的时候，我们从自己简单封装 TCP 协议，切换到了标准的 grpc，那一次直接性能下降了 30%。

后续我们修修补补，实现了 super batching(通过将多个消息合并成一条的方式，减少处理的消息数量从而降低 CPU 开销)，将性能拉回了 2~30%。

以及随着 grpc 库自身的各版本的升级，我们在网络这块的性能应该是被一步步地弥补了回来。

**但是目前尚不能确定，网络这块的性能是否还有一些可以继续挖掘的点**。

字节正好开源了他们的网络框架，是做过一些性能优化的，并且在内部已经被广泛应用了。
所以我们尝试将 TiDB 使用的标准的 grpc 替换成字节的，

如果能有性能提升，并且替换成功，这件事情对于 ByteDance 和 PingCAP，对于 TiDB 和 KiteX 两个开源项目，都是有相互宣传的效果的。

grpc 是一个黑盒，我们更难去推动一些优化。然而 KiteX 这边，我们也可以更加紧密地合作，未来对于网络层这块的性能方面，想象空间也是更大的。

# 项目设计

字节的 [KiteX](https://github.com/cloudwego/kitex) 上层是自己实现的 grpc 协议，底层是绕过了 Go 网络层的标准库，使用的 [netpoll](https://github.com/cloudwego/netpoll/)。

TiDB 对于网络层，使用了标准 grpc 的 streaming 协议和普通消息协议，分别应对 batch client 和 client。

网络栈从上往下看，可能的一些替换形式有这几种：

* KiteX 整体替换 grpc
* http2 层替换
* netpoll 层替换

如果 KiteX 特别完善，跟标准 grpc API 完全兼容的情况下，我们可以用第一种方式，整体替换，这种实现成本应该是最低的。

其次 grpc 是基于 http2 协议的，如果标准 grpc 支持一些特殊的 option 替换掉这一层，第二种方式也是一个可选项。

最后是很底层的 netpoll 替换，相当于将标准库的 net.Conn 替换成 netpoll 的 API，中间可能会需要一些封装和 API 的适配。
也是看标准 grpc 能否支持特殊的 option 做这一层的替换。

第二种，第三种方式，如果修改需要侵入 grpc 的标准代码，实现代价会比较高。优先看第一种方式。

实际操作中发现，KiteX 全局替换 grpc 会遇到一些问题：

* API 差异，要修改的代码不少：Marsha() 和 Size() 等方法没了；导出的函数名变化；包的路径变化引起的修改；
* 影响的 repo 实在太多：理想情况改 kvproto 仓库，其它保持不动，实际情况所有使用 grpc 的位置都要改，连 pd client 那边都需要改
* go.mod 版本依赖地狱：br pd grpc etcdio client-go.. 涉及到的 repo 多了之后各 repo 的 go.mod 处理很头疼
* 重复注册问题：如果 KiteX 生成的 proto 和标准 grpc 生成的 proto 的库同时被依赖，里面的协议注册就会被执行多次，然后报错

最后采用的方式是替换 batch client 一个地方，而消息的编码依然使用标准编码方式。

或者说，使用 KiteX 的 client，来发送标准 grpc 的消息。
这样改动的影响(代码量)就很可控。实际的修改将只改[这一处](https://github.com/tikv/client-go/compare/master...tiancaiamao:hackathon?expand=1#diff-289d0ab3e9da9a88db8dcf8ed7095af6756da3ea8da38212b352742a83d93ba2R457-R474)。

# RFC 保存路径

请将 RFC 提交到团队项目 GitHub 仓库的 Readme

# RFC 方案提交

确认好内容后，提交到：RFC 立项登记表 59
