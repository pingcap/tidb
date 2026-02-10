# AGENTS.md

## Developing Environment Tips

### Code Organization

非 test 文件中的 time 相关函数换成 time_proxy 的实现
每个 test case 需要使用 InstallMockTimeModuleForTest 来设置 mock time 模块

## Testing

模拟注入代码时可考虑 intest.InTest 框架，详见 pkg/util/intest
