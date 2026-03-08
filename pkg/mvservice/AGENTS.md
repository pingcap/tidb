# AGENTS.md

## Developing Environment Tips

### Code Organization

In non-test files, replace time-related functions with `time_proxy` implementations.
Each test case must use `InstallMockTimeModuleForTest` to set up the mock time module when `time_proxy` is used.

## Testing

For mock/injected test code, consider using the `intest.InTest` framework. See `pkg/util/intest`.
