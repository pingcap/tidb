## CI Image

Here are the Dockerfiles for the CI images.

- ```base``` is the base image with golang, development tools and so on.
- ```centos7_jenkins``` is the production image with CI environment tool in tidb repo. it is based on ```base```.
- ```.ci_bazel``` is the global default bazel config. it tell bazel where to get cache.
- ```parser_test``` is the image for running parser tests that require MySQL 8.x.

Note that `parser_test` requires `CAP_SYS_NICE` to run. Either use `docker --cap-add=sys_nice` or add `SYS_NICE` to the capabilities under the `securityContext` when using Kubernetes.
