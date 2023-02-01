## CI Image

Here is the Dockerfile for the CI image.

- ```base``` is the base image with golang, development tools and so on.
- ```centos7_jenkins``` is the production image with CI environment tool in tidb repo. it is based on ```base```.
- ```.ci_bazel``` is the global default bazel config. it tell bazel where to get cache.
