FROM rockylinux:8

RUN dnf -y update && \
    dnf -y install dnf-plugins-core curl git wget tar which && \
    dnf -y install epel-release && \
    dnf config-manager --set-enabled powertools && \
    dnf -y install \
        cmake ninja-build python3 gcc gcc-c++ make \
        clang llvm llvm-devel lld \
        openssl openssl-devel openssl-libs \
        zlib zlib-devel \
        ccache \
        pkgconfig \
    && dnf clean all

RUN curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /workspace
