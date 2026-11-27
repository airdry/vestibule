# Production image for sentry-ratelimiter.
#
# The builder stage installs every native build dependency (Pingora pulls in
# aws-lc-rs and zlib-ng, both of which compile bundled C code through CMake).
# The final binary is linked against musl so the runtime stage can use the
# fully static distroless image.

FROM rust:1.100.0-trixie AS builder

# CMake builds aws-lc-sys and libz-ng-sys; Perl is used by parts of the
# aws-lc build; musl-tools provides the static C toolchain for the musl target.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends cmake musl-tools perl \
    && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl

ENV CARGO_INCREMENTAL=0 \
    CARGO_TERM_COLOR=always \
    CC_x86_64_unknown_linux_musl=musl-gcc \
    CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=musl-gcc

WORKDIR /build

# Compile third-party dependencies in their own layer so source-only changes
# do not invalidate the expensive native builds.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src \
    && printf 'fn main() {}\n' > src/main.rs \
    && cargo build --release --locked --target x86_64-unknown-linux-musl \
    && rm -rf src

# The build context excludes tests/ (see .dockerignore); only the application
# sources are copied into the image.
COPY src ./src

# Force the package targets to rebuild after the source copy while keeping the
# dependency artifacts from the previous layer.
RUN touch src/main.rs src/lib.rs \
    && cargo build --release --locked --target x86_64-unknown-linux-musl \
        --bin sentry-ratelimiter \
    && strip --strip-all \
        /build/target/x86_64-unknown-linux-musl/release/sentry-ratelimiter

# Create the default Ingestion Queue directory with nonroot ownership. The
# application runs with this as its working directory, so the default
# `queue.dir: queue` resolves to a writable path.
RUN mkdir -p /out/var/lib/sentry-ratelimiter \
    && touch /out/var/lib/sentry-ratelimiter/.keep

FROM gcr.io/distroless/static-debian13:nonroot

COPY --from=builder --chown=65532:65532 \
    /build/target/x86_64-unknown-linux-musl/release/sentry-ratelimiter \
    /usr/local/bin/sentry-ratelimiter
COPY --from=builder --chown=65532:65532 \
    /out/var/lib/sentry-ratelimiter \
    /var/lib/sentry-ratelimiter

# The default queue directory and the working directory for relative paths.
WORKDIR /var/lib/sentry-ratelimiter

USER nonroot:nonroot

# Data Plane and Admin Plane ports from the default configuration. Actual
# listeners are controlled by server.data_plane_addr / server.admin_addr.
EXPOSE 8080 8081

STOPSIGNAL SIGTERM

ENTRYPOINT ["/usr/local/bin/sentry-ratelimiter"]
CMD ["--config", "/etc/sentry-ratelimiter/config.yaml"]
