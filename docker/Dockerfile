FROM docker.io/rust:1.66-slim-buster as builder

WORKDIR /application

RUN apt-get update && \
    apt-get install -y libssl-dev openssl pkg-config

ADD Cargo.toml /application/Cargo.toml
ADD src /application/src

RUN cargo test
RUN cargo build --release
RUN mkdir -p /application/bin
RUN cp /application/target/release/mongodb-uploader /application/bin/

FROM docker.io/debian:buster-slim

ARG GIT_REFERENCE=HEAD
ARG VERSION

LABEL "org.opencontainers.image.source"="https://gitlab.com/driyca/mongodb-uploader"
LABEL "org.opencontainers.image.revision"="${GIT_REFERENCE}"
LABEL "org.opencontainers.image.version"="${VERSION}"
LABEL "com.driyca.entrypoint"="mongodb-uploader"
LABEL "com.driyca/healthz"="/health"

ENV RUST_LOG=info

WORKDIR /application

RUN apt-get update && \
    apt-get install -y ca-certificates openssl && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* && \
    update-ca-certificates

COPY --from=builder  /application/bin/* /usr/bin/

ENTRYPOINT [ "mongodb-uploader" ]
