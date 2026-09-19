# syntax=docker/dockerfile:1
FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev openssl-dev openssl-libs-static pkgconfig
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY bosch-ca.pem ./
COPY src ./src
RUN cargo build --release && strip target/release/bosch-cam-proxy

FROM alpine:3.20
RUN apk add --no-cache ffmpeg openssl ca-certificates font-dejavu
COPY --from=builder /src/target/release/bosch-cam-proxy /usr/local/bin/bosch-cam-proxy
ENV TOKEN_STORE=/data/tokens.json
WORKDIR /data

EXPOSE 8554
ENTRYPOINT ["bosch-cam-proxy"]
