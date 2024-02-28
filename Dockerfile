FROM rust:1.76.0 as builder
RUN apt update ; apt upgrade -y ; apt install -y protobuf-compiler
WORKDIR /usr/src/gral
COPY . .
RUN cargo install --path ./gral
RUN cargo install --path ./grupload
FROM debian:12
RUN apt-get update && apt-get install -y curl jq bash libssl-dev && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/gral /usr/local/bin/gral
COPY --from=builder /usr/local/cargo/bin/grupload /usr/local/bin/grupload
CMD ["gral"]
