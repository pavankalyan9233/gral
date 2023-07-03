FROM rust:1.70.0 as builder
WORKDIR /usr/src/gral
COPY . .
RUN cargo install --path ./gral
RUN cargo install --path ./grupload
FROM debian:11-slim
RUN apt-get update && apt-get install -y curl jq bash && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/gral /usr/local/bin/gral
COPY --from=builder /usr/local/cargo/bin/grupload /usr/local/bin/grupload
CMD ["gral"]
