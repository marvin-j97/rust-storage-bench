#
# Build stage
#
FROM rust:slim AS builder
WORKDIR /app

COPY . .

RUN cargo build --release

#
# Runner stage
#
FROM debian:stable-slim AS runner
WORKDIR /app

COPY --from=builder /app/target/release/worker /app/worker

ENTRYPOINT ["/app/worker"]
