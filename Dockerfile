# syntax=docker/dockerfile:1

########## Builder ##########
# rust:bookworm основан на buildpack-deps:bookworm — в базе уже есть весь
# toolchain (gcc, make, libssl-dev, ca-certificates). C-зависимости не
# используются, поэтому apt-пакеты не нужны вовсе.
FROM rust:bookworm AS builder

ENV RUSTFLAGS="-C target-cpu=broadwell"

WORKDIR /app

# Кэширование зависимостей: собираем пустой проект с реальными манифестами
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && rm -rf src

# Настоящий исходный код
COPY src ./src
RUN touch src/main.rs \
    && cargo build --release \
    && strip /app/target/release/kcs-monitor

########## Runner ##########
FROM gcr.io/distroless/cc-debian12 AS runner

WORKDIR /app

# CA-сертификаты копируем из builder (в distroless их нет)
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /app/target/release/kcs-monitor /app/kcs-monitor

# В distroless есть встроенный непривилегированный пользователь nonroot (uid 65532)
USER nonroot

ENTRYPOINT ["/app/kcs-monitor"]
