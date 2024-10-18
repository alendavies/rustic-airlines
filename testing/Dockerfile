# Usa una imagen de Rust como compilador
FROM rust:latest AS builder

# Establece el directorio de trabajo
WORKDIR /app

# Copia todos los archivos de configuración de Cargo y las dependencias locales
COPY Cargo.toml .
COPY Cargo.lock .

# Copia las carpetas de dependencias locales
COPY node ./node
COPY gossip-protocol ./gossip-protocol
COPY partitioner ./partitioner
COPY query-coordinator ./query-coordinator
COPY src ./src

# Compila el proyecto en modo release
RUN cargo build --release

# Usa una imagen más ligera para ejecutar el binario
FROM debian:buster-slim

# Copia el binario compilado desde el compilador al contenedor final
COPY --from=builder /app/target/release/rustic-airlines /usr/local/bin/rustic-airlines
# Usa una imagen de Rust como compilador
FROM rust:latest AS builder

# Establece el directorio de trabajo
WORKDIR /app


