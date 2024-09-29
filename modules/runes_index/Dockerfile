FROM rust:1.76.0-slim-bookworm AS builder

RUN apt update
RUN apt install curl cmake build-essential git vim -y
RUN apt install libpq-dev libssl-dev pkg-config libssl-dev -y
WORKDIR /opt/ord-runes
COPY ord-runes/ .
RUN cargo build --release


FROM node:20-bookworm-slim

RUN apt update
RUN apt install libssl-dev -y
COPY --from=builder /opt/ord-runes/target/release/ord /usr/local/bin/ord-runes

WORKDIR /app
COPY *.js package.json ./
RUN npm install

CMD ["node", "index_runes.js"]