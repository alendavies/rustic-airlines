FROM rust:latest

WORKDIR /app

# TODO: make it --release
CMD ["cargo", "run", "-p", "node_2"]
