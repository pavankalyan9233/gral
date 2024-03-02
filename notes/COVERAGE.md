set -xg CARGO_INCREMENTAL 0
set -xg RUSTFLAGS '-Cinstrument-coverage'
set -xg LLVM_PROFILE_FILE 'cargo-test-%p-%m.profraw'
cargo build

scripts/full_example.fish

rm -rf target/coverage
grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage/html
