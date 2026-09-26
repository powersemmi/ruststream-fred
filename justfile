set shell := ["bash", "-eu", "-o", "pipefail", "-c"]
set dotenv-load := false

export PATH := env("HOME") + "/.cargo/bin:" + env("HOME") + "/.local/bin:" + env("PATH")

default: check

check:
    cargo fmt --all -- --check
    # The benchmark package is left out of the all-features legs on purpose: it is built with the
    # feature set a service ships, and the framework's harness feature is a compile error in it.
    # Its own leg follows.
    cargo clippy --workspace --exclude ruststream-fred-bench --all-targets --all-features -- -D warnings
    cargo clippy -p ruststream-fred-bench --all-targets -- -D warnings
    cargo check --workspace --exclude ruststream-fred-bench --all-targets --all-features
    cargo check --workspace --no-default-features

test:
    cargo test --workspace --all-features

brokers-up:
    docker compose -f docker-compose.test.yml up -d --wait

brokers-down:
    docker compose -f docker-compose.test.yml down -v

# Runs the suites against the whole compose stand: the plain standalone, the Redis 8.4 one, the
# password-protected one, the cluster and the sentinel set, at the addresses that file exposes.
# RUSTSTREAM_REQUIRE_LIVE turns a skipped live test into a failure, so a topology that does not run
# is reported instead of passing quietly.
test-brokers: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    REDIS_TEST_URL=redis://127.0.0.1:6379 \
    REDIS_84_TEST_URL=redis://127.0.0.1:6386 \
    REDIS_AUTH_TEST_URL=redis://127.0.0.1:6385 \
    REDIS_CLUSTER_TEST_URL=127.0.0.1:7000 \
    REDIS_SENTINEL_TEST_URL=127.0.0.1:26379 \
    RUSTSTREAM_REQUIRE_LIVE=1 \
        cargo test --workspace --all-features -- --test-threads=1

# What this crate costs over the `fred` client it wraps: each scenario run as a RustStream service
# and as a hand-written loop, against the standalone server in the compose stand. On demand only -
# it takes minutes and it wants the machine to itself. The page it feeds is docs/benchmarks.md.
bench *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    mkdir -p target
    # RUSTFLAGS is cleared so the numbers are not tied to this machine's CPU: a binary built with
    # `-C target-cpu=native` cannot be reproduced anywhere else.
    RUSTFLAGS="" REDIS_TEST_URL=redis://127.0.0.1:6379 \
    REDIS_CLUSTER_TEST_URL=127.0.0.1:7000 \
    REDIS_SENTINEL_TEST_URL=127.0.0.1:26379 \
    RUSTSTREAM_BENCH_OUT="$PWD/target/bench-paired.json" \
        cargo bench -p ruststream-fred-bench --bench paired {{ ARGS }}
    python3 scripts/bench_results.py target/bench-paired.json docs/benchmarks/results.json

# What this crate's own code costs per message, counted under valgrind: instructions through
# callgrind and allocations through DHAT, each scenario a service on the production broker against
# the standalone server of the compose stand. The counts cover the service's thread, fred's work on
# it included, and not the server. The page it feeds is the code table of docs/benchmarks.md.
# RUSTFLAGS is cleared because valgrind aborts on the instructions a recent CPU advertises. Needs
# valgrind and the runner the benches pin: cargo install --locked gungraun-runner --version =0.19.4
# Extra arguments reach the runner: `just bench-code --save-baseline=main` records a baseline,
# `just bench-code --baseline=main` compares against it.
bench-code *ARGS: brokers-up
    #!/usr/bin/env bash
    set -euo pipefail
    trap 'just brokers-down' EXIT
    mkdir -p target
    RUSTFLAGS="" REDIS_TEST_URL=redis://127.0.0.1:6379 \
        cargo bench -p ruststream-fred-bench --bench consume --bench reply --bench batch \
        -- --output-format=json {{ ARGS }} > target/bench-code.json
    python3 scripts/bench_results.py --code target/bench-code.json docs/benchmarks/results.json

fmt:
    cargo fmt --all

build:
    cargo build --workspace --release

security: deny zizmor

# Dependency-graph checks (advisories, licenses, duplicates, sources).
# Needs cargo-deny: cargo install cargo-deny --locked
deny:
    cargo deny check

zizmor:
    uvx zizmor .github/workflows

typo:
    uvx codespell

clean:
    cargo clean
    rm -rf dist wheels

ci: check test typo security
