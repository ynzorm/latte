.PHONY: fmt
fmt:
	cargo fmt --all

.PHONY: fmt-check
fmt-check:
	cargo fmt --all -- --check

.PHONY: check
check:
	cargo check --all-targets

.PHONY: clippy
clippy:
	RUSTFLAGS="-Dwarnings --cfg tokio_unstable" cargo clippy --all-targets

.PHONY: test
test:
	cargo test

.PHONY: build
build:
	cargo build --examples --benches

.PHONY: clean
clean:
	cargo clean

.PHONY: docker-build
docker-build:
	docker build --target production -t scylladb/latte:latest --compress .

.PHONY: check-alternator
check-alternator:
	cargo check --all-targets --no-default-features --features alternator --bin latte-alternator

.PHONY: clippy-alternator
clippy-alternator:
	RUSTFLAGS="-Dwarnings --cfg tokio_unstable" cargo clippy --all-targets --no-default-features --features alternator --bin latte-alternator

.PHONY: test-alternator
test-alternator:
	cargo test --no-default-features --features alternator --bin latte-alternator

.PHONY: build-alternator
build-alternator:
	cargo build --examples --benches --no-default-features --features alternator --bin latte-alternator

.PHONY: check-alternator-new
check-alternator-new:
	cargo check --all-targets --no-default-features --features alternator-new --bin latte-alternator-new

.PHONY: clippy-alternator-new
clippy-alternator-new:
	RUSTFLAGS="-Dwarnings --cfg tokio_unstable" cargo clippy --all-targets --no-default-features --features alternator-new --bin latte-alternator-new

.PHONY: test-alternator-new
test-alternator-new:
	cargo test --no-default-features --features alternator-new --bin latte-alternator-new

.PHONY: build-alternator-new
build-alternator-new:
	cargo build --examples --benches --no-default-features --features alternator-new --bin latte-alternator-new