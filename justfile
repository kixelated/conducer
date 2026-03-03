#!/usr/bin/env just --justfile

set quiet

default:
  just --list

check:
  #!/usr/bin/env bash
  set -euo pipefail

  cargo check --all-targets
  cargo clippy --all-targets -- -D warnings
  cargo fmt --check
  RUSTDOCFLAGS="-D warnings" cargo doc --no-deps

test:
  cargo test --all-targets

ci:
  #!/usr/bin/env bash
  set -euo pipefail

  just check
  just test
  cargo hack check --each-feature --no-dev-deps

fix:
  cargo clippy --fix --allow-staged --allow-dirty --all-targets
  cargo fmt
