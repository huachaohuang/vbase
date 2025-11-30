cargo-test := `
    if [ -x "$(command -v cargo-nextest)" ]; then
        echo "cargo nextest run"
    else
        echo "cargo test"
    fi
`

cargo-miri := `
    if [ -x "$(command -v cargo-nextest)" ]; then
        echo "cargo +nightly miri nextest run"
    else
        echo "cargo +nightly miri test"
    fi
`

all: test miri shuttle

test:
    {{cargo-test}} -F test

miri:
    #!/usr/bin/env bash
    export MIRIFLAGS="-Zmiri-tree-borrows"
    {{cargo-miri}} -E 'not (test(database))'

shuttle:
    {{cargo-test}} shuttle -F shuttle --release

check:
    cargo clippy
    cargo +nightly fmt --check
    cargo +nightly udeps