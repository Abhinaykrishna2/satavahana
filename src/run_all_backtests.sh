#!/bin/bash
set -e
cd "$(dirname "$0")"

echo "=== Single-Leg Options Backtests (Rust) ==="
for file in ../data/*_options_ticks.csv; do
    base=$(basename "$file")
    if [[ "$base" == *"_trim.csv" ]]; then
        continue
    fi
    echo "--- $base ---"
    cargo run --bin backtest_options --release -- "$file" 2>/dev/null \
        | grep -E "Positions opened|Wins/Losses|Net P&L"
done

echo ""
echo "=== Multi-Leg Backtest (Rust, regime picker, Mon/Tue) ==="
cargo run --bin backtest_multileg --release -- --all 2>/dev/null
