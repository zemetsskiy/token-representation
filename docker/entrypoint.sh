#!/bin/bash
set -e

PIPELINE=${PIPELINE:-both}

echo "============================================================"
echo "TOKEN REPRESENTATION WORKER - ${PIPELINE^^} PIPELINE"
echo "============================================================"
echo "Started at: $(date -u +%Y-%m-%d\ %H:%M:%S) UTC"
echo ""

if [ "$PIPELINE" = "solana" ]; then
    echo "SOLANA Schedule:"
    echo "  - sol_500_swaps_7_days   : every 10 min"
    echo "  - sol_1000_swaps_3_days  : daily at 00:00 UTC"
    echo "  - sol_100_swaps_30_days  : daily at 00:10 UTC"
    crontab /etc/cron.d/crontab-solana
    touch /app/logs/solana-500-swaps-7d.log /app/logs/solana-1000-swaps-3d.log /app/logs/solana-100-swaps-30d.log
    LOG_FILES="/var/log/cron.log /app/logs/solana-500-swaps-7d.log /app/logs/solana-1000-swaps-3d.log /app/logs/solana-100-swaps-30d.log"
elif [ "$PIPELINE" = "evm" ]; then
    echo "EVM Schedule (all chains: eth, base, bsc, polygon):"
    echo "  - evm_1000_swaps_3_days  : every 10 min (at :05, :15, ...)"
    echo "  - evm_500_swaps_7_days   : daily at 00:20 UTC"
    echo "  - evm_100_swaps_30_days  : daily at 00:40 UTC"
    crontab /etc/cron.d/crontab-evm
    touch /app/logs/evm-1000-swaps-3d.log /app/logs/evm-500-swaps-7d.log /app/logs/evm-100-swaps-30d.log
    LOG_FILES="/var/log/cron.log /app/logs/evm-1000-swaps-3d.log /app/logs/evm-500-swaps-7d.log /app/logs/evm-100-swaps-30d.log"
else
    echo "SOLANA Schedule:"
    echo "  - sol_500_swaps_7_days   : every 10 min"
    echo "  - sol_1000_swaps_3_days  : daily at 00:00 UTC"
    echo "  - sol_100_swaps_30_days  : daily at 00:10 UTC"
    echo ""
    echo "EVM Schedule (all chains: eth, base, bsc, polygon):"
    echo "  - evm_1000_swaps_3_days  : every 10 min (at :05, :15, ...)"
    echo "  - evm_500_swaps_7_days   : daily at 00:20 UTC"
    echo "  - evm_100_swaps_30_days  : daily at 00:40 UTC"
    crontab /etc/cron.d/crontab-both
    touch /app/logs/solana-500-swaps-7d.log /app/logs/solana-1000-swaps-3d.log /app/logs/solana-100-swaps-30d.log
    touch /app/logs/evm-1000-swaps-3d.log /app/logs/evm-500-swaps-7d.log /app/logs/evm-100-swaps-30d.log
    LOG_FILES="/var/log/cron.log /app/logs/*.log"
fi

echo ""
echo "Waiting for scheduled jobs..."
echo "============================================================"

# Export all env vars to /etc/environment so cron jobs can source them.
# Values are double-quoted to handle special characters (&, !, #, etc.)
python3 -c "
import os
with open('/etc/environment', 'w') as f:
    for k, v in os.environ.items():
        # Escape double quotes and backslashes in values
        escaped = v.replace('\\\\', '\\\\\\\\').replace('\"', '\\\\\"')
        f.write(f'export {k}=\"{escaped}\"\n')
"

cron

tail -F ${LOG_FILES}
