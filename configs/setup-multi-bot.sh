#!/bin/bash
# =============================================================================
#  setup-multi-bot.sh
#  Run this ONCE on EC2 to set up 5 parallel bot instances via systemd.
#
#  Usage:
#    chmod +x configs/setup-multi-bot.sh
#    sudo configs/setup-multi-bot.sh
#
#  What it does:
#    1. Creates runs/<profile>/ directory for each profile
#    2. Copies .env config into each run directory
#    3. Symlinks shared bootstrap cache (no duplicate API calls)
#    4. Creates systemd service: tradingbot-<profile>.service
#    5. Enables + starts all 5 services
# =============================================================================

BOT_DIR="/home/ec2-user/TradingBot"
PYTHON="$BOT_DIR/.venv/bin/python"
USER="ec2-user"

PROFILES=(max-profit bear-fighter aggressive contrarian balanced)

# ── Prompt for credentials once ──────────────────────────────────────────────
echo ""
echo "Enter your Groww API credentials (used for all 5 bots):"
read -p "GROWW_API_KEY: " API_KEY
read -sp "GROWW_SECRET: " SECRET
echo ""
SNS_ARN="arn:aws:sns:ap-south-1:729756086652:NotifySelft"

# ── Create run directories ────────────────────────────────────────────────────
for profile in "${PROFILES[@]}"; do
    RUN_DIR="$BOT_DIR/runs/$profile"
    mkdir -p "$RUN_DIR/logs"

    # Copy profile .env
    cp "$BOT_DIR/configs/.env.$profile" "$RUN_DIR/.env"

    # Inject real credentials
    sed -i "s/YOUR_API_KEY/$API_KEY/" "$RUN_DIR/.env"
    sed -i "s/YOUR_SECRET/$SECRET/" "$RUN_DIR/.env"
    sed -i "s|SNS_TOPIC_ARN=.*|SNS_TOPIC_ARN=$SNS_ARN|" "$RUN_DIR/.env"

    # Create per-instance cache dir (paper_ledger.json + score_history.json stay local)
    # Symlink only the heavy bootstrap data so nselib is not called 5 times
    mkdir -p "$RUN_DIR/cache"
    for shared in ohlcv fundamentals universe.json; do
        if [ ! -L "$RUN_DIR/cache/$shared" ]; then
            ln -sf "$BOT_DIR/cache/$shared" "$RUN_DIR/cache/$shared"
        fi
    done

    chown -R $USER:$USER "$RUN_DIR"
    echo "✓ Created runs/$profile/"
done

# ── Create systemd service for each profile ───────────────────────────────────
for profile in "${PROFILES[@]}"; do
    RUN_DIR="$BOT_DIR/runs/$profile"
    SERVICE="tradingbot-$profile"

    cat > "/etc/systemd/system/$SERVICE.service" <<EOF
[Unit]
Description=TradingBot — $profile profile
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$RUN_DIR
EnvironmentFile=$RUN_DIR/.env
ExecStart=$PYTHON $BOT_DIR/bot.py
Restart=on-failure
RestartSec=30
StandardOutput=append:$RUN_DIR/logs/bot-server.log
StandardError=append:$RUN_DIR/logs/bot-server.log

[Install]
WantedBy=multi-user.target
EOF

    echo "✓ Created /etc/systemd/system/$SERVICE.service"
done

# ── Stop the old single bot if running ───────────────────────────────────────
systemctl stop tradingbot 2>/dev/null && echo "✓ Stopped old tradingbot service"
systemctl disable tradingbot 2>/dev/null

# ── Enable + start all 5 ─────────────────────────────────────────────────────
systemctl daemon-reload

for profile in "${PROFILES[@]}"; do
    SERVICE="tradingbot-$profile"
    systemctl enable "$SERVICE"
    systemctl start "$SERVICE"
    sleep 5  # stagger starts — avoid all 5 hitting API simultaneously
    STATUS=$(systemctl is-active "$SERVICE")
    echo "✓ $SERVICE: $STATUS"
done

echo ""
echo "============================================================"
echo "All 5 bots running. Useful commands:"
echo ""
echo "  Check all:       sudo systemctl status 'tradingbot-*'"
echo "  Watch max-profit: sudo journalctl -u tradingbot-max-profit -f"
echo "  Stop all:        sudo systemctl stop 'tradingbot-*'"
echo "  Restart all:     sudo systemctl restart 'tradingbot-*'"
echo ""
echo "  Logs:            runs/<profile>/logs/bot-server.log"
echo "  Ledger:          runs/<profile>/cache/paper_ledger.json"
echo "============================================================"
