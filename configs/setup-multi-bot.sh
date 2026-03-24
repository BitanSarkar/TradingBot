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
#    3. All 5 bots share the same cache/ (read-only bootstrap data)
#       Each bot writes its own paper_ledger_<profile>.json inside cache/
#    4. Creates systemd service: tradingbot-<profile>.service
#    5. Enables + starts all 5 services
# =============================================================================

BOT_DIR="/home/ec2-user/TradingBot"
PYTHON="$BOT_DIR/.venv/bin/python"
BOT_USER="ec2-user"

PROFILES=(max-profit bear-fighter aggressive contrarian balanced)

# ── Read credentials from the existing .env (no manual entry needed) ──────────
BASE_ENV="$BOT_DIR/.env"
if [ ! -f "$BASE_ENV" ]; then
    echo "ERROR: $BASE_ENV not found. Run this script from the TradingBot directory."
    exit 1
fi

# Extract credentials from existing .env
GROWW_API_KEY=$(grep -E "^GROWW_API_KEY=" "$BASE_ENV" | cut -d= -f2-)
GROWW_SECRET=$(grep  -E "^GROWW_SECRET="  "$BASE_ENV" | cut -d= -f2-)
SNS_TOPIC_ARN=$(grep -E "^SNS_TOPIC_ARN=" "$BASE_ENV" | cut -d= -f2-)

echo "✓ Credentials loaded from $BASE_ENV"
echo "  API key: ${GROWW_API_KEY:0:6}***"
echo "  SNS ARN: $SNS_TOPIC_ARN"
echo ""

# ── Create run directories ────────────────────────────────────────────────────
for profile in "${PROFILES[@]}"; do
    RUN_DIR="$BOT_DIR/runs/$profile"
    mkdir -p "$RUN_DIR/logs"

    # Start with base .env (gets all credentials + defaults)
    # Then append profile-specific overrides on top
    cp "$BASE_ENV" "$RUN_DIR/.env"
    echo "" >> "$RUN_DIR/.env"
    echo "# ── Profile: $profile overrides ──" >> "$RUN_DIR/.env"
    grep -v "^#" "$BOT_DIR/configs/.env.$profile" | grep -v "^$" >> "$RUN_DIR/.env"

    # Each bot writes its own ledger in ledgers/ (separate from cache/ — not rsynced)
    echo "PAPER_LEDGER_PATH=ledgers/paper_ledger_${profile}.json" >> "$RUN_DIR/.env"
    # Lock this run directory to its fixed profile (prevents auto-detection override)
    echo "STRATEGY_PROFILE_OVERRIDE=$profile" >> "$RUN_DIR/.env"
    echo "STRATEGY_AUTO_SELECT=false" >> "$RUN_DIR/.env"

    chown -R $BOT_USER:$BOT_USER "$RUN_DIR"
    echo "✓ Created runs/$profile/  (ledger → ledgers/paper_ledger_${profile}.json)"
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
User=$BOT_USER
WorkingDirectory=$BOT_DIR
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
    sleep 10  # stagger starts — avoid all 5 hitting Groww API simultaneously
    STATUS=$(systemctl is-active "$SERVICE")
    echo "✓ $SERVICE: $STATUS"
done

echo ""
echo "============================================================"
echo "All 5 bots running. Useful commands:"
echo ""
echo "  Check all:        sudo systemctl status 'tradingbot-*'"
echo "  Watch a profile:  sudo journalctl -u tradingbot-max-profit -f"
echo "  Stop all:         sudo systemctl stop 'tradingbot-*'"
echo "  Restart all:      sudo systemctl restart 'tradingbot-*'"
echo ""
echo "  Logs:    runs/<profile>/logs/bot-server.log"
echo "  Ledgers: ledgers/paper_ledger_<profile>.json"
echo ""
echo "  Compare P&L:"
echo "    for p in max-profit bear-fighter aggressive contrarian balanced; do"
echo "      echo -n \"\$p: \""
echo "      python3 -c \""
echo "import json,os; f='ledgers/paper_ledger_\$p.json'"
echo "d=json.load(open(f)) if os.path.exists(f) else {}"
echo "cash=d.get('cash',0); start=d.get('starting_balance',1000000)"
echo "trades=len([t for t in d.get('trades',[]) if t.get('action')=='SELL'])"
echo "print(f'cash=\u20b9{cash:,.0f}  return={(cash/start-1)*100:+.2f}%  closed_trades={trades}')\""
echo "    done"
echo "============================================================"
