#!/bin/bash
# =============================================================================
#  use-profile.sh — Switch the bot to a different .env profile
#
#  Usage:
#    ./configs/use-profile.sh bear-fighter      # switch to bear-fighter profile
#    ./configs/use-profile.sh list              # list all available profiles
#    ./configs/use-profile.sh current           # show currently active profile
#
#  What it does:
#    1. Copies configs/.env.<profile> to .env
#    2. Copies your real API keys from .env.keys (create this once, see below)
#    3. Shows a summary of the key settings for the chosen profile
#
#  One-time setup:
#    Create .env.keys with just your secrets (this file is gitignored):
#      echo "GROWW_API_KEY=your_real_key" > .env.keys
#      echo "GROWW_SECRET=your_real_secret" >> .env.keys
#      echo "SNS_TOPIC_ARN=arn:aws:sns:..." >> .env.keys
# =============================================================================

CONFIGS_DIR="$(dirname "$0")"
BOT_DIR="$(dirname "$CONFIGS_DIR")"
KEYS_FILE="$BOT_DIR/.env.keys"

list_profiles() {
    echo ""
    echo "Available profiles:"
    echo "──────────────────────────────────────────────────────────────────"
    printf "  %-22s  %s\n" "PROFILE" "PHILOSOPHY"
    echo "──────────────────────────────────────────────────────────────────"
    for f in "$CONFIGS_DIR"/.env.*; do
        name=$(basename "$f" | sed 's/\.env\.//')
        desc=$(grep "^#  PROFILE:" "$f" 2>/dev/null | head -1 | sed 's/#  PROFILE: //')
        if [ -z "$desc" ]; then
            desc=$(grep "^#  Philosophy:" "$f" 2>/dev/null | head -1 | cut -c 16-65)
        fi
        printf "  %-22s  %s\n" "$name" "$desc"
    done
    echo ""
}

show_current() {
    if [ -f "$BOT_DIR/.env" ]; then
        profile=$(grep "^# ACTIVE PROFILE:" "$BOT_DIR/.env" | sed 's/# ACTIVE PROFILE: //')
        if [ -n "$profile" ]; then
            echo "Active profile: $profile"
        else
            echo "Active profile: (unknown — .env was not set via use-profile.sh)"
        fi
    else
        echo "No .env found."
    fi
}

if [ "$1" = "list" ] || [ -z "$1" ]; then
    list_profiles
    exit 0
fi

if [ "$1" = "current" ]; then
    show_current
    exit 0
fi

PROFILE="$1"
PROFILE_FILE="$CONFIGS_DIR/.env.$PROFILE"

if [ ! -f "$PROFILE_FILE" ]; then
    echo "Error: Profile '$PROFILE' not found."
    echo "Run: ./configs/use-profile.sh list"
    exit 1
fi

# Copy profile to .env
cp "$PROFILE_FILE" "$BOT_DIR/.env"

# Inject real API keys if .env.keys exists
if [ -f "$KEYS_FILE" ]; then
    while IFS= read -r line; do
        key=$(echo "$line" | cut -d= -f1)
        if [ -n "$key" ]; then
            # Replace the placeholder line
            escaped=$(echo "$line" | sed 's/[\/&]/\\&/g')
            sed -i.bak "s/^${key}=.*/${escaped}/" "$BOT_DIR/.env"
        fi
    done < "$KEYS_FILE"
    rm -f "$BOT_DIR/.env.bak"
    echo "✓ API keys injected from .env.keys"
else
    echo "⚠  No .env.keys found — edit .env manually to add your API keys"
    echo "   Create: echo 'GROWW_API_KEY=...' > .env.keys"
fi

# Add active profile marker
echo "" >> "$BOT_DIR/.env"
echo "# ACTIVE PROFILE: $PROFILE" >> "$BOT_DIR/.env"

echo "✓ Switched to profile: $PROFILE"
echo ""

# Show key settings
echo "Key settings:"
grep -E "^(ENTRY_BULL_RATIO_MIN|ENTRY_MIN_QUALITY|SCORE_BUY_THRESHOLD|RISK_MAX_HOLDINGS|EXIT_RISK_REWARD_RATIO|EXIT_ATR_STOP_MULT|INTRADAY_PULSE_WEIGHT|BOT_POLL_INTERVAL_OPEN)" "$BOT_DIR/.env" | \
    awk -F= '{ printf "  %-35s = %s\n", $1, $2 }'

echo ""
echo "Restart the bot: sudo systemctl restart tradingbot"
