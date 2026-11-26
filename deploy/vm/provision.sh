#!/usr/bin/env bash
set -euo pipefail

# Hardened Linux VM provisioning script for multi-tenant-webhook-service
APP_USER="webhooksvc"
BIN_PATH="/usr/local/bin/relay-server"
DATA_DIR="/var/lib/webhook-service"

if ! id -u "$APP_USER" >/dev/null 2>&1; then
  sudo useradd --system --no-create-home --shell /usr/sbin/nologin "$APP_USER"
fi

CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /tmp/relay-server ./cmd/server
sudo install -m 0755 /tmp/relay-server "$BIN_PATH"
sudo mkdir -p "$DATA_DIR" /etc/webhook-service
sudo chown -R "$APP_USER:$APP_USER" "$DATA_DIR"

sudo tee /etc/systemd/system/webhook-service.service >/dev/null <<EOF
[Unit]
Description=Relay Multi-Tenant Webhook Service
After=network.target

[Service]
User=$APP_USER
Group=$APP_USER
Environment=PORT=8080
Environment=DB_PATH=$DATA_DIR/webhooks.db
EnvironmentFile=-/etc/webhook-service/env
ExecStart=$BIN_PATH -workers=15
Restart=always
RestartSec=3
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=$DATA_DIR

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now webhook-service.service
