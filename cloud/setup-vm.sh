#!/bin/bash
# ============================================================
#  setup-vm.sh
#  Einmaliges Setup auf einer frischen Debian/Ubuntu-VM.
#  Per SSH ausfuehren: bash setup-vm.sh
# ============================================================
set -euo pipefail

echo "==> System aktualisieren"
sudo apt-get update -y
sudo apt-get upgrade -y

echo "==> Docker + Docker Compose installieren"
sudo apt-get install -y ca-certificates curl gnupg git ufw

if ! command -v docker >/dev/null 2>&1; then
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/debian/gpg | \
        sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    sudo chmod a+r /etc/apt/keyrings/docker.gpg
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" | \
        sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
    sudo apt-get update -y
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io \
        docker-buildx-plugin docker-compose-plugin
    sudo usermod -aG docker "$USER"
fi

echo "==> Firewall: nur 80, 443, SSH"
sudo ufw allow OpenSSH
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp
sudo ufw --force enable

echo "==> App-Verzeichnis"
sudo mkdir -p /opt/all-in-one
sudo chown "$USER":"$USER" /opt/all-in-one

echo
echo "Fertig! Bitte einmal aus- und wieder einloggen, damit Docker"
echo "ohne sudo funktioniert. Danach:"
echo
echo "  cd /opt/all-in-one"
echo "  # Code per scp/git nach /opt/all-in-one bringen"
echo "  cd cloud"
echo "  cp .env.example .env"
echo "  nano .env       # Werte ergaenzen"
echo "  docker compose up -d --build"
echo
