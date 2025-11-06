#!/usr/bin/env bash
set -euo pipefail

cd /var/www/bzr-backend

if [[ -z "${NVM_DIR:-}" && -d "$HOME/.nvm" ]]; then
  export NVM_DIR="$HOME/.nvm"
fi

if [[ -n "${NVM_DIR:-}" && -s "$NVM_DIR/nvm.sh" ]]; then
  # shellcheck disable=SC1090
  . "$NVM_DIR/nvm.sh"
fi

if command -v nvm >/dev/null 2>&1; then
  if [[ -n "${NODE_VERSION:-}" ]]; then
    nvm use "${NODE_VERSION}" >/dev/null 2>&1 || nvm install "${NODE_VERSION}" >/dev/null 2>&1 || true
  elif [[ -f ".nvmrc" ]]; then
    nvm use >/dev/null 2>&1 || true
  fi
fi

export PATH="/usr/local/bin:/usr/bin:/bin:${PATH:-}"

exec node server.js
