#!/bin/bash
set -euo pipefail

# Only run in Claude Code remote (web) sessions
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# ── Ensure Claude CLI is on PATH ─────────────────────────────────────────────
CLAUDE_CLI_DIR="/opt/node22/bin"
if [[ ":$PATH:" != *":$CLAUDE_CLI_DIR:"* ]]; then
  echo "export PATH=\"$CLAUDE_CLI_DIR:\$PATH\"" >> "$CLAUDE_ENV_FILE"
  export PATH="$CLAUDE_CLI_DIR:$PATH"
fi

# ── Git identity (required for committing) ───────────────────────────────────
if [ -z "$(git config --global user.name 2>/dev/null)" ]; then
  git config --global user.name "Claude"
fi
if [ -z "$(git config --global user.email 2>/dev/null)" ]; then
  git config --global user.email "noreply@anthropic.com"
fi

# ── Verify Python 3 is available ─────────────────────────────────────────────
python3 --version

# ── Install/upgrade ruff for linting (idempotent) ────────────────────────────
pip install --quiet --upgrade ruff

echo "Session start: environment ready."
echo "  python3: $(python3 --version)"
echo "  claude:  $(claude --version 2>/dev/null || echo 'not found')"
echo "  ruff:    $(ruff --version 2>/dev/null || echo 'not found')"
