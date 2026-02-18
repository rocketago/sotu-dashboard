#!/bin/bash
# auto_push.sh — commit and push fetch_data.py (and any other staged changes)
# Usage: ./auto_push.sh [optional commit message]
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

MSG="${1:-"chore: update fetch_data.py [$(date -u '+%Y-%m-%dT%H:%M:%SZ')]"}"

git add fetch_data.py political_data.json

if git diff --cached --quiet; then
  echo "Nothing staged — no commit needed."
  exit 0
fi

git commit -m "$MSG"

# Push to the current tracking branch, retrying on transient failures
for delay in 2 4 8 16; do
  git push && break || {
    echo "Push failed; retrying in ${delay}s…"
    sleep "$delay"
  }
done

echo "Done. Latest commit:"
git log -1 --oneline
