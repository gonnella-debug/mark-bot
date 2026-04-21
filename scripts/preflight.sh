#!/usr/bin/env bash
set -e
cd "$(dirname "$0")/.."

echo "  [preflight] AST parse…"
python3 -c "import ast; ast.parse(open('mark_bot_final.py').read())"
python3 -c "import ast; ast.parse(open('content_brain.py').read())"
python3 -c "import ast; ast.parse(open('renderer.py').read())"

echo "  [preflight] import check (mark_bot_final)…"
python3 -c "
import os
os.environ.setdefault('TELEGRAM_BOT_TOKEN', '')
os.environ.setdefault('CLAUDE_API_KEY', '')
os.environ.setdefault('META_SYSTEM_TOKEN', '')
import importlib.util
spec = importlib.util.spec_from_file_location('mb', 'mark_bot_final.py')
mod = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(mod)
except SystemExit:
    pass
print('    imports resolved cleanly')
"

echo "  [preflight] smoke tests…"
if [ -f scripts/smoke.py ]; then
  python3 scripts/smoke.py
fi

echo "  [preflight] OK ✓"
