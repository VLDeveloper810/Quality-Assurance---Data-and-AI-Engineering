#!/usr/bin/env bash
set -e

echo "=== Setting up project environment ==="

if ! command -v python3 >/dev/null 2>&1 && ! command -v python >/dev/null 2>&1; then
  echo "ERROR: Python 3 is not installed or not on PATH." >&2
  exit 1
fi

PYTHON_CMD=python3
if ! command -v python3 >/dev/null 2>&1; then
  PYTHON_CMD=python
fi

if [ ! -d ".venv" ]; then
  $PYTHON_CMD -m venv .venv
  echo "Created virtual environment at .venv"
else
  echo ".venv already exists, skipping creation."
fi

echo "Installing Python dependencies..."
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

if [ ! -f .env ]; then
  cp example.env .env
  echo "Created .env from example.env. Please update it with your AWS settings."
else
  echo ".env already exists, leaving it unchanged."
fi

mkdir -p iceberg_warehouse mlruns

echo "Setup complete. Activate the virtual environment with:"
echo "  source .venv/bin/activate"
echo "Then run: python trigger_pipeline.py"
