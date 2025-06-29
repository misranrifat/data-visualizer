#!/bin/bash

set -e

if ! command -v python3 &> /dev/null; then
    exit 1
fi

if lsof -ti:5000 > /dev/null 2>&1; then
    lsof -ti:5000 | xargs kill -9 2>/dev/null || true
    sleep 1
fi

VENV_DIR=".venv"

if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"

pip install --upgrade pip > /dev/null 2>&1

pip install -r requirements.txt > /dev/null 2>&1

python run.py &
APP_PID=$!

sleep 3

check_server() {
    curl -s http://localhost:5000 > /dev/null 2>&1
}

COUNTER=0
while ! check_server && [ $COUNTER -lt 30 ]; do
    sleep 1
    COUNTER=$((COUNTER + 1))
done

if check_server; then
    if command -v open &> /dev/null; then
        open http://localhost:5000
    elif command -v xdg-open &> /dev/null; then
        xdg-open http://localhost:5000
    elif command -v start &> /dev/null; then
        start http://localhost:5000
    fi
    
    wait $APP_PID
else
    kill $APP_PID 2>/dev/null || true
    exit 1
fi 