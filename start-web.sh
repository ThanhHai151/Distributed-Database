#!/bin/bash

# ============================================================
# QLNS Web App — Start Script
# Khởi động backend + frontend (yêu cầu Docker SQL Server đã chạy)
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$SCRIPT_DIR/qlns-web/backend"
FRONTEND_DIR="$SCRIPT_DIR/qlns-web/frontend"

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║        QLNS Phân Tán — Web Application          ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# Check if Docker SQL Server is running
if ! docker ps | grep -q qlns_master; then
    echo "⚠️  SQL Server containers not running!"
    echo "   Đang khởi động Docker SQL Server..."
    bash "$SCRIPT_DIR/start.sh"
    echo "   Chờ 30 giây để SQL Server khởi động..."
    sleep 30
fi

echo "✅ SQL Server containers: OK"

# Install dependencies if needed
if [ ! -d "$BACKEND_DIR/node_modules" ]; then
    echo "📦 Cài đặt backend dependencies..."
    cd "$BACKEND_DIR" && npm install
fi

if [ ! -d "$FRONTEND_DIR/node_modules" ]; then
    echo "📦 Cài đặt frontend dependencies..."
    cd "$FRONTEND_DIR" && npm install
fi

echo ""
echo "🚀 Khởi động Backend API (port 4000)..."
cd "$BACKEND_DIR" && npm run dev &
BACKEND_PID=$!

sleep 3

echo "🌐 Khởi động Frontend (port 3000)..."
cd "$FRONTEND_DIR" && npm run dev &
FRONTEND_PID=$!

echo ""
echo "════════════════════════════════════════════════════"
echo "  ✅ Hệ thống đang chạy:"
echo "     Frontend: http://localhost:3000"
echo "     Backend:  http://localhost:4000"
echo "     API Docs: http://localhost:4000/"
echo ""
echo "  SQL Server Nodes:"
echo "     Master:   localhost:1432"
echo "     CN1 (HN): localhost:1437"
echo "     CN2 (ĐN): localhost:1435"
echo "     CN3 (HCM):localhost:1436"
echo "════════════════════════════════════════════════════"
echo ""
echo "Nhấn Ctrl+C để dừng..."

trap "echo ''; echo '⛔ Dừng...'; kill $BACKEND_PID $FRONTEND_PID 2>/dev/null; exit 0" SIGINT SIGTERM

wait
