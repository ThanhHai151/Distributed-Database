#!/bin/bash
# ============================================================
#  STOP — Tắt toàn bộ hệ thống CSDL Phân Tán
# ============================================================

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${CYAN}[STOP] Đang tắt tất cả containers...${NC}"

if docker compose version &>/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
else
    COMPOSE_CMD="docker-compose"
fi

$COMPOSE_CMD stop

echo -e "${GREEN}[DONE] Tất cả containers đã dừng. Dữ liệu được giữ nguyên.${NC}"
echo -e "Khởi động lại:  ${YELLOW}bash start.sh${NC}"
