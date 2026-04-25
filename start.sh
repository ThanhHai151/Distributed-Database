#!/bin/bash
# ============================================================
#  START — Khởi động hệ thống CSDL Phân Tán QuanLyNhanSu
#
#  Lần đầu chạy: tạo containers + khởi tạo toàn bộ database
#  Lần sau:      chỉ start lại containers (giữ nguyên data)
# ============================================================

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
BOLD='\033[1m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${CYAN}${BOLD}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║         CSDL PHÂN TÁN — QuanLyNhanSu                   ║"
echo "║  Master:1432 | CN1:1437 | CN2:1435 | CN3:1436           ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"

# ── Kiểm tra Docker ─────────────────────────────────────────
if ! command -v docker &>/dev/null; then
    echo -e "${RED}[LỖI] Docker chưa cài. Chạy: sudo apt-get install docker.io${NC}"
    exit 1
fi

if docker compose version &>/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose &>/dev/null; then
    COMPOSE_CMD="docker-compose"
else
    echo -e "${RED}[LỖI] Docker Compose chưa cài.${NC}"
    exit 1
fi

cd "$SCRIPT_DIR"

# ── Phát hiện trạng thái hiện tại ───────────────────────────
MASTER_RUNNING=$(docker inspect -f '{{.State.Running}}' qlns_master 2>/dev/null || echo "false")
DB_EXISTS=$(docker inspect -f '{{.State.Running}}' qlns_master 2>/dev/null \
    && docker exec qlns_master /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'KetNoi@123' -C \
        -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name='QuanLyNhanSu'" \
        2>/dev/null | grep -E "^[[:space:]]*1" || echo "")

# ── CASE 1: Containers đang chạy và DB đã có ────────────────
if [ "$MASTER_RUNNING" = "true" ] && [ -n "$DB_EXISTS" ]; then
    echo -e "${GREEN}✓ Hệ thống đã đang chạy và có dữ liệu!${NC}"
    echo ""
    docker ps --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" \
        --filter "name=qlns_" 2>/dev/null
    echo ""
    echo -e "Tắt hệ thống:  ${YELLOW}bash stop.sh${NC}"
    exit 0
fi

# ── CASE 2: Containers tồn tại nhưng đang stopped ───────────
CONTAINER_EXISTS=$(docker ps -a --format "{{.Names}}" --filter "name=qlns_master" 2>/dev/null)
VOLUME_EXISTS=$(docker volume ls --format "{{.Name}}" 2>/dev/null | grep "csdlpt_master_data" || echo "")

if [ -n "$CONTAINER_EXISTS" ] && [ -n "$VOLUME_EXISTS" ]; then
    echo -e "${YELLOW}[START] Khởi động lại containers (giữ nguyên dữ liệu)...${NC}"
    $COMPOSE_CMD start
    echo -e "${GREEN}✓ Hệ thống đã khởi động lại!${NC}"
    echo ""
    docker ps --format "  {{.Names}}\t{{.Status}}\t{{.Ports}}" --filter "name=qlns_" 2>/dev/null
    echo ""
    echo -e "${BOLD}Kết nối DBeaver:${NC}"
    echo -e "  Server Gốc : localhost:${GREEN}1432${NC}  |  CN1 Hà Nội : localhost:${GREEN}1437${NC}"
    echo -e "  CN2 Đà Nẵng: localhost:${GREEN}1435${NC}  |  CN3 TP.HCM : localhost:${GREEN}1436${NC}"
    echo -e "  User: ${CYAN}sa${NC}  |  Password: ${CYAN}KetNoi@123${NC}"
    echo ""
    echo -e "Tắt hệ thống:  ${YELLOW}bash stop.sh${NC}"
    exit 0
fi

# ── CASE 3: Lần đầu — tạo mới hoàn toàn ────────────────────
echo -e "${YELLOW}[1/4] Dọn dẹp containers cũ (nếu có)...${NC}"
$COMPOSE_CMD down -v --remove-orphans 2>/dev/null || true

echo -e "${YELLOW}[2/4] Khởi động 4 SQL Server containers...${NC}"
$COMPOSE_CMD up -d sqlserver-master sqlserver-cn1 sqlserver-cn2 sqlserver-cn3

echo ""
echo -e "${CYAN}Chờ SQL Server sẵn sàng...${NC}"

wait_for_sql() {
    local container=$1
    local label=$2
    echo -n "  Chờ $label"
    for i in $(seq 1 50); do
        if docker exec "$container" /opt/mssql-tools18/bin/sqlcmd \
            -S localhost -U sa -P 'KetNoi@123' -Q "SELECT 1" -C -l 5 &>/dev/null 2>&1; then
            echo -e " ${GREEN}✓${NC}"
            return 0
        fi
        echo -n "."
        sleep 3
    done
    echo -e " ${RED}✗ Timeout!${NC}"; return 1
}

wait_for_sql "qlns_master" "Master  (localhost:1432)"
wait_for_sql "qlns_cn1"    "CN1 Hà Nội  (localhost:1437)"
wait_for_sql "qlns_cn2"    "CN2 Đà Nẵng (localhost:1435)"
wait_for_sql "qlns_cn3"    "CN3 TP.HCM  (localhost:1436)"

echo ""
echo -e "${YELLOW}[3/4] Khởi tạo database và dữ liệu...${NC}"

# Copy scripts vao client containers (chi master duoc mount volume)
docker cp "$SCRIPT_DIR/init-scripts/02_cn1_setup.sql" qlns_cn1:/tmp/
docker cp "$SCRIPT_DIR/init-scripts/03_cn2_setup.sql" qlns_cn2:/tmp/
docker cp "$SCRIPT_DIR/init-scripts/04_cn3_setup.sql" qlns_cn3:/tmp/

run_sql() {
    local container=$1
    local path=$2
    local label=$3
    echo -e "  ${CYAN}→ $label${NC}"
    docker exec -i "$container" /opt/mssql-tools18/bin/sqlcmd \
        -S localhost -U sa -P 'KetNoi@123' \
        -i "$path" -C -l 120 2>&1 | grep "===" | head -3 || true
}

run_sql "qlns_master" "/init-scripts/01_master_setup.sql" "Server Gốc  — 12 nhân viên toàn hệ thống"
run_sql "qlns_cn1"    "/tmp/02_cn1_setup.sql"             "CLIENT1 CN1 — 4 NV Hà Nội"
run_sql "qlns_cn2"    "/tmp/03_cn2_setup.sql"             "CLIENT2 CN2 — 4 NV Đà Nẵng"
run_sql "qlns_cn3"    "/tmp/04_cn3_setup.sql"             "CLIENT3 CN3 — 4 NV TP.HCM"

echo ""
echo -e "${YELLOW}[4/4] Xác minh hệ thống...${NC}"
MASTER_NV=$(docker exec -i qlns_master /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -P 'KetNoi@123' -C \
    -Q "SET NOCOUNT ON; USE QuanLyNhanSu; SELECT COUNT(*) FROM NHANVIEN" \
    2>/dev/null | grep -E "^[[:space:]]*[0-9]" | tr -d ' \r') || MASTER_NV="?"
echo -e "  Master: ${GREEN}${MASTER_NV} nhân viên${NC}"

echo ""
echo -e "${GREEN}${BOLD}"
echo "╔══════════════════════════════════════════════════════════╗"
echo "║           ✅  KHỞI TẠO HOÀN TẤT!                       ║"
echo "╠══════════════════════════════════════════════════════════╣"
echo "║  Kết nối DBeaver (Driver: Microsoft SQL Server):         ║"
echo "║                                                          ║"
echo "║  [Server Gốc]  localhost : 1432  (12 NV — đủ hệ thống)  ║"
echo "║  [CN1 Hà Nội]  localhost : 1437  ( 4 NV — phân mảnh)    ║"
echo "║  [CN2 Đà Nẵng] localhost : 1435  ( 4 NV — phân mảnh)    ║"
echo "║  [CN3 TP.HCM]  localhost : 1436  ( 4 NV — phân mảnh)    ║"
echo "║                                                          ║"
echo "║  Username: sa   |   Password: KetNoi@123                 ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo -e "${NC}"
echo -e "Tắt hệ thống:   ${YELLOW}bash stop.sh${NC}"
echo -e "Queries phân tán: ${YELLOW}distributed_queries.sql${NC} (chạy trên port 1432)"
