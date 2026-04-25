# 🏗️ Kiến Trúc Hệ Thống CSDL Phân Tán — QuanLyNhanSu

> **Môn học:** Cơ Sở Dữ Liệu Phân Tán  
> **Hệ thống:** Quản Lý Nhân Sự (QLNS) — Công ty ABC  
> **Công nghệ:** Microsoft SQL Server 2022 · Docker · Linked Servers  
> **Mô hình phân tán:** Phân mảnh ngang (Horizontal Fragmentation) theo chi nhánh

---

## 1. 📌 Tổng Quan Hệ Thống

Hệ thống **QuanLyNhanSu** được triển khai theo mô hình **CSDL phân tán** (Distributed Database System) trên **4 SQL Server riêng biệt**, phản ánh cơ cấu tổ chức của một công ty có văn phòng tại 3 thành phố lớn tại Việt Nam.

### Tại sao đây là hệ thống PHÂN TÁN (không phải 1 CSDL đơn)?

| Tiêu chí | CSDL đơn (Single) | CSDL phân tán (hệ thống này) |
|---|---|---|
| Số server vật lý | 1 | **4 server độc lập** |
| Vị trí lưu trữ | Một nơi | **Hà Nội, Đà Nẵng, TP.HCM + Trung tâm** |
| Dữ liệu NV001 | Trên cùng 1 DB | **Chỉ tồn tại trên Server CN1 (Hà Nội)** |
| Truy vấn liên server | Không cần | **Cần Linked Server / distributed query** |
| Tắt 1 server | Toàn bộ ngừng | **Chi nhánh khác vẫn hoạt động** |
| Mạng nội bộ | Không cần | **Docker network: `172.168.99.0/24`** |

> ⚠️ **Điểm quan trọng**: Dữ liệu nhân viên CN1 **KHÔNG TỒN TẠI** trên server CN2 hay CN3. Muốn truy vấn dữ liệu CN1 từ CN2, phải dùng **Linked Server** — đây chính là đặc trưng của hệ thống phân tán.

---

## 2. 🗺️ Sơ Đồ Kiến Trúc Phân Tán

```
╔══════════════════════════════════════════════════════════════════╗
║               CÔNG TY ABC — HỆ THỐNG QLNS PHÂN TÁN              ║
╚══════════════════════════════════════════════════════════════════╝

                    ┌─────────────────────────────────────┐
                    │          SERVER GỐC (MASTER)         │
                    │       192.168.99.10 : 1433           │
                    │         [localhost : 1432]            │
                    │                                       │
                    │  Database: QuanLyNhanSu              │
                    │  ➤ Chứa TOÀN BỘ 12 nhân viên        │
                    │  ➤ Tất cả các bảng đầy đủ           │
                    │                                       │
                    │  Linked Servers:                      │
                    │  ┌─ QLNS_CN1 → 192.168.99.11 ─────┐ │
                    │  ├─ QLNS_CN2 → 192.168.99.12 ─────┤ │
                    │  └─ QLNS_CN3 → 192.168.99.13 ─────┘ │
                    └──────────┬────────────┬──────────────┘
                               │            │
               ┌───────────────┼────────────┼──────────────────┐
               │               │            │                   │
    ┌──────────▼──────┐  ┌─────▼───────┐  ┌▼─────────────────┐
    │  CLIENT1 (CN1)  │  │ CLIENT2(CN2)│  │   CLIENT3 (CN3)  │
    │  Chi nhánh HN   │  │ Chi nhánh DN│  │ Chi nhánh TP.HCM │
    │ 192.168.99.11   │  │192.168.99.12│  │  192.168.99.13   │
    │ [localhost:1437] │  │[localhost:1435]│  │[localhost:1436]  │
    │                 │  │             │  │                  │
    │  Fragment F_CN1 │  │ Fragment    │  │  Fragment F_CN3  │
    │  NV001,NV004,   │  │ F_CN2       │  │  NV003,NV005,    │
    │  NV008,NV009    │  │ NV002,NV006,│  │  NV007,NV011     │
    │                 │  │ NV010,NV012 │  │                  │
    │ QLNS_MASTER ◄──►│  │QLNS_MASTER  │  │QLNS_MASTER ◄────│
    │ (Linked Server) │  │(Linked Srv) │  │(Linked Server)   │
    └─────────────────┘  └─────────────┘  └──────────────────┘

    Docker Network: qlns_net (172.168.99.0/24)
```

---

## 3. 📦 Cấu Trúc Tệp Dự Án

```
CSDLPT/
├── 📄 QLNS_DEMO.sql                  ← Schema + dữ liệu chạy standalone (1 server)
├── 📄 PHAN_TAN_ARCHITECTURE.md       ← File này: tài liệu kiến trúc phân tán
├── 📄 HUONG_DAN.md                   ← Hướng dẫn setup và kết nối DBeaver
├── 📄 BAI_TAP_PHAN_TAN.sql           ← Bài tập + đáp án truy vấn phân tán
├── 📄 distributed_queries.sql        ← Các câu truy vấn phân tán mẫu
├── 🐳 docker-compose.yml             ← Định nghĩa 4 containers SQL Server
├── 🔧 start.sh                       ← Script khởi động toàn bộ hệ thống
├── 🔧 stop.sh                        ← Script dừng hệ thống
└── 📁 init-scripts/
    ├── 01_master_setup.sql           ← Server Gốc: schema + 12 NV + Linked Servers
    ├── 02_cn1_setup.sql              ← CN1 Hà Nội: chỉ 4 NV thuộc CN1
    ├── 03_cn2_setup.sql              ← CN2 Đà Nẵng: chỉ 4 NV thuộc CN2
    └── 04_cn3_setup.sql              ← CN3 TP.HCM: chỉ 4 NV thuộc CN3
```

---

## 4. 🔀 Chiến Lược Phân Tán Dữ Liệu

### 4.1 Phân Mảnh Ngang (Horizontal Fragmentation)

Bảng **NHANVIEN** được chia theo điều kiện `CHINHANH`:

```
NHANVIEN (toàn bộ — Server Gốc)
┌────────────────────────────────────────────────────────┐
│ NV001 NV002 NV003 NV004 NV005 NV006 NV007 NV008 NV009  │
│ NV010 NV011 NV012                                       │
└────────────────────────────────────────────────────────┘
           │                  │                  │
    ┌──────▼──────┐    ┌──────▼──────┐    ┌──────▼──────┐
    │  F_CN1 (HN) │    │ F_CN2 (DN)  │    │ F_CN3 (HCM) │
    │  SERVER CN1 │    │  SERVER CN2 │    │  SERVER CN3 │
    ├─────────────┤    ├─────────────┤    ├─────────────┤
    │ NV001       │    │ NV002       │    │ NV003       │
    │ NV004       │    │ NV006       │    │ NV005       │
    │ NV008       │    │ NV010       │    │ NV007       │
    │ NV009       │    │ NV012       │    │ NV011       │
    └─────────────┘    └─────────────┘    └─────────────┘

Điều kiện phân mảnh:
  F_CN1: σ(CHINHANH = 'CN1') [NHANVIEN]
  F_CN2: σ(CHINHANH = 'CN2') [NHANVIEN]
  F_CN3: σ(CHINHANH = 'CN3') [NHANVIEN]

Tái tạo (Reconstruction):
  NHANVIEN = F_CN1 ∪ F_CN2 ∪ F_CN3
```

### 4.2 Bảng nào được phân mảnh? Bảng nào được nhân bản?

| Bảng | Chiến lược | Lý do |
|---|---|---|
| `NHANVIEN` | **Phân mảnh ngang** theo `CHINHANH` | NV thuộc chi nhánh nào thì lưu ở đó |
| `BANGCHAMCONG` | **Phân mảnh theo NV** (gián tiếp theo CN) | Chấm công gắn với NV → theo NV |
| `BANGLUONG` | **Phân mảnh theo BCC** (gián tiếp) | Lương tính từ chấm công → theo CN |
| `HOPDONG` | **Phân mảnh theo NV** (gián tiếp) | Hợp đồng gắn với NV |
| `TUYENDUNG` | **Phân mảnh theo IDCN** | Tuyển dụng tại chi nhánh nào |
| `CHINHANH` | **Nhân bản toàn bộ** (Replication) | Cần đủ để tham chiếu FK |
| `CHUCVU` | **Nhân bản toàn bộ** | Lookup table, nhỏ, cần mọi nơi |
| `TRINHDO` | **Nhân bản toàn bộ** | Lookup table |
| `PHONGBAN` | **Nhân bản một phần** | Mỗi CN có phòng ban của mình |
| `LOAIHD` | **Nhân bản toàn bộ** | Tra cứu loại hợp đồng |
| `LOAICONG` | **Nhân bản toàn bộ** | Tra cứu loại công |

---

## 5. 🌐 Cấu Hình Mạng và Kết Nối

### 5.1 Docker Network

```yaml
# docker-compose.yml — Mạng nội bộ riêng biệt
networks:
  qlns_net:
    driver: bridge
    ipam:
      config:
        - subnet: 192.168.99.0/24
```

| Container | IP nội bộ | Port nội bộ | Port host (localhost) | Vai trò |
|---|---|---|---|---|
| `qlns_master` | `192.168.99.10` | `1433` | **`1432`** | Server Gốc (toàn bộ data) |
| `qlns_cn1` | `192.168.99.11` | `1433` | **`1437`** | Chi nhánh Hà Nội |
| `qlns_cn2` | `192.168.99.12` | `1433` | **`1435`** | Chi nhánh Đà Nẵng |
| `qlns_cn3` | `192.168.99.13` | `1433` | **`1436`** | Chi nhánh TP.HCM |

### 5.2 Linked Server — Cầu Nối Giữa Các Server

```
Server Gốc (Master)              Chi nhánh (Clients)
┌─────────────────────┐          ┌─────────────────────┐
│                     │          │                     │
│  QLNS_CN1 ──────────┼──────────► CLIENT1 (CN1)       │
│  QLNS_CN2 ──────────┼──────────► CLIENT2 (CN2)       │
│  QLNS_CN3 ──────────┼──────────► CLIENT3 (CN3)       │
│                     │          │                     │
└─────────────────────┘          │  QLNS_MASTER ───────┼──► Server Gốc
                                 └─────────────────────┘
```

> **Linked Server** cho phép một SQL Server truy vấn trực tiếp dữ liệu từ một SQL Server khác bằng cú pháp 4 phần:
> ```sql
> [TEN_LINKED_SERVER].[Database].[Schema].[Table]
> -- Ví dụ:
> QLNS_CN1.QuanLyNhanSu.dbo.NHANVIEN
> ```

---

## 6. 🔍 Phân Biệt Xử Lý Cục Bộ vs Toàn Cục

### 6.1 Xử Lý Cục Bộ (Local Processing)

Khi một chi nhánh truy vấn **dữ liệu của chính nó** — không cần liên hệ server khác:

```sql
-- Kết nối vào CN1 (localhost:1437), chạy:
USE QuanLyNhanSu;

-- ✅ LOCAL: Lấy danh sách NV tại CN1 (chỉ truy vấn server hiện tại)
SELECT IDNV, TENNV, CHUCVU FROM NHANVIEN;
-- → Trả về 4 NV: NV001, NV004, NV008, NV009

-- ✅ LOCAL: Tính lương nhân viên CN1 trong tháng 1/2024
SELECT nv.TENNV, bl.THUCNHAN
FROM NHANVIEN nv
JOIN BANGCHAMCONG bc ON bc.IDNV = nv.IDNV
JOIN BANGLUONG bl ON bl.IDBC = bc.IDBC
WHERE bc.THANG = 1 AND bc.NAM = 2024;
```

**Đặc điểm xử lý cục bộ:**
- Không có network round-trip đến server khác
- Hiệu năng cao nhất
- Tính nhất quán đơn giản
- Chỉ thấy được dữ liệu của CN mình

### 6.2 Xử Lý Toàn Cục (Global Processing)

Khi cần dữ liệu từ **nhiều server** — phải dùng Linked Server hoặc kết nối từ Master:

```sql
-- Kết nối vào Server Gốc (localhost:1432), chạy:
USE QuanLyNhanSu;

-- ✅ GLOBAL: Tổng hợp số NV từ TẤT CẢ chi nhánh
SELECT 'CN1' AS CN, COUNT(*) AS SO_NV FROM QLNS_CN1.QuanLyNhanSu.dbo.NHANVIEN
UNION ALL
SELECT 'CN2', COUNT(*) FROM QLNS_CN2.QuanLyNhanSu.dbo.NHANVIEN
UNION ALL
SELECT 'CN3', COUNT(*) FROM QLNS_CN3.QuanLyNhanSu.dbo.NHANVIEN;
-- → Kết quả thu thập từ 3 server độc lập, tổng hợp tại Master

-- ✅ GLOBAL: JOIN dữ liệu nhân viên (Master) với lương (CN1)
SELECT m.TENNV, cn1.THUCNHAN
FROM NHANVIEN m
JOIN QLNS_CN1.QuanLyNhanSu.dbo.BANGLUONG cn1 
    ON cn1.IDBC IN (SELECT IDBC FROM QLNS_CN1.QuanLyNhanSu.dbo.BANGCHAMCONG WHERE IDNV = m.IDNV)
WHERE m.CHINHANH = 'CN1';
```

**Đặc điểm xử lý toàn cục:**
- Có network round-trip — phát sinh **distributed query cost**
- SQL Server tự tối ưu **query plan** để giảm data transfer
- Cần quản lý **consistency** khi có cập nhật đồng thời
- Thấy được **toàn bộ dữ liệu** trong công ty

### 6.3 Bảng So Sánh Local vs Global

| Khía cạnh | Local (Cục bộ) | Global (Toàn cục) |
|---|---|---|
| **Phạm vi dữ liệu** | Chỉ 1 chi nhánh | Nhiều / tất cả chi nhánh |
| **Server tham gia** | 1 | 2–4 |
| **Hiệu năng** | ⚡ Cao | 🔄 Phụ thuộc network |
| **Cú pháp SQL** | `FROM NHANVIEN` | `FROM QLNS_CN1.QuanLyNhanSu.dbo.NHANVIEN` |
| **Ví dụ ứng dụng** | Xem lương tháng của mình | Báo cáo toàn công ty |
| **Nơi chạy tốt nhất** | Tại client node | Tại Server Gốc (Master) |

---

## 7. 📊 Mô Hình Dữ Liệu (ERD tóm tắt)

```
CHINHANH ──< NHANVIEN >── PHONGBAN
                │
                ├──< BANGCHAMCONG >── BANGLUONG
                │         │
                │         └──< CHITIET_BANGCONG >── LOAICONG
                │
                └──< HOPDONG >── LOAIHD

CHINHANH ──< TUYENDUNG >──< UNGVIEN_UNGTUYEN >── UNGVIEN >── TRINHDO
NHANVIEN ──< TAIKHOAN >── NHOMQUYEN
NHANVIEN ── CHUCVU
NHANVIEN ── TRINHDO
```

---

## 8. 🔄 Các Giao Thức Phân Tán Được Sử Dụng

### 8.1 Two-Phase Commit (2PC) — Tự động bởi SQL Server

Khi thực hiện distributed transaction:
```sql
BEGIN DISTRIBUTED TRANSACTION;
    -- Cập nhật Master
    UPDATE NHANVIEN SET CHUCVU = 'CV002' WHERE IDNV = 'NV001';
    -- Cập nhật CN1 (qua Linked Server)  
    UPDATE QLNS_CN1.QuanLyNhanSu.dbo.NHANVIEN 
    SET CHUCVU = 'CV002' WHERE IDNV = 'NV001';
COMMIT TRANSACTION;
```

### 8.2 Query Processing trong Distributed DB

```
Khi Master nhận: SELECT * FROM QLNS_CN1.QuanLyNhanSu.dbo.NHANVIEN

1. [Parse]      → Phân tích cú pháp SQL
2. [Optimize]   → Chọn query plan tối ưu:
                   - Gửi sub-query sang CN1?
                   - Kéo toàn bộ data về Master rồi lọc?
3. [Execute]    → Gửi request qua TCP/IP đến 192.168.99.11:1433
4. [Transfer]   → Dữ liệu truyền về qua mạng Docker
5. [Assemble]   → Master tập hợp kết quả trả về client
```

---

## 9. ✅ Kiểm Chứng Tính Phân Tán

Để chứng minh đây là hệ thống phân tán (không phải 1 DB đơn), thực hiện các bước sau:

### Bước 1: Kết nối vào CN1 (port 1437) và truy vấn
```sql
-- Kết nối DBeaver → localhost:1437
USE QuanLyNhanSu;
SELECT COUNT(*) AS SO_NV_CN1 FROM NHANVIEN;
-- Kết quả: 4 (chỉ thấy NV thuộc CN1)
```

### Bước 2: Kết nối vào Server Gốc (port 1432) và truy vấn
```sql
-- Kết nối DBeaver → localhost:1432
USE QuanLyNhanSu;
SELECT COUNT(*) AS TONG_NV FROM NHANVIEN;
-- Kết quả: 12 (thấy tất cả)
```

### Bước 3: Từ CN1, truy vấn ngược về Master
```sql
-- Vẫn kết nối tại localhost:1437
SELECT COUNT(*) AS TONG_NV_MASTER 
FROM QLNS_MASTER.QuanLyNhanSu.dbo.NHANVIEN;
-- Kết quả: 12 (qua Linked Server)
```

### Bước 4: Xem danh sách Linked Server
```sql
-- Tại Server Gốc (1432):
SELECT name, data_source, product FROM sys.servers WHERE is_linked = 1;
-- → Thấy 3 Linked Server: QLNS_CN1, QLNS_CN2, QLNS_CN3
```

### Bước 5: Prove phân mảnh — NV002 không tồn tại ở CN1
```sql
-- Tại CN1 (1437):
SELECT * FROM NHANVIEN WHERE IDNV = 'NV002';
-- → 0 rows (NV002 thuộc CN2, không có ở CN1!)

-- Tại Server Gốc (1432):
SELECT * FROM QLNS_CN2.QuanLyNhanSu.dbo.NHANVIEN WHERE IDNV = 'NV002';
-- → 1 row (truy vấn qua Linked Server sang CN2)
```

---

## 10. 📈 Dữ Liệu Demo — Phân Bố Nhân Viên

| Nhân viên | Họ tên | Chi nhánh | Server lưu trữ |
|---|---|---|---|
| NV001 | Nguyễn Văn A | CN1 - Hà Nội | CLIENT1 (:1437) |
| NV002 | Trần Thị B | CN2 - Đà Nẵng | CLIENT2 (:1435) |
| NV003 | Lê Văn C | CN3 - TP.HCM | CLIENT3 (:1436) |
| NV004 | Phạm Thị D | CN1 - Hà Nội | CLIENT1 (:1437) |
| NV005 | Hoàng Văn E | CN3 - TP.HCM | CLIENT3 (:1436) |
| NV006 | Nguyễn Thị F | CN2 - Đà Nẵng | CLIENT2 (:1435) |
| NV007 | Trần Văn G | CN3 - TP.HCM | CLIENT3 (:1436) |
| NV008 | Lê Thị H | CN1 - Hà Nội | CLIENT1 (:1437) |
| NV009 | Phạm Văn I | CN1 - Hà Nội | CLIENT1 (:1437) |
| NV010 | Hoàng Thị K | CN2 - Đà Nẵng | CLIENT2 (:1435) |
| NV011 | Nguyễn Văn L | CN3 - TP.HCM | CLIENT3 (:1436) |
| NV012 | Trần Thị M | CN2 - Đà Nẵng | CLIENT2 (:1435) |

---

## 11. 🚀 Hướng Dẫn Nhanh Chạy Hệ Thống

```bash
# 1. Khởi động (lần đầu ~2 phút, sau đó ~5 giây)
cd ~/Documents/TMDT/CSDLPT
bash start.sh

# 2. Kết nối 4 connections trong DBeaver:
#    Server Gốc  → localhost:1432  (sa / KetNoi@123)
#    CN1 Hà Nội → localhost:1437  (sa / KetNoi@123)
#    CN2 Đà Nẵng→ localhost:1435  (sa / KetNoi@123)
#    CN3 TP.HCM → localhost:1436  (sa / KetNoi@123)

# 3. Tắt khi không dùng
bash stop.sh
```

---

*Tài liệu này mô tả kiến trúc hệ thống CSDL phân tán cho môn học. Dữ liệu trong hệ thống là dữ liệu demo, không phải dữ liệu thực tế.*
