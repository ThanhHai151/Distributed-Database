<div align="center">
  <img src="https://img.icons8.com/color/120/000000/server.png" alt="Server Icon"/>
  <h1>Hệ Thống Quản Lý Nhân Sự Mạng Phân Tán<br>(Distributed HR Management)</h1>
  
  <p>
    <b>Một dự án thực tế minh họa kiến trúc Cơ Sở Dữ Liệu Phân Tán với Microsoft SQL Server, kết hợp hệ sinh thái Web Node.js (Next.js & Fastify).</b>
  </p>

  <p>
    <img src="https://img.shields.io/badge/Architecture-Distributed%20SQL-blue?style=for-the-badge&logo=microsoftsqlserver" alt="Architecture"/>
    <img src="https://img.shields.io/badge/Frontend-Next.js%2015-black?style=for-the-badge&logo=next.js" alt="Next.js"/>
    <img src="https://img.shields.io/badge/Backend-Fastify-202020?style=for-the-badge&logo=fastify" alt="Fastify"/>
    <img src="https://img.shields.io/badge/Deploys-Docker-2496ED?style=for-the-badge&logo=docker" alt="Docker"/>
  </p>
</div>

<br/>

## 📖 Giới thiệu (Overview)

Dự án này là hệ thống phần mềm quản lý Nhân Sự trực quan được xây dựng trên nền tảng **Cơ sở dữ liệu Phân tán (Distributed Database)**. Thay vì lưu trữ tập trung tại một máy chủ duy nhất, dữ liệu được phân mảnh ngang (Horizontal Fragmentation) về 3 Chi Nhánh (Hà Nội, Đà Nẵng, TP.HCM), được giám sát và điều hướng bởi 1 Server Gốc (Master). 

Mục tiêu cốt lõi của giải pháp là trình diễn năng lực:
✅ **Toàn vẹn dữ liệu** trên các Server khác nhau.  
✅ **Truy vấn phân tán (Distributed Query)** có tính trong suốt ứng dụng qua *Linked Server*.  
✅ **Kiến trúc Web Fullstack hiện đại** giúp người quản trị dễ dàng theo dõi Node Status theo realtime và thao tác SQL từ xa mà không cần cài DBeaver hay SSMS cục bộ.

---

## 📸 Hình ảnh Minh Họa Dự Án (Showcase)

> **💡 Mẹo:** *(Bạn hãy dùng phần mềm quay màn hình chụp lại các ảnh Web/Gif dán đè thay thế vào những chỗ này để khoe nhà tuyển dụng nhé!)*

| Tổng quan (Dashboard) | Quản lý Nhân Viên (Branch Level) |
| :---: | :---: |
| ![Dashboard Placeholder](https://via.placeholder.com/600x350.png?text=Ảnh+Màn+Hình+Dashboard) | ![Employees Placeholder](https://via.placeholder.com/600x350.png?text=Ảnh+Màn+Hình+Nhân+Sự) |
| *Báo cáo tổng hợp số liệu lương, KPI quét từ toàn bộ 4 Server theo thời gian thực* | *Lưới nhân viên hiển thị dữ liệu kết xuất từ chi nhánh cục bộ với độ trễ cực thấp* |

### Tính năng SQL Terminal Phân Tán
Một điểm nhấn kỹ thuật của dự án là **SQL Terminal** ngay trên web. Bạn có thể tương tác viết script SQL (Bao gồm cả DML: `SELECT`, `UPDATE`, `INSERT`, `DELETE`) từ giao diện điều khiển. Backend Fastify sẽ chịu trách nhiệm compile và điều phối command xuống đúng cụm Server vật lý dưới Docker.

![SQL Terminal Placeholder](https://via.placeholder.com/900x400.png?text=Ảnh+Màn+Hình+Web+SQL+Terminal)

---

## 🛠️ Công Nghệ Sử Dụng (Tech Stack)

### ⚙️ Database & Infrastructure
- **Microsoft SQL Server (Linux image)** trong Môi trường Docker Container.
- Cấu hình phân mảnh qua **Linked Servers** (RPC Out/In).
- Networking qua **Docker Compose Bridge**.

### 💻 Web Backend (API Server)
- **Fastify & Node.js:** High-performance web framework.
- Cơ chế quản lý Connection Pools tự động khôi phục kết nối khi 1 Node SQL bị Sập (Auto Retry).
- Parsing và bảo mật Query trước khi xuống Database Engine.

### 🎨 Web Frontend (Client Side)
- **Next.js (App Router):** SSR Framework mạnh mẽ.
- **Tailwind CSS & Framer Motion:** Xây dựng Giao diện UI/UX mượt mà, Darkmode, chuẩn vi mô.
- **Lucide React:** Hệ thống icon vector tối giản.

---

## 🚀 Hướng Dẫn Cài Đặt (Local Setup)

Để chạy được toàn bộ Hệ sinh thái Web và 4 Server SQL 24/7 dưới máy tính cục bộ, bạn chỉ cần thực hiện theo script Docker đã đóng gói sẵn:

###  Yêu cầu tiền quyết
- Máy tính đã cài đặt **Docker** và **Docker Compose**.
- Đã cài đặt **Node.js** (Phiên bản v20+).
- RAM máy tính còn trống ít nhất 4GB.

### Chỉ lệnh chạy (1 Click)

Chạy kịch bản khởi tạo tự động, hệ thống sẽ dựng 4 Máy chủ Ảo Docker SQL và đồng thời mở Cổng kết nối Web Frontend (Cổng 3000) và Web Backend API (Cổng 4000).

```bash
# 1. Cấp quyền thực thi script
chmod +x start.sh start-web.sh stop.sh

# 2. Khởi tạo và liên kết các Node Cơ Sở Dữ Liệu
./start.sh

# 3. Khởi động Giao diện Web Quản trị
./start-web.sh
```

👉 Truy cập ngay vào: **`http://localhost:3000`** để sử dụng phần mềm.

### 🪟 Dành riêng cho Windows (Windows Users)

Nếu máy tính của bạn chạy Windows, các lệnh tĩnh `.sh` sẽ không thể thực thi trực tiếp trên `cmd` hoặc `PowerShell` mặc định. Thay vào đó, bạn có thể thực hiện theo 1 trong 2 cách sau:

**Cách 1: Sử dụng Git Bash (Khuyên dùng - Nhanh nhất)**
1. Cài đặt [Git for Windows](https://gitforwindows.org/) (Nếu máy bạn chưa có).
2. Click chuột phải vào thư mục gốc của dự án này, chọn **"Open Git Bash here"**.
3. Cửa sổ Terminal đen hiện ra, bạn dán y hệt các lệnh bên trên vào để chạy:
   ```bash
   ./start.sh
   ./start-web.sh
   ```

**Cách 2: Thông qua WSL (Windows Subsystem for Linux)**
Nếu bạn đang dùng Docker Desktop thông qua kiến trúc WSL 2, bạn hoàn toàn có thể mở terminal WSL (ví dụ: Ubuntu), dùng lệnh `cd` để trỏ tới thư mục dự án và chạy các tập lệnh `.sh` như một môi trường Linux thực thụ.

---

## 📂 Kiến Trúc Mã Nguồn Thư Mục

```text
/CSDLPT
├── /init-scripts           # Lưu trữ các file SQL tự động seed data đầu đời cho Database
├── /qlns-web               # Thư mục chứa Source Code Hệ thống Website
│   ├── /frontend           # Giao diện UI/UX Next.js (Cổng 3000)
│   └── /backend            # Mã nguồn Fastify Server API điều phối Server (Cổng 4000)
├── docker-compose.yml      # Bản phác thảo Container khởi tạo 4 Server SQL nội bộ
├── docker-compose.app.yml  # Kịch bản Build Nodejs vào Docker (Deploy Online)
└── PHAN_TAN_ARCHITECTURE.md# (Chi tiết Thiết kế Lý thuyết Phân mảnh)
```

---

<div align="center">
  <p><i>Cảm ơn bạn đã xem qua dự án. Chúc bạn có trải nghiệm thiết kế Web & System hay nhất!</i></p>
</div>
# Distributed-Database
