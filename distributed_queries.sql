-- ============================================================
--  CÁC CÂU TRUY VẤN PHÂN TÁN — QuanLyNhanSu
--  Chạy trên SERVER GỐC (localhost:1433)
--  Sử dụng Linked Server: QLNS_CN1, QLNS_CN2, QLNS_CN3
-- ============================================================

USE QuanLyNhanSu;
GO

-- ============================================================
-- PHẦN 1: TRUY VẤN CỤC BỘ TẠI TỪNG NODE
-- ============================================================

-- 1.1 Xem dữ liệu tại SERVER GỐC (đủ tất cả)
SELECT IDNV, TENNV, GIOITINH, CHINHANH FROM NHANVIEN ORDER BY CHINHANH, IDNV;
GO

-- 1.2 Truy vấn dữ liệu từ CLIENT1 (CN1 - Hà Nội) qua Linked Server
SELECT IDNV, TENNV, GIOITINH
FROM QLNS_CN1.QuanLyNhanSu.dbo.NHANVIEN;
GO

-- 1.3 Truy vấn dữ liệu từ CLIENT2 (CN2 - Đà Nẵng) qua Linked Server
SELECT IDNV, TENNV, GIOITINH
FROM QLNS_CN2.QuanLyNhanSu.dbo.NHANVIEN;
GO

-- 1.4 Truy vấn dữ liệu từ CLIENT3 (CN3 - TP.HCM) qua Linked Server
SELECT IDNV, TENNV, GIOITINH
FROM QLNS_CN3.QuanLyNhanSu.dbo.NHANVIEN;
GO


-- ============================================================
-- PHẦN 2: TRUY VẤN PHÂN TÁN — UNION (gộp dữ liệu từ 3 node)
-- Mô phỏng Horizontal Fragmentation Reconstruction
-- ============================================================

-- 2.1 Danh sách toàn bộ nhân viên từ tất cả chi nhánh
SELECT 'CN1 - Ha Noi'   AS NGUON, IDNV, TENNV, CHUCVU
FROM QLNS_CN1.QuanLyNhanSu.dbo.NHANVIEN
UNION ALL
SELECT 'CN2 - Da Nang'  AS NGUON, IDNV, TENNV, CHUCVU
FROM QLNS_CN2.QuanLyNhanSu.dbo.NHANVIEN
UNION ALL
SELECT 'CN3 - TP.HCM'   AS NGUON, IDNV, TENNV, CHUCVU
FROM QLNS_CN3.QuanLyNhanSu.dbo.NHANVIEN
ORDER BY NGUON, IDNV;
GO

-- 2.2 Tổng số nhân viên mỗi chi nhánh (từ các node phân tán)
SELECT 'CN1 - Ha Noi'   AS CHI_NHANH, COUNT(*) AS SO_NV FROM QLNS_CN1.QuanLyNhanSu.dbo.NHANVIEN
UNION ALL
SELECT 'CN2 - Da Nang'  AS CHI_NHANH, COUNT(*) AS SO_NV FROM QLNS_CN2.QuanLyNhanSu.dbo.NHANVIEN
UNION ALL
SELECT 'CN3 - TP.HCM'   AS CHI_NHANH, COUNT(*) AS SO_NV FROM QLNS_CN3.QuanLyNhanSu.dbo.NHANVIEN;
GO


-- ============================================================
-- PHẦN 3: TRUY VẤN PHÂN TÁN — JOIN NHIỀU NODE
-- ============================================================

-- 3.1 Thống kê lương từ CLIENT1, JOIN với bảng NHANVIEN trên Master
SELECT
    m.IDNV,
    m.TENNV,
    m.CHINHANH,
    bl.LUONGCOBAN,
    bl.THUCNHAN
FROM NHANVIEN m
JOIN QLNS_CN1.QuanLyNhanSu.dbo.BANGLUONG bl
    ON bl.IDBC IN (
        SELECT IDBC FROM QLNS_CN1.QuanLyNhanSu.dbo.BANGCHAMCONG
        WHERE IDNV = m.IDNV
    )
WHERE m.CHINHANH = 'CN1';
GO

-- 3.2 Lương tất cả nhân viên gộp từ 3 chi nhánh phân tán
SELECT IDNV, LUONGCOBAN, THUCNHAN, 'CN1' AS CHINHANH
FROM QLNS_CN1.QuanLyNhanSu.dbo.BANGLUONG
UNION ALL
SELECT bc.IDNV, bl.LUONGCOBAN, bl.THUCNHAN, 'CN2'
FROM QLNS_CN2.QuanLyNhanSu.dbo.BANGLUONG bl
JOIN QLNS_CN2.QuanLyNhanSu.dbo.BANGCHAMCONG bc ON bl.IDBC = bc.IDBC
UNION ALL
SELECT bc.IDNV, bl.LUONGCOBAN, bl.THUCNHAN, 'CN3'
FROM QLNS_CN3.QuanLyNhanSu.dbo.BANGLUONG bl
JOIN QLNS_CN3.QuanLyNhanSu.dbo.BANGCHAMCONG bc ON bl.IDBC = bc.IDBC
ORDER BY CHINHANH, IDNV;
GO


-- ============================================================
-- PHẦN 4: TRUY VẤN PHÂN TÁN NÂNG CAO
-- ============================================================

-- 4.1 Nhân viên có lương thực nhận cao nhất tại mỗi chi nhánh
SELECT 'CN1' AS CHI_NHANH, bc.IDNV, MAX(bl.THUCNHAN) AS LUONG_CAO_NHAT
FROM QLNS_CN1.QuanLyNhanSu.dbo.BANGLUONG bl
JOIN QLNS_CN1.QuanLyNhanSu.dbo.BANGCHAMCONG bc ON bl.IDBC = bc.IDBC
GROUP BY bc.IDNV
UNION ALL
SELECT 'CN2', bc.IDNV, MAX(bl.THUCNHAN)
FROM QLNS_CN2.QuanLyNhanSu.dbo.BANGLUONG bl
JOIN QLNS_CN2.QuanLyNhanSu.dbo.BANGCHAMCONG bc ON bl.IDBC = bc.IDBC
GROUP BY bc.IDNV
UNION ALL
SELECT 'CN3', bc.IDNV, MAX(bl.THUCNHAN)
FROM QLNS_CN3.QuanLyNhanSu.dbo.BANGLUONG bl
JOIN QLNS_CN3.QuanLyNhanSu.dbo.BANGCHAMCONG bc ON bl.IDBC = bc.IDBC
GROUP BY bc.IDNV
ORDER BY LUONG_CAO_NHAT DESC;
GO

-- 4.2 Tổng số hợp đồng đang hiệu lực tại từng chi nhánh
SELECT 'SERVER GOC' AS NGUON, COUNT(*) AS SO_HD
FROM HOPDONG WHERE TRANGTHAI = N'Có hiệu lực'
UNION ALL
SELECT 'CN1', COUNT(*) FROM QLNS_CN1.QuanLyNhanSu.dbo.HOPDONG WHERE TRANGTHAI = N'Có hiệu lực'
UNION ALL
SELECT 'CN2', COUNT(*) FROM QLNS_CN2.QuanLyNhanSu.dbo.HOPDONG WHERE TRANGTHAI = N'Có hiệu lực'
UNION ALL
SELECT 'CN3', COUNT(*) FROM QLNS_CN3.QuanLyNhanSu.dbo.HOPDONG WHERE TRANGTHAI = N'Có hiệu lực';
GO

-- 4.3 Số vị trí đang tuyển dụng tại mỗi chi nhánh
SELECT 'CN1 - Ha Noi'   AS CHI_NHANH, COUNT(*) AS SO_VI_TRI_TUYEN
FROM QLNS_CN1.QuanLyNhanSu.dbo.TUYENDUNG WHERE TRANGTHAI = N'Đang tuyển'
UNION ALL
SELECT 'CN2 - Da Nang',  COUNT(*)
FROM QLNS_CN2.QuanLyNhanSu.dbo.TUYENDUNG WHERE TRANGTHAI = N'Đang tuyển'
UNION ALL
SELECT 'CN3 - TP.HCM',  COUNT(*)
FROM QLNS_CN3.QuanLyNhanSu.dbo.TUYENDUNG WHERE TRANGTHAI = N'Đang tuyển';
GO

-- 4.4 Kiểm tra Linked Server có hoạt động không
SELECT name, product, provider, data_source
FROM sys.servers
WHERE is_linked = 1;
GO


-- ============================================================
-- PHẦN 5: CHẠY TỪ CÁC CLIENT NODE
-- (Kết nối DBeaver vào localhost:1434/1435/1436 rồi chạy)
-- ============================================================

/*
-- Từ CLIENT1 (1434): truy vấn ngược về Server Gốc
USE QuanLyNhanSu;
SELECT IDNV, TENNV FROM QLNS_MASTER.QuanLyNhanSu.dbo.NHANVIEN WHERE CHINHANH = 'CN1';
GO

-- Từ CLIENT1: xem toàn bộ bảng lương của server gốc
SELECT * FROM QLNS_MASTER.QuanLyNhanSu.dbo.BANGLUONG;
GO
*/
