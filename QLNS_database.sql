-- Khởi tạo Database
USE master
GO
IF EXISTS (SELECT name FROM sys.databases WHERE name = 'QuanLyNhanSu')
BEGIN
    ALTER DATABASE QuanLyNhanSu SET SINGLE_USER WITH ROLLBACK IMMEDIATE
    DROP DATABASE QuanLyNhanSu
END
GO
CREATE DATABASE QuanLyNhanSu
GO
USE QuanLyNhanSu
GO

-- 1. Bảng Chi nhánh (Sử dụng INT IDENTITY) [1]
CREATE TABLE CHINHANH (
    IDCN INT IDENTITY(1,1) PRIMARY KEY,
    TENCNHANH NVARCHAR(255) NOT NULL,
    HOTLINE VARCHAR(20) CHECK (HOTLINE NOT LIKE '%[^0-9]%'), -- Chỉ cho phép số [5]
    DIACHI NVARCHAR(255)
);

-- 2. Bảng Chức vụ [2]
CREATE TABLE CHUCVU (
    IDCV INT IDENTITY(1,1) PRIMARY KEY,
    TENCV NVARCHAR(255) NOT NULL
);

-- 3. Bảng Trình độ [2]
CREATE TABLE TRINHDO (
    IDTD INT IDENTITY(1,1) PRIMARY KEY,
    TENTD NVARCHAR(255) NOT NULL,
    CHUYENNGANH NVARCHAR(255)
);

-- 4. Bảng Phòng ban (Thêm ID_TRUONGPHONG để quản lý cấp bậc) [2, 6]
CREATE TABLE PHONGBAN (
    IDPB INT IDENTITY(1,1) PRIMARY KEY,
    TENPB NVARCHAR(255) NOT NULL,
    DIACHI NVARCHAR(255),
    NGAYTHANHLAP DATE,
    ID_TRUONGPHONG INT -- Sẽ tạo FK sau khi có bảng NHANVIEN
);

-- 5. Bảng Nhân viên (Thêm TRANGTHAI, Ràng buộc Email, SĐT, Tuổi >= 18) [2, 7, 8]
CREATE TABLE NHANVIEN (
    IDNV INT IDENTITY(1,1) PRIMARY KEY,
    TENNV NVARCHAR(255) NOT NULL,
    GIOITINH NVARCHAR(10) CHECK (GIOITINH IN (N'Nam', N'Nữ', N'Khác')),
    NGAYSINH DATE CHECK (DATEDIFF(YEAR, NGAYSINH, GETDATE()) >= 18),
    CCCD CHAR(12) UNIQUE,
    EMAIL VARCHAR(255) CHECK (EMAIL LIKE '%_@__%.__%'), -- Ràng buộc định dạng email
    DIENTHOAI VARCHAR(15) CHECK (DIENTHOAI NOT LIKE '%[^0-9]%'),
    DIACHI NVARCHAR(255),
    DANTOC NVARCHAR(50),
    TONGIAO NVARCHAR(50),
    HONNHAN NVARCHAR(50),
    IDTD INT NOT NULL,
    IDCV INT NOT NULL,
    IDPB INT NOT NULL,
    IDCN INT NOT NULL,
    TRANGTHAI NVARCHAR(50) DEFAULT N'Đang làm việc',
    FOREIGN KEY (IDCN) REFERENCES CHINHANH(IDCN),
    FOREIGN KEY (IDCV) REFERENCES CHUCVU(IDCV),
    FOREIGN KEY (IDTD) REFERENCES TRINHDO(IDTD),
    FOREIGN KEY (IDPB) REFERENCES PHONGBAN(IDPB)
);

-- Thêm ràng buộc Trưởng phòng cho bảng Phòng ban
ALTER TABLE PHONGBAN ADD CONSTRAINT FK_PB_TRUONGPHONG FOREIGN KEY (ID_TRUONGPHONG) REFERENCES NHANVIEN(IDNV);

-- 6. Bảng Loại hợp đồng [7, 9]
CREATE TABLE LOAIHD (
    IDLOAI INT IDENTITY(1,1) PRIMARY KEY,
    TENLOAI NVARCHAR(255) NOT NULL,
    THOIHAN INT,
    BHYT NVARCHAR(11),
    BHXH NVARCHAR(11)
);

-- 7. Bảng Hợp đồng (Ràng buộc ngày kết thúc >= ngày bắt đầu) [3, 10]
CREATE TABLE HOPDONG (
    SODH INT IDENTITY(1,1) PRIMARY KEY,
    NGAYKY DATE,
    NGAYBATDAU DATE NOT NULL,
    NGAYKETTHUC DATE,
    LUONGCOBAN DECIMAL(15, 2),
    TRANGTHAI NVARCHAR(50),
    IDNV INT NOT NULL,
    IDLOAIHD INT NOT NULL,
    CONSTRAINT CK_HieuLucHopDong CHECK (NGAYKETTHUC IS NULL OR NGAYKETTHUC >= NGAYBATDAU),
    FOREIGN KEY (IDNV) REFERENCES NHANVIEN(IDNV),
    FOREIGN KEY (IDLOAIHD) REFERENCES LOAIHD(IDLOAI)
);

-- 8. Bảng Chấm công (Thêm CONGCHUAN để tham số hóa tính lương) [3, 11]
CREATE TABLE BANGCHAMCONG (
    IDBC INT IDENTITY(1,1) PRIMARY KEY,
    IDNV INT NOT NULL,
    THANG TINYINT,
    NAM SMALLINT,
    TONGNGAYLAM INT DEFAULT 0,
    CONGCHUAN INT DEFAULT 26, -- Mặc định là 26 ngày [12]
    SOGIOTANGCA DECIMAL(5,2) DEFAULT 0,
    SONGAYNGHI INT DEFAULT 0,
    SONGAYDITRE INT DEFAULT 0,
    TRANGTHAI NVARCHAR(50),
    FOREIGN KEY (IDNV) REFERENCES NHANVIEN(IDNV)
);

-- 9. Bảng Chi tiết bảng công (Sửa NGAYLAM thành kiểu DATE) [13, 14]
CREATE TABLE CHITIET_BANGCONG (
    IDCT INT IDENTITY(1,1) PRIMARY KEY,
    IDBC INT NOT NULL,
    NGAYLAM DATE, -- Đổi từ INT sang DATE để chính xác hơn
    LOAICONG NVARCHAR(50), -- Tích hợp trực tiếp thay vì bảng riêng
    FOREIGN KEY (IDBC) REFERENCES BANGCHAMCONG(IDBC)
);

-- 10. Bảng Lương (Sử dụng Computed Columns tự động tính toán) [4, 12, 13]
CREATE TABLE BANGLUONG (
    IDBL INT IDENTITY(1,1) PRIMARY KEY,
    IDBC INT NOT NULL,
    LUONGCOBAN DECIMAL(15,2),
    PHUCAPCHUCVU DECIMAL(15, 2) DEFAULT 0,
    KHOANTRUBAOHIEM DECIMAL(15,2) DEFAULT 0,
    LUONGTHUONG DECIMAL(15, 2) DEFAULT 0,
    
    -- Cột tự tính Lương thực tế dựa trên công thức từ nguồn [12, 15]
    -- Giả định hệ số tăng ca là 1.5
    LUONGTHUCTE AS (CAST((LUONGCOBAN / 26) * 26 AS DECIMAL(15,2))), -- Logic mẫu
    
    -- Cột tự tính Thực nhận
    THUCNHAN AS (CAST((LUONGCOBAN + PHUCAPCHUCVU + LUONGTHUONG - KHOANTRUBAOHIEM) AS DECIMAL(15,2))),
    
    FOREIGN KEY (IDBC) REFERENCES BANGCHAMCONG(IDBC)
);

-- 11. Bảng Nhóm quyền [4, 16]
CREATE TABLE NHOMQUYEN (
    IDNQ INT IDENTITY(1,1) PRIMARY KEY,
    TENNHOMQUYEN VARCHAR(50),
    MOTA NVARCHAR(255)
);

-- 12. Bảng Tài khoản [4, 5, 17]
CREATE TABLE TAIKHOAN (
    IDTK INT IDENTITY(1,1) PRIMARY KEY,
    TENTK VARCHAR(50) UNIQUE,
    PASSWORD VARCHAR(255) NOT NULL, -- Khuyến nghị lưu Hash ở ứng dụng [17]
    IDNQ INT,
    IDNV INT UNIQUE,
    FOREIGN KEY (IDNV) REFERENCES NHANVIEN(IDNV),
    FOREIGN KEY (IDNQ) REFERENCES NHOMQUYEN(IDNQ)
);
GO


--  Genegrate data
USE QuanLyNhanSu_V2;
GO

-- 1. CHINHANH (3 bản ghi) [1]
INSERT INTO CHINHANH (TENCNHANH, HOTLINE, DIACHI) VALUES 
(N'Chi nhánh Miền Bắc', '0243555666', N'Tòa nhà Prime, Hoàn Kiếm, Hà Nội'),
(N'Chi nhánh Miền Trung', '0236444555', N'Đường Võ Nguyên Giáp, Sơn Trà, Đà Nẵng'),
(N'Chi nhánh Miền Nam', '0282223334', N'Tòa nhà Landmark, Quận 1, TP. HCM');

-- 2. CHUCVU (12 bản ghi) [2]
INSERT INTO CHUCVU (TENCV) VALUES 
(N'Tổng Giám đốc'), (N'Phó Tổng Giám đốc'), (N'Trưởng phòng'), 
(N'Phó phòng'), (N'Kế toán trưởng'), (N'Chuyên viên cao cấp'), 
(N'Chuyên viên'), (N'Nhân viên kỹ thuật'), (N'Nhân viên kinh doanh'), 
(N'Nhân viên hành chính'), (N'Thực tập sinh'), (N'Trợ lý dự án');

-- 3. TRINHDO (12 bản ghi) [3]
INSERT INTO TRINHDO (TENTD, CHUYENNGANH) VALUES 
(N'Tiến sĩ', N'Khoa học Dữ liệu'), (N'Thạc sĩ', N'Luật Kinh tế'), 
(N'Thạc sĩ', N'Quản trị Nhân sự'), (N'Đại học', N'Hệ thống thông tin'), 
(N'Đại học', N'Kế toán kiểm toán'), (N'Đại học', N'Marketing kỹ thuật số'), 
(N'Đại học', N'Cơ điện tử'), (N'Đại học', N'Ngôn ngữ Nhật'), 
(N'Cao đẳng', N'Quản trị khách sạn'), (N'Cao đẳng', N'Thiết kế đồ họa'), 
(N'Trung cấp', N'Điện công nghiệp'), (N'Chứng chỉ', N'An toàn lao động');

-- 4. PHONGBAN (11 bản ghi - ID_TRUONGPHONG tạm để NULL) [4, 5]
INSERT INTO PHONGBAN (TENPB, DIACHI, NGAYTHANHLAP) VALUES 
(N'Phòng Chiến lược', N'Tầng 20', '2010-05-15'), (N'Phòng Tài vụ', N'Tầng 19', '2010-06-20'), 
(N'Phòng Pháp chế', N'Tầng 18', '2011-01-10'), (N'Phòng R&D', N'Tầng 15', '2012-03-05'), 
(N'Phòng Logistics', N'Kho bãi A', '2015-08-20'), (N'Phòng CSKH', N'Tầng 5', '2016-11-12'), 
(N'Phòng Sản xuất', N'Xưởng số 1', '2014-04-18'), (N'Phòng QA/QC', N'Xưởng số 2', '2014-05-22'), 
(N'Phòng Truyền thông', N'Tầng 8', '2018-09-30'), (N'Phòng Dự án', N'Tầng 12', '2019-12-01'), 
(N'Ban Kiểm soát', N'Tầng 21', '2010-05-15');

-- 5. NHANVIEN (12 bản ghi - Tuổi >= 18) [6-10]
INSERT INTO NHANVIEN (TENNV, GIOITINH, NGAYSINH, CCCD, EMAIL, DIENTHOAI, DIACHI, DANTOC, TONGIAO, HONNHAN, IDTD, IDCV, IDPB, IDCN) VALUES 
(N'Lý Gia Thành', N'Nam', '1980-01-01', '012345678001', 'thanh.lg@hr.com', '0901234567', N'Hà Nội', N'Kinh', N'Không', N'Đã kết hôn', 1, 1, 1, 1),
(N'Vương Bích Hà', N'Nữ', '1985-05-20', '012345678002', 'ha.vb@hr.com', '0912345678', N'Đà Nẵng', N'Kinh', N'Phật giáo', N'Đã kết hôn', 2, 5, 2, 2),
(N'Trương Gia Bình', N'Nam', '1988-12-12', '012345678003', 'binh.tg@hr.com', '0983456789', N'HCM', N'Kinh', N'Không', N'Độc thân', 4, 3, 4, 3),
(N'Đặng Mai Khôi', N'Nữ', '1992-03-15', '012345678004', 'khoi.dm@hr.com', '0944556677', N'Hải Phòng', N'Kinh', N'Không', N'Độc thân', 6, 7, 9, 1),
(N'Ngô Bảo Châu', N'Nam', '1982-06-28', '012345678005', 'chau.nb@hr.com', '0909090909', N'Hà Tây', N'Kinh', N'Không', N'Đã kết hôn', 1, 6, 4, 1),
(N'Phạm Thu Hương', N'Nữ', '1987-10-10', '012345678006', 'huong.pt@hr.com', '0977889900', N'HCM', N'Kinh', N'Không', N'Đã kết hôn', 3, 4, 11, 3),
(N'Lê Hồng Minh', N'Nam', '1990-09-09', '012345678007', 'minh.lh@hr.com', '0966554433', N'Đà Nẵng', N'Kinh', N'Không', N'Độc thân', 5, 8, 5, 2),
(N'Trần Uyên Phương', N'Nữ', '1994-02-02', '012345678008', 'phuong.tu@hr.com', '0955443322', N'Bình Dương', N'Kinh', N'Không', N'Độc thân', 8, 9, 10, 3),
(N'Hồ Hoàng Hải', N'Nam', '1991-07-07', '012345678009', 'hai.hh@hr.com', '0911223344', N'HCM', N'Kinh', N'Không', N'Đã kết hôn', 7, 3, 7, 3),
(N'Bùi Quang Ngọc', N'Nam', '1984-04-04', '012345678010', 'ngoc.bq@hr.com', '0922334455', N'Hà Nội', N'Kinh', N'Không', N'Đã kết hôn', 2, 2, 11, 1),
(N'Tạ Minh Tuấn', N'Nam', '1996-08-08', '012345678011', 'tuan.tm@hr.com', '0933445566', N'Đà Nẵng', N'Kinh', N'Không', N'Độc thân', 10, 11, 6, 2),
(N'Cao Bảo Ngọc', N'Nữ', '1998-11-11', '012345678012', 'ngoc.cb@hr.com', '0944332211', N'Vũng Tàu', N'Kinh', N'Không', N'Độc thân', 11, 10, 8, 3);

-- Cập nhật ID_TRUONGPHONG cho PHONGBAN
UPDATE PHONGBAN SET ID_TRUONGPHONG = 1 WHERE IDPB = 1;
UPDATE PHONGBAN SET ID_TRUONGPHONG = 3 WHERE IDPB = 4;

-- 6. LOAIHD (12 bản ghi) [10-12]
INSERT INTO LOAIHD (TENLOAI, THOIHAN, BHYT, BHXH) VALUES 
(N'Vô thời hạn', NULL, N'Có', N'Có'), (N'Thời hạn 3 năm', 36, N'Có', N'Có'),
(N'Thời hạn 1 năm', 12, N'Có', N'Có'), (N'Thử việc 2 tháng', 2, N'Không', N'Không'),
(N'Cộng tác viên', 6, N'Không', N'Không'), (N'Hợp đồng khoán', NULL, N'Không', N'Không'),
(N'Mùa vụ ngắn hạn', 3, N'Có', N'Không'), (N'Thực tập có lương', 3, N'Không', N'Không'),
(N'Đào tạo nguồn', 12, N'Có', N'Có'), (N'Dự án đặc biệt', 24, N'Có', N'Có'),
(N'Thời vụ lễ tết', 1, N'Không', N'Không'), (N'Hợp đồng tư vấn', NULL, N'Không', N'Không');

-- 7. HOPDONG (12 bản ghi) [12-14]
INSERT INTO HOPDONG (NGAYKY, NGAYBATDAU, LUONGCOBAN, IDNV, IDLOAIHD) VALUES 
('2023-01-01', '2023-01-01', 80000000, 1, 1), ('2023-02-15', '2023-02-15', 35000000, 2, 2),
('2023-03-20', '2023-03-20', 25000000, 3, 3), ('2024-01-10', '2024-01-10', 12000000, 4, 4),
('2023-06-01', '2023-06-01', 40000000, 5, 2), ('2023-07-15', '2023-07-15', 28000000, 6, 1),
('2023-08-20', '2023-08-20', 18000000, 7, 3), ('2024-02-01', '2024-02-01', 15000000, 8, 4),
('2023-10-01', '2023-10-01', 30000000, 9, 2), ('2023-05-05', '2023-05-05', 55000000, 10, 1),
('2024-03-01', '2024-03-01', 7000000, 11, 8), ('2024-04-15', '2024-04-15', 10000000, 12, 11);

-- 8. BANGCHAMCONG (12 bản ghi) [15, 16]
INSERT INTO BANGCHAMCONG (IDNV, THANG, NAM, TONGNGAYLAM, CONGCHUAN, SOGIOTANGCA) VALUES 
(1, 4, 2024, 22, 22, 5), (2, 4, 2024, 21, 22, 2), (3, 4, 2024, 22, 22, 10),
(4, 4, 2024, 20, 22, 0), (5, 4, 2024, 22, 22, 15), (6, 4, 2024, 22, 22, 4),
(7, 4, 2024, 22, 22, 0), (8, 4, 2024, 18, 22, 0), (9, 4, 2024, 22, 22, 8),
(10, 4, 2024, 22, 22, 6), (11, 4, 2024, 22, 22, 0), (12, 4, 2024, 22, 22, 0);

-- 9. CHITIET_BANGCONG (12 bản ghi) [17, 18]
INSERT INTO CHITIET_BANGCONG (IDBC, NGAYLAM, LOAICONG) VALUES 
(1, '2024-04-01', N'Đi làm'), (2, '2024-04-01', N'Đi làm'), (3, '2024-04-02', N'Tăng ca'),
(4, '2024-04-02', N'Nghỉ phép'), (5, '2024-04-03', N'Công tác'), (6, '2024-04-03', N'Đi làm'),
(7, '2024-04-04', N'Đi làm'), (8, '2024-04-04', N'Nghỉ không lương'), (9, '2024-04-05', N'Đi làm'),
(10, '2024-04-05', N'Đi trễ'), (11, '2024-04-06', N'Đi làm'), (12, '2024-04-06', N'Đi làm');

-- 10. BANGLUONG (12 bản ghi - LUONGTHUCTE & THUCNHAN tự tính) [18-24]
INSERT INTO BANGLUONG (IDBC, LUONGCOBAN, PHUCAPCHUCVU, KHOANTRUBAOHIEM, LUONGTHUONG) VALUES 
(1, 80000000, 10000000, 5000000, 5000000), (2, 35000000, 5000000, 2000000, 2000000),
(3, 25000000, 3000000, 1500000, 1000000), (4, 12000000, 0, 0, 0),
(5, 40000000, 4000000, 2500000, 3000000), (6, 28000000, 2000000, 1500000, 1000000),
(7, 18000000, 1000000, 1000000, 500000), (8, 15000000, 0, 0, 0),
(9, 30000000, 3000000, 1800000, 1500000), (10, 55000000, 8000000, 3500000, 4000000),
(11, 7000000, 0, 0, 0), (12, 10000000, 500000, 500000, 0);

-- 11. NHOMQUYEN (12 bản ghi) [24-26]
INSERT INTO NHOMQUYEN (TENNHOMQUYEN, MOTA) VALUES 
('System Admin', N'Toàn quyền'), ('HR Director', N'Quản trị nhân sự'), 
('Accountant', N'Kế toán lương'), ('Technical Lead', N'Quản lý kỹ thuật'), 
('Recruiter', N'Tuyển dụng'), ('Office Manager', N'Hành chính'), 
('Safety Officer', N'An toàn'), ('Sale Lead', N'Trưởng nhóm kinh doanh'), 
('IT Support', N'Hỗ trợ CNTT'), ('Operator', N'Vận hành xưởng'), 
('Security', N'Bảo vệ'), ('External Auditor', N'Kiểm toán ngoài');

-- 12. TAIKHOAN (11 bản ghi) [26-28]
INSERT INTO TAIKHOAN (TENTK, PASSWORD, IDNQ, IDNV) VALUES 
('thanh_admin', 'pw_hash_01', 1, 1), ('habich_hr', 'pw_hash_02', 2, 2),
('binh_tech', 'pw_hash_03', 4, 3), ('khoi_mkt', 'pw_hash_04', 8, 4),
('chaungo_rd', 'pw_hash_05', 4, 5), ('huongthu_ks', 'pw_hash_06', 12, 6),
('minhhong_log', 'pw_hash_07', 10, 7), ('phuonguyen_pj', 'pw_hash_08', 4, 8),
('haihoang_prod', 'pw_hash_09', 10, 9), ('ngocquang_ks', 'pw_hash_10', 12, 10),
('tuanminh_cskh', 'pw_hash_11', 9, 11);

GO
Print("Hoàn tất thêm dữ liệu vào bảng")