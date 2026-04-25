-- ============================================================
--  SERVER GOC (MASTER) - Toan bo co so du lieu QuanLyNhanSu
--  IP: 192.168.99.10  |  Port: 1433 (mapped 1433)
-- ============================================================

USE master;
GO

IF EXISTS (SELECT name FROM sys.databases WHERE name = 'QuanLyNhanSu')
BEGIN
    ALTER DATABASE QuanLyNhanSu SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE QuanLyNhanSu;
END
GO

CREATE DATABASE QuanLyNhanSu;
GO

USE QuanLyNhanSu;
GO

-- ============================================================
--  TAO BANG
-- ============================================================

CREATE TABLE CHINHANH (
    IDCN CHAR(10) PRIMARY KEY NOT NULL,
    TENCNHANH NVARCHAR(255) NOT NULL,
    HOTLINE VARCHAR(20),
    DIACHI NVARCHAR(255)
);

CREATE TABLE CHUCVU (
    IDCV CHAR(10) PRIMARY KEY NOT NULL,
    TENCV NVARCHAR(255) NOT NULL
);

CREATE TABLE PHONGBAN (
    IDPB CHAR(10) PRIMARY KEY NOT NULL,
    TENPB NVARCHAR(255) NOT NULL,
    DIACHI NVARCHAR(255),
    NGAYTHANHLAP DATE
);

CREATE TABLE TRINHDO (
    IDTD CHAR(10) PRIMARY KEY NOT NULL,
    TENTD NVARCHAR(255) NOT NULL,
    CHUYENNGANH NVARCHAR(255)
);

CREATE TABLE NHANVIEN (
    IDNV CHAR(10) PRIMARY KEY NOT NULL,
    TENNV NVARCHAR(255) NOT NULL,
    GIOITINH NVARCHAR(10),
    NGAYSINH DATE,
    CCCD CHAR(12),
    EMAIL VARCHAR(255),
    DIENTHOAI VARCHAR(15),
    DIACHI NVARCHAR(255),
    DANTOC NVARCHAR(50),
    TONGIAO NVARCHAR(50),
    HONNHAN NVARCHAR(50),
    TRINHDO CHAR(10) NOT NULL,
    CHUCVU CHAR(10) NOT NULL,
    PHONGBAN CHAR(10) NOT NULL,
    CHINHANH CHAR(10) NOT NULL,
    FOREIGN KEY (CHINHANH) REFERENCES CHINHANH(IDCN),
    FOREIGN KEY (CHUCVU) REFERENCES CHUCVU(IDCV),
    FOREIGN KEY (TRINHDO) REFERENCES TRINHDO(IDTD),
    FOREIGN KEY (PHONGBAN) REFERENCES PHONGBAN(IDPB)
);

CREATE TABLE LOAIHD (
    IDLOAI CHAR(10) PRIMARY KEY NOT NULL,
    TENLOAI NVARCHAR(255) NOT NULL,
    THOIHAN INT,
    BHYT NVARCHAR(11),
    BHXH NVARCHAR(11),
    BHTN NVARCHAR(11)
);

CREATE TABLE HOPDONG (
    SODH CHAR(10) PRIMARY KEY NOT NULL,
    NGAYKY DATE,
    NGAYBATDAU DATE,
    NGAYKETTHUC DATE,
    LUONGCOBAN DECIMAL(15, 2),
    TRANGTHAI VARCHAR(20),
    IDNV CHAR(10) NOT NULL,
    LOAIHD CHAR(10) NOT NULL,
    FOREIGN KEY (IDNV) REFERENCES NHANVIEN(IDNV),
    FOREIGN KEY (LOAIHD) REFERENCES LOAIHD(IDLOAI)
);

CREATE TABLE BANGCHAMCONG (
    IDBC CHAR(10) PRIMARY KEY NOT NULL,
    IDNV CHAR(10) NOT NULL,
    THANG TINYINT,
    NAM SMALLINT,
    SOGIOTANGCA DECIMAL(5,2),
    SONGAYNGHI INT,
    SONGAYDITRE INT,
    TONGNGAYLAM INT,
    TRANGTHAI NVARCHAR(50),
    FOREIGN KEY (IDNV) REFERENCES NHANVIEN(IDNV)
);

CREATE TABLE LOAICONG (
    IDLC CHAR(10) PRIMARY KEY NOT NULL,
    TENLC NVARCHAR(50) NOT NULL,
    MOTA NVARCHAR(255)
);

CREATE TABLE CHITIET_BANGCONG (
    IDCT CHAR(10) PRIMARY KEY NOT NULL,
    IDBC CHAR(10) NOT NULL,
    NGAYLAM INT,
    LOAICONG CHAR(10) NOT NULL,
    FOREIGN KEY (IDBC) REFERENCES BANGCHAMCONG(IDBC),
    FOREIGN KEY (LOAICONG) REFERENCES LOAICONG(IDLC)
);

CREATE TABLE BANGLUONG (
    IDBL CHAR(10) PRIMARY KEY NOT NULL,
    IDBC CHAR(10) NOT NULL,
    LUONGCOBAN DECIMAL(15,2),
    LUONGTHUCTE DECIMAL(15, 2),
    THUETNCN DECIMAL(10,2),
    LUONGTHUONG DECIMAL(15, 2),
    PHUCAPCHUCVU DECIMAL(15, 2),
    KHOANTRUBAOHIEM DECIMAL(15,2),
    PHUCAPKHAC DECIMAL(15, 2),
    KHOANTRUKHAC DECIMAL(15,2),
    THUCNHAN DECIMAL(15,2),
    FOREIGN KEY (IDBC) REFERENCES BANGCHAMCONG(IDBC)
);

CREATE TABLE UNGVIEN (
    IDUV CHAR(10) PRIMARY KEY,
    TENUV NVARCHAR(255),
    GIOITINH NVARCHAR(10),
    NGAYSINH DATE,
    CCCD CHAR(12),
    EMAIL VARCHAR(255),
    DIACHI NVARCHAR(255),
    DIENTHOAI VARCHAR(15),
    DANTOC NVARCHAR(50),
    TONGIAO NVARCHAR(50),
    HONNHAN NVARCHAR(50),
    TRINHDO CHAR(10),
    TRANGTHAI NVARCHAR(50),
    LUONG_DEAL DECIMAL(15,2),
    FOREIGN KEY (TRINHDO) REFERENCES TRINHDO(IDTD)
);

CREATE TABLE TUYENDUNG (
    MATD CHAR(10) PRIMARY KEY,
    IDCN CHAR(10),
    VITRITD NVARCHAR(255),
    DOTUOI INT,
    GIOITINH NVARCHAR(10),
    SOLUONG INT,
    HANTD DATE,
    LUONGTOITHIEU DECIMAL(10, 2),
    LUONGTOIDA DECIMAL(10, 2),
    SOHOSODANAOP INT,
    SOHOSODATUYEN INT,
    TRANGTHAI NVARCHAR(50),
    FOREIGN KEY (IDCN) REFERENCES CHINHANH(IDCN)
);

CREATE TABLE UNGVIEN_UNGTUYEN (
    IDUV CHAR(10),
    IDTD CHAR(10),
    NGAYUT DATE,
    PRIMARY KEY (IDUV, IDTD),
    FOREIGN KEY (IDUV) REFERENCES UNGVIEN(IDUV),
    FOREIGN KEY (IDTD) REFERENCES TUYENDUNG(MATD)
);

CREATE TABLE NHOMQUYEN (
    IDNQ CHAR(10) PRIMARY KEY,
    TENNHOMQUYEN VARCHAR(50),
    MOTA NVARCHAR(255)
);

CREATE TABLE TAIKHOAN (
    IDTK CHAR(10) PRIMARY KEY,
    TENTK VARCHAR(50),
    PASSWORD VARCHAR(255),
    QUYEN CHAR(10),
    IDNV CHAR(10) UNIQUE,
    FOREIGN KEY (IDNV) REFERENCES NHANVIEN(IDNV),
    FOREIGN KEY (QUYEN) REFERENCES NHOMQUYEN(IDNQ)
);
GO

-- ============================================================
--  INSERT DU LIEU
-- ============================================================

INSERT INTO CHINHANH (IDCN, TENCNHANH, HOTLINE, DIACHI) VALUES
('CN1', N'Chi nhánh Hà Nội',     '0281234567', N'123 Đường Láng, Đống Đa, Hà Nội'),
('CN2', N'Chi nhánh Đà Nẵng',    '0247654321', N'456 Nguyễn Văn Linh, Hải Châu, Đà Nẵng'),
('CN3', N'Chi nhánh Hồ Chí Minh','0236111222', N'789 Võ Văn Tần, Quận 3, TP. Hồ Chí Minh');
GO

INSERT INTO CHUCVU (IDCV, TENCV) VALUES
('CV001', N'Giám đốc'),
('CV002', N'Trưởng phòng'),
('CV003', N'Nhân viên'),
('CV004', N'Chuyên viên'),
('CV005', N'Thực tập sinh'),
('CV006', N'Quản lý'),
('CV007', N'Trưởng nhóm'),
('CV008', N'Phó giám đốc'),
('CV009', N'Kế toán trưởng'),
('CV010', N'Nhân viên hành chính'),
('CV011', N'Nhân viên IT'),
('CV012', N'Nhân viên kinh doanh');
GO

INSERT INTO TRINHDO (IDTD, TENTD, CHUYENNGANH) VALUES
('TD001', N'Đại học', N'Công nghệ thông tin'),
('TD002', N'Đại học', N'Quản trị kinh doanh'),
('TD003', N'Đại học', N'Kế toán'),
('TD004', N'Đại học', N'Marketing'),
('TD005', N'Cao đẳng', N'Quản trị văn phòng'),
('TD006', N'Cao đẳng', N'Kỹ thuật điện'),
('TD007', N'Thạc sĩ', N'Khoa học máy tính'),
('TD008', N'Tiến sĩ', N'Kinh tế học'),
('TD009', N'Cao đẳng', N'Du lịch'),
('TD010', N'Trung cấp', N'Công nghệ thông tin'),
('TD011', N'Đại học', N'Luật'),
('TD012', N'Đại học', N'Ngôn ngữ Anh');
GO

INSERT INTO PHONGBAN (IDPB, TENPB, DIACHI, NGAYTHANHLAP) VALUES
('PB_KD1',  N'Phòng Kinh doanh 1',    N'Tầng 2, 123 Đường Láng',         '2020-01-15'),
('PB_KT1',  N'Phòng Kỹ thuật 1',      N'Tầng 3, 123 Đường Láng',         '2020-02-20'),
('PB_KT2',  N'Phòng Kỹ thuật 2',      N'Tầng 1, 456 Nguyễn Văn Linh',    '2020-02-20'),
('PB_HC1',  N'Phòng Hành chính 1',    N'Tầng 1, 123 Đường Láng',         '2019-11-01'),
('PB_KD2',  N'Phòng Kinh doanh 2',    N'Lầu 2, 456 Nguyễn Văn Linh',     '2021-03-10'),
('PB_MKT2', N'Phòng Marketing 2',     N'Lầu 3, 456 Nguyễn Văn Linh',     '2021-05-01'),
('PB_NS2',  N'Phòng Nhân sự 2',       N'Lầu 1, 456 Nguyễn Văn Linh',     '2021-02-15'),
('PB_KD3',  N'Phòng Kinh doanh 3',    N'Lầu 5, 789 Võ Văn Tần',          '2018-07-01'),
('PB_KT3',  N'Phòng Kỹ thuật 3',      N'Lầu 6, 789 Võ Văn Tần',          '2018-08-10'),
('PB_TC3',  N'Phòng Tài chính 3',     N'Lầu 4, 789 Võ Văn Tần',          '2018-06-01'),
('PB_DA3',  N'Ban Quản lý Dự án 3',   N'Lầu 7, 789 Võ Văn Tần',          '2019-01-20');
GO

INSERT INTO NHANVIEN (IDNV, TENNV, GIOITINH, NGAYSINH, CCCD, EMAIL, DIENTHOAI, DIACHI, DANTOC, TONGIAO, HONNHAN, TRINHDO, CHUCVU, PHONGBAN, CHINHANH) VALUES
('NV001', N'Nguyễn Văn A',  N'Nam', '1990-05-10', '001123456789', 'anv@example.com', '0901112223', N'123 Đường A, Hà Nội',    N'Kinh', N'Không',          N'Đã kết hôn', 'TD001', 'CV001', 'PB_KD1',  'CN1'),
('NV002', N'Trần Thị B',    N'Nữ',  '1992-08-20', '001234567890', 'btt@example.com', '0904445556', N'456 Đường B, Hà Nội',    N'Kinh', N'Phật giáo',      N'Độc thân',   'TD002', 'CV002', 'PB_KD2',  'CN2'),
('NV003', N'Lê Văn C',      N'Nam', '1988-11-30', '001345678901', 'clv@example.com', '0907778889', N'789 Đường C, Đà Nẵng',   N'Kinh', N'Không',          N'Đã kết hôn', 'TD003', 'CV003', 'PB_KD3',  'CN3'),
('NV004', N'Phạm Thị D',    N'Nữ',  '1995-02-15', '001456789012', 'dtp@example.com', '0909990001', N'101 Đường D, Hà Nội',    N'Kinh', N'Thiên Chúa giáo',N'Độc thân',   'TD004', 'CV004', 'PB_MKT2', 'CN1'),
('NV005', N'Hoàng Văn E',   N'Nam', '1993-07-01', '001567890123', 'ehv@example.com', '0911112223', N'202 Đường E, TP.HCM',    N'Hoa',  N'Không',          N'Độc thân',   'TD001', 'CV003', 'PB_KT1',  'CN3'),
('NV006', N'Nguyễn Thị F',  N'Nữ',  '1991-04-25', '001678901234', 'fnt@example.com', '0914445556', N'303 Đường F, Hà Nội',    N'Kinh', N'Phật giáo',      N'Đã kết hôn', 'TD002', 'CV003', 'PB_KT3',  'CN2'),
('NV007', N'Trần Văn G',    N'Nam', '1996-09-05', '001789012345', 'gtv@example.com', '0917778889', N'404 Đường G, Đà Nẵng',   N'Kinh', N'Không',          N'Độc thân',   'TD003', 'CV004', 'PB_NS2',  'CN3'),
('NV008', N'Lê Thị H',      N'Nữ',  '1989-12-18', '001890123456', 'hlt@example.com', '0919990001', N'505 Đường H, TP.HCM',    N'Kinh', N'Thiên Chúa giáo',N'Đã kết hôn', 'TD004', 'CV003', 'PB_DA3',  'CN1'),
('NV009', N'Phạm Văn I',    N'Nam', '1994-06-14', '001901234567', 'ipv@example.com', '0931112223', N'606 Đường I, TP.HCM',    N'Chăm', N'Không',          N'Độc thân',   'TD001', 'CV003', 'PB_TC3',  'CN1'),
('NV010', N'Hoàng Thị K',   N'Nữ',  '1997-03-08', '002012345678', 'kht@example.com', '0934445556', N'707 Đường K, Hà Nội',    N'Kinh', N'Không',          N'Độc thân',   'TD002', 'CV005', 'PB_TC3',  'CN2'),
('NV011', N'Nguyễn Văn L',  N'Nam', '1985-01-22', '002123456789', 'lnv@example.com', '0937778889', N'808 Đường L, Đà Nẵng',   N'Kinh', N'Không',          N'Đã kết hôn', 'TD007', 'CV002', 'PB_KD2',  'CN3'),
('NV012', N'Trần Thị M',    N'Nữ',  '1998-11-11', '002234567890', 'mtt@example.com', '0939990001', N'909 Đường M, Đà Nẵng',   N'Kinh', N'Phật giáo',      N'Độc thân',   'TD005', 'CV003', 'PB_MKT2', 'CN2');
GO

INSERT INTO LOAIHD (IDLOAI, TENLOAI, THOIHAN, BHYT, BHXH, BHTN) VALUES
('HD001', N'Hợp đồng Không xác định thời hạn',      NULL, N'Có',    N'Có',    N'Có'),
('HD002', N'Hợp đồng Xác định thời hạn 12 tháng',   12,   N'Có',    N'Có',    N'Có'),
('HD003', N'Hợp đồng Xác định thời hạn 24 tháng',   24,   N'Có',    N'Có',    N'Có'),
('HD004', N'Hợp đồng Thử việc',                      2,    N'Không', N'Không', N'Không'),
('HD005', N'Hợp đồng Cộng tác viên',                NULL, N'Không', N'Không', N'Không'),
('HD006', N'Hợp đồng Học việc',                       3,   N'Không', N'Không', N'Không'),
('HD007', N'Hợp đồng Khoán việc',                   NULL, N'Không', N'Không', N'Không'),
('HD008', N'Hợp đồng Theo mùa vụ',                    6,   N'Có',    N'Có',    N'Không'),
('HD009', N'Hợp đồng Trọn gói',                     NULL, N'Có',    N'Có',    N'Có'),
('HD010', N'Hợp đồng Part-time',                    NULL, N'Có',    N'Có',    N'Có'),
('HD011', N'Hợp đồng Full-time',                    NULL, N'Có',    N'Có',    N'Có'),
('HD012', N'Hợp đồng Đào tạo nghề',                 NULL, N'Không', N'Không', N'Không');
GO

INSERT INTO HOPDONG (SODH, NGAYKY, NGAYBATDAU, NGAYKETTHUC, LUONGCOBAN, TRANGTHAI, IDNV, LOAIHD) VALUES
('HDNV001', '2023-01-01', '2023-01-01', NULL,         25000000.00, N'Có hiệu lực', 'NV001', 'HD001'),
('HDNV002', '2024-03-15', '2024-03-15', '2025-03-14', 18000000.00, N'Có hiệu lực', 'NV002', 'HD002'),
('HDNV003', '2023-07-01', '2023-07-01', NULL,         15000000.00, N'Có hiệu lực', 'NV003', 'HD001'),
('HDNV004', '2024-01-20', '2024-01-20', '2026-01-19', 16000000.00, N'Có hiệu lực', 'NV004', 'HD003'),
('HDNV005', '2024-05-10', '2024-05-10', '2024-08-09',  8000000.00, N'Có hiệu lực', 'NV005', 'HD004'),
('HDNV006', '2023-09-01', '2023-09-01', NULL,         14000000.00, N'Có hiệu lực', 'NV006', 'HD001'),
('HDNV007', '2024-02-28', '2024-02-28', '2025-02-27', 17000000.00, N'Có hiệu lực', 'NV007', 'HD002'),
('HDNV008', '2023-04-05', '2023-04-05', NULL,         19000000.00, N'Có hiệu lực', 'NV008', 'HD001'),
('HDNV009', '2024-06-01', '2024-06-01', '2026-05-31', 13000000.00, N'Có hiệu lực', 'NV009', 'HD003'),
('HDNV010', '2024-07-15', '2024-07-15', '2024-10-14',  7000000.00, N'Có hiệu lực', 'NV010', 'HD004'),
('HDNV011', '2023-02-10', '2023-02-10', NULL,         20000000.00, N'Có hiệu lực', 'NV011', 'HD001'),
('HDNV012', '2024-08-01', '2024-08-01', '2025-07-31', 10000000.00, N'Có hiệu lực', 'NV012', 'HD002');
GO

INSERT INTO BANGCHAMCONG (IDBC, IDNV, THANG, NAM, SOGIOTANGCA, SONGAYNGHI, SONGAYDITRE, TONGNGAYLAM, TRANGTHAI) VALUES
('BCC001', 'NV001', 1, 2024, 10.5, 1, 0, 25, N'Đã duyệt'),
('BCC002', 'NV002', 1, 2024,  5.0, 2, 1, 24, N'Đã duyệt'),
('BCC003', 'NV003', 1, 2024,  8.0, 0, 0, 26, N'Đã duyệt'),
('BCC004', 'NV004', 1, 2024,  2.5, 3, 2, 23, N'Đã duyệt'),
('BCC005', 'NV005', 1, 2024,  0.0, 1, 0, 25, N'Đã duyệt'),
('BCC006', 'NV006', 1, 2024,  6.0, 0, 1, 25, N'Đã duyệt'),
('BCC007', 'NV007', 1, 2024,  4.0, 2, 0, 24, N'Đã duyệt'),
('BCC008', 'NV008', 1, 2024,  9.0, 1, 3, 22, N'Đã duyệt'),
('BCC009', 'NV009', 1, 2024,  7.0, 0, 0, 26, N'Đã duyệt'),
('BCC010', 'NV010', 1, 2024,  1.0, 4, 0, 22, N'Đã duyệt'),
('BCC011', 'NV011', 1, 2024, 12.0, 0, 0, 26, N'Đã duyệt'),
('BCC012', 'NV012', 1, 2024,  3.0, 1, 5, 20, N'Đã duyệt');
GO

INSERT INTO LOAICONG (IDLC, TENLC, MOTA) VALUES
('DL',  N'Đi làm',           N'Ngày làm việc bình thường'),
('NP',  N'Nghỉ phép',        N'Ngày nghỉ có hưởng lương'),
('NKL', N'Nghỉ không lương', N'Ngày nghỉ không hưởng lương'),
('CT',  N'Công tác',         N'Đi công tác'),
('TC1', N'Tăng ca',          N'Tăng ca 1 tiếng'),
('TC2', N'Tăng ca',          N'Tăng ca 2 tiếng'),
('TC3', N'Tăng ca',          N'Tăng ca 3 tiếng'),
('TC4', N'Tăng ca',          N'Tăng ca 4 tiếng'),
('DT',  N'Đi trễ',           N'Đi làm muộn');
GO

INSERT INTO CHITIET_BANGCONG (IDCT, IDBC, NGAYLAM, LOAICONG) VALUES
('CTCC001', 'BCC001',  1, 'DL'),
('CTCC002', 'BCC001',  2, 'DL'),
('CTCC003', 'BCC002',  1, 'NP'),
('CTCC004', 'BCC002',  3, 'NKL'),
('CTCC005', 'BCC003',  5, 'CT'),
('CTCC006', 'BCC003',  6, 'DL'),
('CTCC007', 'BCC004', 10, 'DL'),
('CTCC008', 'BCC004', 11, 'DL'),
('CTCC009', 'BCC005', 15, 'DT'),
('CTCC010', 'BCC005', 16, 'DT'),
('CTCC011', 'BCC006', 20, 'DL'),
('CTCC012', 'BCC006', 21, 'TC2');
GO

INSERT INTO BANGLUONG (IDBL, IDBC, LUONGCOBAN, LUONGTHUCTE, THUETNCN, LUONGTHUONG, PHUCAPCHUCVU, KHOANTRUBAOHIEM, PHUCAPKHAC, KHOANTRUKHAC, THUCNHAN) VALUES
('BL001', 'BCC001', 25000000.00, 25000000.00*(25.0/26.0)+(10.5*(25000000.00/26/8)*1.5), 500000.00, 1000000.00, 3000000.00, 2000000.00, 500000.00, 0.00,    (25000000.00*(25.0/26.0)+(10.5*(25000000.00/26/8)*1.5))-500000.00+1000000.00+3000000.00-2000000.00+500000.00),
('BL002', 'BCC002', 18000000.00, 18000000.00*(24.0/26.0)+(5.0*(18000000.00/26/8)*1.5),  300000.00,  500000.00, 2000000.00, 1500000.00, 300000.00, 100000.00,(18000000.00*(24.0/26.0)+(5.0*(18000000.00/26/8)*1.5))-300000.00+500000.00+2000000.00-1500000.00+300000.00-100000.00),
('BL003', 'BCC003', 15000000.00, 15000000.00*(26.0/26.0)+(8.0*(15000000.00/26/8)*1.5),  200000.00,  800000.00,       0.00, 1200000.00, 200000.00,   0.00,   (15000000.00*(26.0/26.0)+(8.0*(15000000.00/26/8)*1.5))-200000.00+800000.00+0.00-1200000.00+200000.00),
('BL004', 'BCC004', 16000000.00, 16000000.00*(23.0/26.0)+(2.5*(16000000.00/26/8)*1.5),  250000.00,  300000.00, 1000000.00, 1300000.00, 100000.00,  50000.00,(16000000.00*(23.0/26.0)+(2.5*(16000000.00/26/8)*1.5))-250000.00+300000.00+1000000.00-1300000.00+100000.00-50000.00),
('BL005', 'BCC005',  8000000.00,  8000000.00*(25.0/26.0)+(0.0*(8000000.00/26/8)*1.5),        0.00,       0.00,       0.00,       0.00,      0.00,   0.00,   (8000000.00*(25.0/26.0))),
('BL006', 'BCC006', 14000000.00, 14000000.00*(25.0/26.0)+(6.0*(14000000.00/26/8)*1.5),  150000.00,  400000.00,       0.00, 1100000.00, 150000.00,   0.00,   (14000000.00*(25.0/26.0)+(6.0*(14000000.00/26/8)*1.5))-150000.00+400000.00+0.00-1100000.00+150000.00),
('BL007', 'BCC007', 17000000.00, 17000000.00*(24.0/26.0)+(4.0*(17000000.00/26/8)*1.5),  280000.00,  600000.00,       0.00, 1400000.00, 250000.00,   0.00,   (17000000.00*(24.0/26.0)+(4.0*(17000000.00/26/8)*1.5))-280000.00+600000.00+0.00-1400000.00+250000.00),
('BL008', 'BCC008', 19000000.00, 19000000.00*(22.0/26.0)+(9.0*(19000000.00/26/8)*1.5),  350000.00,  700000.00,       0.00, 1600000.00, 350000.00, 200000.00,(19000000.00*(22.0/26.0)+(9.0*(19000000.00/26/8)*1.5))-350000.00+700000.00+0.00-1600000.00+350000.00-200000.00),
('BL009', 'BCC009', 13000000.00, 13000000.00*(26.0/26.0)+(7.0*(13000000.00/26/8)*1.5),  100000.00,  300000.00,       0.00, 1000000.00, 100000.00,   0.00,   (13000000.00*(26.0/26.0)+(7.0*(13000000.00/26/8)*1.5))-100000.00+300000.00+0.00-1000000.00+100000.00),
('BL010', 'BCC010',  7000000.00,  7000000.00*(22.0/26.0)+(1.0*(7000000.00/26/8)*1.5),        0.00,       0.00,       0.00,       0.00,      0.00,   0.00,   (7000000.00*(22.0/26.0)+(1.0*(7000000.00/26/8)*1.5))),
('BL011', 'BCC011', 20000000.00, 20000000.00*(26.0/26.0)+(12.0*(20000000.00/26/8)*1.5), 400000.00,  900000.00, 2500000.00, 1800000.00, 400000.00,   0.00,   (20000000.00*(26.0/26.0)+(12.0*(20000000.00/26/8)*1.5))-400000.00+900000.00+2500000.00-1800000.00+400000.00),
('BL012', 'BCC012', 10000000.00, 10000000.00*(20.0/26.0)+(3.0*(10000000.00/26/8)*1.5),   50000.00,  200000.00,       0.00,  800000.00,  50000.00, 100000.00,(10000000.00*(20.0/26.0)+(3.0*(10000000.00/26/8)*1.5))-50000.00+200000.00+0.00-800000.00+50000.00-100000.00);
GO

INSERT INTO UNGVIEN (IDUV, TENUV, GIOITINH, NGAYSINH, CCCD, EMAIL, DIACHI, DIENTHOAI, DANTOC, TONGIAO, HONNHAN, TRINHDO, TRANGTHAI, LUONG_DEAL) VALUES
('UV001', N'Nguyễn Văn Z',  N'Nam', '1996-08-18', '003123456789', 'znv@example.com', N'789 Đường Z, TP.HCM',  '0941112223', N'Kinh',  N'Không',          N'Độc thân',   'TD001', N'Mới',          12000000.00),
('UV002', N'Trần Thị Y',    N'Nữ',  '1998-01-25', '003234567890', 'ytt@example.com', N'101 Đường Y, Hà Nội',  '0944445556', N'Kinh',  N'Phật giáo',      N'Độc thân',   'TD002', N'Đang xử lý',  10000000.00),
('UV003', N'Lê Văn X',      N'Nam', '1994-11-05', '003345678901', 'xlv@example.com', N'202 Đường X, Đà Nẵng', '0947778889', N'Kinh',  N'Không',          N'Đã kết hôn', 'TD003', N'Đã phỏng vấn',11000000.00),
('UV004', N'Phạm Thị W',    N'Nữ',  '1999-03-14', '003456789012', 'wtp@example.com', N'303 Đường W, Hà Nội',  '0949990001', N'Kinh',  N'Thiên Chúa giáo',N'Độc thân',   'TD004', N'Mới',          9000000.00),
('UV005', N'Hoàng Văn V',   N'Nam', '1997-07-22', '003567890123', 'vhv@example.com', N'404 Đường V, TP.HCM',  '0961112223', N'Hoa',   N'Không',          N'Độc thân',   'TD001', N'Đang xử lý',  13000000.00),
('UV006', N'Nguyễn Thị U',  N'Nữ',  '1995-05-01', '003678901234', 'unt@example.com', N'505 Đường U, Hà Nội',  '0964445556', N'Kinh',  N'Phật giáo',      N'Đã kết hôn', 'TD002', N'Đã phỏng vấn',10500000.00),
('UV007', N'Trần Văn T',    N'Nam', '1993-09-09', '003789012345', 'ttv@example.com', N'606 Đường T, Đà Nẵng', '0967778889', N'Kinh',  N'Không',          N'Độc thân',   'TD003', N'Mới',          11500000.00),
('UV008', N'Lê Thị S',      N'Nữ',  '1990-12-03', '003890123456', 'slt@example.com', N'707 Đường S, TP.HCM',  '0969990001', N'Kinh',  N'Thiên Chúa giáo',N'Đã kết hôn', 'TD004', N'Đang xử lý',   9500000.00),
('UV009', N'Phạm Văn R',    N'Nam', '1992-06-20', '003901234567', 'rpv@example.com', N'808 Đường R, TP.HCM',  '0981112223', N'Chăm',  N'Hồi giáo',       N'Độc thân',   'TD001', N'Đã phỏng vấn',12500000.00),
('UV010', N'Hoàng Thị Q',   N'Nữ',  '2000-04-11', '004012345678', 'qht@example.com', N'909 Đường Q, Hà Nội',  '0984445556', N'Kinh',  N'Không',          N'Độc thân',   'TD005', N'Mới',          8000000.00),
('UV011', N'Nguyễn Văn P',  N'Nam', '1987-02-28', '004123456789', 'pnv@example.com', N'111 Đường P, Đà Nẵng', '0987778889', N'Kinh',  N'Phật giáo',      N'Đã kết hôn', 'TD007', N'Đã phỏng vấn',18000000.00),
('UV012', N'Trần Thị O',    N'Nữ',  '1999-10-07', '004234567890', 'ott@example.com', N'222 Đường O, Đà Nẵng', '0989990001', N'Khmer', N'Phật giáo',      N'Độc thân',   'TD005', N'Đang xử lý',   8500000.00);
GO

INSERT INTO TUYENDUNG (MATD, IDCN, VITRITD, DOTUOI, GIOITINH, SOLUONG, HANTD, LUONGTOITHIEU, LUONGTOIDA, SOHOSODANAOP, SOHOSODATUYEN, TRANGTHAI) VALUES
('TD001', 'CN1', N'Nhân viên Kinh doanh',         22, N'Không', 5, '2024-12-31', 8000000.00,  15000000.00, 15, 3, N'Đang tuyển'),
('TD002', 'CN2', N'Chuyên viên Marketing',         25, N'Không', 3, '2024-11-30', 10000000.00, 18000000.00, 10, 2, N'Đang tuyển'),
('TD003', 'CN3', N'Nhân viên Kế toán',             22, N'Nữ',    2, '2024-10-31', 9000000.00,  14000000.00,  8, 1, N'Đang tuyển'),
('TD004', 'CN3', N'Nhân viên IT Helpdesk',         23, N'Nam',   2, '2024-12-15', 10000000.00, 16000000.00, 12, 1, N'Đang tuyển'),
('TD005', 'CN2', N'Thực tập sinh Nhân sự',         20, N'Không', 3, '2024-09-30', 3000000.00,   5000000.00, 20, 2, N'Đang tuyển'),
('TD006', 'CN3', N'Trưởng phòng Kinh doanh',       30, N'Không', 1, '2024-11-15', 20000000.00, 30000000.00,  5, 0, N'Đang tuyển'),
('TD007', 'CN1', N'Nhân viên Chăm sóc khách hàng', 22, N'Nữ',   4, '2024-12-01', 7000000.00,  12000000.00, 18, 3, N'Đang tuyển'),
('TD008', 'CN1', N'Nhân viên Hành chính văn phòng', 22, N'Không',2, '2024-10-20', 8000000.00,  13000000.00,  9, 1, N'Đang tuyển'),
('TD009', 'CN2', N'Nhân viên Phát triển phần mềm', 24, N'Nam',  3, '2024-11-10', 15000000.00, 25000000.00, 11, 1, N'Đang tuyển'),
('TD010', 'CN1', N'Thiết kế đồ họa',               23, N'Không', 2, '2024-12-25', 10000000.00, 17000000.00,  7, 1, N'Đang tuyển'),
('TD011', 'CN2', N'Nhân viên Digital Marketing',   24, N'Không', 3, '2024-11-20', 11000000.00, 19000000.00, 14, 2, N'Đang tuyển'),
('TD012', 'CN2', N'Nhân viên Tư vấn tài chính',    25, N'Không', 4, '2024-12-10', 9000000.00,  18000000.00, 16, 2, N'Đang tuyển');
GO

INSERT INTO UNGVIEN_UNGTUYEN (IDUV, IDTD, NGAYUT) VALUES
('UV001', 'TD001', '2024-08-01'), ('UV002', 'TD002', '2024-08-05'),
('UV003', 'TD003', '2024-08-02'), ('UV004', 'TD001', '2024-08-03'),
('UV005', 'TD004', '2024-08-06'), ('UV006', 'TD002', '2024-08-07'),
('UV007', 'TD001', '2024-08-04'), ('UV008', 'TD003', '2024-08-08'),
('UV009', 'TD004', '2024-08-09'), ('UV010', 'TD005', '2024-08-10'),
('UV001', 'TD006', '2024-08-11'), ('UV002', 'TD007', '2024-08-12');
GO

INSERT INTO NHOMQUYEN (IDNQ, TENNHOMQUYEN, MOTA) VALUES
('NQ001', 'Admin',           N'Toàn quyền quản trị hệ thống'),
('NQ002', 'HR Manager',      N'Quản lý các chức năng liên quan đến Nhân sự và Lương'),
('NQ003', 'Employee',        N'Quyền xem thông tin cá nhân, chấm công, hợp đồng'),
('NQ004', 'Department Head', N'Quản lý, xem và duyệt thông tin nhân viên trong phòng ban mình'),
('NQ005', 'Payroll Officer', N'Xử lý, tính toán và quản lý bảng lương'),
('NQ006', 'Branch Manager',  N'Quản lý các hoạt động và nhân viên tại chi nhánh'),
('NQ007', 'Recruiter',       N'Quản lý quy trình tuyển dụng và thông tin ứng viên'),
('NQ008', 'IT Support',      N'Hỗ trợ kỹ thuật và quản lý tài khoản người dùng'),
('NQ009', 'Read Only',       N'Chỉ có quyền xem dữ liệu ở một số mục'),
('NQ010', 'Guest',           N'Quyền truy cập rất hạn chế'),
('NQ011', 'Finance Manager', N'Quản lý các giao dịch tài chính, báo cáo liên quan đến lương'),
('NQ012', 'Supervisor',      N'Giám sát và đánh giá công việc của nhân viên cấp dưới');
GO

INSERT INTO TAIKHOAN (IDTK, TENTK, PASSWORD, QUYEN, IDNV) VALUES
('TK001', 'anv_admin',     'hashed_pw_1',  'NQ001', 'NV001'),
('TK002', 'btt_hr',        'hashed_pw_2',  'NQ002', 'NV002'),
('TK003', 'clv_acc',       'hashed_pw_3',  'NQ003', 'NV003'),
('TK004', 'dtp_mkt',       'hashed_pw_4',  'NQ004', 'NV004'),
('TK005', 'ehv_tech',      'hashed_pw_5',  'NQ003', 'NV005'),
('TK006', 'fnt_cskh',      'hashed_pw_6',  'NQ003', 'NV006'),
('TK007', 'gtv_pd',        'hashed_pw_7',  'NQ003', 'NV007'),
('TK008', 'hlt_it',        'hashed_pw_8',  'NQ008', 'NV008'),
('TK009', 'ipv_legal',     'hashed_pw_9',  'NQ003', 'NV009'),
('TK010', 'kht_intern',    'hashed_pw_10', 'NQ003', 'NV010'),
('TK011', 'lnv_techlead',  'hashed_pw_11', 'NQ004', 'NV011');
GO

-- ============================================================
--  TAO USER KET NOI (dung cho Linked Server tu client)
-- ============================================================

USE master;
GO
CREATE LOGIN ket_noi WITH PASSWORD = 'KetNoi@123', CHECK_POLICY = OFF;
GO
USE QuanLyNhanSu;
GO
CREATE USER ket_noi FOR LOGIN ket_noi;
ALTER ROLE db_owner ADD MEMBER ket_noi;
GO

-- ============================================================
--  TAO LINKED SERVER tro den CLIENT1, CLIENT2, CLIENT3
-- ============================================================

USE master;
GO

-- Linked Server -> CLIENT1 (CN1 - Ha Noi)
IF EXISTS (SELECT name FROM sys.servers WHERE name = 'QLNS_CN1')
    EXEC sp_dropserver 'QLNS_CN1', 'droplogins';
GO
EXEC sp_addlinkedserver
    @server     = 'QLNS_CN1',
    @srvproduct = '',
    @provider   = 'MSOLEDBSQL',
    @datasrc    = '192.168.99.11,1433';
GO
EXEC sp_addlinkedsrvlogin
    @rmtsrvname  = 'QLNS_CN1',
    @useself     = 'FALSE',
    @rmtuser     = 'sa',
    @rmtpassword = 'KetNoi@123';
GO

-- Linked Server -> CLIENT2 (CN2 - Da Nang)
IF EXISTS (SELECT name FROM sys.servers WHERE name = 'QLNS_CN2')
    EXEC sp_dropserver 'QLNS_CN2', 'droplogins';
GO
EXEC sp_addlinkedserver
    @server     = 'QLNS_CN2',
    @srvproduct = '',
    @provider   = 'MSOLEDBSQL',
    @datasrc    = '192.168.99.12,1433';
GO
EXEC sp_addlinkedsrvlogin
    @rmtsrvname  = 'QLNS_CN2',
    @useself     = 'FALSE',
    @rmtuser     = 'sa',
    @rmtpassword = 'KetNoi@123';
GO

-- Linked Server -> CLIENT3 (CN3 - TP.HCM)
IF EXISTS (SELECT name FROM sys.servers WHERE name = 'QLNS_CN3')
    EXEC sp_dropserver 'QLNS_CN3', 'droplogins';
GO
EXEC sp_addlinkedserver
    @server     = 'QLNS_CN3',
    @srvproduct = '',
    @provider   = 'MSOLEDBSQL',
    @datasrc    = '192.168.99.13,1433';
GO
EXEC sp_addlinkedsrvlogin
    @rmtsrvname  = 'QLNS_CN3',
    @useself     = 'FALSE',
    @rmtuser     = 'sa',
    @rmtpassword = 'KetNoi@123';
GO

PRINT N'=== SERVER GOC (MASTER) da khoi tao xong! ===';
GO
