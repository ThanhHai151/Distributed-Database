"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const faker_1 = require("@faker-js/faker");
const database_1 = require("./config/database");
const mssql_1 = __importDefault(require("mssql"));
async function main() {
    console.log('🔌 Connecting to databases for seeding...');
    await database_1.db.connectAll();
    const branches = database_1.db.getAllBranchNodeIds();
    if (branches.length === 0) {
        console.error('❌ No branch nodes are configured. Please run the server and add nodes first.');
        process.exit(1);
    }
    for (const nodeId of branches) {
        if (!database_1.db.isOnline(nodeId)) {
            console.warn(`⏭️  Skipping offline node ${nodeId}`);
            continue;
        }
        const pool = await database_1.db.getPoolOrThrow(nodeId);
        const info = database_1.db.nodes[nodeId];
        console.log(`\n🌱 Seeding branch ${info.branch} on node ${nodeId}...`);
        // Fetch existing lookup data for foreign keys
        const chucvu = (await pool.request().query('SELECT IDCV FROM CHUCVU')).recordset.map(r => r.IDCV);
        const trinhdo = (await pool.request().query('SELECT IDTD FROM TRINHDO')).recordset.map(r => r.IDTD);
        const phongban = (await pool.request().query('SELECT IDPB FROM PHONGBAN')).recordset.map(r => r.IDPB);
        const loaihd = (await pool.request().query('SELECT IDLOAI FROM LOAIHD')).recordset.map(r => r.IDLOAI);
        if (!chucvu.length || !trinhdo.length || !phongban.length || !loaihd.length) {
            console.warn(`⏭️  Skipping ${nodeId} due to missing lookup tables (CHUCVU, TRINHDO, PHONGBAN, LOAIHD)`);
            continue;
        }
        const branchEmpCount = 50;
        for (let i = 0; i < branchEmpCount; i++) {
            // 1. NHANVIEN
            const suffix = Math.floor(Math.random() * 100000).toString().padStart(5, '0');
            const shortPrefix = info.branch.replace('CN', 'C');
            const idnv = `N${shortPrefix}${suffix}`.substring(0, 8); // Max 8 chars: "NC112345"
            const ten = faker_1.fakerVI.person.fullName();
            const gioiTinh = faker_1.fakerVI.helpers.arrayElement(['Nam', 'Nữ']);
            const ngaySinh = faker_1.fakerVI.date.birthdate({ min: 18, max: 60, mode: 'age' });
            await pool.request()
                .input('IDNV', mssql_1.default.VarChar, idnv)
                .input('TENNV', mssql_1.default.NVarChar, ten)
                .input('GIOITINH', mssql_1.default.NVarChar, gioiTinh)
                .input('NGAYSINH', mssql_1.default.Date, ngaySinh)
                .input('CCCD', mssql_1.default.VarChar, faker_1.fakerVI.string.numeric(12))
                .input('EMAIL', mssql_1.default.VarChar, faker_1.fakerVI.internet.email())
                .input('DIENTHOAI', mssql_1.default.VarChar, faker_1.fakerVI.phone.number())
                .input('DIACHI', mssql_1.default.NVarChar, faker_1.fakerVI.location.streetAddress())
                .input('DANTOC', mssql_1.default.NVarChar, 'Kinh')
                .input('TONGIAO', mssql_1.default.NVarChar, 'Không')
                .input('HONNHAN', mssql_1.default.NVarChar, faker_1.fakerVI.helpers.arrayElement(['Độc thân', 'Đã kết hôn']))
                .input('TRINHDO', mssql_1.default.VarChar, faker_1.fakerVI.helpers.arrayElement(trinhdo))
                .input('CHUCVU', mssql_1.default.VarChar, faker_1.fakerVI.helpers.arrayElement(chucvu))
                .input('PHONGBAN', mssql_1.default.VarChar, faker_1.fakerVI.helpers.arrayElement(phongban))
                .input('CHINHANH', mssql_1.default.Char, info.branch)
                .query(`
          INSERT INTO NHANVIEN (IDNV, TENNV, GIOITINH, NGAYSINH, CCCD, EMAIL, DIENTHOAI, DIACHI, DANTOC, TONGIAO, HONNHAN, TRINHDO, CHUCVU, PHONGBAN, CHINHANH)
          VALUES (@IDNV, @TENNV, @GIOITINH, @NGAYSINH, @CCCD, @EMAIL, @DIENTHOAI, @DIACHI, @DANTOC, @TONGIAO, @HONNHAN, @TRINHDO, @CHUCVU, @PHONGBAN, @CHINHANH)
        `);
            // 2. HOPDONG
            // HĐ length max 10
            const soHD = `H${idnv.substring(1)}`.substring(0, 10);
            const ngayKy = faker_1.fakerVI.date.recent({ days: 365 });
            const lc = faker_1.fakerVI.number.int({ min: 5, max: 30 }) * 1000000;
            await pool.request()
                .input('SODH', mssql_1.default.VarChar, soHD)
                .input('NGAYKY', mssql_1.default.Date, ngayKy)
                .input('NGAYBATDAU', mssql_1.default.Date, ngayKy)
                .input('NGAYKETTHUC', mssql_1.default.Date, faker_1.fakerVI.date.future({ years: 2, refDate: ngayKy }))
                .input('LUONGCOBAN', mssql_1.default.Float, lc)
                .input('TRANGTHAI', mssql_1.default.NVarChar, 'Có hiệu lực')
                .input('IDNV', mssql_1.default.VarChar, idnv)
                .input('LOAIHD', mssql_1.default.VarChar, faker_1.fakerVI.helpers.arrayElement(loaihd))
                .query(`
          INSERT INTO HOPDONG (SODH, NGAYKY, NGAYBATDAU, NGAYKETTHUC, LUONGCOBAN, TRANGTHAI, IDNV, LOAIHD)
          VALUES (@SODH, @NGAYKY, @NGAYBATDAU, @NGAYKETTHUC, @LUONGCOBAN, @TRANGTHAI, @IDNV, @LOAIHD)
        `);
            // 3. BANGCHAMCONG & BANGLUONG
            for (let month = 1; month <= 3; month++) {
                const idbc = `B${idnv.substring(1)}${month}`.substring(0, 10);
                const ngayNghi = faker_1.fakerVI.number.int({ min: 0, max: 3 });
                const diTre = faker_1.fakerVI.number.int({ min: 0, max: 2 });
                await pool.request()
                    .input('IDBC', mssql_1.default.VarChar, idbc)
                    .input('IDNV', mssql_1.default.VarChar, idnv)
                    .input('THANG', mssql_1.default.Int, month)
                    .input('NAM', mssql_1.default.Int, 2024)
                    .input('SOGIOTANGCA', mssql_1.default.Int, faker_1.fakerVI.number.int({ min: 0, max: 20 }))
                    .input('SONGAYNGHI', mssql_1.default.Int, ngayNghi)
                    .input('SONGAYDITRE', mssql_1.default.Int, diTre)
                    .input('TONGNGAYLAM', mssql_1.default.Int, 22 - ngayNghi)
                    .input('TRANGTHAI', mssql_1.default.NVarChar, 'Đã chốt')
                    .query(`
            INSERT INTO BANGCHAMCONG (IDBC, IDNV, THANG, NAM, SOGIOTANGCA, SONGAYNGHI, SONGAYDITRE, TONGNGAYLAM, TRANGTHAI)
            VALUES (@IDBC, @IDNV, @THANG, @NAM, @SOGIOTANGCA, @SONGAYNGHI, @SONGAYDITRE, @TONGNGAYLAM, @TRANGTHAI)
          `);
                const idbl = `L${idbc.substring(1)}`.substring(0, 10);
                await pool.request()
                    .input('IDBL', mssql_1.default.VarChar, idbl)
                    .input('IDBC', mssql_1.default.VarChar, idbc)
                    .input('LUONGCOBAN', mssql_1.default.Float, lc)
                    .input('LUONGTHUCTE', mssql_1.default.Float, lc * ((22 - ngayNghi) / 22))
                    .input('THUETNCN', mssql_1.default.Float, 0)
                    .input('LUONGTHUONG', mssql_1.default.Float, faker_1.fakerVI.number.int({ min: 0, max: 2 }) * 1000000)
                    .input('PHUCAPCHUCVU', mssql_1.default.Float, 500000)
                    .input('KHOANTRUBAOHIEM', mssql_1.default.Float, lc * 0.105)
                    .input('PHUCAPKHAC', mssql_1.default.Float, 0)
                    .input('KHOANTRUKHAC', mssql_1.default.Float, diTre * 100000)
                    .input('THUCNHAN', mssql_1.default.Float, (lc * ((22 - ngayNghi) / 22)) + 500000 - (lc * 0.105) - (diTre * 100000))
                    .query(`
            INSERT INTO BANGLUONG (IDBL, IDBC, LUONGCOBAN, LUONGTHUCTE, THUETNCN, LUONGTHUONG, PHUCAPCHUCVU, KHOANTRUBAOHIEM, PHUCAPKHAC, KHOANTRUKHAC, THUCNHAN)
            VALUES (@IDBL, @IDBC, @LUONGCOBAN, @LUONGTHUCTE, @THUETNCN, @LUONGTHUONG, @PHUCAPCHUCVU, @KHOANTRUBAOHIEM, @PHUCAPKHAC, @KHOANTRUKHAC, @THUCNHAN)
          `);
            }
        }
        // 4. TUYENDUNG
        for (let j = 0; j < 5; j++) {
            const pfx = info.branch.replace('CN', 'T');
            const matd = `${pfx}${Math.floor(Math.random() * 100000)}`.substring(0, 10);
            const sl = faker_1.fakerVI.number.int({ min: 1, max: 10 });
            await pool.request()
                .input('MATD', mssql_1.default.VarChar, matd)
                .input('IDCN', mssql_1.default.Char, info.branch)
                .input('VITRITD', mssql_1.default.NVarChar, faker_1.fakerVI.person.jobTitle().substring(0, 50))
                .input('DOTUOI', mssql_1.default.Int, 18)
                .input('GIOITINH', mssql_1.default.NVarChar, 'Bất kỳ')
                .input('SOLUONG', mssql_1.default.Int, sl)
                .input('HANTD', mssql_1.default.Date, faker_1.fakerVI.date.future())
                .input('LUONGTOITHIEU', mssql_1.default.Float, faker_1.fakerVI.number.int({ min: 5, max: 10 }) * 1000000)
                .input('LUONGTOIDA', mssql_1.default.Float, faker_1.fakerVI.number.int({ min: 10, max: 30 }) * 1000000)
                .input('SOHOSODANAOP', mssql_1.default.Int, faker_1.fakerVI.number.int({ min: 0, max: sl * 3 }))
                .input('SOHOSODATUYEN', mssql_1.default.Int, faker_1.fakerVI.number.int({ min: 0, max: sl }))
                .input('TRANGTHAI', mssql_1.default.NVarChar, 'Đang tuyển')
                .query(`
          INSERT INTO TUYENDUNG (MATD, IDCN, VITRITD, DOTUOI, GIOITINH, SOLUONG, HANTD, LUONGTOITHIEU, LUONGTOIDA, SOHOSODANAOP, SOHOSODATUYEN, TRANGTHAI)
          VALUES (@MATD, @IDCN, @VITRITD, @DOTUOI, @GIOITINH, @SOLUONG, @HANTD, @LUONGTOITHIEU, @LUONGTOIDA, @SOHOSODANAOP, @SOHOSODATUYEN, @TRANGTHAI)
        `);
        }
        console.log(`✅ Seeded branch ${info.branch} successfully!`);
    }
    process.exit(0);
}
main().catch(err => {
    console.error(err);
    process.exit(1);
});
//# sourceMappingURL=seed.js.map