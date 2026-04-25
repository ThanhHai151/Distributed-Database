import { fakerVI as faker } from '@faker-js/faker';
import { db } from './config/database';
import sql from 'mssql';

async function main() {
  console.log('🔌 Connecting to databases for seeding...');
  await db.connectAll();

  const branches = db.getAllBranchNodeIds();
  if (branches.length === 0) {
    console.error('❌ No branch nodes are configured. Please run the server and add nodes first.');
    process.exit(1);
  }

  for (const nodeId of branches) {
    if (!db.isOnline(nodeId)) {
      console.warn(`⏭️  Skipping offline node ${nodeId}`);
      continue;
    }
    const pool = await db.getPoolOrThrow(nodeId);
    const info = db.nodes[nodeId];
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
      const ten = faker.person.fullName();
      const gioiTinh = faker.helpers.arrayElement(['Nam', 'Nữ']);
      const ngaySinh = faker.date.birthdate({ min: 18, max: 60, mode: 'age' });
      
      await pool.request()
        .input('IDNV', sql.VarChar, idnv)
        .input('TENNV', sql.NVarChar, ten)
        .input('GIOITINH', sql.NVarChar, gioiTinh)
        .input('NGAYSINH', sql.Date, ngaySinh)
        .input('CCCD', sql.VarChar, faker.string.numeric(12))
        .input('EMAIL', sql.VarChar, faker.internet.email())
        .input('DIENTHOAI', sql.VarChar, faker.phone.number())
        .input('DIACHI', sql.NVarChar, faker.location.streetAddress())
        .input('DANTOC', sql.NVarChar, 'Kinh')
        .input('TONGIAO', sql.NVarChar, 'Không')
        .input('HONNHAN', sql.NVarChar, faker.helpers.arrayElement(['Độc thân', 'Đã kết hôn']))
        .input('TRINHDO', sql.VarChar, faker.helpers.arrayElement(trinhdo))
        .input('CHUCVU', sql.VarChar, faker.helpers.arrayElement(chucvu))
        .input('PHONGBAN', sql.VarChar, faker.helpers.arrayElement(phongban))
        .input('CHINHANH', sql.Char, info.branch)
        .query(`
          INSERT INTO NHANVIEN (IDNV, TENNV, GIOITINH, NGAYSINH, CCCD, EMAIL, DIENTHOAI, DIACHI, DANTOC, TONGIAO, HONNHAN, TRINHDO, CHUCVU, PHONGBAN, CHINHANH)
          VALUES (@IDNV, @TENNV, @GIOITINH, @NGAYSINH, @CCCD, @EMAIL, @DIENTHOAI, @DIACHI, @DANTOC, @TONGIAO, @HONNHAN, @TRINHDO, @CHUCVU, @PHONGBAN, @CHINHANH)
        `);

      // 2. HOPDONG
      // HĐ length max 10
      const soHD = `H${idnv.substring(1)}`.substring(0, 10);
      const ngayKy = faker.date.recent({ days: 365 });
      const lc = faker.number.int({ min: 5, max: 30 }) * 1000000;
      await pool.request()
        .input('SODH', sql.VarChar, soHD)
        .input('NGAYKY', sql.Date, ngayKy)
        .input('NGAYBATDAU', sql.Date, ngayKy)
        .input('NGAYKETTHUC', sql.Date, faker.date.future({ years: 2, refDate: ngayKy }))
        .input('LUONGCOBAN', sql.Float, lc)
        .input('TRANGTHAI', sql.NVarChar, 'Có hiệu lực')
        .input('IDNV', sql.VarChar, idnv)
        .input('LOAIHD', sql.VarChar, faker.helpers.arrayElement(loaihd))
        .query(`
          INSERT INTO HOPDONG (SODH, NGAYKY, NGAYBATDAU, NGAYKETTHUC, LUONGCOBAN, TRANGTHAI, IDNV, LOAIHD)
          VALUES (@SODH, @NGAYKY, @NGAYBATDAU, @NGAYKETTHUC, @LUONGCOBAN, @TRANGTHAI, @IDNV, @LOAIHD)
        `);

      // 3. BANGCHAMCONG & BANGLUONG
      for (let month = 1; month <= 3; month++) {
        const idbc = `B${idnv.substring(1)}${month}`.substring(0, 10);
        const ngayNghi = faker.number.int({ min: 0, max: 3 });
        const diTre = faker.number.int({ min: 0, max: 2 });
        await pool.request()
          .input('IDBC', sql.VarChar, idbc)
          .input('IDNV', sql.VarChar, idnv)
          .input('THANG', sql.Int, month)
          .input('NAM', sql.Int, 2024)
          .input('SOGIOTANGCA', sql.Int, faker.number.int({ min: 0, max: 20 }))
          .input('SONGAYNGHI', sql.Int, ngayNghi)
          .input('SONGAYDITRE', sql.Int, diTre)
          .input('TONGNGAYLAM', sql.Int, 22 - ngayNghi)
          .input('TRANGTHAI', sql.NVarChar, 'Đã chốt')
          .query(`
            INSERT INTO BANGCHAMCONG (IDBC, IDNV, THANG, NAM, SOGIOTANGCA, SONGAYNGHI, SONGAYDITRE, TONGNGAYLAM, TRANGTHAI)
            VALUES (@IDBC, @IDNV, @THANG, @NAM, @SOGIOTANGCA, @SONGAYNGHI, @SONGAYDITRE, @TONGNGAYLAM, @TRANGTHAI)
          `);

        const idbl = `L${idbc.substring(1)}`.substring(0, 10);
        await pool.request()
          .input('IDBL', sql.VarChar, idbl)
          .input('IDBC', sql.VarChar, idbc)
          .input('LUONGCOBAN', sql.Float, lc)
          .input('LUONGTHUCTE', sql.Float, lc * ((22 - ngayNghi) / 22))
          .input('THUETNCN', sql.Float, 0)
          .input('LUONGTHUONG', sql.Float, faker.number.int({ min: 0, max: 2 }) * 1000000)
          .input('PHUCAPCHUCVU', sql.Float, 500000)
          .input('KHOANTRUBAOHIEM', sql.Float, lc * 0.105)
          .input('PHUCAPKHAC', sql.Float, 0)
          .input('KHOANTRUKHAC', sql.Float, diTre * 100000)
          .input('THUCNHAN', sql.Float, (lc * ((22 - ngayNghi) / 22)) + 500000 - (lc * 0.105) - (diTre * 100000))
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
      const sl = faker.number.int({ min: 1, max: 10 });
      await pool.request()
        .input('MATD', sql.VarChar, matd)
        .input('IDCN', sql.Char, info.branch)
        .input('VITRITD', sql.NVarChar, faker.person.jobTitle().substring(0, 50))
        .input('DOTUOI', sql.Int, 18)
        .input('GIOITINH', sql.NVarChar, 'Bất kỳ')
        .input('SOLUONG', sql.Int, sl)
        .input('HANTD', sql.Date, faker.date.future())
        .input('LUONGTOITHIEU', sql.Float, faker.number.int({ min: 5, max: 10 }) * 1000000)
        .input('LUONGTOIDA', sql.Float, faker.number.int({ min: 10, max: 30 }) * 1000000)
        .input('SOHOSODANAOP', sql.Int, faker.number.int({ min: 0, max: sl * 3 }))
        .input('SOHOSODATUYEN', sql.Int, faker.number.int({ min: 0, max: sl }))
        .input('TRANGTHAI', sql.NVarChar, 'Đang tuyển')
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
