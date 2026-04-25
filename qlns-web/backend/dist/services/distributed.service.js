"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.getAllEmployees = getAllEmployees;
exports.getEmployeeById = getEmployeeById;
exports.createEmployee = createEmployee;
exports.updateEmployee = updateEmployee;
exports.deleteEmployee = deleteEmployee;
exports.getSalaries = getSalaries;
exports.getAttendance = getAttendance;
exports.getContracts = getContracts;
exports.getRecruitments = getRecruitments;
exports.getRecruitmentById = getRecruitmentById;
exports.createRecruitment = createRecruitment;
exports.updateRecruitment = updateRecruitment;
exports.deleteRecruitment = deleteRecruitment;
exports.getGlobalStats = getGlobalStats;
exports.getBranches = getBranches;
exports.getPositions = getPositions;
exports.getEducations = getEducations;
exports.getDepartments = getDepartments;
const mssql_1 = __importDefault(require("mssql"));
const database_1 = require("../config/database");
// ============================================================
// DISTRIBUTED QUERY SERVICE
// Handles all distributed database operations
// Supports 3 modes: linked | direct | replication
// ============================================================
const QUERY_MODE = (process.env.QUERY_MODE || 'direct');
// ── Helper: run query on a node, return [] if offline ───────
async function queryNode(nodeId, queryFn) {
    try {
        const pool = await database_1.db.getPoolOrThrow(nodeId);
        const result = await queryFn(pool);
        return { rows: result.recordset, nodeId, success: true };
    }
    catch (err) {
        console.warn(`[DistQuery] Node ${nodeId} failed: ${err.message}`);
        return { rows: [], nodeId, success: false };
    }
}
// ── Helper: collect from all branch nodes concurrently ──────
async function queryAllBranches(queryFn) {
    const branchNodes = database_1.db.getAllBranchNodeIds();
    const results = await Promise.all(branchNodes.map((n) => queryNode(n, (pool) => queryFn(pool, n))));
    const rows = [];
    const sourceNodes = [];
    for (const r of results) {
        if (r.success) {
            rows.push(...r.rows.map((row) => ({ ...row, _sourceNode: r.nodeId })));
            sourceNodes.push(r.nodeId);
        }
    }
    return { rows, sourceNodes };
}
// ============================================================
// EMPLOYEES SERVICE
// ============================================================
async function getAllEmployees(filterBranch) {
    const baseQuery = `
    SELECT nv.*, cv.TENCV AS TEN_CHUCVU, td.TENTD AS TEN_TRINHDO,
           td.CHUYENNGANH, pb.TENPB AS TEN_PHONGBAN, cn.TENCNHANH AS TEN_CHINHANH
    FROM NHANVIEN nv
    LEFT JOIN CHUCVU cv ON nv.CHUCVU = cv.IDCV
    LEFT JOIN TRINHDO td ON nv.TRINHDO = td.IDTD
    LEFT JOIN PHONGBAN pb ON nv.PHONGBAN = pb.IDPB
    LEFT JOIN CHINHANH cn ON nv.CHINHANH = cn.IDCN
    WHERE nv.IsDeleted = 0 OR nv.IsDeleted IS NULL
  `;
    if (filterBranch) {
        // Query specific branch node directly
        const nodeId = database_1.db.getNodeForBranch(filterBranch);
        const result = await queryNode(nodeId, (pool) => pool.request().query(baseQuery));
        return { data: result.rows.map(r => ({ ...r, _sourceNode: result.nodeId })), sourceNodes: [nodeId] };
    }
    if (QUERY_MODE === 'linked') {
        // Use Linked Servers from Master
        const masterPool = await database_1.db.getPoolOrThrow('master');
        const result = await masterPool.request().query(`
      SELECT nv.*, cv.TENCV AS TEN_CHUCVU, td.TENTD AS TEN_TRINHDO, td.CHUYENNGANH,
             pb.TENPB AS TEN_PHONGBAN, cn.TENCNHANH AS TEN_CHINHANH, 'CN1' AS _src
      FROM QLNS_CN1.QuanLyNhanSu.dbo.NHANVIEN nv
      LEFT JOIN QLNS_CN1.QuanLyNhanSu.dbo.CHUCVU cv ON nv.CHUCVU = cv.IDCV
      LEFT JOIN QLNS_CN1.QuanLyNhanSu.dbo.TRINHDO td ON nv.TRINHDO = td.IDTD
      LEFT JOIN QLNS_CN1.QuanLyNhanSu.dbo.PHONGBAN pb ON nv.PHONGBAN = pb.IDPB
      LEFT JOIN QLNS_CN1.QuanLyNhanSu.dbo.CHINHANH cn ON nv.CHINHANH = cn.IDCN
      WHERE nv.IsDeleted = 0 OR nv.IsDeleted IS NULL
      UNION ALL
      SELECT nv.*, cv.TENCV, td.TENTD, td.CHUYENNGANH, pb.TENPB, cn.TENCNHANH, 'CN2'
      FROM QLNS_CN2.QuanLyNhanSu.dbo.NHANVIEN nv
      LEFT JOIN QLNS_CN2.QuanLyNhanSu.dbo.CHUCVU cv ON nv.CHUCVU = cv.IDCV
      LEFT JOIN QLNS_CN2.QuanLyNhanSu.dbo.TRINHDO td ON nv.TRINHDO = td.IDTD
      LEFT JOIN QLNS_CN2.QuanLyNhanSu.dbo.PHONGBAN pb ON nv.PHONGBAN = pb.IDPB
      LEFT JOIN QLNS_CN2.QuanLyNhanSu.dbo.CHINHANH cn ON nv.CHINHANH = cn.IDCN
      WHERE nv.IsDeleted = 0 OR nv.IsDeleted IS NULL
      UNION ALL
      SELECT nv.*, cv.TENCV, td.TENTD, td.CHUYENNGANH, pb.TENPB, cn.TENCNHANH, 'CN3'
      FROM QLNS_CN3.QuanLyNhanSu.dbo.NHANVIEN nv
      LEFT JOIN QLNS_CN3.QuanLyNhanSu.dbo.CHUCVU cv ON nv.CHUCVU = cv.IDCV
      LEFT JOIN QLNS_CN3.QuanLyNhanSu.dbo.TRINHDO td ON nv.TRINHDO = td.IDTD
      LEFT JOIN QLNS_CN3.QuanLyNhanSu.dbo.PHONGBAN pb ON nv.PHONGBAN = pb.IDPB
      LEFT JOIN QLNS_CN3.QuanLyNhanSu.dbo.CHINHANH cn ON nv.CHINHANH = cn.IDCN
      WHERE nv.IsDeleted = 0 OR nv.IsDeleted IS NULL
    `);
        return { data: result.recordset, sourceNodes: ['master'] };
    }
    // DIRECT mode — query all branch nodes in parallel
    const { rows, sourceNodes } = await queryAllBranches((pool) => pool.request().query(baseQuery));
    return { data: rows, sourceNodes };
}
async function getEmployeeById(idnv) {
    const branches = database_1.db.getAllBranchNodeIds();
    for (const nodeId of branches) {
        const result = await queryNode(nodeId, (pool) => pool.request()
            .input('id', mssql_1.default.VarChar, idnv)
            .query(`
          SELECT nv.*, cv.TENCV AS TEN_CHUCVU, td.TENTD AS TEN_TRINHDO,
                 td.CHUYENNGANH, pb.TENPB AS TEN_PHONGBAN, cn.TENCNHANH AS TEN_CHINHANH
          FROM NHANVIEN nv
          LEFT JOIN CHUCVU cv ON nv.CHUCVU = cv.IDCV
          LEFT JOIN TRINHDO td ON nv.TRINHDO = td.IDTD
          LEFT JOIN PHONGBAN pb ON nv.PHONGBAN = pb.IDPB
          LEFT JOIN CHINHANH cn ON nv.CHINHANH = cn.IDCN
          WHERE nv.IDNV = @id AND (nv.IsDeleted = 0 OR nv.IsDeleted IS NULL)
        `));
        if (result.success && result.rows.length > 0) {
            return { data: { ...result.rows[0], _sourceNode: nodeId }, sourceNode: nodeId };
        }
    }
    return { data: null, sourceNode: null };
}
async function createEmployee(data) {
    const branchNodeId = database_1.db.getNodeForBranch(data.CHINHANH);
    const sourceNodes = [];
    const insertQuery = (req) => req
        .input('IDNV', mssql_1.default.VarChar, data.IDNV)
        .input('TENNV', mssql_1.default.NVarChar, data.TENNV)
        .input('GIOITINH', mssql_1.default.NVarChar, data.GIOITINH)
        .input('NGAYSINH', mssql_1.default.Date, new Date(data.NGAYSINH))
        .input('CCCD', mssql_1.default.Char, data.CCCD)
        .input('EMAIL', mssql_1.default.VarChar, data.EMAIL)
        .input('DIENTHOAI', mssql_1.default.VarChar, data.DIENTHOAI)
        .input('DIACHI', mssql_1.default.NVarChar, data.DIACHI)
        .input('DANTOC', mssql_1.default.NVarChar, data.DANTOC || 'Kinh')
        .input('TONGIAO', mssql_1.default.NVarChar, data.TONGIAO || 'Không')
        .input('HONNHAN', mssql_1.default.NVarChar, data.HONNHAN || 'Độc thân')
        .input('TRINHDO', mssql_1.default.Char, data.TRINHDO)
        .input('CHUCVU', mssql_1.default.Char, data.CHUCVU)
        .input('PHONGBAN', mssql_1.default.Char, data.PHONGBAN)
        .input('CHINHANH', mssql_1.default.Char, data.CHINHANH)
        .query(`
        INSERT INTO NHANVIEN 
        (IDNV,TENNV,GIOITINH,NGAYSINH,CCCD,EMAIL,DIENTHOAI,DIACHI,DANTOC,TONGIAO,HONNHAN,TRINHDO,CHUCVU,PHONGBAN,CHINHANH)
        VALUES (@IDNV,@TENNV,@GIOITINH,@NGAYSINH,@CCCD,@EMAIL,@DIENTHOAI,@DIACHI,@DANTOC,@TONGIAO,@HONNHAN,@TRINHDO,@CHUCVU,@PHONGBAN,@CHINHANH)
      `);
    // 1. Insert vào chi nhánh (local node)
    const branchPool = await database_1.db.getPoolOrThrow(branchNodeId);
    await insertQuery(branchPool.request());
    sourceNodes.push(branchNodeId);
    // 2. Insert vào Master (nếu master khác branch và online)
    if (branchNodeId !== 'master' && database_1.db.isOnline('master')) {
        try {
            const masterPool = await database_1.db.getPoolOrThrow('master');
            await insertQuery(masterPool.request());
            sourceNodes.push('master');
        }
        catch (err) {
            console.warn('[CreateEmployee] Failed to sync to master:', err.message);
        }
    }
    return { sourceNodes };
}
async function updateEmployee(idnv, data) {
    // First find which node has this employee
    const { data: emp, sourceNode } = await getEmployeeById(idnv);
    if (!emp || !sourceNode)
        throw new Error(`Employee ${idnv} not found in any node`);
    const updates = [];
    const req = (pool) => {
        const r = pool.request().input('id', mssql_1.default.VarChar, idnv);
        if (data.TENNV !== undefined) {
            updates.push('TENNV=@TENNV');
            r.input('TENNV', mssql_1.default.NVarChar, data.TENNV);
        }
        if (data.EMAIL !== undefined) {
            updates.push('EMAIL=@EMAIL');
            r.input('EMAIL', mssql_1.default.VarChar, data.EMAIL);
        }
        if (data.DIENTHOAI !== undefined) {
            updates.push('DIENTHOAI=@DT');
            r.input('DT', mssql_1.default.VarChar, data.DIENTHOAI);
        }
        if (data.DIACHI !== undefined) {
            updates.push('DIACHI=@DIACHI');
            r.input('DIACHI', mssql_1.default.NVarChar, data.DIACHI);
        }
        if (data.CHUCVU !== undefined) {
            updates.push('CHUCVU=@CV');
            r.input('CV', mssql_1.default.Char, data.CHUCVU);
        }
        if (data.PHONGBAN !== undefined) {
            updates.push('PHONGBAN=@PB');
            r.input('PB', mssql_1.default.Char, data.PHONGBAN);
        }
        if (data.HONNHAN !== undefined) {
            updates.push('HONNHAN=@HN');
            r.input('HN', mssql_1.default.NVarChar, data.HONNHAN);
        }
        return r.query(`UPDATE NHANVIEN SET ${updates.join(',')} WHERE IDNV=@id`);
    };
    const sourceNodes = [];
    // Update on branch node
    const branchPool = await database_1.db.getPoolOrThrow(sourceNode);
    await req(branchPool);
    sourceNodes.push(sourceNode);
    // Sync to master
    if (sourceNode !== 'master' && database_1.db.isOnline('master')) {
        try {
            const masterPool = await database_1.db.getPoolOrThrow('master');
            await req(masterPool);
            sourceNodes.push('master');
        }
        catch (err) {
            console.warn('[UpdateEmployee] Failed to sync to master:', err.message);
        }
    }
    return { sourceNodes };
}
async function deleteEmployee(idnv) {
    const { data: emp, sourceNode } = await getEmployeeById(idnv);
    if (!emp || !sourceNode)
        throw new Error(`Employee ${idnv} not found`);
    const doDelete = async (nodeId) => {
        try {
            const pool = await database_1.db.getPoolOrThrow(nodeId);
            await pool.request().input('id', mssql_1.default.VarChar, idnv).query('UPDATE NHANVIEN SET IsDeleted = 1 WHERE IDNV=@id');
            return true;
        }
        catch {
            return false;
        }
    };
    const sourceNodes = [];
    if (await doDelete(sourceNode))
        sourceNodes.push(sourceNode);
    if (sourceNode !== 'master' && database_1.db.isOnline('master')) {
        if (await doDelete('master'))
            sourceNodes.push('master');
    }
    return { sourceNodes };
}
// ============================================================
// SALARY SERVICE
// ============================================================
async function getSalaries(filterBranch, thang, nam) {
    const buildQuery = (hasDateFilter) => `
    SELECT bl.*, bc.IDNV, bc.THANG, bc.NAM, nv.TENNV, nv.CHINHANH
    FROM BANGLUONG bl
    JOIN BANGCHAMCONG bc ON bl.IDBC = bc.IDBC
    JOIN NHANVIEN nv ON bc.IDNV = nv.IDNV
    WHERE (nv.IsDeleted = 0 OR nv.IsDeleted IS NULL)
    ${hasDateFilter ? 'AND bc.THANG = @thang AND bc.NAM = @nam' : ''}
  `;
    const runQuery = (pool) => {
        const r = pool.request();
        if (thang)
            r.input('thang', mssql_1.default.TinyInt, thang);
        if (nam)
            r.input('nam', mssql_1.default.SmallInt, nam);
        return r.query(buildQuery(!!(thang && nam)));
    };
    if (filterBranch) {
        const nodeId = database_1.db.getNodeForBranch(filterBranch);
        const result = await queryNode(nodeId, runQuery);
        return { data: result.rows.map(r => ({ ...r, _sourceNode: nodeId })), sourceNodes: [nodeId] };
    }
    const { rows, sourceNodes } = await queryAllBranches((pool) => runQuery(pool));
    return { data: rows, sourceNodes };
}
// ============================================================
// ATTENDANCE SERVICE
// ============================================================
async function getAttendance(filterBranch, thang, nam) {
    const buildQuery = (hasFilter) => `
    SELECT bc.*, nv.TENNV, nv.CHINHANH
    FROM BANGCHAMCONG bc
    JOIN NHANVIEN nv ON bc.IDNV = nv.IDNV
    WHERE (nv.IsDeleted = 0 OR nv.IsDeleted IS NULL)
    ${hasFilter ? 'AND bc.THANG = @thang AND bc.NAM = @nam' : ''}
  `;
    const runQuery = (pool) => {
        const r = pool.request();
        if (thang)
            r.input('thang', mssql_1.default.TinyInt, thang);
        if (nam)
            r.input('nam', mssql_1.default.SmallInt, nam);
        return r.query(buildQuery(!!(thang && nam)));
    };
    if (filterBranch) {
        const nodeId = database_1.db.getNodeForBranch(filterBranch);
        const result = await queryNode(nodeId, runQuery);
        return { data: result.rows.map(r => ({ ...r, _sourceNode: nodeId })), sourceNodes: [nodeId] };
    }
    const { rows, sourceNodes } = await queryAllBranches((pool) => runQuery(pool));
    return { data: rows, sourceNodes };
}
// ============================================================
// CONTRACTS SERVICE
// ============================================================
async function getContracts(filterBranch) {
    const query = `
    SELECT hd.*, lhd.TENLOAI AS TEN_LOAIHD, nv.TENNV, nv.CHINHANH
    FROM HOPDONG hd
    JOIN LOAIHD lhd ON hd.LOAIHD = lhd.IDLOAI
    JOIN NHANVIEN nv ON hd.IDNV = nv.IDNV
    WHERE (nv.IsDeleted = 0 OR nv.IsDeleted IS NULL)
  `;
    if (filterBranch) {
        const nodeId = database_1.db.getNodeForBranch(filterBranch);
        const result = await queryNode(nodeId, (pool) => pool.request().query(query));
        return { data: result.rows.map(r => ({ ...r, _sourceNode: nodeId })), sourceNodes: [nodeId] };
    }
    const { rows, sourceNodes } = await queryAllBranches((pool) => pool.request().query(query));
    return { data: rows, sourceNodes };
}
// ============================================================
// RECRUITMENT SERVICE
// ============================================================
async function getRecruitments(filterBranch) {
    const query = `
    SELECT td.*, cn.TENCNHANH AS TEN_CHINHANH
    FROM TUYENDUNG td
    LEFT JOIN CHINHANH cn ON td.IDCN = cn.IDCN
    WHERE (td.IsDeleted = 0 OR td.IsDeleted IS NULL)
  `;
    if (filterBranch) {
        const nodeId = database_1.db.getNodeForBranch(filterBranch);
        const result = await queryNode(nodeId, (pool) => pool.request().query(query));
        return { data: result.rows.map(r => ({ ...r, _sourceNode: nodeId })), sourceNodes: [nodeId] };
    }
    const { rows, sourceNodes } = await queryAllBranches((pool) => pool.request().query(query));
    return { data: rows, sourceNodes };
}
async function getRecruitmentById(matd) {
    const branches = database_1.db.getAllBranchNodeIds();
    for (const nodeId of branches) {
        const result = await queryNode(nodeId, (pool) => pool.request()
            .input('id', mssql_1.default.VarChar, matd)
            .query(`
          SELECT td.*, cn.TENCNHANH AS TEN_CHINHANH
          FROM TUYENDUNG td
          LEFT JOIN CHINHANH cn ON td.IDCN = cn.IDCN
          WHERE td.MATD = @id AND (td.IsDeleted = 0 OR td.IsDeleted IS NULL)
        `));
        if (result.success && result.rows.length > 0) {
            return { data: { ...result.rows[0], _sourceNode: nodeId }, sourceNode: nodeId };
        }
    }
    return { data: null, sourceNode: null };
}
async function createRecruitment(data) {
    const branchNodeId = database_1.db.getNodeForBranch(data.IDCN);
    const sourceNodes = [];
    const insertQuery = (req) => req
        .input('MATD', mssql_1.default.VarChar, data.MATD)
        .input('IDCN', mssql_1.default.Char, data.IDCN)
        .input('VITRITD', mssql_1.default.NVarChar, data.VITRITD)
        .input('DOTUOI', mssql_1.default.Int, data.DOTUOI)
        .input('GIOITINH', mssql_1.default.NVarChar, data.GIOITINH)
        .input('SOLUONG', mssql_1.default.Int, data.SOLUONG)
        .input('HANTD', mssql_1.default.Date, new Date(data.HANTD))
        .input('LUONGTOITHIEU', mssql_1.default.Float, data.LUONGTOITHIEU)
        .input('LUONGTOIDA', mssql_1.default.Float, data.LUONGTOIDA)
        .input('SOHOSODANAOP', mssql_1.default.Int, 0)
        .input('SOHOSODATUYEN', mssql_1.default.Int, 0)
        .input('TRANGTHAI', mssql_1.default.NVarChar, data.TRANGTHAI || 'Đang tuyển')
        .query(`
        INSERT INTO TUYENDUNG 
        (MATD,IDCN,VITRITD,DOTUOI,GIOITINH,SOLUONG,HANTD,LUONGTOITHIEU,LUONGTOIDA,SOHOSODANAOP,SOHOSODATUYEN,TRANGTHAI)
        VALUES (@MATD,@IDCN,@VITRITD,@DOTUOI,@GIOITINH,@SOLUONG,@HANTD,@LUONGTOITHIEU,@LUONGTOIDA,@SOHOSODANAOP,@SOHOSODATUYEN,@TRANGTHAI)
      `);
    // Insert to branch
    const branchPool = await database_1.db.getPoolOrThrow(branchNodeId);
    await insertQuery(branchPool.request());
    sourceNodes.push(branchNodeId);
    // Sync to master
    if (branchNodeId !== 'master' && database_1.db.isOnline('master')) {
        try {
            const masterPool = await database_1.db.getPoolOrThrow('master');
            await insertQuery(masterPool.request());
            sourceNodes.push('master');
        }
        catch (err) {
            console.warn('[CreateRecruitment] Failed to sync to master:', err.message);
        }
    }
    return { sourceNodes };
}
async function updateRecruitment(matd, data) {
    const { data: rec, sourceNode } = await getRecruitmentById(matd);
    if (!rec || !sourceNode)
        throw new Error(`Recruitment ${matd} not found`);
    const updates = [];
    const req = (pool) => {
        const r = pool.request().input('id', mssql_1.default.VarChar, matd);
        if (data.VITRITD !== undefined) {
            updates.push('VITRITD=@VITRITD');
            r.input('VITRITD', mssql_1.default.NVarChar, data.VITRITD);
        }
        if (data.DOTUOI !== undefined) {
            updates.push('DOTUOI=@DOTUOI');
            r.input('DOTUOI', mssql_1.default.Int, data.DOTUOI);
        }
        if (data.GIOITINH !== undefined) {
            updates.push('GIOITINH=@GIOITINH');
            r.input('GIOITINH', mssql_1.default.NVarChar, data.GIOITINH);
        }
        if (data.SOLUONG !== undefined) {
            updates.push('SOLUONG=@SOLUONG');
            r.input('SOLUONG', mssql_1.default.Int, data.SOLUONG);
        }
        if (data.HANTD !== undefined) {
            updates.push('HANTD=@HANTD');
            r.input('HANTD', mssql_1.default.Date, new Date(data.HANTD));
        }
        if (data.LUONGTOITHIEU !== undefined) {
            updates.push('LUONGTOITHIEU=@LTT');
            r.input('LTT', mssql_1.default.Float, data.LUONGTOITHIEU);
        }
        if (data.LUONGTOIDA !== undefined) {
            updates.push('LUONGTOIDA=@LTD');
            r.input('LTD', mssql_1.default.Float, data.LUONGTOIDA);
        }
        if (data.SOHOSODANAOP !== undefined) {
            updates.push('SOHOSODANAOP=@SHSN');
            r.input('SHSN', mssql_1.default.Int, data.SOHOSODANAOP);
        }
        if (data.SOHOSODATUYEN !== undefined) {
            updates.push('SOHOSODATUYEN=@SHSDT');
            r.input('SHSDT', mssql_1.default.Int, data.SOHOSODATUYEN);
        }
        if (data.TRANGTHAI !== undefined) {
            updates.push('TRANGTHAI=@TRANGTHAI');
            r.input('TRANGTHAI', mssql_1.default.NVarChar, data.TRANGTHAI);
        }
        return r.query(`UPDATE TUYENDUNG SET ${updates.join(',')} WHERE MATD=@id`);
    };
    const sourceNodes = [];
    const branchPool = await database_1.db.getPoolOrThrow(sourceNode);
    await req(branchPool);
    sourceNodes.push(sourceNode);
    if (sourceNode !== 'master' && database_1.db.isOnline('master')) {
        try {
            const masterPool = await database_1.db.getPoolOrThrow('master');
            await req(masterPool);
            sourceNodes.push('master');
        }
        catch { }
    }
    return { sourceNodes };
}
async function deleteRecruitment(matd) {
    const { data: rec, sourceNode } = await getRecruitmentById(matd);
    if (!rec || !sourceNode)
        throw new Error(`Recruitment ${matd} not found`);
    const doDelete = async (nodeId) => {
        try {
            const pool = await database_1.db.getPoolOrThrow(nodeId);
            await pool.request().input('id', mssql_1.default.VarChar, matd).query('UPDATE TUYENDUNG SET IsDeleted = 1 WHERE MATD=@id');
            return true;
        }
        catch {
            return false;
        }
    };
    const sourceNodes = [];
    if (await doDelete(sourceNode))
        sourceNodes.push(sourceNode);
    if (sourceNode !== 'master' && database_1.db.isOnline('master')) {
        if (await doDelete('master'))
            sourceNodes.push('master');
    }
    return { sourceNodes };
}
// ============================================================
// GLOBAL STATS — aggregate from all branches
// ============================================================
async function getGlobalStats() {
    const branches = database_1.db.getAllBranchNodeIds();
    const statsPromises = branches.map(async (nodeId) => {
        const result = await queryNode(nodeId, (pool) => pool.request().query(`SELECT TOP 1 CHINHANH AS branch, COUNT(*) AS count FROM NHANVIEN WHERE IsDeleted = 0 OR IsDeleted IS NULL GROUP BY CHINHANH`));
        return { nodeId, rows: result.rows, success: result.success };
    });
    const countResults = await Promise.all(statsPromises);
    // Avg salary, active contracts, open recruitment — from branches in parallel
    const [salaryRes, contractRes, recruitRes] = await Promise.all([
        queryAllBranches((pool) => pool.request().query(`
        SELECT AVG(bl.THUCNHAN) AS avgSalary, SUM(bl.THUCNHAN) AS totalSalary
        FROM BANGLUONG bl
      `)),
        queryAllBranches((pool) => pool.request().query(`
        SELECT COUNT(*) AS activeContracts FROM HOPDONG WHERE TRANGTHAI = N'Có hiệu lực'
      `)),
        queryAllBranches((pool) => pool.request().query(`
        SELECT COUNT(*) AS openRecruitments FROM TUYENDUNG WHERE TRANGTHAI = N'Đang tuyển'
      `)),
    ]);
    const totalEmployees = countResults.reduce((a, r) => a + (r.rows[0]?.count || 0), 0);
    const avgSalary = salaryRes.rows.reduce((a, r) => a + (r.avgSalary || 0), 0) / (salaryRes.rows.length || 1);
    const totalSalaryPaid = salaryRes.rows.reduce((a, r) => a + (r.totalSalary || 0), 0);
    const activeContracts = contractRes.rows.reduce((a, r) => a + (r.activeContracts || 0), 0);
    const openRecruitments = recruitRes.rows.reduce((a, r) => a + (r.openRecruitments || 0), 0);
    const byBranch = countResults
        .filter(r => r.success && r.rows[0])
        .map(r => {
        const bn = r.rows[0].branch.trim();
        const nodeInfo = database_1.db.nodes[r.nodeId];
        return {
            branch: bn,
            city: nodeInfo?.city || '',
            count: r.rows[0].count,
            nodeId: r.nodeId,
        };
    });
    return {
        data: { totalEmployees, byBranch, avgSalary, totalSalaryPaid, activeContracts, openRecruitments },
        sourceNodes: countResults.filter(r => r.success).map(r => r.nodeId),
    };
}
// ============================================================
// LOOKUP DATA
// ============================================================
async function getBranches() {
    const nodeId = database_1.db.getAllBranchNodeIds().find(id => database_1.db.isOnline(id)) || 'master';
    const result = await queryNode(nodeId, (pool) => pool.request().query('SELECT * FROM CHINHANH'));
    return result.rows;
}
async function getPositions() {
    const nodeId = database_1.db.getAllBranchNodeIds().find(id => database_1.db.isOnline(id)) || 'master';
    if (!nodeId)
        return [];
    const result = await queryNode(nodeId, pool => pool.request().query('SELECT * FROM CHUCVU'));
    return result.success ? result.rows : [];
}
async function getEducations() {
    const nodeId = database_1.db.getAllBranchNodeIds().find(id => database_1.db.isOnline(id)) || 'master';
    if (!nodeId)
        return [];
    const result = await queryNode(nodeId, pool => pool.request().query('SELECT * FROM TRINHDO'));
    return result.success ? result.rows : [];
}
async function getDepartments() {
    const nodeId = database_1.db.getAllBranchNodeIds().find(id => database_1.db.isOnline(id)) || 'master';
    if (!nodeId)
        return [];
    const result = await queryNode(nodeId, pool => pool.request().query('SELECT * FROM PHONGBAN'));
    return result.success ? result.rows : [];
}
//# sourceMappingURL=distributed.service.js.map