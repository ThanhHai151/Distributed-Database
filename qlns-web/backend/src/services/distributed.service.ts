import sql from 'mssql';
import { db, NodeId } from '../config/database';
import type {
  Employee, CreateEmployeeDto, UpdateEmployeeDto,
  Salary, Attendance, Contract, Recruitment,
  GlobalStats, BranchSalaryReport, QueryMode,
  Position, Education, Department
} from '../types';

// ============================================================
// DISTRIBUTED QUERY SERVICE
// Handles all distributed database operations
// Supports 3 modes: linked | direct | replication
// ============================================================

const QUERY_MODE = (process.env.QUERY_MODE || 'direct') as QueryMode;

// ── Helper: run query on a node, return [] if offline ───────
async function queryNode<T>(
  nodeId: NodeId,
  queryFn: (pool: sql.ConnectionPool) => Promise<sql.IResult<T>>
): Promise<{ rows: T[]; nodeId: NodeId; success: boolean }> {
  try {
    const pool = await db.getPoolOrThrow(nodeId);
    const result = await queryFn(pool);
    return { rows: result.recordset, nodeId, success: true };
  } catch (err) {
    console.warn(`[DistQuery] Node ${nodeId} failed: ${(err as Error).message}`);
    return { rows: [], nodeId, success: false };
  }
}

// ── Helper: collect from all branch nodes concurrently ──────
async function queryAllBranches<T>(
  queryFn: (pool: sql.ConnectionPool, nodeId: NodeId) => Promise<sql.IResult<T>>
): Promise<{ rows: T[]; sourceNodes: NodeId[] }> {
  const branchNodes: NodeId[] = db.getAllBranchNodeIds();
  const results = await Promise.all(
    branchNodes.map((n) => queryNode(n, (pool) => queryFn(pool, n)))
  );
  const rows: T[] = [];
  const sourceNodes: NodeId[] = [];
  for (const r of results) {
    if (r.success) {
      rows.push(...r.rows.map((row) => ({ ...row, _sourceNode: r.nodeId } as any)));
      sourceNodes.push(r.nodeId);
    }
  }
  return { rows, sourceNodes };
}

// ============================================================
// EMPLOYEES SERVICE
// ============================================================
export async function getAllEmployees(
  filterBranch?: string
): Promise<{ data: Employee[]; sourceNodes: NodeId[] }> {
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
    const nodeId = db.getNodeForBranch(filterBranch);
    const result = await queryNode<Employee>(nodeId, (pool) =>
      pool.request().query(baseQuery)
    );
    return { data: result.rows.map(r => ({ ...r, _sourceNode: result.nodeId })), sourceNodes: [nodeId] };
  }

  if (QUERY_MODE === 'linked') {
    // Use Linked Servers from Master
    const masterPool = await db.getPoolOrThrow('master');
    const result = await masterPool.request().query<Employee>(`
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
  const { rows, sourceNodes } = await queryAllBranches<Employee>(
    (pool) => pool.request().query(baseQuery)
  );
  return { data: rows, sourceNodes };
}

export async function getEmployeeById(
  idnv: string
): Promise<{ data: Employee | null; sourceNode: NodeId | null }> {
  const branches = db.getAllBranchNodeIds();
  for (const nodeId of branches) {
    const result = await queryNode<Employee>(nodeId, (pool) =>
      pool.request()
        .input('id', sql.VarChar, idnv)
        .query(`
          SELECT nv.*, cv.TENCV AS TEN_CHUCVU, td.TENTD AS TEN_TRINHDO,
                 td.CHUYENNGANH, pb.TENPB AS TEN_PHONGBAN, cn.TENCNHANH AS TEN_CHINHANH
          FROM NHANVIEN nv
          LEFT JOIN CHUCVU cv ON nv.CHUCVU = cv.IDCV
          LEFT JOIN TRINHDO td ON nv.TRINHDO = td.IDTD
          LEFT JOIN PHONGBAN pb ON nv.PHONGBAN = pb.IDPB
          LEFT JOIN CHINHANH cn ON nv.CHINHANH = cn.IDCN
          WHERE nv.IDNV = @id AND (nv.IsDeleted = 0 OR nv.IsDeleted IS NULL)
        `)
    );
    if (result.success && result.rows.length > 0) {
      return { data: { ...result.rows[0], _sourceNode: nodeId as NodeId }, sourceNode: nodeId };
    }
  }
  return { data: null, sourceNode: null };
}

export async function createEmployee(data: CreateEmployeeDto): Promise<{ sourceNodes: NodeId[] }> {
  const branchNodeId = db.getNodeForBranch(data.CHINHANH);
  const sourceNodes: NodeId[] = [];

  const insertQuery = (req: sql.Request) =>
    req
      .input('IDNV', sql.VarChar, data.IDNV)
      .input('TENNV', sql.NVarChar, data.TENNV)
      .input('GIOITINH', sql.NVarChar, data.GIOITINH)
      .input('NGAYSINH', sql.Date, new Date(data.NGAYSINH))
      .input('CCCD', sql.Char, data.CCCD)
      .input('EMAIL', sql.VarChar, data.EMAIL)
      .input('DIENTHOAI', sql.VarChar, data.DIENTHOAI)
      .input('DIACHI', sql.NVarChar, data.DIACHI)
      .input('DANTOC', sql.NVarChar, data.DANTOC || 'Kinh')
      .input('TONGIAO', sql.NVarChar, data.TONGIAO || 'Không')
      .input('HONNHAN', sql.NVarChar, data.HONNHAN || 'Độc thân')
      .input('TRINHDO', sql.Char, data.TRINHDO)
      .input('CHUCVU', sql.Char, data.CHUCVU)
      .input('PHONGBAN', sql.Char, data.PHONGBAN)
      .input('CHINHANH', sql.Char, data.CHINHANH)
      .query(`
        INSERT INTO NHANVIEN 
        (IDNV,TENNV,GIOITINH,NGAYSINH,CCCD,EMAIL,DIENTHOAI,DIACHI,DANTOC,TONGIAO,HONNHAN,TRINHDO,CHUCVU,PHONGBAN,CHINHANH)
        VALUES (@IDNV,@TENNV,@GIOITINH,@NGAYSINH,@CCCD,@EMAIL,@DIENTHOAI,@DIACHI,@DANTOC,@TONGIAO,@HONNHAN,@TRINHDO,@CHUCVU,@PHONGBAN,@CHINHANH)
      `);

  // 1. Insert vào chi nhánh (local node)
  const branchPool = await db.getPoolOrThrow(branchNodeId);
  await insertQuery(branchPool.request());
  sourceNodes.push(branchNodeId);

  // 2. Insert vào Master (nếu master khác branch và online)
  if (branchNodeId !== 'master' && db.isOnline('master')) {
    try {
      const masterPool = await db.getPoolOrThrow('master');
      await insertQuery(masterPool.request());
      sourceNodes.push('master');
    } catch (err) {
      console.warn('[CreateEmployee] Failed to sync to master:', (err as Error).message);
    }
  }

  return { sourceNodes };
}

export async function updateEmployee(
  idnv: string,
  data: UpdateEmployeeDto
): Promise<{ sourceNodes: NodeId[] }> {
  // First find which node has this employee
  const { data: emp, sourceNode } = await getEmployeeById(idnv);
  if (!emp || !sourceNode) throw new Error(`Employee ${idnv} not found in any node`);

  const updates: string[] = [];
  const req = (pool: sql.ConnectionPool) => {
    const r = pool.request().input('id', sql.VarChar, idnv);
    if (data.TENNV  !== undefined) { updates.push('TENNV=@TENNV');   r.input('TENNV', sql.NVarChar, data.TENNV); }
    if (data.EMAIL  !== undefined) { updates.push('EMAIL=@EMAIL');   r.input('EMAIL', sql.VarChar,  data.EMAIL); }
    if (data.DIENTHOAI !== undefined) { updates.push('DIENTHOAI=@DT'); r.input('DT', sql.VarChar, data.DIENTHOAI); }
    if (data.DIACHI !== undefined) { updates.push('DIACHI=@DIACHI'); r.input('DIACHI', sql.NVarChar, data.DIACHI); }
    if (data.CHUCVU !== undefined) { updates.push('CHUCVU=@CV');     r.input('CV', sql.Char, data.CHUCVU); }
    if (data.PHONGBAN !== undefined) { updates.push('PHONGBAN=@PB'); r.input('PB', sql.Char, data.PHONGBAN); }
    if (data.HONNHAN !== undefined) { updates.push('HONNHAN=@HN');   r.input('HN', sql.NVarChar, data.HONNHAN); }
    return r.query(`UPDATE NHANVIEN SET ${updates.join(',')} WHERE IDNV=@id`);
  };

  const sourceNodes: NodeId[] = [];

  // Update on branch node
  const branchPool = await db.getPoolOrThrow(sourceNode);
  await req(branchPool);
  sourceNodes.push(sourceNode);

  // Sync to master
  if (sourceNode !== 'master' && db.isOnline('master')) {
    try {
      const masterPool = await db.getPoolOrThrow('master');
      await req(masterPool);
      sourceNodes.push('master');
    } catch (err) {
      console.warn('[UpdateEmployee] Failed to sync to master:', (err as Error).message);
    }
  }

  return { sourceNodes };
}

export async function deleteEmployee(idnv: string): Promise<{ sourceNodes: NodeId[] }> {
  const { data: emp, sourceNode } = await getEmployeeById(idnv);
  if (!emp || !sourceNode) throw new Error(`Employee ${idnv} not found`);

  const doDelete = async (nodeId: NodeId): Promise<boolean> => {
    try {
      const pool = await db.getPoolOrThrow(nodeId);
      await pool.request().input('id', sql.VarChar, idnv).query('UPDATE NHANVIEN SET IsDeleted = 1 WHERE IDNV=@id');
      return true;
    } catch { return false; }
  };

  const sourceNodes: NodeId[] = [];
  if (await doDelete(sourceNode)) sourceNodes.push(sourceNode);
  if (sourceNode !== 'master' && db.isOnline('master')) {
    if (await doDelete('master')) sourceNodes.push('master');
  }
  return { sourceNodes };
}

// ============================================================
// SALARY SERVICE
// ============================================================
export async function getSalaries(
  filterBranch?: string, thang?: number, nam?: number
): Promise<{ data: Salary[]; sourceNodes: NodeId[] }> {
  const buildQuery = (hasDateFilter: boolean) => `
    SELECT bl.*, bc.IDNV, bc.THANG, bc.NAM, nv.TENNV, nv.CHINHANH
    FROM BANGLUONG bl
    JOIN BANGCHAMCONG bc ON bl.IDBC = bc.IDBC
    JOIN NHANVIEN nv ON bc.IDNV = nv.IDNV
    WHERE (nv.IsDeleted = 0 OR nv.IsDeleted IS NULL)
    ${hasDateFilter ? 'AND bc.THANG = @thang AND bc.NAM = @nam' : ''}
  `;

  const runQuery = (pool: sql.ConnectionPool) => {
    const r = pool.request();
    if (thang) r.input('thang', sql.TinyInt, thang);
    if (nam)   r.input('nam', sql.SmallInt, nam);
    return r.query<Salary>(buildQuery(!!(thang && nam)));
  };

  if (filterBranch) {
    const nodeId = db.getNodeForBranch(filterBranch);
    const result = await queryNode<Salary>(nodeId, runQuery);
    return { data: result.rows.map(r => ({ ...r, _sourceNode: nodeId as NodeId })), sourceNodes: [nodeId] };
  }

  const { rows, sourceNodes } = await queryAllBranches<Salary>((pool) => runQuery(pool));
  return { data: rows, sourceNodes };
}

// ============================================================
// ATTENDANCE SERVICE
// ============================================================
export async function getAttendance(
  filterBranch?: string, thang?: number, nam?: number
): Promise<{ data: Attendance[]; sourceNodes: NodeId[] }> {
  const buildQuery = (hasFilter: boolean) => `
    SELECT bc.*, nv.TENNV, nv.CHINHANH
    FROM BANGCHAMCONG bc
    JOIN NHANVIEN nv ON bc.IDNV = nv.IDNV
    WHERE (nv.IsDeleted = 0 OR nv.IsDeleted IS NULL)
    ${hasFilter ? 'AND bc.THANG = @thang AND bc.NAM = @nam' : ''}
  `;

  const runQuery = (pool: sql.ConnectionPool) => {
    const r = pool.request();
    if (thang) r.input('thang', sql.TinyInt, thang);
    if (nam)   r.input('nam', sql.SmallInt, nam);
    return r.query<Attendance>(buildQuery(!!(thang && nam)));
  };

  if (filterBranch) {
    const nodeId = db.getNodeForBranch(filterBranch);
    const result = await queryNode<Attendance>(nodeId, runQuery);
    return { data: result.rows.map(r => ({ ...r, _sourceNode: nodeId as NodeId })), sourceNodes: [nodeId] };
  }

  const { rows, sourceNodes } = await queryAllBranches<Attendance>((pool) => runQuery(pool));
  return { data: rows, sourceNodes };
}

// ============================================================
// CONTRACTS SERVICE
// ============================================================
export async function getContracts(filterBranch?: string): Promise<{ data: Contract[]; sourceNodes: NodeId[] }> {
  const query = `
    SELECT hd.*, lhd.TENLOAI AS TEN_LOAIHD, nv.TENNV, nv.CHINHANH
    FROM HOPDONG hd
    JOIN LOAIHD lhd ON hd.LOAIHD = lhd.IDLOAI
    JOIN NHANVIEN nv ON hd.IDNV = nv.IDNV
    WHERE (nv.IsDeleted = 0 OR nv.IsDeleted IS NULL)
  `;

  if (filterBranch) {
    const nodeId = db.getNodeForBranch(filterBranch);
    const result = await queryNode<Contract>(nodeId, (pool) => pool.request().query(query));
    return { data: result.rows.map(r => ({ ...r, _sourceNode: nodeId as NodeId })), sourceNodes: [nodeId] };
  }

  const { rows, sourceNodes } = await queryAllBranches<Contract>((pool) => pool.request().query(query));
  return { data: rows, sourceNodes };
}

// ============================================================
// RECRUITMENT SERVICE
// ============================================================
export async function getRecruitments(filterBranch?: string): Promise<{ data: Recruitment[]; sourceNodes: NodeId[] }> {
  const query = `
    SELECT td.*, cn.TENCNHANH AS TEN_CHINHANH
    FROM TUYENDUNG td
    LEFT JOIN CHINHANH cn ON td.IDCN = cn.IDCN
    WHERE (td.IsDeleted = 0 OR td.IsDeleted IS NULL)
  `;

  if (filterBranch) {
    const nodeId = db.getNodeForBranch(filterBranch);
    const result = await queryNode<Recruitment>(nodeId, (pool) => pool.request().query(query));
    return { data: result.rows.map(r => ({ ...r, _sourceNode: nodeId as NodeId })), sourceNodes: [nodeId] };
  }

  const { rows, sourceNodes } = await queryAllBranches<Recruitment>((pool) => pool.request().query(query));
  return { data: rows, sourceNodes };
}

export async function getRecruitmentById(matd: string): Promise<{ data: Recruitment | null; sourceNode: NodeId | null }> {
  const branches = db.getAllBranchNodeIds();
  for (const nodeId of branches) {
    const result = await queryNode<Recruitment>(nodeId, (pool) =>
      pool.request()
        .input('id', sql.VarChar, matd)
        .query(`
          SELECT td.*, cn.TENCNHANH AS TEN_CHINHANH
          FROM TUYENDUNG td
          LEFT JOIN CHINHANH cn ON td.IDCN = cn.IDCN
          WHERE td.MATD = @id AND (td.IsDeleted = 0 OR td.IsDeleted IS NULL)
        `)
    );
    if (result.success && result.rows.length > 0) {
      return { data: { ...result.rows[0], _sourceNode: nodeId as NodeId }, sourceNode: nodeId };
    }
  }
  return { data: null, sourceNode: null };
}

export async function createRecruitment(data: import('../types').CreateRecruitmentDto): Promise<{ sourceNodes: NodeId[] }> {
  const branchNodeId = db.getNodeForBranch(data.IDCN);
  const sourceNodes: NodeId[] = [];

  const insertQuery = (req: sql.Request) =>
    req
      .input('MATD', sql.VarChar, data.MATD)
      .input('IDCN', sql.Char, data.IDCN)
      .input('VITRITD', sql.NVarChar, data.VITRITD)
      .input('DOTUOI', sql.Int, data.DOTUOI)
      .input('GIOITINH', sql.NVarChar, data.GIOITINH)
      .input('SOLUONG', sql.Int, data.SOLUONG)
      .input('HANTD', sql.Date, new Date(data.HANTD))
      .input('LUONGTOITHIEU', sql.Float, data.LUONGTOITHIEU)
      .input('LUONGTOIDA', sql.Float, data.LUONGTOIDA)
      .input('SOHOSODANAOP', sql.Int, 0)
      .input('SOHOSODATUYEN', sql.Int, 0)
      .input('TRANGTHAI', sql.NVarChar, data.TRANGTHAI || 'Đang tuyển')
      .query(`
        INSERT INTO TUYENDUNG 
        (MATD,IDCN,VITRITD,DOTUOI,GIOITINH,SOLUONG,HANTD,LUONGTOITHIEU,LUONGTOIDA,SOHOSODANAOP,SOHOSODATUYEN,TRANGTHAI)
        VALUES (@MATD,@IDCN,@VITRITD,@DOTUOI,@GIOITINH,@SOLUONG,@HANTD,@LUONGTOITHIEU,@LUONGTOIDA,@SOHOSODANAOP,@SOHOSODATUYEN,@TRANGTHAI)
      `);

  // Insert to branch
  const branchPool = await db.getPoolOrThrow(branchNodeId);
  await insertQuery(branchPool.request());
  sourceNodes.push(branchNodeId);

  // Sync to master
  if (branchNodeId !== 'master' && db.isOnline('master')) {
    try {
      const masterPool = await db.getPoolOrThrow('master');
      await insertQuery(masterPool.request());
      sourceNodes.push('master');
    } catch (err) {
      console.warn('[CreateRecruitment] Failed to sync to master:', (err as Error).message);
    }
  }

  return { sourceNodes };
}

export async function updateRecruitment(matd: string, data: import('../types').UpdateRecruitmentDto): Promise<{ sourceNodes: NodeId[] }> {
  const { data: rec, sourceNode } = await getRecruitmentById(matd);
  if (!rec || !sourceNode) throw new Error(`Recruitment ${matd} not found`);

  const updates: string[] = [];
  const req = (pool: sql.ConnectionPool) => {
    const r = pool.request().input('id', sql.VarChar, matd);
    if (data.VITRITD !== undefined) { updates.push('VITRITD=@VITRITD'); r.input('VITRITD', sql.NVarChar, data.VITRITD); }
    if (data.DOTUOI !== undefined) { updates.push('DOTUOI=@DOTUOI'); r.input('DOTUOI', sql.Int, data.DOTUOI); }
    if (data.GIOITINH !== undefined) { updates.push('GIOITINH=@GIOITINH'); r.input('GIOITINH', sql.NVarChar, data.GIOITINH); }
    if (data.SOLUONG !== undefined) { updates.push('SOLUONG=@SOLUONG'); r.input('SOLUONG', sql.Int, data.SOLUONG); }
    if (data.HANTD !== undefined) { updates.push('HANTD=@HANTD'); r.input('HANTD', sql.Date, new Date(data.HANTD)); }
    if (data.LUONGTOITHIEU !== undefined) { updates.push('LUONGTOITHIEU=@LTT'); r.input('LTT', sql.Float, data.LUONGTOITHIEU); }
    if (data.LUONGTOIDA !== undefined) { updates.push('LUONGTOIDA=@LTD'); r.input('LTD', sql.Float, data.LUONGTOIDA); }
    if (data.SOHOSODANAOP !== undefined) { updates.push('SOHOSODANAOP=@SHSN'); r.input('SHSN', sql.Int, data.SOHOSODANAOP); }
    if (data.SOHOSODATUYEN !== undefined) { updates.push('SOHOSODATUYEN=@SHSDT'); r.input('SHSDT', sql.Int, data.SOHOSODATUYEN); }
    if (data.TRANGTHAI !== undefined) { updates.push('TRANGTHAI=@TRANGTHAI'); r.input('TRANGTHAI', sql.NVarChar, data.TRANGTHAI); }
    return r.query(`UPDATE TUYENDUNG SET ${updates.join(',')} WHERE MATD=@id`);
  };

  const sourceNodes: NodeId[] = [];
  const branchPool = await db.getPoolOrThrow(sourceNode);
  await req(branchPool);
  sourceNodes.push(sourceNode);

  if (sourceNode !== 'master' && db.isOnline('master')) {
    try {
      const masterPool = await db.getPoolOrThrow('master');
      await req(masterPool);
      sourceNodes.push('master');
    } catch {}
  }
  return { sourceNodes };
}

export async function deleteRecruitment(matd: string): Promise<{ sourceNodes: NodeId[] }> {
  const { data: rec, sourceNode } = await getRecruitmentById(matd);
  if (!rec || !sourceNode) throw new Error(`Recruitment ${matd} not found`);

  const doDelete = async (nodeId: NodeId): Promise<boolean> => {
    try {
      const pool = await db.getPoolOrThrow(nodeId);
      await pool.request().input('id', sql.VarChar, matd).query('UPDATE TUYENDUNG SET IsDeleted = 1 WHERE MATD=@id');
      return true;
    } catch { return false; }
  };

  const sourceNodes: NodeId[] = [];
  if (await doDelete(sourceNode)) sourceNodes.push(sourceNode);
  if (sourceNode !== 'master' && db.isOnline('master')) {
    if (await doDelete('master')) sourceNodes.push('master');
  }
  return { sourceNodes };
}

// ============================================================
// GLOBAL STATS — aggregate from all branches
// ============================================================
export async function getGlobalStats(): Promise<{ data: GlobalStats; sourceNodes: NodeId[] }> {
  const branches = db.getAllBranchNodeIds();
  
  const statsPromises = branches.map(async (nodeId) => {
    const result = await queryNode<{ branch: string; count: number }>(nodeId, (pool) =>
      pool.request().query(`SELECT TOP 1 CHINHANH AS branch, COUNT(*) AS count FROM NHANVIEN WHERE IsDeleted = 0 OR IsDeleted IS NULL GROUP BY CHINHANH`)
    );
    return { nodeId, rows: result.rows, success: result.success };
  });

  const countResults = await Promise.all(statsPromises);

  // Avg salary, active contracts, open recruitment — from branches in parallel
  const [salaryRes, contractRes, recruitRes] = await Promise.all([
    queryAllBranches<{ avgSalary: number; totalSalary: number }>((pool) =>
      pool.request().query(`
        SELECT AVG(bl.THUCNHAN) AS avgSalary, SUM(bl.THUCNHAN) AS totalSalary
        FROM BANGLUONG bl
      `)
    ),
    queryAllBranches<{ activeContracts: number }>((pool) =>
      pool.request().query(`
        SELECT COUNT(*) AS activeContracts FROM HOPDONG WHERE TRANGTHAI = N'Có hiệu lực'
      `)
    ),
    queryAllBranches<{ openRecruitments: number }>((pool) =>
      pool.request().query(`
        SELECT COUNT(*) AS openRecruitments FROM TUYENDUNG WHERE TRANGTHAI = N'Đang tuyển'
      `)
    ),
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
      const nodeInfo = db.nodes[r.nodeId as any];
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
export async function getBranches() {
  const nodeId = db.getAllBranchNodeIds().find(id => db.isOnline(id)) || 'master';
  const result = await queryNode(nodeId, (pool) => pool.request().query('SELECT * FROM CHINHANH'));
  return result.rows;
}

export async function getPositions(): Promise<Position[]> {
  const nodeId = db.getAllBranchNodeIds().find(id => db.isOnline(id)) || 'master';
  if (!nodeId) return [];
  const result = await queryNode<Position>(nodeId, pool => pool.request().query('SELECT * FROM CHUCVU'));
  return result.success ? result.rows : [];
}

export async function getEducations(): Promise<Education[]> {
  const nodeId = db.getAllBranchNodeIds().find(id => db.isOnline(id)) || 'master';
  if (!nodeId) return [];
  const result = await queryNode<Education>(nodeId, pool => pool.request().query('SELECT * FROM TRINHDO'));
  return result.success ? result.rows : [];
}

export async function getDepartments(): Promise<Department[]> {
  const nodeId = db.getAllBranchNodeIds().find(id => db.isOnline(id)) || 'master';
  if (!nodeId) return [];
  const result = await queryNode<Department>(nodeId, pool => pool.request().query('SELECT * FROM PHONGBAN'));
  return result.success ? result.rows : [];
}
