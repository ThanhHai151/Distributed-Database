export type NodeId = 'master' | 'cn1' | 'cn2' | 'cn3';
export type QueryMode = 'linked' | 'direct' | 'replication';
export type BranchId = 'CN1' | 'CN2' | 'CN3';
export interface NodeStatus {
    id: NodeId;
    name: string;
    branch: string;
    city: string;
    port: number;
    status: 'online' | 'offline' | 'connecting';
    latencyMs?: number;
}
export interface Branch {
    IDCN: string;
    TENCNHANH: string;
    HOTLINE: string;
    DIACHI: string;
}
export interface Position {
    IDCV: string;
    TENCV: string;
}
export interface Education {
    IDTD: string;
    TENTD: string;
    CHUYENNGANH: string;
}
export interface Department {
    IDPB: string;
    TENPB: string;
    DIACHI: string;
    NGAYTHANHLAP: string;
}
export interface Employee {
    IDNV: string;
    TENNV: string;
    GIOITINH: string;
    NGAYSINH: string;
    CCCD: string;
    EMAIL: string;
    DIENTHOAI: string;
    DIACHI: string;
    DANTOC: string;
    TONGIAO: string;
    HONNHAN: string;
    TRINHDO: string;
    CHUCVU: string;
    PHONGBAN: string;
    CHINHANH: string;
    TEN_CHUCVU?: string;
    TEN_TRINHDO?: string;
    CHUYENNGANH?: string;
    TEN_PHONGBAN?: string;
    TEN_CHINHANH?: string;
    _sourceNode?: NodeId;
}
export interface CreateEmployeeDto {
    IDNV: string;
    TENNV: string;
    GIOITINH: string;
    NGAYSINH: string;
    CCCD: string;
    EMAIL: string;
    DIENTHOAI: string;
    DIACHI: string;
    DANTOC?: string;
    TONGIAO?: string;
    HONNHAN?: string;
    TRINHDO: string;
    CHUCVU: string;
    PHONGBAN: string;
    CHINHANH: string;
}
export interface UpdateEmployeeDto extends Partial<Omit<CreateEmployeeDto, 'IDNV' | 'CHINHANH'>> {
}
export interface Attendance {
    IDBC: string;
    IDNV: string;
    THANG: number;
    NAM: number;
    SOGIOTANGCA: number;
    SONGAYNGHI: number;
    SONGAYDITRE: number;
    TONGNGAYLAM: number;
    TRANGTHAI: string;
    TENNV?: string;
    CHINHANH?: string;
    _sourceNode?: NodeId;
}
export interface Salary {
    IDBL: string;
    IDBC: string;
    LUONGCOBAN: number;
    LUONGTHUCTE: number;
    THUETNCN: number;
    LUONGTHUONG: number;
    PHUCAPCHUCVU: number;
    KHOANTRUBAOHIEM: number;
    PHUCAPKHAC: number;
    KHOANTRUKHAC: number;
    THUCNHAN: number;
    IDNV?: string;
    TENNV?: string;
    THANG?: number;
    NAM?: number;
    CHINHANH?: string;
    _sourceNode?: NodeId;
}
export interface Contract {
    SODH: string;
    NGAYKY: string;
    NGAYBATDAU: string;
    NGAYKETTHUC: string;
    LUONGCOBAN: number;
    TRANGTHAI: string;
    IDNV: string;
    LOAIHD: string;
    TENNV?: string;
    TEN_LOAIHD?: string;
    CHINHANH?: string;
    _sourceNode?: NodeId;
}
export interface Recruitment {
    MATD: string;
    IDCN: string;
    VITRITD: string;
    DOTUOI: number;
    GIOITINH: string;
    SOLUONG: number;
    HANTD: string;
    LUONGTOITHIEU: number;
    LUONGTOIDA: number;
    SOHOSODANAOP: number;
    SOHOSODATUYEN: number;
    TRANGTHAI: string;
    TEN_CHINHANH?: string;
    _sourceNode?: NodeId;
}
export interface CreateRecruitmentDto {
    MATD: string;
    IDCN: string;
    VITRITD: string;
    DOTUOI: number;
    GIOITINH: string;
    SOLUONG: number;
    HANTD: string;
    LUONGTOITHIEU: number;
    LUONGTOIDA: number;
    TRANGTHAI: string;
}
export interface UpdateRecruitmentDto extends Partial<Omit<CreateRecruitmentDto, 'MATD' | 'IDCN'>> {
    SOHOSODANAOP?: number;
    SOHOSODATUYEN?: number;
}
export interface GlobalStats {
    totalEmployees: number;
    byBranch: {
        branch: string;
        city: string;
        count: number;
        nodeId: NodeId;
    }[];
    avgSalary: number;
    totalSalaryPaid: number;
    activeContracts: number;
    openRecruitments: number;
}
export interface BranchSalaryReport {
    nodeId: NodeId;
    branch: string;
    city: string;
    employees: {
        IDNV: string;
        TENNV: string;
        THANG: number;
        NAM: number;
        LUONGCOBAN: number;
        THUCNHAN: number;
    }[];
}
export interface ApiResponse<T> {
    success: boolean;
    data?: T;
    error?: string;
    meta?: {
        sourceNodes: NodeId[];
        queryMode: QueryMode;
        executionTimeMs: number;
        totalRows?: number;
    };
}
//# sourceMappingURL=index.d.ts.map