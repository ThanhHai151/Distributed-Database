import { NodeId } from '../config/database';
import type { Employee, CreateEmployeeDto, UpdateEmployeeDto, Salary, Attendance, Contract, Recruitment, GlobalStats } from '../types';
export declare function getAllEmployees(filterBranch?: string): Promise<{
    data: Employee[];
    sourceNodes: NodeId[];
}>;
export declare function getEmployeeById(idnv: string): Promise<{
    data: Employee | null;
    sourceNode: NodeId | null;
}>;
export declare function createEmployee(data: CreateEmployeeDto): Promise<{
    sourceNodes: NodeId[];
}>;
export declare function updateEmployee(idnv: string, data: UpdateEmployeeDto): Promise<{
    sourceNodes: NodeId[];
}>;
export declare function deleteEmployee(idnv: string): Promise<{
    sourceNodes: NodeId[];
}>;
export declare function getSalaries(filterBranch?: string, thang?: number, nam?: number): Promise<{
    data: Salary[];
    sourceNodes: NodeId[];
}>;
export declare function getAttendance(filterBranch?: string, thang?: number, nam?: number): Promise<{
    data: Attendance[];
    sourceNodes: NodeId[];
}>;
export declare function getContracts(filterBranch?: string): Promise<{
    data: Contract[];
    sourceNodes: NodeId[];
}>;
export declare function getRecruitments(filterBranch?: string): Promise<{
    data: Recruitment[];
    sourceNodes: NodeId[];
}>;
export declare function getRecruitmentById(matd: string): Promise<{
    data: Recruitment | null;
    sourceNode: NodeId | null;
}>;
export declare function createRecruitment(data: import('../types').CreateRecruitmentDto): Promise<{
    sourceNodes: NodeId[];
}>;
export declare function updateRecruitment(matd: string, data: import('../types').UpdateRecruitmentDto): Promise<{
    sourceNodes: NodeId[];
}>;
export declare function deleteRecruitment(matd: string): Promise<{
    sourceNodes: NodeId[];
}>;
export declare function getGlobalStats(): Promise<{
    data: GlobalStats;
    sourceNodes: NodeId[];
}>;
export declare function getBranches(): Promise<any[]>;
export declare function getPositions(): Promise<Position[]>;
export declare function getEducations(): Promise<Education[]>;
export declare function getDepartments(): Promise<Department[]>;
//# sourceMappingURL=distributed.service.d.ts.map