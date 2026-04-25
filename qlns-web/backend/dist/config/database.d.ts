import sql from 'mssql';
export type NodeId = string;
export interface NodeInfo {
    id: string;
    name: string;
    branch: string;
    city: string;
    host: string;
    port: number;
    user: string;
    password?: string;
    database: string;
}
export declare class DatabaseManager {
    private pools;
    private status;
    nodes: Record<NodeId, NodeInfo>;
    constructor();
    loadNodes(): void;
    saveNodes(): void;
    connectAll(): Promise<void>;
    connectNode(nodeId: NodeId): Promise<void>;
    getPool(nodeId: NodeId): sql.ConnectionPool | null;
    getStatus(): Record<NodeId, {
        status: 'online' | 'offline' | 'connecting';
        info: NodeInfo;
    }>;
    isOnline(nodeId: NodeId): boolean;
    getNodeForBranch(chinhanh: string): NodeId;
    getAllBranchNodeIds(): string[];
    getPoolOrThrow(nodeId: NodeId): Promise<sql.ConnectionPool>;
    closeAll(): Promise<void>;
    addNode(info: NodeInfo): Promise<void>;
    deleteNode(id: NodeId): Promise<void>;
}
export declare const db: DatabaseManager;
//# sourceMappingURL=database.d.ts.map