import sql from 'mssql';
import dotenv from 'dotenv';
import fs from 'fs';
import path from 'path';

dotenv.config();

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

const NODES_FILE = path.join(__dirname, 'nodes.json');

function makeConfig(host: string, port: number, user: string, password?: string, database?: string): sql.config {
  return {
    user,
    password: password || 'KetNoi@123',
    server: host,
    port,
    database: database || 'QuanLyNhanSu',
    options: {
      encrypt: false,
      trustServerCertificate: true,
      enableArithAbort: true,
    },
    pool: {
      max: Number(process.env.DB_POOL_MAX) || 10,
      min: Number(process.env.DB_POOL_MIN) || 2,
      idleTimeoutMillis: 30000,
    },
    connectionTimeout: Number(process.env.DB_CONNECT_TIMEOUT) || 15000,
    requestTimeout: Number(process.env.DB_REQUEST_TIMEOUT) || 30000,
  };
}

export class DatabaseManager {
  private pools: Record<NodeId, sql.ConnectionPool> = {};
  private status: Record<NodeId, 'online' | 'offline' | 'connecting'> = {};
  public nodes: Record<NodeId, NodeInfo> = {};

  constructor() {
    this.loadNodes();
  }

  public loadNodes() {
    if (fs.existsSync(NODES_FILE)) {
      try {
        const data = fs.readFileSync(NODES_FILE, 'utf-8');
        this.nodes = JSON.parse(data);
      } catch (err) {
        console.error('Failed to load nodes.json', err);
      }
    } else {
      // Default initial from env
      this.nodes = {
        master: { id: 'master', name: 'Server Gốc (Master)', branch: 'ALL', city: 'Tất cả chi nhánh', host: process.env.MASTER_HOST || 'localhost', port: Number(process.env.MASTER_PORT) || 1432, user: process.env.MASTER_USER || 'sa', password: process.env.MASTER_PASSWORD || 'KetNoi@123', database: process.env.MASTER_DB || 'QuanLyNhanSu' },
        cn1: { id: 'cn1', name: 'Chi nhánh Hà Nội', branch: 'CN1', city: 'Hà Nội', host: process.env.CN1_HOST || 'localhost', port: Number(process.env.CN1_PORT) || 1437, user: process.env.CN1_USER || 'sa', password: process.env.CN1_PASSWORD || 'KetNoi@123', database: process.env.CN1_DB || 'QuanLyNhanSu' },
        cn2: { id: 'cn2', name: 'Chi nhánh Đà Nẵng', branch: 'CN2', city: 'Đà Nẵng', host: process.env.CN2_HOST || 'localhost', port: Number(process.env.CN2_PORT) || 1435, user: process.env.CN2_USER || 'sa', password: process.env.CN2_PASSWORD || 'KetNoi@123', database: process.env.CN2_DB || 'QuanLyNhanSu' },
        cn3: { id: 'cn3', name: 'Chi nhánh TP.HCM', branch: 'CN3', city: 'TP. Hồ Chí Minh', host: process.env.CN3_HOST || 'localhost', port: Number(process.env.CN3_PORT) || 1436, user: process.env.CN3_USER || 'sa', password: process.env.CN3_PASSWORD || 'KetNoi@123', database: process.env.CN3_DB || 'QuanLyNhanSu' },
      };
      this.saveNodes();
    }
  }

  public saveNodes() {
    fs.writeFileSync(NODES_FILE, JSON.stringify(this.nodes, null, 2));
  }

  async connectAll(): Promise<void> {
    await Promise.allSettled(Object.keys(this.nodes).map((nodeId) => this.connectNode(nodeId)));
  }

  async connectNode(nodeId: NodeId): Promise<void> {
    this.status[nodeId] = 'connecting';
    const info = this.nodes[nodeId];
    if (!info) return;

    try {
      const config = makeConfig(info.host, info.port, info.user, info.password, info.database);
      const pool = new sql.ConnectionPool(config);
      await pool.connect();
      if (this.pools[nodeId]) {
        await this.pools[nodeId].close(); // close old connection if present
      }
      this.pools[nodeId] = pool;
      this.status[nodeId] = 'online';
      console.log(`✅ [DB] ${info.name} connected (port ${info.port})`);
    } catch (err) {
      this.status[nodeId] = 'offline';
      console.warn(`⚠️  [DB] ${info.name} OFFLINE (port ${info.port}): ${(err as Error).message}`);
    }
  }

  getPool(nodeId: NodeId): sql.ConnectionPool | null {
    return this.pools[nodeId] || null;
  }

  getStatus(): Record<NodeId, { status: 'online' | 'offline' | 'connecting'; info: NodeInfo }> {
    return Object.fromEntries(
      Object.keys(this.nodes).map((id) => [
        id, { status: this.status[id] || 'offline', info: this.nodes[id] }
      ])
    );
  }

  isOnline(nodeId: NodeId): boolean {
    return this.status[nodeId] === 'online';
  }

  getNodeForBranch(chinhanh: string): NodeId {
    // Find the node id whose branch matches the requested branch
    // Exclude master
    const target = Object.values(this.nodes).find(n => n.branch === chinhanh && n.id !== 'master');
    return target ? target.id : 'master';
  }

  getAllBranchNodeIds(): string[] {
    return Object.keys(this.nodes).filter(k => k !== 'master');
  }

  async getPoolOrThrow(nodeId: NodeId): Promise<sql.ConnectionPool> {
    if (!this.isOnline(nodeId)) {
      await this.connectNode(nodeId);
    }
    const pool = this.pools[nodeId];
    if (!pool) throw new Error(`Node ${nodeId} is offline`);
    return pool;
  }

  async closeAll(): Promise<void> {
    await Promise.allSettled(Object.values(this.pools).map((p) => p.close()));
  }

  // --- Map CRUD API ---
  async addNode(info: NodeInfo): Promise<void> {
    this.nodes[info.id] = info;
    this.saveNodes();
    await this.connectNode(info.id);
  }

  async deleteNode(id: NodeId): Promise<void> {
    if (this.pools[id]) {
      await this.pools[id].close();
      delete this.pools[id];
    }
    delete this.status[id];
    delete this.nodes[id];
    this.saveNodes();
  }
}

export const db = new DatabaseManager();
