"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.queryRoutes = queryRoutes;
exports.schemaRoutes = schemaRoutes;
const database_1 = require("../config/database");
// SQL keywords for syntax suggestion
const SQL_KEYWORDS = [
    'SELECT', 'FROM', 'WHERE', 'JOIN', 'LEFT', 'RIGHT', 'INNER', 'OUTER',
    'GROUP BY', 'ORDER BY', 'HAVING', 'INSERT', 'UPDATE', 'DELETE', 'CREATE',
    'DROP', 'ALTER', 'TABLE', 'INDEX', 'VIEW', 'TOP', 'DISTINCT', 'AS',
    'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN', 'IS NULL', 'IS NOT NULL',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CAST', 'CONVERT', 'CASE', 'WHEN',
    'THEN', 'ELSE', 'END', 'WITH', 'UNION', 'ALL', 'EXCEPT', 'INTERSECT',
    'LIMIT', 'OFFSET', 'ASC', 'DESC', 'ON', 'SET', 'INTO', 'VALUES',
];
// Generate SQL error suggestions based on error message
function getSuggestions(errorMsg, sqlText) {
    const hints = [];
    const upper = errorMsg.toUpperCase();
    if (upper.includes('INVALID OBJECT NAME') || upper.includes('INVALID COLUMN NAME')) {
        const match = errorMsg.match(/'([^']+)'/);
        if (match) {
            hints.push(`Tên "${match[1]}" không tồn tại — kiểm tra chính tả tên bảng/cột`);
            hints.push(`Chạy: SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES để xem danh sách bảng`);
        }
    }
    if (upper.includes('SYNTAX ERROR') || upper.includes('INCORRECT SYNTAX')) {
        hints.push('Lỗi cú pháp SQL — kiểm tra dấu phẩy, ngoặc đơn/kép');
        hints.push('Đảm bảo các từ khóa viết đúng: SELECT, FROM, WHERE...');
    }
    if (upper.includes('CONVERSION FAILED') || upper.includes('ARITHMETIC')) {
        hints.push('Lỗi kiểu dữ liệu — sử dụng CAST() hoặc CONVERT() để chuyển đổi');
    }
    if (upper.includes('AMBIGUOUS COLUMN')) {
        hints.push('Tên cột không rõ ràng — thêm tên bảng: bảng.cột (VD: NhanVien.IDNV)');
    }
    if (upper.includes('DIVIDE BY ZERO')) {
        hints.push('Lỗi chia cho 0 — sử dụng: NULLIF(denominator, 0) để tránh lỗi này');
    }
    if (upper.includes('PERMISSION') || upper.includes('ACCESS')) {
        hints.push('Không có quyền truy cập — kiểm tra tài khoản SQL Server');
    }
    if (upper.includes('LINKED SERVER') || upper.includes('OPENQUERY')) {
        hints.push('Lỗi Linked Server — kiểm tra kết nối giữa các node');
        hints.push('Sử dụng OPENQUERY(server_name, \'query\') hoặc server.db.schema.table');
    }
    // Check for unmatched parentheses
    const opens = (sqlText.match(/\(/g) || []).length;
    const closes = (sqlText.match(/\)/g) || []).length;
    if (opens !== closes) {
        hints.push(`Số lượng dấu ngoặc không khớp: ${opens} "(" nhưng ${closes} ")"`);
    }
    if (hints.length === 0) {
        hints.push('Kiểm tra lại cú pháp SQL và tên bảng/cột');
    }
    return hints;
}
// Find error line number from SQL
function findErrorLine(errorMsg, sqlText) {
    // SQL Server usually says "Line X"
    const lineMatch = errorMsg.match(/[Ll]ine\s+(\d+)/);
    if (lineMatch)
        return parseInt(lineMatch[1]);
    // Try to find by token
    const tokenMatch = errorMsg.match(/'([^']+)'/);
    if (tokenMatch) {
        const lines = sqlText.split('\n');
        for (let i = 0; i < lines.length; i++) {
            if (lines[i].includes(tokenMatch[1]))
                return i + 1;
        }
    }
    return null;
}
async function queryRoutes(app) {
    // POST /api/query — execute SQL on a node
    app.post('/', async (req, reply) => {
        const start = Date.now();
        const { sql: sqlText, node = 'master' } = req.body;
        if (!sqlText || !sqlText.trim()) {
            return reply.status(400).send({ success: false, error: 'SQL query is required' });
        }
        const nodeId = node;
        const validNodes = ['master', 'cn1', 'cn2', 'cn3'];
        if (!validNodes.includes(nodeId)) {
            return reply.status(400).send({ success: false, error: `Invalid node. Use: ${validNodes.join(', ')}` });
        }
        // Allow DDL only if env says so, otherwise restrict to SELECT/WITH
        const allowDDL = process.env.ALLOW_DDL === 'true';
        const trimmed = sqlText.trim().toUpperCase();
        const allowedPrefixes = allowDDL
            ? ['SELECT', 'WITH', 'EXEC', 'EXECUTE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER']
            : ['SELECT', 'WITH', 'EXEC', 'EXECUTE'];
        if (!allowedPrefixes.some(p => trimmed.startsWith(p))) {
            return reply.status(400).send({
                success: false,
                error: `Chỉ cho phép các lệnh: ${allowedPrefixes.join(', ')}`,
                suggestions: ['Viết câu truy vấn bắt đầu bằng SELECT hoặc WITH'],
                errorLine: 1,
            });
        }
        try {
            const pool = await database_1.db.getPoolOrThrow(nodeId);
            const result = await pool.request().query(sqlText);
            const recordset = result.recordset || [];
            const columns = recordset.length > 0 ? Object.keys(recordset[0]) : [];
            return reply.send({
                success: true,
                data: {
                    columns,
                    rows: recordset,
                    rowCount: recordset.length,
                    node: nodeId,
                    nodeInfo: database_1.db.nodes[nodeId],
                },
                meta: {
                    executionTimeMs: Date.now() - start,
                    sourceNodes: [nodeId],
                    queryMode: process.env.QUERY_MODE || 'direct',
                },
            });
        }
        catch (err) {
            const errorMsg = err.message;
            return reply.status(422).send({
                success: false,
                error: errorMsg,
                errorLine: findErrorLine(errorMsg, sqlText),
                suggestions: getSuggestions(errorMsg, sqlText),
                node: nodeId,
            });
        }
    });
}
async function schemaRoutes(app) {
    // GET /api/schema/:nodeId — fetch INFORMATION_SCHEMA for a node
    app.get('/:nodeId', async (req, reply) => {
        const start = Date.now();
        const nodeId = req.params.nodeId;
        const validNodes = ['master', 'cn1', 'cn2', 'cn3'];
        if (!validNodes.includes(nodeId)) {
            return reply.status(400).send({ success: false, error: 'Invalid node ID' });
        }
        try {
            const pool = await database_1.db.getPoolOrThrow(nodeId);
            // Fetch tables + columns in one query
            const result = await pool.request().query(`
        SELECT
          t.TABLE_NAME,
          t.TABLE_TYPE,
          c.COLUMN_NAME,
          c.DATA_TYPE,
          c.CHARACTER_MAXIMUM_LENGTH,
          c.IS_NULLABLE,
          c.COLUMN_DEFAULT,
          c.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.TABLES t
        LEFT JOIN INFORMATION_SCHEMA.COLUMNS c
          ON t.TABLE_NAME = c.TABLE_NAME AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE t.TABLE_SCHEMA = 'dbo'
          AND t.TABLE_TYPE IN ('BASE TABLE', 'VIEW')
        ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION
      `);
            // Group by table
            const tablesMap = {};
            for (const row of result.recordset) {
                if (!tablesMap[row.TABLE_NAME]) {
                    tablesMap[row.TABLE_NAME] = { type: row.TABLE_TYPE, columns: [] };
                }
                if (row.COLUMN_NAME) {
                    tablesMap[row.TABLE_NAME].columns.push({
                        name: row.COLUMN_NAME,
                        type: row.DATA_TYPE,
                        maxLength: row.CHARACTER_MAXIMUM_LENGTH,
                        nullable: row.IS_NULLABLE === 'YES',
                        default: row.COLUMN_DEFAULT,
                        position: row.ORDINAL_POSITION,
                    });
                }
            }
            const tables = Object.entries(tablesMap).map(([name, info]) => ({
                name,
                type: info.type,
                columns: info.columns,
            }));
            return reply.send({
                success: true,
                data: {
                    node: nodeId,
                    nodeInfo: database_1.db.nodes[nodeId],
                    tables,
                    tableCount: tables.filter(t => t.type === 'BASE TABLE').length,
                    viewCount: tables.filter(t => t.type === 'VIEW').length,
                },
                meta: {
                    executionTimeMs: Date.now() - start,
                    sourceNodes: [nodeId],
                },
            });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
}
//# sourceMappingURL=query.routes.js.map