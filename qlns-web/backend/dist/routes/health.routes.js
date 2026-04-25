"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.healthRoutes = healthRoutes;
const database_1 = require("../config/database");
async function healthRoutes(app) {
    // GET /api/health — check all 4 nodes
    app.get('/', async (req, reply) => {
        const start = Date.now();
        const statusMap = database_1.db.getStatus();
        // Ping each online node
        const nodes = await Promise.all(Object.entries(statusMap).map(async ([id, info]) => {
            let latencyMs = null;
            if (info.status === 'online') {
                const t0 = Date.now();
                try {
                    const pool = database_1.db.getPool(id);
                    if (pool)
                        await pool.request().query('SELECT 1 AS ping');
                    latencyMs = Date.now() - t0;
                }
                catch {
                    latencyMs = null;
                }
            }
            return {
                id,
                ...info.info,
                status: info.status,
                latencyMs,
            };
        }));
        const onlineCount = nodes.filter(n => n.status === 'online').length;
        return reply.send({
            success: true,
            data: {
                nodes,
                summary: {
                    total: nodes.length,
                    online: onlineCount,
                    offline: nodes.length - onlineCount,
                    queryMode: process.env.QUERY_MODE || 'direct',
                },
            },
            meta: { executionTimeMs: Date.now() - start, sourceNodes: [], queryMode: process.env.QUERY_MODE },
        });
    });
    // POST /api/health/reconnect/:nodeId — try to reconnect a node
    app.post('/reconnect/:nodeId', async (req, reply) => {
        const { nodeId } = req.params;
        if (!database_1.db.nodes[nodeId]) {
            return reply.status(400).send({ success: false, error: 'Invalid node ID' });
        }
        await database_1.db.connectNode(nodeId);
        const status = database_1.db.getStatus();
        return reply.send({
            success: true,
            data: { nodeId, status: status[nodeId]?.status },
        });
    });
}
//# sourceMappingURL=health.routes.js.map