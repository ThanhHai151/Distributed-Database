"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const fastify_1 = __importDefault(require("fastify"));
const cors_1 = __importDefault(require("@fastify/cors"));
const helmet_1 = __importDefault(require("@fastify/helmet"));
const dotenv_1 = __importDefault(require("dotenv"));
dotenv_1.default.config();
const database_1 = require("./config/database");
const health_routes_1 = require("./routes/health.routes");
const employee_routes_1 = require("./routes/employee.routes");
const other_routes_1 = require("./routes/other.routes");
const query_routes_1 = require("./routes/query.routes");
const system_routes_1 = require("./routes/system.routes");
const PORT = Number(process.env.PORT) || 4000;
const HOST = process.env.HOST || '0.0.0.0';
async function bootstrap() {
    const app = (0, fastify_1.default)({
        logger: {
            transport: process.env.NODE_ENV === 'development'
                ? { target: 'pino-pretty', options: { colorize: true } }
                : undefined,
        },
    });
    // ── Security & CORS ────────────────────────────────────────
    await app.register(helmet_1.default, { contentSecurityPolicy: false });
    await app.register(cors_1.default, {
        origin: ['http://localhost:3000', 'http://127.0.0.1:3000'],
        methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
        credentials: true,
    });
    // ── Routes ─────────────────────────────────────────────────
    app.register(health_routes_1.healthRoutes, { prefix: '/api/health' });
    app.register(employee_routes_1.employeeRoutes, { prefix: '/api/employees' });
    app.register(other_routes_1.salaryRoutes, { prefix: '/api/salaries' });
    app.register(other_routes_1.attendanceRoutes, { prefix: '/api/attendance' });
    app.register(other_routes_1.contractRoutes, { prefix: '/api/contracts' });
    app.register(other_routes_1.recruitmentRoutes, { prefix: '/api/recruitment' });
    app.register(other_routes_1.reportRoutes, { prefix: '/api/reports' });
    app.register(other_routes_1.lookupRoutes, { prefix: '/api/lookup' });
    app.register(query_routes_1.queryRoutes, { prefix: '/api/query' });
    app.register(query_routes_1.schemaRoutes, { prefix: '/api/schema' });
    app.register(system_routes_1.systemRoutes, { prefix: '/api/system' });
    // ── Root info endpoint ─────────────────────────────────────
    app.get('/', async () => ({
        name: 'QLNS Distributed API',
        version: '1.0.0',
        queryMode: process.env.QUERY_MODE || 'direct',
        nodes: ['master:1432', 'cn1:1437', 'cn2:1435', 'cn3:1436'],
        endpoints: [
            'GET  /api/health',
            'GET  /api/employees',
            'POST /api/employees',
            'PUT  /api/employees/:id',
            'DEL  /api/employees/:id',
            'GET  /api/salaries',
            'GET  /api/attendance',
            'GET  /api/contracts',
            'GET  /api/recruitment',
            'GET  /api/reports/global',
            'GET  /api/lookup/branches',
            'GET  /api/lookup/positions',
            'GET  /api/lookup/educations',
            'GET  /api/lookup/departments',
            'POST /api/query',
            'GET  /api/schema/:nodeId',
        ],
    }));
    // ── Connect to all 4 SQL Server nodes ─────────────────────
    console.log('🔌 Connecting to distributed SQL Server nodes...');
    await database_1.db.connectAll();
    // ── Start server ───────────────────────────────────────────
    await app.listen({ port: PORT, host: HOST });
    console.log(`\n🚀 QLNS API running at http://${HOST}:${PORT}`);
    console.log(`📊 Query Mode: ${process.env.QUERY_MODE || 'direct'}`);
    // ── Graceful shutdown ──────────────────────────────────────
    const shutdown = async (signal) => {
        console.log(`\n⛔ ${signal} received — shutting down...`);
        await app.close();
        await database_1.db.closeAll();
        process.exit(0);
    };
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
}
bootstrap().catch((err) => {
    console.error('❌ Fatal error:', err);
    process.exit(1);
});
//# sourceMappingURL=index.js.map