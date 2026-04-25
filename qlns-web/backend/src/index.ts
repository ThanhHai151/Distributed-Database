import Fastify from 'fastify';
import cors from '@fastify/cors';
import helmet from '@fastify/helmet';
import dotenv from 'dotenv';

dotenv.config();

import { db } from './config/database';
import { healthRoutes } from './routes/health.routes';
import { employeeRoutes } from './routes/employee.routes';
import {
  salaryRoutes,
  attendanceRoutes,
  contractRoutes,
  recruitmentRoutes,
  reportRoutes,
  lookupRoutes,
} from './routes/other.routes';
import { queryRoutes, schemaRoutes } from './routes/query.routes';
import { systemRoutes } from './routes/system.routes';

const PORT = Number(process.env.PORT) || 4000;
const HOST = process.env.HOST || '0.0.0.0';

async function bootstrap() {
  const app = Fastify({
    logger: {
      transport: process.env.NODE_ENV === 'development'
        ? { target: 'pino-pretty', options: { colorize: true } }
        : undefined,
    },
  });

  // ── Security & CORS ────────────────────────────────────────
  await app.register(helmet, { contentSecurityPolicy: false });
  await app.register(cors, {
    origin: ['http://localhost:3000', 'http://127.0.0.1:3000'],
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    credentials: true,
  });

  // ── Routes ─────────────────────────────────────────────────
  app.register(healthRoutes,       { prefix: '/api/health' });
  app.register(employeeRoutes,     { prefix: '/api/employees' });
  app.register(salaryRoutes,       { prefix: '/api/salaries' });
  app.register(attendanceRoutes,   { prefix: '/api/attendance' });
  app.register(contractRoutes,     { prefix: '/api/contracts' });
  app.register(recruitmentRoutes,  { prefix: '/api/recruitment' });
  app.register(reportRoutes,       { prefix: '/api/reports' });
  app.register(lookupRoutes,       { prefix: '/api/lookup' });
  app.register(queryRoutes,        { prefix: '/api/query' });
  app.register(schemaRoutes,       { prefix: '/api/schema' });
  app.register(systemRoutes,       { prefix: '/api/system' });

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
  await db.connectAll();

  // ── Start server ───────────────────────────────────────────
  await app.listen({ port: PORT, host: HOST });
  console.log(`\n🚀 QLNS API running at http://${HOST}:${PORT}`);
  console.log(`📊 Query Mode: ${process.env.QUERY_MODE || 'direct'}`);

  // ── Graceful shutdown ──────────────────────────────────────
  const shutdown = async (signal: string) => {
    console.log(`\n⛔ ${signal} received — shutting down...`);
    await app.close();
    await db.closeAll();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));
}

bootstrap().catch((err) => {
  console.error('❌ Fatal error:', err);
  process.exit(1);
});
