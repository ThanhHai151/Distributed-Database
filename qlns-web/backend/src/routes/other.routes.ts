import { FastifyInstance } from 'fastify';
import * as svc from '../services/distributed.service';

export async function salaryRoutes(app: FastifyInstance) {
  // GET /api/salaries?branch=CN1&thang=1&nam=2024
  app.get<{ Querystring: { branch?: string; thang?: string; nam?: string } }>('/', async (req, reply) => {
    const start = Date.now();
    const { branch, thang, nam } = req.query;
    try {
      const { data, sourceNodes } = await svc.getSalaries(
        branch, thang ? Number(thang) : undefined, nam ? Number(nam) : undefined
      );
      return reply.send({
        success: true, data,
        meta: { totalRows: data.length, sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
      });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });
}

export async function attendanceRoutes(app: FastifyInstance) {
  app.get<{ Querystring: { branch?: string; thang?: string; nam?: string } }>('/', async (req, reply) => {
    const start = Date.now();
    const { branch, thang, nam } = req.query;
    try {
      const { data, sourceNodes } = await svc.getAttendance(
        branch, thang ? Number(thang) : undefined, nam ? Number(nam) : undefined
      );
      return reply.send({
        success: true, data,
        meta: { totalRows: data.length, sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
      });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });
}

export async function contractRoutes(app: FastifyInstance) {
  app.get<{ Querystring: { branch?: string } }>('/', async (req, reply) => {
    const start = Date.now();
    try {
      const { data, sourceNodes } = await svc.getContracts(req.query.branch);
      return reply.send({
        success: true, data,
        meta: { totalRows: data.length, sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
      });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });
}

export async function recruitmentRoutes(app: FastifyInstance) {
  app.get<{ Querystring: { branch?: string } }>('/', async (req, reply) => {
    const start = Date.now();
    try {
      const { data, sourceNodes } = await svc.getRecruitments(req.query.branch);
      return reply.send({
        success: true, data,
        meta: { totalRows: data.length, sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
      });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });

  app.post<{ Body: import('../types').CreateRecruitmentDto }>('/', async (req, reply) => {
    try {
      const { sourceNodes } = await svc.createRecruitment(req.body);
      return reply.send({ success: true, meta: { sourceNodes } });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });

  app.put<{ Params: { id: string }, Body: import('../types').UpdateRecruitmentDto }>('/:id', async (req, reply) => {
    try {
      const { sourceNodes } = await svc.updateRecruitment(req.params.id, req.body);
      return reply.send({ success: true, meta: { sourceNodes } });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });

  app.delete<{ Params: { id: string } }>('/:id', async (req, reply) => {
    try {
      const { sourceNodes } = await svc.deleteRecruitment(req.params.id);
      return reply.send({ success: true, meta: { sourceNodes } });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });
}

export async function reportRoutes(app: FastifyInstance) {
  // GET /api/reports/global — aggregate from all branches
  app.get('/global', async (req, reply) => {
    const start = Date.now();
    try {
      const { data, sourceNodes } = await svc.getGlobalStats();
      return reply.send({
        success: true, data,
        meta: { sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
      });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });
}

export async function lookupRoutes(app: FastifyInstance) {
  app.get('/branches', async (_, reply) => reply.send({ success: true, data: await svc.getBranches() }));
  app.get('/positions', async (_, reply) => reply.send({ success: true, data: await svc.getPositions() }));
  app.get('/educations', async (_, reply) => reply.send({ success: true, data: await svc.getEducations() }));
  app.get('/departments', async (_, reply) => reply.send({ success: true, data: await svc.getDepartments() }));
}
