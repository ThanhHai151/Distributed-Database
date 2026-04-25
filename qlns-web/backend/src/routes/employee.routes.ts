import { FastifyInstance } from 'fastify';
import * as svc from '../services/distributed.service';
import type { CreateEmployeeDto, UpdateEmployeeDto } from '../types';

export async function employeeRoutes(app: FastifyInstance) {
  // GET /api/employees?branch=CN1
  app.get<{ Querystring: { branch?: string } }>('/', async (req, reply) => {
    const start = Date.now();
    const { branch } = req.query;
    try {
      const { data, sourceNodes } = await svc.getAllEmployees(branch);
      return reply.send({
        success: true,
        data,
        meta: {
          totalRows: data.length,
          sourceNodes,
          queryMode: process.env.QUERY_MODE,
          executionTimeMs: Date.now() - start,
        },
      });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });

  // GET /api/employees/:id
  app.get<{ Params: { id: string } }>('/:id', async (req, reply) => {
    const start = Date.now();
    try {
      const { data, sourceNode } = await svc.getEmployeeById(req.params.id);
      if (!data) return reply.status(404).send({ success: false, error: 'Employee not found' });
      return reply.send({
        success: true,
        data,
        meta: { sourceNodes: sourceNode ? [sourceNode] : [], queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
      });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });

  // POST /api/employees
  app.post<{ Body: CreateEmployeeDto }>('/', async (req, reply) => {
    const start = Date.now();
    try {
      const { sourceNodes } = await svc.createEmployee(req.body);
      return reply.status(201).send({
        success: true,
        data: { message: 'Employee created successfully', idnv: req.body.IDNV },
        meta: { sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
      });
    } catch (err) {
      return reply.status(400).send({ success: false, error: (err as Error).message });
    }
  });

  // PUT /api/employees/:id
  app.put<{ Params: { id: string }; Body: UpdateEmployeeDto }>('/:id', async (req, reply) => {
    const start = Date.now();
    try {
      const { sourceNodes } = await svc.updateEmployee(req.params.id, req.body);
      return reply.send({
        success: true,
        data: { message: 'Employee updated successfully' },
        meta: { sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
      });
    } catch (err) {
      return reply.status(400).send({ success: false, error: (err as Error).message });
    }
  });

  // DELETE /api/employees/:id
  app.delete<{ Params: { id: string } }>('/:id', async (req, reply) => {
    const start = Date.now();
    try {
      const { sourceNodes } = await svc.deleteEmployee(req.params.id);
      return reply.send({
        success: true,
        data: { message: 'Employee deleted successfully' },
        meta: { sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
      });
    } catch (err) {
      return reply.status(400).send({ success: false, error: (err as Error).message });
    }
  });
}
