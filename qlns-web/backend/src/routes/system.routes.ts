import { FastifyInstance } from 'fastify';
import { db, NodeInfo } from '../config/database';

export async function systemRoutes(app: FastifyInstance) {
  // GET all nodes config
  app.get('/nodes', async (_, reply) => {
    return reply.send({ success: true, data: db.getStatus() });
  });

  // ADD new node
  app.post<{ Body: NodeInfo }>('/nodes', async (req, reply) => {
    try {
      if (!req.body.id) throw new Error('Missing node id');
      await db.addNode(req.body);
      return reply.send({ success: true, message: 'Node added/updated correctly' });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });

  // UPDATE node
  app.put<{ Params: { id: string }, Body: NodeInfo }>('/nodes/:id', async (req, reply) => {
    try {
      const data = { ...req.body, id: req.params.id };
      await db.addNode(data);
      return reply.send({ success: true, message: 'Node updated correctly' });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });

  // DELETE node
  app.delete<{ Params: { id: string } }>('/nodes/:id', async (req, reply) => {
    try {
      if (req.params.id === 'master') throw new Error('Cannot delete master node');
      await db.deleteNode(req.params.id);
      return reply.send({ success: true, message: 'Node deleted correctly' });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });

  // API to trigger reconnect
  app.post<{ Params: { id: string } }>('/nodes/:id/reconnect', async (req, reply) => {
    try {
      await db.connectNode(req.params.id);
      return reply.send({ success: true, message: 'Reconnect requested' });
    } catch (err) {
      return reply.status(500).send({ success: false, error: (err as Error).message });
    }
  });
}
