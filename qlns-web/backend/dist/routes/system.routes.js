"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.systemRoutes = systemRoutes;
const database_1 = require("../config/database");
async function systemRoutes(app) {
    // GET all nodes config
    app.get('/nodes', async (_, reply) => {
        return reply.send({ success: true, data: database_1.db.getStatus() });
    });
    // ADD new node
    app.post('/nodes', async (req, reply) => {
        try {
            if (!req.body.id)
                throw new Error('Missing node id');
            await database_1.db.addNode(req.body);
            return reply.send({ success: true, message: 'Node added/updated correctly' });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
    // UPDATE node
    app.put('/nodes/:id', async (req, reply) => {
        try {
            const data = { ...req.body, id: req.params.id };
            await database_1.db.addNode(data);
            return reply.send({ success: true, message: 'Node updated correctly' });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
    // DELETE node
    app.delete('/nodes/:id', async (req, reply) => {
        try {
            if (req.params.id === 'master')
                throw new Error('Cannot delete master node');
            await database_1.db.deleteNode(req.params.id);
            return reply.send({ success: true, message: 'Node deleted correctly' });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
    // API to trigger reconnect
    app.post('/nodes/:id/reconnect', async (req, reply) => {
        try {
            await database_1.db.connectNode(req.params.id);
            return reply.send({ success: true, message: 'Reconnect requested' });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
}
//# sourceMappingURL=system.routes.js.map