"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.employeeRoutes = employeeRoutes;
const svc = __importStar(require("../services/distributed.service"));
async function employeeRoutes(app) {
    // GET /api/employees?branch=CN1
    app.get('/', async (req, reply) => {
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
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
    // GET /api/employees/:id
    app.get('/:id', async (req, reply) => {
        const start = Date.now();
        try {
            const { data, sourceNode } = await svc.getEmployeeById(req.params.id);
            if (!data)
                return reply.status(404).send({ success: false, error: 'Employee not found' });
            return reply.send({
                success: true,
                data,
                meta: { sourceNodes: sourceNode ? [sourceNode] : [], queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
            });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
    // POST /api/employees
    app.post('/', async (req, reply) => {
        const start = Date.now();
        try {
            const { sourceNodes } = await svc.createEmployee(req.body);
            return reply.status(201).send({
                success: true,
                data: { message: 'Employee created successfully', idnv: req.body.IDNV },
                meta: { sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
            });
        }
        catch (err) {
            return reply.status(400).send({ success: false, error: err.message });
        }
    });
    // PUT /api/employees/:id
    app.put('/:id', async (req, reply) => {
        const start = Date.now();
        try {
            const { sourceNodes } = await svc.updateEmployee(req.params.id, req.body);
            return reply.send({
                success: true,
                data: { message: 'Employee updated successfully' },
                meta: { sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
            });
        }
        catch (err) {
            return reply.status(400).send({ success: false, error: err.message });
        }
    });
    // DELETE /api/employees/:id
    app.delete('/:id', async (req, reply) => {
        const start = Date.now();
        try {
            const { sourceNodes } = await svc.deleteEmployee(req.params.id);
            return reply.send({
                success: true,
                data: { message: 'Employee deleted successfully' },
                meta: { sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
            });
        }
        catch (err) {
            return reply.status(400).send({ success: false, error: err.message });
        }
    });
}
//# sourceMappingURL=employee.routes.js.map