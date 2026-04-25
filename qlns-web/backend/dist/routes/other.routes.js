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
exports.salaryRoutes = salaryRoutes;
exports.attendanceRoutes = attendanceRoutes;
exports.contractRoutes = contractRoutes;
exports.recruitmentRoutes = recruitmentRoutes;
exports.reportRoutes = reportRoutes;
exports.lookupRoutes = lookupRoutes;
const svc = __importStar(require("../services/distributed.service"));
async function salaryRoutes(app) {
    // GET /api/salaries?branch=CN1&thang=1&nam=2024
    app.get('/', async (req, reply) => {
        const start = Date.now();
        const { branch, thang, nam } = req.query;
        try {
            const { data, sourceNodes } = await svc.getSalaries(branch, thang ? Number(thang) : undefined, nam ? Number(nam) : undefined);
            return reply.send({
                success: true, data,
                meta: { totalRows: data.length, sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
            });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
}
async function attendanceRoutes(app) {
    app.get('/', async (req, reply) => {
        const start = Date.now();
        const { branch, thang, nam } = req.query;
        try {
            const { data, sourceNodes } = await svc.getAttendance(branch, thang ? Number(thang) : undefined, nam ? Number(nam) : undefined);
            return reply.send({
                success: true, data,
                meta: { totalRows: data.length, sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
            });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
}
async function contractRoutes(app) {
    app.get('/', async (req, reply) => {
        const start = Date.now();
        try {
            const { data, sourceNodes } = await svc.getContracts(req.query.branch);
            return reply.send({
                success: true, data,
                meta: { totalRows: data.length, sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
            });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
}
async function recruitmentRoutes(app) {
    app.get('/', async (req, reply) => {
        const start = Date.now();
        try {
            const { data, sourceNodes } = await svc.getRecruitments(req.query.branch);
            return reply.send({
                success: true, data,
                meta: { totalRows: data.length, sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
            });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
    app.post('/', async (req, reply) => {
        try {
            const { sourceNodes } = await svc.createRecruitment(req.body);
            return reply.send({ success: true, meta: { sourceNodes } });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
    app.put('/:id', async (req, reply) => {
        try {
            const { sourceNodes } = await svc.updateRecruitment(req.params.id, req.body);
            return reply.send({ success: true, meta: { sourceNodes } });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
    app.delete('/:id', async (req, reply) => {
        try {
            const { sourceNodes } = await svc.deleteRecruitment(req.params.id);
            return reply.send({ success: true, meta: { sourceNodes } });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
}
async function reportRoutes(app) {
    // GET /api/reports/global — aggregate from all branches
    app.get('/global', async (req, reply) => {
        const start = Date.now();
        try {
            const { data, sourceNodes } = await svc.getGlobalStats();
            return reply.send({
                success: true, data,
                meta: { sourceNodes, queryMode: process.env.QUERY_MODE, executionTimeMs: Date.now() - start },
            });
        }
        catch (err) {
            return reply.status(500).send({ success: false, error: err.message });
        }
    });
}
async function lookupRoutes(app) {
    app.get('/branches', async (_, reply) => reply.send({ success: true, data: await svc.getBranches() }));
    app.get('/positions', async (_, reply) => reply.send({ success: true, data: await svc.getPositions() }));
    app.get('/educations', async (_, reply) => reply.send({ success: true, data: await svc.getEducations() }));
    app.get('/departments', async (_, reply) => reply.send({ success: true, data: await svc.getDepartments() }));
}
//# sourceMappingURL=other.routes.js.map