import { Hono, Context, Next } from "hono";
import { cors } from "hono/cors";
import { handleRest } from './rest';

export interface Env {
    MCW_DB: D1Database;
    UTILITY_DB: D1Database;
    CATALOG_DB: D1Database;
    SECRET: SecretsStoreSecret;
}

// # List all users
// GET /rest/users

// # Get filtered and sorted users
// GET /rest/users?age=25&sort_by=name&order=desc

// # Get paginated results
// GET /rest/users?limit=10&offset=20

// # Create a new user
// POST /rest/users
// { "name": "John", "age": 30 }

// # Update a user
// PATCH /rest/users/123
// { "age": 31 }

// # Delete a user
// DELETE /rest/users/123

export default {
    async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
        const app = new Hono<{ Bindings: Env }>();

        // Apply CORS to all routes
        app.use('*', async (c, next) => {
            return cors()(c, next);
        })

        // Secret Store key value that we have set
        const secret = await env.SECRET.get();

        // Authentication middleware that verifies the Authorization header
        // is sent in on each request and matches the value of our Secret key.
        // If a match is not found we return a 401 and prevent further access.
        const authMiddleware = async (c: Context, next: Next) => {
            const authHeader = c.req.header('Authorization');
            if (!authHeader) {
                return c.json({ error: 'Unauthorized' }, 401);
            }

            const token = authHeader.startsWith('Bearer ')
                ? authHeader.substring(7)
                : authHeader;

            if (token !== secret) {
                return c.json({ error: 'Unauthorized' }, 401);
            }

            return next();
        };

        // CRUD REST endpoints made available to all of our tables
        app.all('/rest/*', authMiddleware, handleRest);

        // Execute a raw SQL statement with parameters with this route
        app.post('/query/*', authMiddleware, async (c) => {
            try {
                const url = new URL(c.req.url);
                const pathParts = url.pathname.split('/').filter(Boolean);
    
                if (pathParts.length < 2) {
                    return c.json({ error: 'Invalid path. Expected format: /rest/{db_name}' }, 400);
                }

                const db_name = pathParts[1];
                const body = await c.req.json();
                const { query, params } = body;

                if (!query) {
                    return c.json({ error: 'Query is required' }, 400);
                }

                // Execute the query against D1 database
                switch (db_name) {
                    case 'mcw_db':
                        const results = await env.MCW_DB.prepare(query)
                        .bind(...(params || []))
                        .all();
                        return c.json(results);
                    case 'utility_db':
                        const utilityResults = await env.UTILITY_DB.prepare(query)
                        .bind(...(params || []))
                        .all();
                        return c.json(utilityResults);
                    case 'catalog_db':
                        const catalogResults = await env.CATALOG_DB.prepare(query)
                        .bind(...(params || []))
                        .all();
                        return c.json(catalogResults);
                    default:
                        return c.json({ error: 'Unknown database name' }, 400);
                }
            } catch (error: any) {
                return c.json({ error: error.message }, 500);
            }
        });

        return app.fetch(request, env, ctx);
    }
} satisfies ExportedHandler<Env>;
