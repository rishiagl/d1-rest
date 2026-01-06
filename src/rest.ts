import { Context } from 'hono';
import type { Env } from './index';
import { ContentfulStatusCode } from 'hono/utils/http-status';

/**
 * Sanitizes an identifier by removing all non-alphanumeric characters except underscores.
 */
function sanitizeIdentifier(identifier: string): string {
    return identifier.replace(/[^a-zA-Z0-9_]/g, '');
}

/**
 * Processing when the table name is a keyword in SQLite.
 */
function sanitizeKeyword(identifier: string): string {
    return '`'+sanitizeIdentifier(identifier)+'`';
}

/**
 * Handles GET requests to fetch records from a table
 */
async function handleGet(c: Context<{ Bindings: Env }>, tableName: string, id?: string): Promise<{status: ContentfulStatusCode, message: string, params?: any[]}> {
    const table = sanitizeKeyword(tableName);
    const searchParams = new URL(c.req.url).searchParams;
    
    try {
        let query = `SELECT * FROM ${table}`;
        const params: any[] = [];
        const conditions: string[] = [];

        // Handle ID filter
        if (id) {
            conditions.push('id = ?');
            params.push(id);
        }

        // Handle search parameters (basic filtering)
        for (const [key, value] of searchParams.entries()) {
            if (['sort_by', 'order', 'limit', 'offset'].includes(key)) continue;
            
            const sanitizedKey = sanitizeIdentifier(key);
            conditions.push(`${sanitizedKey} = ?`);
            params.push(value);
        }

        // Add WHERE clause if there are conditions
        if (conditions.length > 0) {
            query += ` WHERE ${conditions.join(' AND ')}`;
        }

        // Handle sorting
        const sortBy = searchParams.get('sort_by');
        if (sortBy) {
            const order = searchParams.get('order')?.toUpperCase() === 'DESC' ? 'DESC' : 'ASC';
            query += ` ORDER BY ${sanitizeIdentifier(sortBy)} ${order}`;
        }

        // Handle pagination
        const limit = searchParams.get('limit');
        if (limit) {
            query += ` LIMIT ?`;
            params.push(parseInt(limit));

            const offset = searchParams.get('offset');
            if (offset) {
                query += ` OFFSET ?`;
                params.push(parseInt(offset));
            }
        }
        return {status: 200, message: query, params};
    } catch (error: any) {
        return {status: 500, message: error.message};
    }
}

/**
 * Handles POST requests to create new records
 */
async function handlePost(c: Context<{ Bindings: Env }>, tableName: string): Promise<{status: ContentfulStatusCode, message: string, params?: any[]}> {
    const table = sanitizeKeyword(tableName);
    const data = await c.req.json();

    if (!data || typeof data !== 'object' || Array.isArray(data)) {
        return {status: 400, message: 'Invalid data format'};
    }

    try {
        const columns = Object.keys(data).map(sanitizeIdentifier);
        const placeholders = columns.map(() => '?').join(', ');
        const query = `INSERT INTO ${table} (${columns.join(', ')}) VALUES (${placeholders})`;
        const params = columns.map(col => data[col]);

        return {status: 200, message: query, params};
    } catch (error: any) {
        return {status: 500, message: error.message};
    }
}

/**
 * Handles PUT/PATCH requests to update records
 */
async function handleUpdate(c: Context<{ Bindings: Env }>, tableName: string, id: string): Promise<{status: ContentfulStatusCode, message: string, params?: any[]}> {
    const table = sanitizeKeyword(tableName);
    const data = await c.req.json();

    if (!data || typeof data !== 'object' || Array.isArray(data)) {
        return {status: 400, message: 'Invalid data format'};
    }

    try {
        const setColumns = Object.keys(data)
            .map(sanitizeIdentifier)
            .map(col => `${col} = ?`)
            .join(', ');

        const query = `UPDATE ${table} SET ${setColumns} WHERE id = ?`;
        const params = [...Object.values(data), id];

        return {status: 200, message: query, params};
    } catch (error: any) {
         return {status: 500, message: error.message};
    }
}

/**
 * Handles DELETE requests to remove records
 */
async function handleDelete(c: Context<{ Bindings: Env }>, tableName: string, id: string): Promise<{status: ContentfulStatusCode, message: string}> {
    const table = sanitizeKeyword(tableName);

    try {
        const query = `DELETE FROM ${table} WHERE id = ?`;
        return {status: 200, message: query};
    } catch (error: any) {
        return {status: 500, message: error.message};
    }
}

/**
 * Main REST handler that routes requests to appropriate handlers
 */
export async function handleRest(c: Context<{ Bindings: Env }>): Promise<Response> {
    const url = new URL(c.req.url);
    const pathParts = url.pathname.split('/').filter(Boolean);
    
    if (pathParts.length < 3) {
        return c.json({ error: 'Invalid path. Expected format: /rest/{db_name}/{tableName}/{id?}' }, 400);
    }

    const db_name = pathParts[1];
    const tableName = pathParts[2];
    const id = pathParts[3];
    let db = null;
    switch (c.req.method) {
        case 'GET':
            const result = await handleGet(c, tableName, id);
            const { status, message, params } = result;
            if (status !== 200) {
                return c.json({ error: message }, status);
            }
            const query = message;
            switch (db_name) {
                case 'mcw_db':
                    db = c.env.MCW_DB;
                    break;
                case 'utility_db':
                    db = c.env.UTILITY_DB;
                    break;
                case 'catalog_db':
                    db = c.env.CATALOG_DB;
                    break;
                default:
                    return c.json({ error: 'Unknown database name' }, 400);
            }
            try {
                const results = await db.prepare(query)
                .bind(...(params || []))
                .all();
                return c.json(results);
            }
            catch (error: any) {
                return c.json({ error: error.message }, 500);
            }   
        case 'POST':
            const postResult = await handlePost(c, tableName);
            const { status: postStatus, message: postMessage, params: postParams } = postResult;
            if (postStatus !== 200) {
                return c.json({ error: postMessage }, postStatus);
            }
            switch (db_name) {
                case 'mcw_db':
                    db = c.env.MCW_DB;
                    break;
                case 'utility_db':
                    db = c.env.UTILITY_DB;
                    break;
                case 'catalog_db':
                    db = c.env.CATALOG_DB;
                    break;
                default:
                    return c.json({ error: 'Unknown database name' }, 400);
            }
            try {
                const result = await db.prepare(postMessage)
                .bind(...(postParams || []))
                .run();
                return c.json({ message: 'Resource created successfully', data: result }, 201);
            }
            catch (error: any) {
                return c.json({ error: error.message }, 500);
            }
        case 'PUT':
        case 'PATCH':
            if (!id) return c.json({ error: 'ID is required for updates' }, 400);
            const patchResult = await handleUpdate(c, tableName, id);
            const { status: patchStatus, message: patchMessage, params: patchParams } = patchResult;
            if (patchStatus !== 200) {
                return c.json({ error: patchMessage }, patchStatus);
            }
            switch (db_name) {
                case 'mcw_db':
                    db = c.env.MCW_DB;
                    break;
                case 'utility_db':
                    db = c.env.UTILITY_DB;
                    break;
                case 'catalog_db':
                    db = c.env.CATALOG_DB;
                    break;
                default:
                    return c.json({ error: 'Unknown database name' }, 400);
            }
            try {
                const results = await db.prepare(patchMessage)
                .bind(...(params || []))
                .all();
                return c.json("Resource updated successfully", 200);
            }
            catch (error: any) {
                return c.json({ error: error.message }, 500);
            }
        case 'DELETE':
            if (!id) return c.json({ error: 'ID is required for deletion' }, 400);
            const deleteResult = await handleDelete(c, tableName, id);
            const { status: deleteStatus, message: deleteMessage } = deleteResult;
            if (deleteStatus !== 200) {
                return c.json({ error: deleteMessage }, deleteStatus);
            }
            switch (db_name) {
                case 'mcw_db':
                    db = c.env.MCW_DB;
                    break;
                case 'utility_db':
                    db = c.env.UTILITY_DB;
                    break;
                case 'catalog_db':
                    db = c.env.CATALOG_DB;
                    break;
                default:
                    return c.json({ error: 'Unknown database name' }, 400);
            }
            try {
                const results = await db.prepare(deleteMessage)
                .bind()
                .run();
                return c.json({ message: 'Resource deleted successfully' }, 200);
            }
            catch (error: any) {
                return c.json({ error: error.message }, 500);
            }
        default:
            return c.json({ error: 'Method not allowed' }, 405);
    }
} 