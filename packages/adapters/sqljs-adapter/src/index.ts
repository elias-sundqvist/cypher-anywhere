import {
  StorageAdapter,
  NodeRecord,
  RelRecord,
  NodeScanSpec,
  TransactionCtx,
  IndexMetadata,
  parseMany,
  MatchReturnQuery
} from '@cypher-anywhere/core';
import type * as fsType from 'fs';
import initSqlJs, { Database } from 'sql.js';

// Declare Node's require for TypeScript without pulling in Node types
declare var require: (module: string) => unknown;

export interface Dataset {
  nodes: NodeRecord[];
  relationships: RelRecord[];
}

export interface SqlJsAdapterOptions {
  datasetPath?: string;
  dataset?: Dataset;
  indexes?: IndexMetadata[];
}

export class SqlJsAdapter implements StorageAdapter {
  private db!: Database;
  private ready: Promise<void>;
  private indexes: IndexMetadata[];

  constructor(options: SqlJsAdapterOptions) {
    this.indexes = options.indexes ?? [];
    this.ready = this.init(options);
  }

  private async init(options: SqlJsAdapterOptions): Promise<void> {
    const SQL = await initSqlJs();
    this.db = new SQL.Database();
    this.db.run(
      'CREATE TABLE nodes (id INTEGER PRIMARY KEY, labels TEXT, properties TEXT)'
    );
    this.db.run(
      'CREATE TABLE edges (id INTEGER PRIMARY KEY, type TEXT, startNode INTEGER, endNode INTEGER, properties TEXT)'
    );
    let data: Dataset | undefined = options.dataset;
    if (!data && options.datasetPath) {
      const fs = require('fs') as typeof fsType;
      const text = fs.readFileSync(options.datasetPath, 'utf8');
      data = JSON.parse(text);
    }
    if (!data) throw new Error('dataset or datasetPath must be provided');
    const insertNode = this.db.prepare(
      'INSERT INTO nodes VALUES (?, ?, ?)'
    );
    for (const n of data.nodes) {
      insertNode.run([n.id, JSON.stringify(n.labels), JSON.stringify(n.properties)]);
    }
    insertNode.free();
    const insertRel = this.db.prepare(
      'INSERT INTO edges VALUES (?, ?, ?, ?, ?)'
    );
    for (const r of data.relationships) {
      insertRel.run([
        r.id,
        r.type,
        r.startNode,
        r.endNode,
        JSON.stringify(r.properties),
      ]);
    }
    insertRel.free();
  }

  private async ensureReady(): Promise<void> {
    await this.ready;
  }

  private rowToNode(row: any): NodeRecord {
    return {
      id: row.id,
      labels: JSON.parse(row.labels),
      properties: JSON.parse(row.properties || '{}'),
    };
  }

  private rowToRel(row: any): RelRecord {
    return {
      id: row.id,
      type: row.type,
      startNode: row.startNode,
      endNode: row.endNode,
      properties: JSON.parse(row.properties || '{}'),
    };
  }

  async getNodeById(id: number | string): Promise<NodeRecord | null> {
    await this.ensureReady();
    const stmt = this.db.prepare(
      'SELECT id, labels, properties FROM nodes WHERE id = ?'
    );
    stmt.bind([id]);
    const row = stmt.step() ? stmt.getAsObject() : null;
    stmt.free();
    return row ? this.rowToNode(row) : null;
  }

  async *scanNodes(spec: NodeScanSpec = {}): AsyncIterable<NodeRecord> {
    await this.ensureReady();
    const stmt = this.db.prepare(
      'SELECT id, labels, properties FROM nodes'
    );
    while (stmt.step()) {
      const row = this.rowToNode(stmt.getAsObject());
      const { label, labels } = spec;
      const labelMatch = label ? row.labels.includes(label) : true;
      const labelsMatch = labels ? labels.every(l => row.labels.includes(l)) : true;
      if (labelMatch && labelsMatch) {
        yield row;
      }
    }
    stmt.free();
  }

  async createNode(labels: string[], properties: Record<string, unknown>): Promise<NodeRecord> {
    await this.ensureReady();
    const stmt = this.db.prepare('SELECT MAX(id) as id FROM nodes');
    const row = stmt.step() ? stmt.getAsObject() : { id: 0 };
    stmt.free();
    const id = Number(row.id) + 1;
    const node: NodeRecord = { id, labels, properties };
    this.db.run('INSERT INTO nodes VALUES (?, ?, ?)', [id, JSON.stringify(labels), JSON.stringify(properties)]);
    return node;
  }

  async deleteNode(id: number | string): Promise<void> {
    await this.ensureReady();
    this.db.run('DELETE FROM edges WHERE startNode = ? OR endNode = ?', [id, id]);
    this.db.run('DELETE FROM nodes WHERE id = ?', [id]);
  }

  async updateNodeProperties(id: number | string, properties: Record<string, unknown>): Promise<void> {
    await this.ensureReady();
    const node = await this.getNodeById(id);
    if (!node) throw new Error('node not found');
    Object.assign(node.properties, properties);
    this.db.run('UPDATE nodes SET properties = ? WHERE id = ?', [JSON.stringify(node.properties), id]);
  }

  async findNode(labels: string[], properties: Record<string, unknown>): Promise<NodeRecord | null> {
    await this.ensureReady();
    const stmt = this.db.prepare('SELECT id, labels, properties FROM nodes');
    while (stmt.step()) {
      const row = this.rowToNode(stmt.getAsObject());
      if (labels.length && !labels.every(l => row.labels.includes(l))) continue;
      let ok = true;
      for (const [k, v] of Object.entries(properties)) {
        if (row.properties[k] !== v) {
          ok = false;
          break;
        }
      }
      if (ok) {
        stmt.free();
        return row;
      }
    }
    stmt.free();
    return null;
  }

  async *indexLookup(label: string | undefined, property: string, value: unknown): AsyncIterable<NodeRecord> {
    await this.ensureReady();
    const stmt = this.db.prepare('SELECT id, labels, properties FROM nodes');
    while (stmt.step()) {
      const row = this.rowToNode(stmt.getAsObject());
      if (label && !row.labels.includes(label)) continue;
      if (row.properties[property] === value) {
        yield row;
      }
    }
    stmt.free();
  }

  async listIndexes(): Promise<IndexMetadata[]> {
    await this.ensureReady();
    return this.indexes;
  }

  async getRelationshipById(id: number | string): Promise<RelRecord | null> {
    await this.ensureReady();
    const stmt = this.db.prepare(
      'SELECT id, type, startNode, endNode, properties FROM edges WHERE id = ?'
    );
    stmt.bind([id]);
    const row = stmt.step() ? stmt.getAsObject() : null;
    stmt.free();
    return row ? this.rowToRel(row) : null;
  }

  async *scanRelationships(): AsyncIterable<RelRecord> {
    await this.ensureReady();
    const stmt = this.db.prepare(
      'SELECT id, type, startNode, endNode, properties FROM edges'
    );
    while (stmt.step()) {
      yield this.rowToRel(stmt.getAsObject());
    }
    stmt.free();
  }

  async createRelationship(type: string, startNode: number | string, endNode: number | string, properties: Record<string, unknown>): Promise<RelRecord> {
    await this.ensureReady();
    const stmt = this.db.prepare('SELECT MAX(id) as id FROM edges');
    const row = stmt.step() ? stmt.getAsObject() : { id: 0 };
    stmt.free();
    const id = Number(row.id) + 1;
    const rel: RelRecord = { id, type, startNode, endNode, properties };
    this.db.run('INSERT INTO edges VALUES (?, ?, ?, ?, ?)', [id, type, startNode, endNode, JSON.stringify(properties)]);
    return rel;
  }

  async deleteRelationship(id: number | string): Promise<void> {
    await this.ensureReady();
    this.db.run('DELETE FROM edges WHERE id = ?', [id]);
  }

  async updateRelationshipProperties(id: number | string, properties: Record<string, unknown>): Promise<void> {
    await this.ensureReady();
    const rel = await this.getRelationshipById(id);
    if (!rel) throw new Error('relationship not found');
    Object.assign(rel.properties, properties);
    this.db.run('UPDATE edges SET properties = ? WHERE id = ?', [JSON.stringify(rel.properties), id]);
  }

  async beginTransaction(): Promise<TransactionCtx> {
    await this.ensureReady();
    this.db.run('BEGIN TRANSACTION');
    return {};
  }

  async commit(_: TransactionCtx): Promise<void> {
    await this.ensureReady();
    this.db.run('COMMIT');
  }

  async rollback(_: TransactionCtx): Promise<void> {
    await this.ensureReady();
    this.db.run('ROLLBACK');
  }

  supportsTranspilation = true;

  runTranspiled(
    cypher: string,
    params: Record<string, any>
  ): AsyncIterable<Record<string, unknown>> | null {
    const asts = parseMany(cypher);
    if (asts.length !== 1) return null;
    const ast = asts[0];
    if (ast.type !== 'MatchReturn') return null;
    const matchAst = ast as MatchReturnQuery;
    if (
      matchAst.isRelationship ||
      (matchAst.labels && matchAst.labels.length > 1) ||
      matchAst.optional
    )
      return null;
    for (const ri of matchAst.returnItems) {
      const expr = ri.expression;
      if (expr.type === 'Variable') {
        if (expr.name !== matchAst.variable) return null;
      } else if (expr.type === 'Property') {
        if (expr.variable !== matchAst.variable) return null;
      } else if (expr.type === 'Id' || expr.type === 'Labels') {
        if (expr.variable !== matchAst.variable) return null;
      } else if (expr.type === 'All') {
        if (matchAst.returnItems.length !== 1) return null;
      } else {
        return null;
      }
    }
    const aliasMap = new Map<string, any>();
    if (
      matchAst.returnItems.length === 1 &&
      matchAst.returnItems[0].expression.type === 'All'
    ) {
      const name = matchAst.returnItems[0].alias || matchAst.variable;
      aliasMap.set(name, { type: 'Variable', name: matchAst.variable });
    } else {
      for (const ri of matchAst.returnItems) {
        const expr = ri.expression as any;
        const name =
          ri.alias ||
          (expr.type === 'Variable'
            ? expr.name
            : expr.type === 'Property'
            ? expr.property
            : expr.type === 'Id'
            ? 'id'
            : expr.type === 'Labels'
            ? 'labels'
            : 'val');
        aliasMap.set(name, ri.expression);
      }
    }

    let sql = 'SELECT id, labels, properties FROM nodes';
    const paramsArr: any[] = [];
    const conds: string[] = [];
    if (matchAst.labels && matchAst.labels.length === 1) {
      conds.push('labels LIKE ?');
      paramsArr.push(`%"${matchAst.labels[0]}"%`);
    }
    if (matchAst.properties) {
      for (const [k, v] of Object.entries(matchAst.properties)) {
        const val =
          v && typeof v === 'object' && 'type' in v
            ? v.type === 'Literal'
              ? (v as any).value
              : v.type === 'Parameter'
              ? params[(v as any).name]
              : undefined
            : v;
        if (val === null) {
          conds.push(`json_extract(properties, '$.${k}') IS NULL`);
        } else {
          conds.push(`json_extract(properties, '$.${k}') = ?`);
          paramsArr.push(val);
        }
      }
    }

    function toValue(expr: any): any {
      if (!expr) return undefined;
      if (typeof expr === 'object' && 'type' in expr) {
        if (expr.type === 'Literal') return expr.value;
        if (expr.type === 'Parameter') return params[expr.name];
      }
      return expr;
    }

    function convertWhere(where: any): [string, any[]] | null {
      if (!where) return ['1', []];
      if (where.type === 'Condition') {
        if (where.left.type !== 'Property' || where.left.variable !== matchAst.variable)
          return null;
        const col = `json_extract(properties, '$.${where.left.property}')`;
        switch (where.operator) {
          case '=':
          case '<>': {
            const val = toValue(where.right);
            if (val === undefined) return null;
            if (val === null) {
              return [
                `${col} ${where.operator === '=' ? 'IS' : 'IS NOT'} NULL`,
                [],
              ];
            }
            return [`${col} ${where.operator} ?`, [val]];
          }
          case '>':
          case '>=':
          case '<':
          case '<=': {
            const val = toValue(where.right);
            if (val === undefined) return null;
            return [`${col} ${where.operator} ?`, [val]];
          }
          case 'IN': {
            const val = toValue(where.right);
            if (!Array.isArray(val)) return null;
            if (val.length === 0) return ['0', []];
            const placeholders = val.map(() => '?').join(', ');
            return [`${col} IN (${placeholders})`, val];
          }
          case 'IS NULL':
            return [`${col} IS NULL`, []];
          case 'IS NOT NULL':
            return [`${col} IS NOT NULL`, []];
          case 'STARTS WITH': {
            const val = toValue(where.right);
            if (typeof val !== 'string') return null;
            return [`${col} LIKE ?`, [val + '%']];
          }
          case 'ENDS WITH': {
            const val = toValue(where.right);
            if (typeof val !== 'string') return null;
            return [`${col} LIKE ?`, ['%' + val]];
          }
          case 'CONTAINS': {
            const val = toValue(where.right);
            if (typeof val !== 'string') return null;
            return [`${col} LIKE ?`, ['%' + val + '%']];
          }
          default:
            return null;
        }
      }
      if (where.type === 'And' || where.type === 'Or') {
        const l = convertWhere(where.left);
        const r = convertWhere(where.right);
        if (!l || !r) return null;
        const op = where.type === 'And' ? 'AND' : 'OR';
        return [`(${l[0]} ${op} ${r[0]})`, [...l[1], ...r[1]]];
      }
      if (where.type === 'Not') {
        const inner = convertWhere(where.clause);
        if (!inner) return null;
        return [`NOT (${inner[0]})`, inner[1]];
      }
      return null;
    }

    const whereSql = convertWhere(matchAst.where);
    if (!whereSql) return null;
    if (whereSql[0] !== '1') {
      conds.push(whereSql[0]);
      paramsArr.push(...whereSql[1]);
    }
    if (conds.length > 0) sql += ' WHERE ' + conds.join(' AND ');
    const self = this;
    async function* gen() {
      await self.ensureReady();
      const stmt = self.db.prepare(sql);
      stmt.bind(paramsArr);
      let rows: { node: NodeRecord; data: Record<string, any> }[] = [];
      while (stmt.step()) {
        const node = self.rowToNode(stmt.getAsObject());
        const data: Record<string, any> = {};
        for (const [name, expr] of aliasMap.entries()) {
          if (expr.type === 'Variable') {
            data[name] = node;
          } else if (expr.type === 'Property') {
            data[name] = node.properties[expr.property];
          } else if (expr.type === 'Id') {
            data[name] = node.id;
          } else if (expr.type === 'Labels') {
            data[name] = node.labels;
          }
        }
        rows.push({ node, data });
      }
      stmt.free();

      if (matchAst.distinct) {
        const seen = new Set<string>();
        const uniq: { node: NodeRecord; data: Record<string, any> }[] = [];
        for (const r of rows) {
          const key = JSON.stringify(r.data);
          if (!seen.has(key)) {
            seen.add(key);
            uniq.push(r);
          }
        }
        rows = uniq;
      }

      if (matchAst.orderBy && matchAst.orderBy.length > 0) {
        rows.sort((a, b) => {
          for (const ob of matchAst.orderBy!) {
            let aval: any;
            let bval: any;
            const expr =
              ob.expression.type === 'Variable' && aliasMap.has(ob.expression.name)
                ? aliasMap.get(ob.expression.name)
                : ob.expression;
            if (expr.type === 'Variable') {
              aval = a.node;
              bval = b.node;
            } else if (expr.type === 'Property') {
              aval = a.node.properties[expr.property];
              bval = b.node.properties[expr.property];
            } else if (expr.type === 'Id') {
              aval = a.node.id;
              bval = b.node.id;
            } else if (expr.type === 'Labels') {
              aval = a.node.labels.join(',');
              bval = b.node.labels.join(',');
            } else {
              aval = undefined;
              bval = undefined;
            }
            if (aval < bval) return ob.direction === 'DESC' ? 1 : -1;
            if (aval > bval) return ob.direction === 'DESC' ? -1 : 1;
          }
          return 0;
        });
      }

      let start = 0;
      if (matchAst.skip) {
        const v = toValue(matchAst.skip);
        start = typeof v === 'number' ? v : 0;
      }
      let end: number | undefined = undefined;
      if (matchAst.limit) {
        const v = toValue(matchAst.limit);
        if (typeof v === 'number') {
          end = start + v;
        }
      }
      const slice = rows.slice(start, end);
      for (const r of slice) {
        yield r.data;
      }
    }
    return gen();
  }
}
