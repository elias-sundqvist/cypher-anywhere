import {
  StorageAdapter,
  NodeRecord,
  RelRecord,
  NodeScanSpec,
  TransactionCtx,
  IndexMetadata,
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
}
