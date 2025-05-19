import { StorageAdapter, NodeRecord, RelRecord, NodeScanSpec, TransactionCtx } from '@cypher-anywhere/core';
import * as fs from 'fs';

interface Dataset {
  nodes: NodeRecord[];
  relationships: RelRecord[];
}

export interface JsonAdapterOptions {
  datasetPath?: string;
  dataset?: Dataset;
}

export class JsonAdapter implements StorageAdapter {
  private data: Dataset;
  private txData?: Dataset;

  constructor(options: JsonAdapterOptions) {
    if (options.dataset) {
      this.data = options.dataset;
    } else if (options.datasetPath) {
      const text = fs.readFileSync(options.datasetPath, 'utf8');
      this.data = JSON.parse(text);
    } else {
      throw new Error('dataset or datasetPath must be provided');
    }
  }

  async getNodeById(id: number | string): Promise<NodeRecord | null> {
    const src = this.txData ?? this.data;
    return src.nodes.find(n => n.id === id) || null;
  }

  async *scanNodes(spec: NodeScanSpec = {}): AsyncIterable<NodeRecord> {
    const { label } = spec;
    const src = this.txData ?? this.data;
    for (const node of src.nodes) {
      if (!label || node.labels.includes(label)) {
        yield node;
      }
    }
  }

  async createNode(labels: string[], properties: Record<string, unknown>): Promise<NodeRecord> {
    const target = this.txData ?? this.data;
    const maxId = target.nodes.reduce((m, n) => Math.max(m, Number(n.id)), 0);
    const id = maxId + 1;
    const node: NodeRecord = { id, labels, properties };
    target.nodes.push(node);
    return node;
  }

  async deleteNode(id: number | string): Promise<void> {
    const target = this.txData ?? this.data;
    target.nodes = target.nodes.filter(n => n.id !== id);
    target.relationships = target.relationships.filter(r => r.startNode !== id && r.endNode !== id);
  }

  async updateNodeProperties(id: number | string, properties: Record<string, unknown>): Promise<void> {
    const target = this.txData ?? this.data;
    const node = target.nodes.find(n => n.id === id);
    if (!node) throw new Error('node not found');
    Object.assign(node.properties, properties);
  }

  async findNode(labels: string[], properties: Record<string, unknown>): Promise<NodeRecord | null> {
    const src = this.txData ?? this.data;
    for (const node of src.nodes) {
      if (labels.length && !labels.every(l => node.labels.includes(l))) {
        continue;
      }
      let ok = true;
      for (const [k, v] of Object.entries(properties)) {
        if (node.properties[k] !== v) {
          ok = false;
          break;
        }
      }
      if (ok) return node;
    }
    return null;
  }

  // Relationship APIs left unimplemented for this MVP
  async getRelationshipById(id: number | string): Promise<RelRecord | null> {
    const src = this.txData ?? this.data;
    return src.relationships.find(r => r.id === id) || null;
  }

  async *scanRelationships(): AsyncIterable<RelRecord> {
    const src = this.txData ?? this.data;
    for (const rel of src.relationships) {
      yield rel;
    }
  }

  async createRelationship(type: string, startNode: number | string, endNode: number | string, properties: Record<string, unknown>): Promise<RelRecord> {
    const target = this.txData ?? this.data;
    const maxId = target.relationships.reduce((m, r) => Math.max(m, Number(r.id)), 0);
    const id = maxId + 1;
    const rel: RelRecord = { id, type, startNode, endNode, properties };
    target.relationships.push(rel);
    return rel;
  }

  async deleteRelationship(id: number | string): Promise<void> {
    const target = this.txData ?? this.data;
    target.relationships = target.relationships.filter(r => r.id !== id);
  }

  async updateRelationshipProperties(id: number | string, properties: Record<string, unknown>): Promise<void> {
    const target = this.txData ?? this.data;
    const rel = target.relationships.find(r => r.id === id);
    if (!rel) throw new Error('relationship not found');
    Object.assign(rel.properties, properties);
  }

  async beginTransaction(): Promise<TransactionCtx> {
    if (this.txData) throw new Error('transaction already in progress');
    this.txData = JSON.parse(JSON.stringify(this.data));
    return {};
  }

  async commit(_: TransactionCtx): Promise<void> {
    if (this.txData) {
      this.data = this.txData;
      this.txData = undefined;
    }
  }

  async rollback(_: TransactionCtx): Promise<void> {
    this.txData = undefined;
  }
}
