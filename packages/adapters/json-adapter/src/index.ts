import { StorageAdapter, NodeRecord, RelRecord, NodeScanSpec } from '@cypher-anywhere/core';
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
    return this.data.nodes.find(n => n.id === id) || null;
  }

  async *scanNodes(spec: NodeScanSpec = {}): AsyncIterable<NodeRecord> {
    const { label } = spec;
    for (const node of this.data.nodes) {
      if (!label || node.labels.includes(label)) {
        yield node;
      }
    }
  }

  // Relationship APIs left unimplemented for this MVP
  async getRelationshipById(id: number | string): Promise<RelRecord | null> {
    return this.data.relationships.find(r => r.id === id) || null;
  }

  async *scanRelationships(): AsyncIterable<RelRecord> {
    for (const rel of this.data.relationships) {
      yield rel;
    }
  }
}
