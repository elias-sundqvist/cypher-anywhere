export interface NodeRecord {
  id: number | string;
  labels: string[];
  properties: Record<string, unknown>;
}

export interface RelRecord {
  id: number | string;
  type: string;
  startNode: number | string;
  endNode: number | string;
  properties: Record<string, unknown>;
}

export interface NodeScanSpec {
  label?: string;
}

export interface StorageAdapter {
  getNodeById(id: number | string): Promise<NodeRecord | null>;
  scanNodes(spec?: NodeScanSpec): AsyncIterable<NodeRecord>;

  getRelationshipById?(id: number | string): Promise<RelRecord | null>;
  scanRelationships?(): AsyncIterable<RelRecord>;
}

export interface IndexMetadata {
  label?: string;
  type?: string;
  properties: string[];
  unique: boolean;
  selectivity?: number;
}

export interface GraphStatistics {
  nodeCount(label?: string): Promise<number>;
  relCount(type?: string): Promise<number>;
  distinctValues(label: string, property: string): Promise<number>;
}
