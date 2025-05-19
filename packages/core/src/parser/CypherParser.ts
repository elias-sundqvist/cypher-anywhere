export interface MatchReturnQuery {
  type: 'MatchReturn';
  variable: string;
  labels?: string[];
  properties?: Record<string, unknown>;
  where?: WhereClause;
}

export interface CreateQuery {
  type: 'Create';
  variable: string;
  labels?: string[];
  properties?: Record<string, unknown>;
  returnVariable?: string;
}

export interface MergeQuery {
  type: 'Merge';
  variable: string;
  labels?: string[];
  properties?: Record<string, unknown>;
  returnVariable?: string;
}

export interface MatchDeleteQuery {
  type: 'MatchDelete';
  variable: string;
  labels?: string[];
  properties?: Record<string, unknown>;
  isRelationship?: boolean;
  where?: WhereClause;
}

export type Expression =
  | { type: 'Literal'; value: string | number | boolean }
  | { type: 'Property'; variable: string; property: string }
  | { type: 'Variable'; name: string }
  | { type: 'Add'; left: Expression; right: Expression }
  | { type: 'Nodes'; variable: string };

export interface WhereClause {
  left: Expression;
  operator: '=' | '>' | '>=';
  right: Expression;
}

export interface MatchSetQuery {
  type: 'MatchSet';
  variable: string;
  labels?: string[];
  properties?: Record<string, unknown>;
  property: string;
  value: Expression;
  isRelationship?: boolean;
  returnVariable?: string;
  where?: WhereClause;
}

export interface CreateRelQuery {
  type: 'CreateRel';
  relVariable: string;
  relType: string;
  relProperties?: Record<string, unknown>;
  start: {
    labels?: string[];
    properties?: Record<string, unknown>;
  };
  end: {
    labels?: string[];
    properties?: Record<string, unknown>;
  };
  returnVariable?: string;
}

export interface MergeRelQuery {
  type: 'MergeRel';
  relVariable: string;
  relType: string;
  relProperties?: Record<string, unknown>;
  startVariable: string;
  endVariable: string;
  returnVariable?: string;
}

export interface MatchPathQuery {
  type: 'MatchPath';
  pathVariable: string;
  start: {
    labels?: string[];
    properties?: Record<string, unknown>;
  };
  end: {
    labels?: string[];
    properties?: Record<string, unknown>;
  };
  returnVariable?: string;
}

export interface ForeachQuery {
  type: 'Foreach';
  variable: string;
  list: unknown[] | Expression;
  statement: CypherAST;
}

export type CypherAST =
  | MatchReturnQuery
  | CreateQuery
  | MergeQuery
  | MatchDeleteQuery
  | MatchSetQuery
  | CreateRelQuery
  | MergeRelQuery
  | MatchPathQuery
  | ForeachQuery;

interface Token {
  type: 'keyword' | 'identifier' | 'number' | 'string' | 'punct';
  value: string;
}

function tokenize(input: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;
  while (i < input.length) {
    const rest = input.slice(i);
    const ws = /^\s+/.exec(rest);
    if (ws) {
      i += ws[0].length;
      continue;
    }
    const keyword = /^(MATCH|RETURN|CREATE|MERGE|SET|DELETE|WHERE|FOREACH|IN)\b/i.exec(rest);
    if (keyword) {
      tokens.push({ type: 'keyword', value: keyword[1].toUpperCase() });
      i += keyword[0].length;
      continue;
    }
    const ident = /^[_A-Za-z][_A-Za-z0-9]*/.exec(rest);
    if (ident) {
      tokens.push({ type: 'identifier', value: ident[0] });
      i += ident[0].length;
      continue;
    }
    const str = /^"([^"\\]|\\.)*"|^'([^'\\]|\\.)*'/.exec(rest);
    if (str) {
      tokens.push({ type: 'string', value: str[0] });
      i += str[0].length;
      continue;
    }
    const num = /^\d+(?:\.\d+)?/.exec(rest);
    if (num) {
      tokens.push({ type: 'number', value: num[0] });
      i += num[0].length;
      continue;
    }
    const punct = /^[(){}:,\.\[\]=>+\-*]/.exec(rest);
    if (punct) {
      tokens.push({ type: 'punct', value: punct[0] });
      i += punct[0].length;
      continue;
    }
    throw new Error(`Unexpected token near: ${rest}`);
  }
  return tokens;
}

class Parser {
  private pos = 0;
  constructor(private tokens: Token[]) {}

  private current(): Token | undefined {
    return this.tokens[this.pos];
  }

  private consume(type: Token['type'], value?: string): Token {
    const token = this.current();
    if (!token || token.type !== type || (value && token.value !== value)) {
      throw new Error(`Expected ${value ?? type}`);
    }
    this.pos++;
    return token;
  }

  private optional(type: Token['type'], value?: string): Token | null {
    const token = this.current();
    if (token && token.type === type && (!value || token.value === value)) {
      this.pos++;
      return token;
    }
    return null;
  }

  parse(): CypherAST {
    const tok = this.current();
    if (!tok || tok.type !== 'keyword') {
      throw new Error('Expected query keyword');
    }
    if (tok.value === 'MATCH') return this.parseMatch();
    if (tok.value === 'CREATE') return this.parseCreate();
    if (tok.value === 'MERGE') return this.parseMerge();
    if (tok.value === 'FOREACH') return this.parseForeach();
    throw new Error('Parse error: unsupported query');
  }

  private parseIdentifier(): string {
    return this.consume('identifier').value;
  }

  private parseNodePattern() {
    this.consume('punct', '(');
    const variable = this.parseIdentifier();
    const labels: string[] = [];
    if (this.optional('punct', ':')) {
      labels.push(this.parseIdentifier());
      while (this.optional('punct', ':')) {
        labels.push(this.parseIdentifier());
      }
    }
    let properties: Record<string, unknown> | undefined;
    if (this.optional('punct', '{')) {
      properties = this.parseProperties();
      this.consume('punct', '}');
    }
    this.consume('punct', ')');
    return { variable, labels, properties };
  }

  private parseMaybeNodePattern() {
    this.consume('punct', '(');
    let variable: string | undefined;
    if (this.current()?.type === 'identifier') {
      variable = this.parseIdentifier();
    }
    const labels: string[] = [];
    if (this.optional('punct', ':')) {
      labels.push(this.parseIdentifier());
      while (this.optional('punct', ':')) {
        labels.push(this.parseIdentifier());
      }
    }
    let properties: Record<string, unknown> | undefined;
    if (this.optional('punct', '{')) {
      properties = this.parseProperties();
      this.consume('punct', '}');
    }
    this.consume('punct', ')');
    return { variable, labels, properties };
  }

  private parseRelationshipPattern() {
    this.parseMaybeNodePattern();
    this.consume('punct', '-');
    this.consume('punct', '[');
    const variable = this.parseIdentifier();
    let type: string | undefined;
    if (this.optional('punct', ':')) {
      type = this.parseIdentifier();
    }
    let properties: Record<string, unknown> | undefined;
    if (this.optional('punct', '{')) {
      properties = this.parseProperties();
      this.consume('punct', '}');
    }
    this.consume('punct', ']');
    this.consume('punct', '-');
    this.consume('punct', '>');
    this.parseMaybeNodePattern();
    return { variable, type, properties };
  }

  private parseProperties(): Record<string, unknown> {
    const props: Record<string, unknown> = {};
    let first = true;
    while (!this.optional('punct', '}')) {
      if (!first) {
        this.consume('punct', ',');
      }
      const key = this.parseIdentifier();
      this.consume('punct', ':');
      props[key] = this.parseLiteralValue();
      first = false;
      if (this.current()?.value === '}') {
        break;
      }
    }
    return props;
  }

  private parseLiteralValue(): unknown {
    const tok = this.current();
    if (!tok) throw new Error('Unexpected end of input');
    if (tok.type === 'string') {
      this.pos++;
      return tok.value.slice(1, -1);
    }
    if (tok.type === 'number') {
      this.pos++;
      return Number(tok.value);
    }
    if (tok.type === 'identifier' && (tok.value === 'true' || tok.value === 'false')) {
      this.pos++;
      return tok.value === 'true';
    }
    throw new Error('Unexpected value');
  }

  private parseValue(): Expression {
    let left = this.parseValueAtom();
    while (this.current()?.value === '+') {
      this.consume('punct', '+');
      const right = this.parseValueAtom();
      left = { type: 'Add', left, right };
    }
    return left;
  }

  private parseValueAtom(): Expression {
    const tok = this.current();
    if (!tok) throw new Error('Unexpected end of input');
    if (tok.type === 'string') {
      this.pos++;
      return { type: 'Literal', value: tok.value.slice(1, -1) };
    }
    if (tok.type === 'number') {
      this.pos++;
      return { type: 'Literal', value: Number(tok.value) };
    }
    if (tok.type === 'identifier') {
      if (tok.value === 'true' || tok.value === 'false') {
        this.pos++;
        return { type: 'Literal', value: tok.value === 'true' };
      }
      if (tok.value === 'nodes') {
        this.pos++;
        this.consume('punct', '(');
        const inner = this.parseIdentifier();
        this.consume('punct', ')');
        return { type: 'Nodes', variable: inner };
      }
      const variable = this.parseIdentifier();
      if (this.optional('punct', '.')) {
        const prop = this.parseIdentifier();
        return { type: 'Property', variable, property: prop };
      }
      return { type: 'Variable', name: variable };
    }
    throw new Error('Unexpected value');
  }

  private parseReturnVariable(): string | undefined {
    if (this.optional('keyword', 'RETURN')) {
      return this.parseIdentifier();
    }
    return undefined;
  }

  private parseMatch(): CypherAST {
    this.consume('keyword', 'MATCH');
    if (
      this.current()?.type === 'identifier' &&
      this.tokens[this.pos + 1]?.value === '='
    ) {
      const pathVariable = this.parseIdentifier();
      this.consume('punct', '=');
      const start = this.parseNodePattern();
      this.consume('punct', '-');
      this.consume('punct', '[');
      this.consume('punct', '*');
      this.consume('punct', ']');
      this.consume('punct', '-');
      this.consume('punct', '>');
      const end = this.parseNodePattern();
      const ret = this.parseReturnVariable();
      if (ret && ret !== pathVariable)
        throw new Error('Parse error: return variable mismatch');
      return {
        type: 'MatchPath',
        pathVariable,
        start: { labels: start.labels, properties: start.properties },
        end: { labels: end.labels, properties: end.properties },
        returnVariable: ret,
      };
    }
    let pattern: { variable: string; labels?: string[]; properties?: Record<string, unknown>; isRel?: boolean };
    const start = this.parseMaybeNodePattern();
    if (this.current()?.value === '-') {
      this.consume('punct', '-');
      this.consume('punct', '[');
      const relVar = this.parseIdentifier();
      let relType: string | undefined;
      if (this.optional('punct', ':')) {
        relType = this.parseIdentifier();
      }
      let relProps: Record<string, unknown> | undefined;
      if (this.optional('punct', '{')) {
        relProps = this.parseProperties();
        this.consume('punct', '}');
      }
      this.consume('punct', ']');
      this.consume('punct', '-');
      this.consume('punct', '>');
      this.parseMaybeNodePattern();
      pattern = { variable: relVar, labels: relType ? [relType] : undefined, properties: relProps, isRel: true };
    } else {
      if (!start.variable) throw new Error('Parse error: node variable required');
      pattern = { variable: start.variable, labels: start.labels, properties: start.properties, isRel: false };
    }
    let where: WhereClause | undefined;
    if (this.current()?.value === 'WHERE') {
      this.consume('keyword', 'WHERE');
      const left = this.parseValue();
      const opTok = this.consume('punct');
      let op: '=' | '>' | '>=' = opTok.value as any;
      if (opTok.value === '>' && this.optional('punct', '=')) {
        op = '>=';
      }
      const right = this.parseValue();
      where = { left, operator: op, right };
    }
    const next = this.current();
    if (!next || next.type !== 'keyword') throw new Error('Expected keyword');
    if (next.value === 'RETURN') {
      this.consume('keyword', 'RETURN');
      const ret = this.parseIdentifier();
      if (ret !== pattern.variable) throw new Error('Parse error: return variable mismatch');
      return { type: 'MatchReturn', variable: pattern.variable, labels: pattern.labels, properties: pattern.properties, where };
    }
    if (next.value === 'DELETE') {
      this.consume('keyword', 'DELETE');
      const id = this.parseIdentifier();
      if (id !== pattern.variable) throw new Error('Parse error: delete variable mismatch');
      return { type: 'MatchDelete', variable: pattern.variable, labels: pattern.labels, properties: pattern.properties, isRelationship: pattern.isRel, where };
    }
    if (next.value === 'SET') {
      this.consume('keyword', 'SET');
      const id = this.parseIdentifier();
      if (id !== pattern.variable) throw new Error('Parse error: set variable mismatch');
      this.consume('punct', '.');
      const prop = this.parseIdentifier();
      this.consume('punct', '=');
      const value = this.parseValue();
      const ret = this.parseReturnVariable();
      if (ret && ret !== pattern.variable) throw new Error('Parse error: return variable mismatch');
      return {
        type: 'MatchSet',
        variable: pattern.variable,
        labels: pattern.labels,
        properties: pattern.properties,
        property: prop,
        value,
        isRelationship: pattern.isRel,
        returnVariable: ret,
        where,
      };
    }
    throw new Error('Parse error: unsupported MATCH clause');
  }

  private parseCreate(): CypherAST {
    this.consume('keyword', 'CREATE');
    const start = this.parseNodePattern();
    if (this.current()?.value === '-') {
      this.consume('punct', '-');
      this.consume('punct', '[');
      const relVar = this.parseIdentifier();
      this.consume('punct', ':');
      const relType = this.parseIdentifier();
      let relProps: Record<string, unknown> | undefined;
      if (this.optional('punct', '{')) {
        relProps = this.parseProperties();
        this.consume('punct', '}');
      }
      this.consume('punct', ']');
      this.consume('punct', '-');
      this.consume('punct', '>');
      const end = this.parseNodePattern();
      const ret = this.parseReturnVariable();
      if (ret && ret !== relVar) throw new Error('Parse error: return variable mismatch');
      return {
        type: 'CreateRel',
        relVariable: relVar,
        relType,
        relProperties: relProps,
        start: { labels: start.labels, properties: start.properties },
        end: { labels: end.labels, properties: end.properties },
        returnVariable: ret,
      };
    }
    const ret = this.parseReturnVariable();
    if (ret && ret !== start.variable) {
      throw new Error('Parse error: return variable mismatch');
    }
    return { type: 'Create', variable: start.variable, labels: start.labels, properties: start.properties, returnVariable: ret };
  }

  private parseMerge(): MergeQuery | MergeRelQuery {
    this.consume('keyword', 'MERGE');
    const start = this.parseMaybeNodePattern();
    if (this.current()?.value === '-') {
      this.consume('punct', '-');
      this.consume('punct', '[');
      const relVar = this.parseIdentifier();
      this.consume('punct', ':');
      const relType = this.parseIdentifier();
      let relProps: Record<string, unknown> | undefined;
      if (this.optional('punct', '{')) {
        relProps = this.parseProperties();
        this.consume('punct', '}');
      }
      this.consume('punct', ']');
      this.consume('punct', '-');
      this.consume('punct', '>');
      const end = this.parseMaybeNodePattern();
      const ret = this.parseReturnVariable();
      if (ret && ret !== relVar) throw new Error('Parse error: return variable mismatch');
      if (!start.variable || !end.variable) throw new Error('Parse error: node variables required');
      return {
        type: 'MergeRel',
        relVariable: relVar,
        relType,
        relProperties: relProps,
        startVariable: start.variable,
        endVariable: end.variable,
        returnVariable: ret,
      };
    }
    if (!start.variable) throw new Error('Parse error: node variable required');
    const ret = this.parseReturnVariable();
    if (ret && ret !== start.variable) {
      throw new Error('Parse error: return variable mismatch');
    }
    return { type: 'Merge', variable: start.variable, labels: start.labels, properties: start.properties, returnVariable: ret };
  }

  private parseForeach(): ForeachQuery {
    this.consume('keyword', 'FOREACH');
    const variable = this.parseIdentifier();
    this.consume('keyword', 'IN');
    let list: unknown[] | Expression;
    if (this.current()?.value === '[') {
      this.consume('punct', '[');
      const arr: unknown[] = [];
      if (this.current()?.value !== ']') {
        while (true) {
          arr.push(this.parseLiteralValue());
          if (!this.optional('punct', ',')) break;
        }
      }
      this.consume('punct', ']');
      list = arr;
    } else {
      list = this.parseValue();
    }
    const statement = this.parse();
    return { type: 'Foreach', variable, list, statement };
  }
}

export function parse(query: string): CypherAST {
  const tokens = tokenize(query);
  const parser = new Parser(tokens);
  const ast = parser.parse();
  if (parser['pos'] !== tokens.length) {
    throw new Error('Unexpected tokens at end of query');
  }
  return ast;
}

export function parseMany(query: string): CypherAST[] {
  return query
    .split(';')
    .map(q => q.trim())
    .filter(q => q.length > 0)
    .map(q => parse(q));
}
