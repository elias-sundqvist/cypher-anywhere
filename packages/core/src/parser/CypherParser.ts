export interface MatchReturnQuery {
  type: 'MatchReturn';
  variable: string;
  label?: string;
  properties?: Record<string, unknown>;
}

export interface CreateQuery {
  type: 'Create';
  variable: string;
  label?: string;
  properties?: Record<string, unknown>;
  returnVariable?: string;
}

export interface MergeQuery {
  type: 'Merge';
  variable: string;
  label?: string;
  properties?: Record<string, unknown>;
  returnVariable?: string;
}

export interface MatchDeleteQuery {
  type: 'MatchDelete';
  variable: string;
  label?: string;
  properties?: Record<string, unknown>;
  isRelationship?: boolean;
}

export interface MatchSetQuery {
  type: 'MatchSet';
  variable: string;
  label?: string;
  properties?: Record<string, unknown>;
  property: string;
  value: unknown;
  isRelationship?: boolean;
  returnVariable?: string;
}

export interface CreateRelQuery {
  type: 'CreateRel';
  relVariable: string;
  relType: string;
  relProperties?: Record<string, unknown>;
  start: {
    label?: string;
    properties?: Record<string, unknown>;
  };
  end: {
    label?: string;
    properties?: Record<string, unknown>;
  };
  returnVariable?: string;
}

export type CypherAST =
  | MatchReturnQuery
  | CreateQuery
  | MergeQuery
  | MatchDeleteQuery
  | MatchSetQuery
  | CreateRelQuery;

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
    const keyword = /^(MATCH|RETURN|CREATE|MERGE|SET|DELETE)\b/i.exec(rest);
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
    const punct = /^[(){}:,\.\[\]=>-]/.exec(rest);
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
    throw new Error('Parse error: unsupported query');
  }

  private parseIdentifier(): string {
    return this.consume('identifier').value;
  }

  private parseNodePattern() {
    this.consume('punct', '(');
    const variable = this.parseIdentifier();
    let label: string | undefined;
    if (this.optional('punct', ':')) {
      label = this.parseIdentifier();
    }
    let properties: Record<string, unknown> | undefined;
    if (this.optional('punct', '{')) {
      properties = this.parseProperties();
      this.consume('punct', '}');
    }
    this.consume('punct', ')');
    return { variable, label, properties };
  }

  private parseMaybeNodePattern() {
    this.consume('punct', '(');
    let variable: string | undefined;
    if (this.current()?.type === 'identifier') {
      variable = this.parseIdentifier();
    }
    let label: string | undefined;
    if (this.optional('punct', ':')) {
      label = this.parseIdentifier();
    }
    let properties: Record<string, unknown> | undefined;
    if (this.optional('punct', '{')) {
      properties = this.parseProperties();
      this.consume('punct', '}');
    }
    this.consume('punct', ')');
    return { variable, label, properties };
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
      props[key] = this.parseValue();
      first = false;
      if (this.current()?.value === '}') {
        break;
      }
    }
    return props;
  }

  private parseValue(): unknown {
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

  private parseReturnVariable(): string | undefined {
    if (this.optional('keyword', 'RETURN')) {
      return this.parseIdentifier();
    }
    return undefined;
  }

  private parseMatch(): CypherAST {
    this.consume('keyword', 'MATCH');
    let pattern: { variable: string; label?: string; properties?: Record<string, unknown>; isRel?: boolean };
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
      pattern = { variable: relVar, label: relType, properties: relProps, isRel: true };
    } else {
      if (!start.variable) throw new Error('Parse error: node variable required');
      pattern = { variable: start.variable, label: start.label, properties: start.properties, isRel: false };
    }
    const next = this.current();
    if (!next || next.type !== 'keyword') throw new Error('Expected keyword');
    if (next.value === 'RETURN') {
      this.consume('keyword', 'RETURN');
      const ret = this.parseIdentifier();
      if (ret !== pattern.variable) throw new Error('Parse error: return variable mismatch');
      return { type: 'MatchReturn', variable: pattern.variable, label: pattern.label, properties: pattern.properties };
    }
    if (next.value === 'DELETE') {
      this.consume('keyword', 'DELETE');
      const id = this.parseIdentifier();
      if (id !== pattern.variable) throw new Error('Parse error: delete variable mismatch');
      return { type: 'MatchDelete', variable: pattern.variable, label: pattern.label, properties: pattern.properties, isRelationship: pattern.isRel };
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
        label: pattern.label,
        properties: pattern.properties,
        property: prop,
        value,
        isRelationship: pattern.isRel,
        returnVariable: ret,
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
        start: { label: start.label, properties: start.properties },
        end: { label: end.label, properties: end.properties },
        returnVariable: ret,
      };
    }
    const ret = this.parseReturnVariable();
    if (ret && ret !== start.variable) {
      throw new Error('Parse error: return variable mismatch');
    }
    return { type: 'Create', variable: start.variable, label: start.label, properties: start.properties, returnVariable: ret };
  }

  private parseMerge(): MergeQuery {
    this.consume('keyword', 'MERGE');
    const pattern = this.parseNodePattern();
    const ret = this.parseReturnVariable();
    if (ret && ret !== pattern.variable) {
      throw new Error('Parse error: return variable mismatch');
    }
    return { type: 'Merge', variable: pattern.variable, label: pattern.label, properties: pattern.properties, returnVariable: ret };
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
