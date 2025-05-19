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

export type CypherAST = MatchReturnQuery | CreateQuery | MergeQuery;

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
    const keyword = /^(MATCH|RETURN|CREATE|MERGE)\b/i.exec(rest);
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
    const punct = /^[(){}:,]/.exec(rest);
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
    if (tok.value === 'MATCH') return this.parseMatchReturn();
    if (tok.value === 'CREATE') return this.parseCreate();
    if (tok.value === 'MERGE') return this.parseMerge();
    throw new Error('Parse error: unsupported query');
  }

  private parseIdentifier(): string {
    return this.consume('identifier').value;
  }

  private parsePattern() {
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

  private parseMatchReturn(): MatchReturnQuery {
    this.consume('keyword', 'MATCH');
    const pattern = this.parsePattern();
    this.consume('keyword', 'RETURN');
    const ret = this.parseIdentifier();
    if (ret !== pattern.variable) {
      throw new Error('Parse error: return variable mismatch');
    }
    return { type: 'MatchReturn', variable: pattern.variable, label: pattern.label, properties: pattern.properties };
  }

  private parseCreate(): CreateQuery {
    this.consume('keyword', 'CREATE');
    const pattern = this.parsePattern();
    const ret = this.parseReturnVariable();
    if (ret && ret !== pattern.variable) {
      throw new Error('Parse error: return variable mismatch');
    }
    return { type: 'Create', variable: pattern.variable, label: pattern.label, properties: pattern.properties, returnVariable: ret };
  }

  private parseMerge(): MergeQuery {
    this.consume('keyword', 'MERGE');
    const pattern = this.parsePattern();
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
