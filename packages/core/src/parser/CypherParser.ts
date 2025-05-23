export interface ReturnItem {
  expression: Expression;
  alias?: string;
}

export interface MatchReturnQuery {
  type: 'MatchReturn';
  variable: string;
  labels?: string[];
  properties?: Record<string, unknown>;
  isRelationship?: boolean;
  optional?: boolean;
  where?: WhereClause;
  returnItems: ReturnItem[];
  orderBy?: { expression: Expression; direction?: 'ASC' | 'DESC' }[];
  skip?: Expression;
  limit?: Expression;
  distinct?: boolean;
}

export interface MatchMultiReturnQuery {
  type: 'MatchMultiReturn';
  patterns: {
    variable: string;
    labels?: string[];
    properties?: Record<string, unknown>;
  }[];
  optional?: boolean;
  where?: WhereClause;
  returnItems: ReturnItem[];
  orderBy?: { expression: Expression; direction?: 'ASC' | 'DESC' }[];
  skip?: Expression;
  limit?: Expression;
  distinct?: boolean;
}

export interface CreateQuery {
  type: 'Create';
  variable: string;
  labels?: string[];
  properties?: Record<string, unknown>;
  setProperties?: Record<string, Expression>;
  returnVariable?: string;
}

export interface MergeQuery {
  type: 'Merge';
  variable: string;
  labels?: string[];
  properties?: Record<string, unknown>;
  onCreateSet?: Record<string, Expression>;
  onMatchSet?: Record<string, Expression>;
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

export interface ReturnQuery {
  type: 'Return';
  returnItems: ReturnItem[];
  orderBy?: { expression: Expression; direction?: 'ASC' | 'DESC' }[];
  skip?: Expression;
  limit?: Expression;
  distinct?: boolean;
}

export type Expression =
  | { type: 'Literal'; value: string | number | boolean | unknown[] | null }
  | { type: 'Property'; variable: string; property: string }
  | { type: 'Variable'; name: string }
  | { type: 'Parameter'; name: string }
  | { type: 'Add'; left: Expression; right: Expression }
  | { type: 'Sub'; left: Expression; right: Expression }
  | { type: 'Mul'; left: Expression; right: Expression }
  | { type: 'Div'; left: Expression; right: Expression }
  | { type: 'Neg'; expression: Expression }
  | { type: 'Nodes'; variable: string }
  | { type: 'Id'; variable: string }
  | { type: 'Count'; expression: Expression | null; distinct?: boolean }
  | { type: 'Sum'; expression: Expression; distinct?: boolean }
  | { type: 'Min'; expression: Expression; distinct?: boolean }
  | { type: 'Max'; expression: Expression; distinct?: boolean }
  | { type: 'Avg'; expression: Expression; distinct?: boolean }
  | { type: 'Collect'; expression: Expression; distinct?: boolean }
  | { type: 'Length'; expression: Expression }
  | { type: 'Labels'; variable: string }
  | { type: 'Type'; variable: string }
  | { type: 'Relationships'; variable: string }
  | { type: 'All' };

export type WhereClause =
  | {
      type: 'Condition';
      left: Expression;
      operator:
        | '='
        | '>'
        | '>='
        | '<'
        | '<='
        | '<>'
        | 'IN'
        | 'IS NULL'
        | 'IS NOT NULL'
        | 'STARTS WITH'
        | 'ENDS WITH'
        | 'CONTAINS';
      right?: Expression;
    }
  | { type: 'And'; left: WhereClause; right: WhereClause }
  | { type: 'Or'; left: WhereClause; right: WhereClause }
  | { type: 'Not'; clause: WhereClause };

export interface MatchSetQuery {
  type: 'MatchSet';
  variable: string;
  labels?: string[];
  properties?: Record<string, unknown>;
  updates: Record<string, Expression>;
  isRelationship?: boolean;
  returnVariable?: string;
  where?: WhereClause;
}

export interface CreateRelQuery {
  type: 'CreateRel';
  relVariable?: string;
  relType: string;
  relProperties?: Record<string, unknown>;
  start: {
    variable: string;
    labels?: string[];
    properties?: Record<string, unknown>;
  };
  end: {
    variable: string;
    labels?: string[];
    properties?: Record<string, unknown>;
  };
  returnVariable?: string;
}

export interface MergeRelQuery {
  type: 'MergeRel';
  relVariable?: string;
  relType: string;
  relProperties?: Record<string, unknown>;
  start: {
    variable?: string;
    labels?: string[];
    properties?: Record<string, unknown>;
  };
  end: {
    variable?: string;
    labels?: string[];
    properties?: Record<string, unknown>;
  };
  onCreateSet?: Record<string, Expression>;
  onMatchSet?: Record<string, Expression>;
  returnVariable?: string;
}

export interface MatchPathQuery {
  type: 'MatchPath';
  pathVariable: string;
  start: {
    variable?: string;
    labels?: string[];
    properties?: Record<string, unknown>;
  };
  end: {
    variable?: string;
    labels?: string[];
    properties?: Record<string, unknown>;
  };
  relType?: string;
  minHops?: number;
  maxHops?: number;
  direction?: 'out' | 'in' | 'none';
  returnItems?: ReturnItem[];
  orderBy?: { expression: Expression; direction?: 'ASC' | 'DESC' }[];
  skip?: Expression;
  limit?: Expression;
  distinct?: boolean;
}

export interface ForeachQuery {
  type: 'Foreach';
  variable: string;
  list: unknown[] | Expression;
  statement: CypherAST;
}

export interface UnwindQuery {
  type: 'Unwind';
  list: unknown[] | Expression;
  variable: string;
  returnExpression: Expression;
  returnAlias?: string;
}

export interface UnionQuery {
  type: 'Union';
  left: CypherAST;
  right: CypherAST;
  all?: boolean;
  orderBy?: { expression: Expression; direction?: 'ASC' | 'DESC' }[];
  skip?: Expression;
  limit?: Expression;
}

export interface CallQuery {
  type: 'Call';
  subquery: CypherAST[];
  returnItems: ReturnItem[];
  distinct?: boolean;
  orderBy?: { expression: Expression; direction?: 'ASC' | 'DESC' }[];
  skip?: Expression;
  limit?: Expression;
}

export interface MatchChainQuery {
  type: 'MatchChain';
  start: {
    variable: string;
    labels?: string[];
    properties?: Record<string, unknown>;
  };
  hops: {
    rel: {
      variable?: string;
      type?: string;
      properties?: Record<string, unknown>;
      direction: 'out' | 'in' | 'none';
    };
    node: {
      variable: string;
      labels?: string[];
      properties?: Record<string, unknown>;
    };
  }[];
  pathVariable?: string;
  where?: WhereClause;
  returnItems: ReturnItem[];
  orderBy?: { expression: Expression; direction?: 'ASC' | 'DESC' }[];
  skip?: Expression;
  limit?: Expression;
  distinct?: boolean;
  optional?: boolean;
}

export interface WithQuery {
  type: 'With';
  source: MatchReturnQuery | MatchChainQuery | ReturnQuery;
  where?: WhereClause;
  next: CypherAST;
}

export type CypherAST =
  | MatchReturnQuery
  | MatchMultiReturnQuery
  | ReturnQuery
  | CreateQuery
  | MergeQuery
  | MatchDeleteQuery
  | MatchSetQuery
  | CreateRelQuery
  | MergeRelQuery
  | MatchPathQuery
  | MatchChainQuery
  | WithQuery
  | ForeachQuery
  | UnwindQuery
  | UnionQuery
  | CallQuery;

interface Token {
  type: 'keyword' | 'identifier' | 'number' | 'string' | 'punct' | 'parameter';
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
    const keyword = /^(MATCH|RETURN|CREATE|MERGE|SET|DELETE|WHERE|FOREACH|IN|ON|UNWIND|AS|ORDER|BY|LIMIT|SKIP|OPTIONAL|WITH|CALL|UNION|ALL|AND|OR|NOT|ASC|DESC|DISTINCT|IS|STARTS|ENDS|CONTAINS)\b/i.exec(rest);
    if (keyword) {
      tokens.push({ type: 'keyword', value: keyword[1].toUpperCase() });
      i += keyword[0].length;
      continue;
    }
    const quoted = /^`([^`]|``)*`/.exec(rest);
    if (quoted) {
      tokens.push({
        type: 'identifier',
        value: quoted[0].slice(1, -1).replace(/``/g, '`'),
      });
      i += quoted[0].length;
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
    const num = /^-?\d+(?:\.\d+)?/.exec(rest);
    if (num) {
      tokens.push({ type: 'number', value: num[0] });
      i += num[0].length;
      continue;
    }
    const param = /^\$[_A-Za-z][_A-Za-z0-9]*/.exec(rest);
    if (param) {
      tokens.push({ type: 'parameter', value: param[0].slice(1) });
      i += param[0].length;
      continue;
    }
    const punct = /^[(){}:,.;\[\]=>+\-*<\/]/.exec(rest);
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
  private anonId = 0;
  constructor(private tokens: Token[]) {}

  private genAnonVar(): string {
    return `_anon${this.anonId++}`;
  }

  private current(): Token | undefined {
    return this.tokens[this.pos];
  }

  private lookahead(n = 1): Token | undefined {
    return this.tokens[this.pos + n];
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

  private parseSingle(): CypherAST {
    const tok = this.current();
    if (!tok || tok.type !== 'keyword') {
      throw new Error('Expected query keyword');
    }
    if (tok.value === 'MATCH') return this.parseMatch(false);
    if (tok.value === 'OPTIONAL') {
      this.consume('keyword', 'OPTIONAL');
      return this.parseMatch(true);
    }
    if (tok.value === 'CREATE') return this.parseCreate();
    if (tok.value === 'MERGE') return this.parseMerge();
    if (tok.value === 'FOREACH') return this.parseForeach();
    if (tok.value === 'UNWIND') return this.parseUnwind();
    if (tok.value === 'CALL') return this.parseCall();
    if (tok.value === 'WITH') {
      const withClause = this.parseWithClause();
      const source: ReturnQuery = {
        type: 'Return',
        returnItems: withClause.items,
        orderBy: withClause.orderBy,
        skip: withClause.skip,
        limit: withClause.limit,
        distinct: withClause.distinct,
      };
      const next = this.parse();
      return { type: 'With', source, where: withClause.where, next };
    }
    if (tok.value === 'RETURN') return this.parseReturnOnly();
    throw new Error('Parse error: unsupported query');
  }

  parse(): CypherAST {
    let left = this.parseSingle();
    while (this.current()?.value === 'UNION') {
      this.consume('keyword', 'UNION');
      const all = this.optional('keyword', 'ALL') !== null;
      const right = this.parseSingle();
      left = { type: 'Union', left, right, all };
    }
    if (left.type === 'Union') {
      let orderBy: { expression: Expression; direction?: 'ASC' | 'DESC' }[] | undefined;
      if (this.current()?.value === 'ORDER') {
        this.consume('keyword', 'ORDER');
        this.consume('keyword', 'BY');
        orderBy = [];
        while (true) {
          const expr = this.parseValue();
          let direction: 'ASC' | 'DESC' | undefined;
          if (this.current()?.value === 'ASC' || this.current()?.value === 'DESC') {
            direction = this.current()!.value as 'ASC' | 'DESC';
            this.consume('keyword');
          }
          orderBy.push({ expression: expr, direction });
          if (!this.optional('punct', ',')) break;
        }
      }
      let skip: Expression | undefined;
      if (this.current()?.value === 'SKIP') {
        this.consume('keyword', 'SKIP');
        skip = this.parseValue();
      }
      let limit: Expression | undefined;
      if (this.current()?.value === 'LIMIT') {
        this.consume('keyword', 'LIMIT');
        limit = this.parseValue();
      }
      if (orderBy || skip || limit) {
        left = { ...left, orderBy, skip, limit } as UnionQuery;
      }
    }
    return left;
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
      props[key] = this.parseValue();
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
    if (tok.type === 'punct' && tok.value === '[') {
      this.pos++;
      const arr: unknown[] = [];
      while (this.current() && this.current()!.value !== ']') {
        arr.push(this.parseLiteralValue());
        if (!this.optional('punct', ',')) break;
      }
      this.consume('punct', ']');
      return arr;
    }
    if (tok.type === 'parameter') {
      this.pos++;
      return { __param: tok.value };
    }
    if (
      tok.type === 'identifier' &&
      (tok.value === 'true' || tok.value === 'false' || tok.value === 'null')
    ) {
      this.pos++;
      if (tok.value === 'null') return null;
      return tok.value === 'true';
    }
    throw new Error('Unexpected value');
  }

  private parseValue(): Expression {
    let left = this.parseTerm();
    while (this.current()?.value === '+' || this.current()?.value === '-') {
      const op = this.current()!.value;
      this.consume('punct', op);
      const right = this.parseTerm();
      if (op === '+') left = { type: 'Add', left, right };
      else left = { type: 'Sub', left, right };
    }
    return left;
  }

  private parseTerm(): Expression {
    let left = this.parseValueAtom();
    while (this.current()?.value === '*' || this.current()?.value === '/') {
      const op = this.current()!.value;
      this.consume('punct', op);
      const right = this.parseValueAtom();
      if (op === '*') left = { type: 'Mul', left, right };
      else left = { type: 'Div', left, right };
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
    if (tok.type === 'punct' && tok.value === '-') {
      this.pos++;
      return { type: 'Neg', expression: this.parseValueAtom() };
    }
    if (tok.type === 'punct' && tok.value === '[') {
      this.pos++;
      const arr: unknown[] = [];
      while (this.current() && this.current()!.value !== ']') {
        arr.push(this.parseLiteralValue());
        if (!this.optional('punct', ',')) break;
      }
      this.consume('punct', ']');
      return { type: 'Literal', value: arr };
    }
    if (tok.type === 'punct' && tok.value === '(') {
      this.pos++;
      const expr = this.parseValue();
      this.consume('punct', ')');
      return expr;
    }
    if (tok.type === 'punct' && tok.value === '*') {
      this.pos++;
      return { type: 'All' };
    }
    if (tok.type === 'parameter') {
      this.pos++;
      return { type: 'Parameter', name: tok.value };
    }
    if (tok.type === 'identifier') {
      if (tok.value === 'true' || tok.value === 'false' || tok.value === 'null') {
        this.pos++;
        if (tok.value === 'null') return { type: 'Literal', value: null };
        return { type: 'Literal', value: tok.value === 'true' };
      }
      if (tok.value === 'null') {
        this.pos++;
        return { type: 'Literal', value: null };
      }
      const func = tok.value.toLowerCase();
      if (
        ['count', 'sum', 'min', 'max', 'avg', 'collect'].includes(func) &&
        this.lookahead()?.value === '('
      ) {
        this.pos++;
        this.consume('punct', '(');
        const distinct = this.optional('keyword', 'DISTINCT') !== null;
        let expr: Expression | null = null;
        if (this.current()?.value === '*') {
          this.pos++;
        } else {
          expr = this.parseValue();
        }
        this.consume('punct', ')');
        const type = func.charAt(0).toUpperCase() + func.slice(1) as
          | 'Count'
          | 'Sum'
          | 'Min'
          | 'Max'
          | 'Avg'
          | 'Collect';
        return { type, expression: expr, distinct } as Expression;
      }
      if (tok.value === 'nodes' && this.lookahead()?.value === '(') {
        this.pos++;
        this.consume('punct', '(');
        const inner = this.parseIdentifier();
        this.consume('punct', ')');
        return { type: 'Nodes', variable: inner };
      }
      if (tok.value === 'relationships' && this.lookahead()?.value === '(') {
        this.pos++;
        this.consume('punct', '(');
        const inner = this.parseIdentifier();
        this.consume('punct', ')');
        return { type: 'Relationships', variable: inner };
      }
      if ((tok.value === 'length' || tok.value === 'size') && this.lookahead()?.value === '(') {
        this.pos++;
        this.consume('punct', '(');
        const inner = this.parseValue();
        this.consume('punct', ')');
        return { type: 'Length', expression: inner };
      }
      if (tok.value === 'labels' && this.lookahead()?.value === '(') {
        this.pos++;
        this.consume('punct', '(');
        const inner = this.parseIdentifier();
        this.consume('punct', ')');
        return { type: 'Labels', variable: inner };
      }
      if (tok.value === 'type' && this.lookahead()?.value === '(') {
        this.pos++;
        this.consume('punct', '(');
        const inner = this.parseIdentifier();
        this.consume('punct', ')');
        return { type: 'Type', variable: inner };
      }
      if (tok.value === 'id' && this.lookahead()?.value === '(') {
        this.pos++;
        this.consume('punct', '(');
        const inner = this.parseIdentifier();
        this.consume('punct', ')');
        return { type: 'Id', variable: inner };
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

  private parseReturnExpression(): Expression | undefined {
    if (this.optional('keyword', 'RETURN')) {
      return this.parseValue();
    }
    return undefined;
  }

  private parseReturnClause(): {
    items: ReturnItem[];
    orderBy?: { expression: Expression; direction?: 'ASC' | 'DESC' }[];
    skip?: Expression;
    limit?: Expression;
    distinct?: boolean;
  } {
    this.consume('keyword', 'RETURN');
    const distinct = this.optional('keyword', 'DISTINCT') !== null;
    const items: ReturnItem[] = [];
    let idx = 0;
    while (true) {
      const expr = this.parseValue();
      let alias: string | undefined;
      if (this.optional('keyword', 'AS')) {
        alias = this.parseIdentifier();
      }
      items.push({ expression: expr, alias });
      idx++;
      if (!this.optional('punct', ',')) break;
    }
    let orderBy: { expression: Expression; direction?: 'ASC' | 'DESC' }[] | undefined;
    if (this.current()?.value === 'ORDER') {
      this.consume('keyword', 'ORDER');
      this.consume('keyword', 'BY');
      orderBy = [];
      while (true) {
        const expr = this.parseValue();
        let direction: 'ASC' | 'DESC' | undefined;
        if (this.current()?.value === 'ASC' || this.current()?.value === 'DESC') {
          direction = this.current()!.value as 'ASC' | 'DESC';
          this.consume('keyword');
        }
        orderBy.push({ expression: expr, direction });
        if (!this.optional('punct', ',')) break;
      }
    }
    let skip: Expression | undefined;
    if (this.current()?.value === 'SKIP') {
      this.consume('keyword', 'SKIP');
      skip = this.parseValue();
    }
    let limit: Expression | undefined;
    if (this.current()?.value === 'LIMIT') {
      this.consume('keyword', 'LIMIT');
      limit = this.parseValue();
    }
    return { items, orderBy, skip, limit, distinct };
  }

  private parseWithClause(): {
    items: ReturnItem[];
    where?: WhereClause;
    orderBy?: { expression: Expression; direction?: 'ASC' | 'DESC' }[];
    skip?: Expression;
    limit?: Expression;
    distinct?: boolean;
  } {
    this.consume('keyword', 'WITH');
    const distinct = this.optional('keyword', 'DISTINCT') !== null;
    const items: ReturnItem[] = [];
    while (true) {
      const expr = this.parseValue();
      let alias: string | undefined;
      if (this.optional('keyword', 'AS')) {
        alias = this.parseIdentifier();
      }
      items.push({ expression: expr, alias });
      if (!this.optional('punct', ',')) break;
    }
    let where: WhereClause | undefined;
    if (this.current()?.value === 'WHERE') {
      this.consume('keyword', 'WHERE');
      where = this.parseWhereClause();
    }
    let orderBy: { expression: Expression; direction?: 'ASC' | 'DESC' }[] | undefined;
    if (this.current()?.value === 'ORDER') {
      this.consume('keyword', 'ORDER');
      this.consume('keyword', 'BY');
      orderBy = [];
      while (true) {
        const expr = this.parseValue();
        let direction: 'ASC' | 'DESC' | undefined;
        if (this.current()?.value === 'ASC' || this.current()?.value === 'DESC') {
          direction = this.current()!.value as 'ASC' | 'DESC';
          this.consume('keyword');
        }
        orderBy.push({ expression: expr, direction });
        if (!this.optional('punct', ',')) break;
      }
    }
    let skip: Expression | undefined;
    if (this.current()?.value === 'SKIP') {
      this.consume('keyword', 'SKIP');
      skip = this.parseValue();
    }
    let limit: Expression | undefined;
    if (this.current()?.value === 'LIMIT') {
      this.consume('keyword', 'LIMIT');
      limit = this.parseValue();
    }
    return { items, where, orderBy, skip, limit, distinct };
  }

  private parseReturnOnly(): ReturnQuery {
    const ret = this.parseReturnClause();
    return {
      type: 'Return',
      returnItems: ret.items,
      orderBy: ret.orderBy,
      skip: ret.skip,
      limit: ret.limit,
      distinct: ret.distinct,
    };
  }

  private parseWhereClause(): WhereClause {
    const parseNot = (): WhereClause => {
      if (this.current()?.value === 'NOT') {
        this.consume('keyword', 'NOT');
        return { type: 'Not', clause: parseNot() };
      }
      if (this.current()?.value === '(') {
        this.consume('punct', '(');
        const inner = this.parseWhereClause();
        this.consume('punct', ')');
        return inner;
      }
      const left = this.parseValue();
      if (this.current()?.value === 'IN') {
        this.consume('keyword', 'IN');
        const right = this.parseValue();
        return { type: 'Condition', left, operator: 'IN', right };
      }
      if (this.current()?.value === 'STARTS') {
        this.consume('keyword', 'STARTS');
        this.consume('keyword', 'WITH');
        const right = this.parseValue();
        return { type: 'Condition', left, operator: 'STARTS WITH', right };
      }
      if (this.current()?.value === 'ENDS') {
        this.consume('keyword', 'ENDS');
        this.consume('keyword', 'WITH');
        const right = this.parseValue();
        return { type: 'Condition', left, operator: 'ENDS WITH', right };
      }
      if (this.current()?.value === 'CONTAINS') {
        this.consume('keyword', 'CONTAINS');
        const right = this.parseValue();
        return { type: 'Condition', left, operator: 'CONTAINS', right };
      }
      if (this.current()?.value === 'IS') {
        this.consume('keyword', 'IS');
        const not = this.optional('keyword', 'NOT') !== null;
        if (
          this.current() &&
          this.current()!.type === 'identifier' &&
          this.current()!.value.toLowerCase() === 'null'
        ) {
          this.consume('identifier');
          return {
            type: 'Condition',
            left,
            operator: not ? 'IS NOT NULL' : 'IS NULL',
          };
        }
        throw new Error('Expected NULL');
      }
      const opTok = this.consume('punct');
      let op: '=' | '>' | '>=' | '<' | '<=' | '<>' = opTok.value as any;
      if (opTok.value === '<' && this.optional('punct', '>')) {
        op = '<>';
      } else if (opTok.value === '>' && this.optional('punct', '=')) {
        op = '>=';
      } else if (opTok.value === '<' && this.optional('punct', '=')) {
        op = '<=';
      }
      const right = this.parseValue();
      return { type: 'Condition', left, operator: op, right };
    };

    const parseAnd = (): WhereClause => {
      let left = parseNot();
      while (this.current()?.value === 'AND') {
        this.consume('keyword', 'AND');
        const right = parseNot();
        left = { type: 'And', left, right };
      }
      return left;
    };

    let left = parseAnd();
    while (this.current()?.value === 'OR') {
      this.consume('keyword', 'OR');
      const right = parseAnd();
      left = { type: 'Or', left, right };
    }
    return left;
  }

  private parseChainPattern(start: {
    variable?: string;
    labels?: string[];
    properties?: Record<string, unknown>;
  }): { startNode: { variable: string; labels?: string[]; properties?: Record<string, unknown> }; hops: MatchChainQuery['hops'] } {
    const startVar = start.variable ?? this.genAnonVar();
    const startNode = {
      variable: startVar,
      labels: start.labels,
      properties: start.properties,
    };
    const hops: MatchChainQuery['hops'] = [];
    let current = startNode;
    while (this.current()?.value === '-' || this.current()?.value === '<') {
      let direction: 'out' | 'in' | 'none' = 'out';
      if (this.current()?.value === '<') {
        this.consume('punct', '<');
        this.consume('punct', '-');
        direction = 'in';
      } else {
        this.consume('punct', '-');
      }
      this.consume('punct', '[');
      let relVar: string | undefined;
      if (this.current()?.type === 'identifier') {
        relVar = this.parseIdentifier();
      }
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
      if (direction === 'out') {
        if (this.optional('punct', '>')) {
          direction = 'out';
        } else {
          direction = 'none';
        }
      }
      const next = this.parseMaybeNodePattern();
      const nodeVar = next.variable ?? this.genAnonVar();
      hops.push({
        rel: { variable: relVar, type: relType, properties: relProps, direction },
        node: {
          variable: nodeVar,
          labels: next.labels,
          properties: next.properties,
        },
      });
      current = { variable: nodeVar, labels: next.labels, properties: next.properties };
    }
    return { startNode, hops };
  }

  private parseMatchChain(start: {
    variable?: string;
    labels?: string[];
    properties?: Record<string, unknown>;
  }, optional = false): MatchChainQuery {
    const { startNode, hops } = this.parseChainPattern(start);
    let where: WhereClause | undefined;
    if (this.current()?.value === 'WHERE') {
      this.consume('keyword', 'WHERE');
      where = this.parseWhereClause();
    }
    const ret = this.parseReturnClause();
    if (!ret.items.length) throw new Error('Parse error: RETURN required');
    return {
      type: 'MatchChain',
      start: startNode,
      hops,
      where,
      returnItems: ret.items,
      orderBy: ret.orderBy,
      skip: ret.skip,
      limit: ret.limit,
      distinct: ret.distinct,
      optional,
    };
  }

  private parseMatch(optional = false): CypherAST {
    this.consume('keyword', 'MATCH');
    if (
      this.current()?.type === 'identifier' &&
      this.tokens[this.pos + 1]?.value === '='
    ) {
      const pathVariable = this.parseIdentifier();
      this.consume('punct', '=');
      const start = this.parseNodePattern();
      const save = this.pos;
      try {
        let direction: 'out' | 'in' | 'none' = 'out';
        if (this.current()?.value === '<') {
          this.consume('punct', '<');
          this.consume('punct', '-');
          direction = 'in';
        } else {
          this.consume('punct', '-');
        }
        this.consume('punct', '[');
        let relType: string | undefined;
        if (this.optional('punct', ':')) {
          relType = this.parseIdentifier();
        }
        if (this.optional('punct', '*')) {
          let minHops: number | undefined;
          let maxHops: number | undefined;
          if (this.current()?.type === 'number') {
            minHops = Number(this.consume('number').value);
          }
          if (this.current()?.value === '.' && this.lookahead()?.value === '.') {
            this.consume('punct', '.');
            this.consume('punct', '.');
            if (this.current()?.type === 'number') {
              maxHops = Number(this.consume('number').value);
            }
          } else if (minHops !== undefined) {
            maxHops = minHops;
          }
          this.consume('punct', ']');
          this.consume('punct', '-');
          if (direction === 'out') {
            if (this.optional('punct', '>')) {
              direction = 'out';
            } else {
              direction = 'none';
            }
          }
          const end = this.parseNodePattern();
          let returnItems: ReturnItem[] | undefined;
          let orderBy;
          let skip;
          let limit;
          let distinct;
          if (this.current()?.value === 'RETURN') {
            const ret = this.parseReturnClause();
            returnItems = ret.items;
            orderBy = ret.orderBy;
            skip = ret.skip;
            limit = ret.limit;
            distinct = ret.distinct;
          }
          return {
            type: 'MatchPath',
            pathVariable,
            start: { variable: start.variable, labels: start.labels, properties: start.properties },
            end: { variable: end.variable, labels: end.labels, properties: end.properties },
            relType,
            minHops,
            maxHops,
            direction,
            returnItems,
            orderBy,
            skip,
            limit,
            distinct,
          };
        } else {
          this.pos = save;
          const { startNode, hops } = this.parseChainPattern(start);
          let where: WhereClause | undefined;
          if (this.current()?.value === 'WHERE') {
            this.consume('keyword', 'WHERE');
            where = this.parseWhereClause();
          }
          const ret = this.parseReturnClause();
          if (!ret.items.length) throw new Error('Parse error: RETURN required');
          return {
            type: 'MatchChain',
            start: startNode,
            hops,
            pathVariable,
            where,
            returnItems: ret.items,
            orderBy: ret.orderBy,
            skip: ret.skip,
            limit: ret.limit,
            distinct: ret.distinct,
            optional,
          };
        }
      } catch (err) {
        this.pos = save;
        throw err;
      }
    }
    let pattern: { variable: string; labels?: string[]; properties?: Record<string, unknown>; isRel?: boolean };
    const start = this.parseMaybeNodePattern();
    if (this.current()?.value === ',') {
      if (!start.variable) throw new Error('Parse error: node variable required');
      const patterns = [
        { variable: start.variable, labels: start.labels, properties: start.properties }
      ];
      while (this.optional('punct', ',')) {
        const next = this.parseNodePattern();
        patterns.push({ variable: next.variable, labels: next.labels, properties: next.properties });
      }
      let where: WhereClause | undefined;
      if (this.current()?.value === 'WHERE') {
        this.consume('keyword', 'WHERE');
        where = this.parseWhereClause();
      }
      const nextTok = this.current();
      if (!nextTok || nextTok.type !== 'keyword') throw new Error('Expected keyword');
      if (nextTok.value !== 'RETURN') throw new Error('Parse error: unsupported MATCH clause');
      const ret = this.parseReturnClause();
      return {
        type: 'MatchMultiReturn',
        patterns,
        optional,
        where,
        returnItems: ret.items,
        orderBy: ret.orderBy,
        skip: ret.skip,
        limit: ret.limit,
        distinct: ret.distinct,
      };
    }
    if (this.current()?.value === '-' || this.current()?.value === '<') {
      const save = this.pos;
      try {
        const { startNode, hops } = this.parseChainPattern(start);
        let where: WhereClause | undefined;
        if (this.current()?.value === 'WHERE') {
          this.consume('keyword', 'WHERE');
          where = this.parseWhereClause();
        }
        const hasAnonRel = hops.some(h => !h.rel.variable);
        const nextTok = this.current();
        if (nextTok?.value === 'RETURN') {
          const ret = this.parseReturnClause();
          if (
            hops.length === 1 &&
            ret.items.length === 1 &&
            ret.items[0].expression.type === 'Variable' &&
            hops[0].rel.variable === ret.items[0].expression.name &&
            !hasAnonRel &&
            !where
          ) {
            // treat as simple pattern
            this.pos = save;
          } else {
            return {
              type: 'MatchChain',
              start: startNode,
              hops,
              where,
              returnItems: ret.items,
              orderBy: ret.orderBy,
              skip: ret.skip,
              limit: ret.limit,
              distinct: ret.distinct,
              optional,
            };
          }
        } else if (nextTok?.value === 'WITH') {
          const withClause = this.parseWithClause();
          const source: MatchChainQuery = {
            type: 'MatchChain',
            start: startNode,
            hops,
            where,
            returnItems: withClause.items,
            orderBy: withClause.orderBy,
            skip: withClause.skip,
            limit: withClause.limit,
            distinct: withClause.distinct,
            optional,
          };
          const nextStmt = this.parse();
          return { type: 'With', source, where: withClause.where, next: nextStmt };
        } else {
          this.pos = save;
        }
      } catch {
        this.pos = save;
      }
    }
    if (this.current()?.value === '-') {
      this.consume('punct', '-');
      this.consume('punct', '[');
      let relVar = '';
      if (this.current()?.type === 'identifier') {
        relVar = this.parseIdentifier();
      }
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
      this.optional('punct', '>');
      this.parseMaybeNodePattern();
      pattern = { variable: relVar, labels: relType ? [relType] : undefined, properties: relProps, isRel: true };
    } else {
      if (!start.variable) {
        start.variable = this.genAnonVar();
      }
      pattern = {
        variable: start.variable,
        labels: start.labels,
        properties: start.properties,
        isRel: false,
      };
    }
    let where: WhereClause | undefined;
    if (this.current()?.value === 'WHERE') {
      this.consume('keyword', 'WHERE');
      where = this.parseWhereClause();
    }
    const next = this.current();
    if (!next || next.type !== 'keyword') throw new Error('Expected keyword');
    if (next.value === 'RETURN') {
      const ret = this.parseReturnClause();
      return {
        type: 'MatchReturn',
        variable: pattern.variable,
        labels: pattern.labels,
        properties: pattern.properties,
        isRelationship: pattern.isRel,
        optional,
        where,
        returnItems: ret.items,
        orderBy: ret.orderBy,
        skip: ret.skip,
        limit: ret.limit,
        distinct: ret.distinct,
      };
    }
    if (next.value === 'DELETE') {
      this.consume('keyword', 'DELETE');
      const id = this.parseIdentifier();
      if (id !== pattern.variable) throw new Error('Parse error: delete variable mismatch');
      return { type: 'MatchDelete', variable: pattern.variable, labels: pattern.labels, properties: pattern.properties, isRelationship: pattern.isRel, where };
    }
    if (next.value === 'SET') {
      this.consume('keyword', 'SET');
      const updates: Record<string, Expression> = {};
      while (true) {
        const id = this.parseIdentifier();
        if (id !== pattern.variable)
          throw new Error('Parse error: set variable mismatch');
        this.consume('punct', '.');
        const prop = this.parseIdentifier();
        this.consume('punct', '=');
        const value = this.parseValue();
        updates[prop] = value;
        if (!this.optional('punct', ',')) break;
      }
      const ret = this.parseReturnVariable();
      if (ret && ret !== pattern.variable)
        throw new Error('Parse error: return variable mismatch');
      return {
        type: 'MatchSet',
        variable: pattern.variable,
        labels: pattern.labels,
        properties: pattern.properties,
        updates,
        isRelationship: pattern.isRel,
        returnVariable: ret,
        where,
      };
    }
    if (next.value === 'WITH') {
      const withClause = this.parseWithClause();
      const source: MatchReturnQuery = {
        type: 'MatchReturn',
        variable: pattern.variable,
        labels: pattern.labels,
        properties: pattern.properties,
        isRelationship: pattern.isRel,
        optional,
        where,
        returnItems: withClause.items,
        orderBy: withClause.orderBy,
        skip: withClause.skip,
        limit: withClause.limit,
        distinct: withClause.distinct,
      };
      const nextStmt = this.parse();
      return { type: 'With', source, where: withClause.where, next: nextStmt };
    }
    throw new Error('Parse error: unsupported MATCH clause');
  }

  private parseCreate(): CypherAST {
    this.consume('keyword', 'CREATE');
    const start = this.parseNodePattern();
    if (this.current()?.value === '-') {
      this.consume('punct', '-');
      this.consume('punct', '[');
      let relVar: string;
      if (this.current()?.type === 'identifier' && this.lookahead()?.value === ':') {
        relVar = this.parseIdentifier();
        this.consume('punct', ':');
      } else {
        relVar = this.genAnonVar();
        this.consume('punct', ':');
      }
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
        start: { variable: start.variable, labels: start.labels, properties: start.properties },
        end: { variable: end.variable, labels: end.labels, properties: end.properties },
        returnVariable: ret,
      };
    }
    let setProps: Record<string, Expression> | undefined;
    if (this.current()?.value === 'SET') {
      this.consume('keyword', 'SET');
      setProps = {};
      while (true) {
        const varName = this.parseIdentifier();
        if (varName !== start.variable)
          throw new Error('Parse error: set variable mismatch');
        this.consume('punct', '.');
        const prop = this.parseIdentifier();
        this.consume('punct', '=');
        const val = this.parseValue();
        setProps[prop] = val;
        if (!this.optional('punct', ',')) break;
      }
    }
    const ret = this.parseReturnVariable();
    if (ret && ret !== start.variable) {
      throw new Error('Parse error: return variable mismatch');
    }
    return {
      type: 'Create',
      variable: start.variable,
      labels: start.labels,
      properties: start.properties,
      setProperties: setProps,
      returnVariable: ret,
    };
  }

  private parseMerge(): MergeQuery | MergeRelQuery {
    this.consume('keyword', 'MERGE');
    const start = this.parseMaybeNodePattern();
    if (this.current()?.value === '-') {
      this.consume('punct', '-');
      this.consume('punct', '[');
      let relVar: string;
      if (this.current()?.type === 'identifier' && this.lookahead()?.value === ':') {
        relVar = this.parseIdentifier();
        this.consume('punct', ':');
      } else {
        relVar = this.genAnonVar();
        this.consume('punct', ':');
      }
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
      let onCreate: Record<string, Expression> | undefined;
      let onMatch: Record<string, Expression> | undefined;
      while (this.current()?.value === 'ON') {
        this.consume('keyword', 'ON');
        const which = this.consume('keyword').value;
        if (which !== 'CREATE' && which !== 'MATCH')
          throw new Error('Expected CREATE or MATCH');
        this.consume('keyword', 'SET');
        const target: Record<string, Expression> = {};
        while (true) {
          const varName = this.parseIdentifier();
          if (varName !== relVar)
            throw new Error('Parse error: set variable mismatch');
          this.consume('punct', '.');
          const prop = this.parseIdentifier();
          this.consume('punct', '=');
          const val = this.parseValue();
          target[prop] = val;
          if (!this.optional('punct', ',')) break;
        }
        if (which === 'CREATE') onCreate = target;
        else onMatch = target;
      }
      const ret = this.parseReturnVariable();
      if (ret && ret !== relVar) throw new Error('Parse error: return variable mismatch');
      return {
        type: 'MergeRel',
        relVariable: relVar,
        relType,
        relProperties: relProps,
        start: { variable: start.variable, labels: start.labels, properties: start.properties },
        end: { variable: end.variable, labels: end.labels, properties: end.properties },
        returnVariable: ret,
        onCreateSet: onCreate,
        onMatchSet: onMatch,
      };
    }
    if (!start.variable) throw new Error('Parse error: node variable required');
    let onCreate: Record<string, Expression> | undefined;
    let onMatch: Record<string, Expression> | undefined;
    while (this.current()?.value === 'ON') {
      this.consume('keyword', 'ON');
      const which = this.consume('keyword').value;
      if (which !== 'CREATE' && which !== 'MATCH')
        throw new Error('Expected CREATE or MATCH');
      this.consume('keyword', 'SET');
      const target: Record<string, Expression> = {};
      while (true) {
        const varName = this.parseIdentifier();
        if (varName !== start.variable)
          throw new Error('Parse error: set variable mismatch');
        this.consume('punct', '.');
        const prop = this.parseIdentifier();
        this.consume('punct', '=');
        const val = this.parseValue();
        target[prop] = val;
        if (!this.optional('punct', ',')) break;
      }
      if (which === 'CREATE') onCreate = target; else onMatch = target;
    }
    const ret = this.parseReturnVariable();
    if (ret && ret !== start.variable) {
      throw new Error('Parse error: return variable mismatch');
    }
    return {
      type: 'Merge',
      variable: start.variable,
      labels: start.labels,
      properties: start.properties,
      onCreateSet: onCreate,
      onMatchSet: onMatch,
      returnVariable: ret,
    };
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

  private parseUnwind(): UnwindQuery {
    this.consume('keyword', 'UNWIND');
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
    this.consume('keyword', 'AS');
    const variable = this.parseIdentifier();
    this.consume('keyword', 'RETURN');
    const returnExpression = this.parseValue();
    let returnAlias: string | undefined;
    if (this.optional('keyword', 'AS')) {
      returnAlias = this.parseIdentifier();
    }
    return { type: 'Unwind', list, variable, returnExpression, returnAlias };
  }

  private parseCall(): CallQuery {
    this.consume('keyword', 'CALL');
    this.consume('punct', '{');
    const start = this.pos;
    let depth = 1;
    while (depth > 0) {
      const tok = this.current();
      if (!tok) throw new Error('Unclosed CALL subquery');
      this.pos++;
      if (tok.type === 'punct' && tok.value === '{') depth++;
      else if (tok.type === 'punct' && tok.value === '}') depth--;
    }
    const innerTokens = this.tokens.slice(start, this.pos - 1);
    const innerQuery = innerTokens.map(t => t.value).join(' ');
    const subquery = parseMany(innerQuery);
    const ret = this.parseReturnClause();
    return {
      type: 'Call',
      subquery,
      returnItems: ret.items,
      distinct: ret.distinct,
      orderBy: ret.orderBy,
      skip: ret.skip,
      limit: ret.limit,
    };
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
  const tokens = tokenize(query);
  const parser = new Parser(tokens);
  const out: CypherAST[] = [];
  while ((parser as any).pos < tokens.length) {
    const ast = parser.parse();
    out.push(ast);
    const next = (parser as any).tokens[(parser as any).pos];
    if (next && next.type === 'punct' && next.value === ';') {
      (parser as any).pos++;
    }
  }
  return out;
}
