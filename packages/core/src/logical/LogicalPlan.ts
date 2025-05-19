import {
  CypherAST,
  MatchReturnQuery,
  CreateQuery,
  MergeQuery,
  MatchDeleteQuery,
  MatchSetQuery,
  CreateRelQuery,
  MergeRelQuery,
  MatchPathQuery,
  MatchChainQuery,
  ForeachQuery,
  UnwindQuery,
} from '../parser/CypherParser';

// Logical plan nodes mirror the parsed AST for now but live in a separate layer
// so that optimizers can operate on them before physical compilation.
export type LogicalPlan =
  | LogicalMatchReturn
  | LogicalCreate
  | LogicalMerge
  | LogicalMatchDelete
  | LogicalMatchSet
  | LogicalCreateRel
  | LogicalMergeRel
  | LogicalMatchPath
  | LogicalMatchChain
  | LogicalForeach
  | LogicalUnwind;

export type LogicalMatchReturn = MatchReturnQuery;
export type LogicalCreate = CreateQuery;
export type LogicalMerge = MergeQuery;
export type LogicalMatchDelete = MatchDeleteQuery;
export type LogicalMatchSet = MatchSetQuery;
export type LogicalCreateRel = CreateRelQuery;
export type LogicalMergeRel = MergeRelQuery;
export type LogicalMatchPath = MatchPathQuery;
export type LogicalMatchChain = MatchChainQuery;
export type LogicalForeach = ForeachQuery;
export type LogicalUnwind = UnwindQuery;

export function astToLogical(ast: CypherAST): LogicalPlan {
  // In this MVP the AST shape already matches the logical plan
  return ast as unknown as LogicalPlan;
}
