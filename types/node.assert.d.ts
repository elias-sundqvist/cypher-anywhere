declare module 'node:assert' {
  function ok(value: unknown): void;
  function strictEqual(actual: unknown, expected: unknown): void;
  export { ok, strictEqual };
}
