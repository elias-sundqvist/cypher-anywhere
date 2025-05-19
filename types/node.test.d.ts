declare module 'node:test' {
  export default function test(name: string, fn: () => unknown | Promise<unknown>): void;
}
