module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/tests'],
  moduleNameMapper: {
    '^@cypher-anywhere/core(.*)$': '<rootDir>/packages/core/src$1',
    '^@cypher-anywhere/json-adapter(.*)$': '<rootDir>/packages/adapters/json-adapter/src$1'
  }
};
