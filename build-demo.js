const esbuild = require('esbuild');

esbuild.build({
  entryPoints: ['demo/demo.ts'],
  bundle: true,
  outfile: 'docs/bundle.js',
  format: 'esm',
  sourcemap: true,
  target: ['es2019']
}).catch(() => process.exit(1));
