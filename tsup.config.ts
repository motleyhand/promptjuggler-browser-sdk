import { defineConfig } from 'tsup';

// Dual-format (ESM + CJS) with declarations, three entries: the core stream
// client and the optional React and Angular adapters (separate entries so
// consumers never pull the other framework into their graph). Zero runtime
// dependencies.
export default defineConfig({
  entry: ['src/index.ts', 'src/react.ts', 'src/angular.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  clean: true,
  sourcemap: true,
  treeshake: true,
  external: ['react', '@angular/core'],
});
