import { defineConfig } from 'tsup';

// Dual-format (ESM + CJS) with declarations, two entries: the core stream client
// and the optional React hook (a separate entry so non-React consumers never pull
// react into their graph). Zero runtime dependencies.
export default defineConfig({
  entry: ['src/index.ts', 'src/react.ts'],
  format: ['esm', 'cjs'],
  dts: true,
  clean: true,
  sourcemap: true,
  treeshake: true,
  external: ['react'],
});
