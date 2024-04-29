import {defineConfig} from 'vitest/config';

export default defineConfig({
  test: {
    globalSetup: './helpers/globalSetupBenchmark.js'
  },
});