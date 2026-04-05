import { defineConfig } from 'astro/config';
import tailwind from '@astrojs/tailwind';

export default defineConfig({
  site: 'https://zipcodedetails.netlify.app',
  integrations: [tailwind()],
  output: 'static',
  build: {
    format: 'directory',
    concurrency: 50,
  },
  vite: {
    build: {
      chunkSizeWarningLimit: 2000,
    },
  },
});
