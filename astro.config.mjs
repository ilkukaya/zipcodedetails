import { defineConfig } from 'astro/config';
import tailwind from '@astrojs/tailwind';

export default defineConfig({
  site: 'https://zipcodedetails.com',
  integrations: [tailwind()],
  output: 'static',
  build: {
    format: 'directory',
  },
  vite: {
    build: {
      chunkSizeWarningLimit: 1000,
    },
  },
});
