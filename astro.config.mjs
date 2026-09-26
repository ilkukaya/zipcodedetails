import { defineConfig } from 'astro/config';
import tailwind from '@astrojs/tailwind';

export default defineConfig({
  site: (process.env.SITE_URL || 'https://zipcodedetails.netlify.app').replace(/\/$/, ''),
  // Every URL ends with a slash — matches Netlify's directory-style serving,
  // so canonical, sitemap and internal links never hit a redirect.
  trailingSlash: 'always',
  integrations: [tailwind({ applyBaseStyles: false })],
  output: 'static',
  compressHTML: true,
  build: {
    format: 'directory',
    concurrency: 8,
    inlineStylesheets: 'never',
  },
  vite: {
    build: {
      chunkSizeWarningLimit: 2000,
      // Keep every script as a cacheable file instead of inlining it into 72k pages.
      assetsInlineLimit: 0,
    },
  },
});
