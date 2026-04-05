import fs from 'node:fs';
import path from 'node:path';
import type { APIRoute } from 'astro';

const SITE = 'https://zipcodedetails.netlify.app';

export const GET: APIRoute = () => {
  // Static pages
  const pages = [
    { loc: '/', priority: '1.0', changefreq: 'monthly' },
    { loc: '/search', priority: '0.8', changefreq: 'monthly' },
    { loc: '/about', priority: '0.5', changefreq: 'yearly' },
    { loc: '/privacy', priority: '0.3', changefreq: 'yearly' },
    { loc: '/terms', priority: '0.3', changefreq: 'yearly' },
    { loc: '/guides', priority: '0.8', changefreq: 'monthly' },
    { loc: '/guides/how-to-find-your-zip-code', priority: '0.7', changefreq: 'yearly' },
    { loc: '/guides/what-is-a-zip-code', priority: '0.7', changefreq: 'yearly' },
    { loc: '/guides/zip-code-vs-postal-code', priority: '0.7', changefreq: 'yearly' },
    { loc: '/guides/us-zip-code-format', priority: '0.7', changefreq: 'yearly' },
  ];

  // Top pages for each state
  const stateIndex = JSON.parse(
    fs.readFileSync(path.join(process.cwd(), 'data', 'state_index.json'), 'utf-8')
  );
  for (const abbr of Object.keys(stateIndex)) {
    pages.push({ loc: `/top/${abbr.toLowerCase()}`, priority: '0.6', changefreq: 'yearly' });
  }

  const urls = pages
    .map((p) => `  <url><loc>${SITE}${p.loc}</loc><priority>${p.priority}</priority><changefreq>${p.changefreq}</changefreq></url>`)
    .join('\n');

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
