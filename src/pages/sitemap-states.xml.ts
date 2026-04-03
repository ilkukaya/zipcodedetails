import fs from 'node:fs';
import path from 'node:path';
import type { APIRoute } from 'astro';

const SITE = 'https://zipcodedetails.com';

export const GET: APIRoute = () => {
  const stateIndex = JSON.parse(
    fs.readFileSync(path.join(process.cwd(), 'data', 'state_index.json'), 'utf-8')
  );

  const urls = Object.keys(stateIndex)
    .sort()
    .map(
      (abbr) =>
        `  <url><loc>${SITE}/state/${abbr.toLowerCase()}</loc><changefreq>yearly</changefreq><priority>0.9</priority></url>`
    )
    .join('\n');

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>${SITE}/</loc><changefreq>monthly</changefreq><priority>1.0</priority></url>
${urls}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
