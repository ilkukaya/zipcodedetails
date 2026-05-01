import fs from 'node:fs';
import path from 'node:path';
import type { APIRoute } from 'astro';

const SITE = 'https://zipcodedetails.netlify.app';

export const GET: APIRoute = () => {
  const countyIndex = JSON.parse(
    fs.readFileSync(path.join(process.cwd(), 'data', 'county_index.json'), 'utf-8')
  );

  const urls = Object.keys(countyIndex)
    .sort()
    .map(
      (slug) =>
        `  <url><loc>${SITE}/county/${slug}</loc><changefreq>yearly</changefreq><priority>0.7</priority></url>`
    )
    .join('\n');

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
