import fs from 'node:fs';
import path from 'node:path';
import type { APIRoute } from 'astro';
import { citySlug } from '../lib/formatters';

const SITE = 'https://zipcodedetails.netlify.app';

export const GET: APIRoute = () => {
  const cityIndex = JSON.parse(
    fs.readFileSync(path.join(process.cwd(), 'data', 'city_index.json'), 'utf-8')
  );

  const urls = Object.values(cityIndex)
    .map((info: any) => {
      const slug = citySlug(info.city, info.state);
      return `  <url><loc>${SITE}/city/${slug}</loc><changefreq>yearly</changefreq><priority>0.7</priority></url>`;
    })
    .join('\n');

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
