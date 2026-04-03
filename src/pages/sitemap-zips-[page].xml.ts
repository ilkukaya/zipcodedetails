import fs from 'node:fs';
import path from 'node:path';
import type { APIRoute, GetStaticPaths } from 'astro';

const SITE = 'https://zipcodedetails.com';
const ZIPS_PER_SITEMAP = 10000;

export const getStaticPaths: GetStaticPaths = () => {
  const zipsDir = path.join(process.cwd(), 'data', 'zips');
  const zipFiles = fs.readdirSync(zipsDir).filter((f) => f.endsWith('.json'));
  const totalZips = zipFiles.length;
  const numPages = Math.ceil(totalZips / ZIPS_PER_SITEMAP);
  return Array.from({ length: numPages }, (_, i) => ({
    params: { page: String(i + 1) },
  }));
};

export const GET: APIRoute = ({ params }) => {
  const page = parseInt(params.page || '1', 10);
  const zipsDir = path.join(process.cwd(), 'data', 'zips');
  const zipFiles = fs
    .readdirSync(zipsDir)
    .filter((f) => f.endsWith('.json'))
    .sort();

  const start = (page - 1) * ZIPS_PER_SITEMAP;
  const end = start + ZIPS_PER_SITEMAP;
  const pageZips = zipFiles.slice(start, end);

  const urls = pageZips
    .map((f) => {
      const code = f.replace('.json', '');
      return `  <url><loc>${SITE}/zip/${code}</loc><changefreq>yearly</changefreq><priority>0.8</priority></url>`;
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
