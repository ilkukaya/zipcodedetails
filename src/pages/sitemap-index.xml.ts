import fs from 'node:fs';
import path from 'node:path';
import type { APIRoute } from 'astro';

const SITE = 'https://zipcodedetails.netlify.app';
const ZIPS_PER_SITEMAP = 10000;

export const GET: APIRoute = () => {
  const zipsDir = path.join(process.cwd(), 'data', 'zips');
  const zipFiles = fs.readdirSync(zipsDir).filter((f) => f.endsWith('.json'));
  const totalZips = zipFiles.length;
  const numZipSitemaps = Math.ceil(totalZips / ZIPS_PER_SITEMAP);

  let sitemaps = '';

  for (let i = 1; i <= numZipSitemaps; i++) {
    sitemaps += `  <sitemap><loc>${SITE}/sitemap-zips-${i}.xml</loc></sitemap>\n`;
  }

  sitemaps += `  <sitemap><loc>${SITE}/sitemap-states.xml</loc></sitemap>\n`;
  sitemaps += `  <sitemap><loc>${SITE}/sitemap-cities.xml</loc></sitemap>\n`;

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${sitemaps}</sitemapindex>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
