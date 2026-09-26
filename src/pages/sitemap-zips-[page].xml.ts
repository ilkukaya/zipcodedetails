import type { APIRoute } from 'astro';
import { urlset, zipPaths, zipSitemapCount } from '../lib/sitemap';

export function getStaticPaths() {
  return Array.from({ length: zipSitemapCount() }, (_, i) => ({ params: { page: String(i + 1) } }));
}

export const GET: APIRoute = ({ params }) => urlset(zipPaths(parseInt(params.page || '1', 10)));
