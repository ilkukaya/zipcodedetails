import { urlset, staticPaths } from '../lib/sitemap';
export const GET = () => urlset(staticPaths());
