import { urlset, countyPaths } from '../lib/sitemap';
export const GET = () => urlset(countyPaths());
