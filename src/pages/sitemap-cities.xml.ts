import { urlset, cityPaths } from '../lib/sitemap';
export const GET = () => urlset(cityPaths());
