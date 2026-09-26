import { INDEXNOW_KEY } from '../config/site';

// IndexNow ownership file: https://www.indexnow.org/documentation
export function getStaticPaths() {
  return [{ params: { indexnow: INDEXNOW_KEY } }];
}
export const GET = () => new Response(INDEXNOW_KEY, { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
