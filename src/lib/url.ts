import { SITE } from '../config/site';

/** Normalise an internal path so it always ends with a slash (no-op for files). */
export function u(path: string): string {
  if (!path.startsWith('/')) path = '/' + path;
  const [p, rest] = splitSuffix(path);
  if (p.endsWith('/') || /\.[a-z0-9]+$/i.test(p)) return p + rest;
  return p + '/' + rest;
}

/** Absolute URL for an internal path. */
export function abs(path: string): string {
  return SITE.url + u(path);
}

function splitSuffix(path: string): [string, string] {
  const i = path.search(/[?#]/);
  return i === -1 ? [path, ''] : [path.slice(0, i), path.slice(i)];
}

export const zipUrl = (zip: string) => `/zip/${zip}/`;
export const cityUrl = (slug: string) => `/city/${slug}/`;
export const countyUrl = (slug: string) => `/county/${slug}/`;
export const stateUrl = (abbr: string) => `/state/${abbr.toLowerCase()}/`;
export const topUrl = (abbr: string) => `/top/${abbr.toLowerCase()}/`;
