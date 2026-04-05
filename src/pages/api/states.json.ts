import fs from 'node:fs';
import path from 'node:path';
import type { APIRoute } from 'astro';

export const GET: APIRoute = () => {
  const stateIndex = JSON.parse(
    fs.readFileSync(path.join(process.cwd(), 'data', 'state_index.json'), 'utf-8')
  );

  const states = Object.entries(stateIndex).map(([abbr, info]: [string, any]) => ({
    abbreviation: abbr,
    name: info.name,
    zip_count: info.zip_count || 0,
  }));

  return new Response(JSON.stringify({ states }, null, 2), {
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=86400',
    },
  });
};
