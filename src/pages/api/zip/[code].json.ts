import fs from 'node:fs';
import path from 'node:path';
import type { APIRoute, GetStaticPaths } from 'astro';

export const getStaticPaths: GetStaticPaths = () => {
  const zipsDir = path.join(process.cwd(), 'data', 'zips');
  const files = fs.readdirSync(zipsDir).filter((f) => f.endsWith('.json'));
  return files.map((file) => {
    const code = file.replace('.json', '');
    const raw = fs.readFileSync(path.join(zipsDir, file), 'utf-8');
    const data = JSON.parse(raw);
    return { params: { code }, props: { data } };
  });
};

export const GET: APIRoute = ({ props }) => {
  const d = props.data as any;

  const response = {
    zip: d.zip,
    city: d.city,
    state: d.state,
    state_full: d.state_full,
    county: d.county,
    timezone: d.timezone,
    dst: d.dst,
    lat: d.lat,
    lng: d.lng,
    land_area_sqmi: d.land_area_sqmi,
    water_area_sqmi: d.water_area_sqmi,
    zip_type: d.zip_type || 'Standard',
    cbsa: d.cbsa || null,
    cbsa_code: d.cbsa_code || null,
    surrounding_zips: d.surrounding_zips || [],
  };

  return new Response(JSON.stringify(response, null, 2), {
    headers: {
      'Content-Type': 'application/json',
      'Access-Control-Allow-Origin': '*',
      'Cache-Control': 'public, max-age=86400',
    },
  });
};
