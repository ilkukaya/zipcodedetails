import { ADS } from '../config/site';

// Required by AdSense once approved: authorises Google to sell our inventory.
export const GET = () => {
  const pub = ADS.adsenseClient.replace(/^ca-/, '');
  const body = pub ? `google.com, ${pub}, DIRECT, f08c47fec0942fa0\n` : '# Ad sellers will be listed here once advertising is enabled.\n';
  return new Response(body, { headers: { 'Content-Type': 'text/plain; charset=utf-8' } });
};
