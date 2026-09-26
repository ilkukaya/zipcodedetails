import { SITE } from '../config/site';

// One group for all crawlers — search engines and AI assistants alike are welcome.
const AGENTS = [
  '*', 'Googlebot', 'Bingbot', 'Applebot', 'DuckDuckBot', 'YandexBot',
  'GPTBot', 'OAI-SearchBot', 'ChatGPT-User', 'ClaudeBot', 'Claude-SearchBot', 'Claude-User',
  'PerplexityBot', 'Perplexity-User', 'Google-Extended', 'Applebot-Extended', 'Amazonbot',
  'meta-externalagent', 'DuckAssistBot', 'MistralAI-User', 'CCBot',
];

export const GET = () =>
  new Response(
    `${AGENTS.map((a) => `User-agent: ${a}`).join('\n')}
Allow: /
Disallow: /search/?
Disallow: /contact/thanks/

Sitemap: ${SITE.url}/sitemap-index.xml
`,
    { headers: { 'Content-Type': 'text/plain; charset=utf-8' } }
  );
