/**
 * Central site configuration.
 *
 * Every value can be overridden with an environment variable of the same
 * name at build time (GitHub Actions → repository "Variables"). Values left
 * empty simply switch the related feature off — nothing broken is rendered.
 */

function env(name: string, fallback = ''): string {
  const v = process.env[name];
  return v && v.trim() ? v.trim() : fallback;
}

export const SITE = {
  name: 'ZIPCodeDetails',
  brand: 'ZIPCodeDetails.com',
  url: env('SITE_URL', 'https://zipcodedetails.netlify.app').replace(/\/$/, ''),
  tagline: 'Every US ZIP code, explained.',
  description:
    'Free lookup for every US ZIP code: city, county, time zone, map, population, income, home values and nearby ZIP codes.',
  locale: 'en_US',
  twitter: env('TWITTER_HANDLE'),
  contactEmail: env('CONTACT_EMAIL'),
  repo: 'https://github.com/ilkukaya/zipcodedetails',
  // Vintage of the demographic figures in the source dataset.
  dataVintage: '2010 Census & ACS 5-year estimates',
};

/** Advertising. Leave ADSENSE_CLIENT empty until AdSense approves the site. */
export const ADS = {
  adsenseClient: env('ADSENSE_CLIENT'), // e.g. ca-pub-1234567890123456
  slots: {
    top: env('ADSENSE_SLOT_TOP'),
    inContent: env('ADSENSE_SLOT_IN_CONTENT'),
    sidebar: env('ADSENSE_SLOT_SIDEBAR'),
  },
};

/** Analytics — all free. Any combination may be enabled. */
export const ANALYTICS = {
  ga4: env('GA4_ID'), // G-XXXXXXXXXX
  cloudflareToken: env('CF_ANALYTICS_TOKEN'),
  plausibleDomain: env('PLAUSIBLE_DOMAIN'),
};

/** Search-engine ownership verification meta tags. */
export const VERIFICATION = {
  google: env('GOOGLE_SITE_VERIFICATION'),
  bing: env('BING_SITE_VERIFICATION'),
  yandex: env('YANDEX_VERIFICATION'),
  pinterest: env('PINTEREST_VERIFICATION'),
};

/** IndexNow key (Bing, Yandex, Seznam, Naver). Public by design. */
export const INDEXNOW_KEY = env('INDEXNOW_KEY', '6f1c2a9e4b7d4e0f9a3c5b8d2e1f7a64');

/**
 * Affiliate partners. Each entry renders only when its URL template is set.
 * Placeholders: {zip} {city} {state} {stateFull} {lat} {lng} {q}
 * ({q} = URL-encoded "City, ST"). IDs/tags go directly inside the template.
 */
export const AFFILIATES = {
  hotels: {
    // Stay22 (free to join, instant approval): replace YOUR_AID
    url: env('AFF_HOTELS_URL'),
    // e.g. https://www.stay22.com/allez/roam?aid=YOUR_AID&lat={lat}&lng={lng}&campaign=zip-{zip}
  },
  movers: {
    url: env('AFF_MOVERS_URL'),
  },
  internet: {
    url: env('AFF_INTERNET_URL'),
  },
  insurance: {
    url: env('AFF_INSURANCE_URL'),
  },
  security: {
    url: env('AFF_SECURITY_URL'),
  },
  amazonTag: env('AMAZON_TAG'), // e.g. zipcodedetails-20
};

export type AffiliateKey = 'hotels' | 'movers' | 'internet' | 'insurance' | 'security';

export function affiliateUrl(
  key: AffiliateKey,
  p: { zip?: string; city: string; state: string; stateFull: string; lat?: number; lng?: number }
): string | null {
  const tpl = AFFILIATES[key].url;
  if (!tpl) return null;
  const rep: Record<string, string> = {
    zip: p.zip ?? '',
    city: encodeURIComponent(p.city),
    state: p.state,
    stateFull: encodeURIComponent(p.stateFull),
    lat: p.lat != null ? String(p.lat) : '',
    lng: p.lng != null ? String(p.lng) : '',
    q: encodeURIComponent(`${p.city}, ${p.state}`),
  };
  return tpl.replace(/\{(\w+)\}/g, (_, k) => rep[k] ?? '');
}

export function amazonUrl(query: string): string | null {
  if (!AFFILIATES.amazonTag) return null;
  return `https://www.amazon.com/s?k=${encodeURIComponent(query)}&tag=${encodeURIComponent(AFFILIATES.amazonTag)}`;
}

export const hasAnyAffiliate =
  Object.values(AFFILIATES).some((v) => (typeof v === 'string' ? !!v : !!v.url));
