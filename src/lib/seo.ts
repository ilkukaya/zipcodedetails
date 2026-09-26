import { SITE } from '../config/site';
import { abs } from './url';

export interface FAQ {
  question: string;
  answer: string;
}

export function faqSchema(faqs: FAQ[]) {
  return {
    '@context': 'https://schema.org',
    '@type': 'FAQPage',
    mainEntity: faqs.map((f) => ({
      '@type': 'Question',
      name: f.question,
      acceptedAnswer: { '@type': 'Answer', text: f.answer },
    })),
  };
}

export interface Crumb {
  name: string;
  path: string;
}

export function breadcrumbSchema(items: Crumb[]) {
  return {
    '@context': 'https://schema.org',
    '@type': 'BreadcrumbList',
    itemListElement: items.map((c, i) => ({
      '@type': 'ListItem',
      position: i + 1,
      name: c.name,
      item: abs(c.path),
    })),
  };
}

export const ORG_ID = `${SITE.url}/#organization`;
export const WEBSITE_ID = `${SITE.url}/#website`;

export function organizationSchema() {
  return {
    '@context': 'https://schema.org',
    '@type': 'Organization',
    '@id': ORG_ID,
    name: SITE.brand,
    url: SITE.url + '/',
    logo: {
      '@type': 'ImageObject',
      url: `${SITE.url}/icon-512.png`,
      width: 512,
      height: 512,
    },
    sameAs: [SITE.repo],
  };
}

export function websiteSchema() {
  return {
    '@context': 'https://schema.org',
    '@type': 'WebSite',
    '@id': WEBSITE_ID,
    name: SITE.brand,
    alternateName: SITE.name,
    url: SITE.url + '/',
    description: SITE.description,
    inLanguage: 'en-US',
    publisher: { '@id': ORG_ID },
    potentialAction: {
      '@type': 'SearchAction',
      target: { '@type': 'EntryPoint', urlTemplate: `${SITE.url}/search/?q={search_term_string}` },
      'query-input': 'required name=search_term_string',
    },
  };
}

export function webPageSchema(opts: {
  path: string;
  name: string;
  description: string;
  dateModified: string;
  about?: object;
  type?: string;
}) {
  return {
    '@context': 'https://schema.org',
    '@type': opts.type || 'WebPage',
    '@id': abs(opts.path) + '#webpage',
    url: abs(opts.path),
    name: opts.name,
    description: opts.description,
    inLanguage: 'en-US',
    isPartOf: { '@id': WEBSITE_ID },
    publisher: { '@id': ORG_ID },
    dateModified: opts.dateModified,
    ...(opts.about ? { about: opts.about } : {}),
  };
}

/** Trim a meta description to ~155 chars on a word boundary. */
export function clampDescription(s: string, max = 158): string {
  if (s.length <= max) return s;
  const cut = s.slice(0, max - 1);
  return cut.slice(0, cut.lastIndexOf(' ')).replace(/[,;:\s]+$/, '') + '…';
}
