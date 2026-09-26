export interface GuideMeta {
  slug: string;
  title: string;
  short: string;
  excerpt: string;
  published: string;
}

export const GUIDES: GuideMeta[] = [
  {
    slug: 'what-is-a-zip-code',
    title: 'What Is a ZIP Code? History, Format & How It Works',
    short: 'What is a ZIP code?',
    excerpt: 'Where ZIP codes came from, what each digit means, and the four types of ZIP code.',
    published: '2026-04-20',
  },
  {
    slug: 'how-to-find-your-zip-code',
    title: 'How to Find Your ZIP Code (5 Quick Ways)',
    short: 'How to find your ZIP code',
    excerpt: 'Five fast ways to find the ZIP code for your address — or anyone else’s.',
    published: '2026-04-20',
  },
  {
    slug: 'us-zip-code-format',
    title: 'US ZIP Code Format: What Every Digit Means',
    short: 'ZIP code format',
    excerpt: 'The 5-digit structure, the national regions behind the first digit, and ZIP+4.',
    published: '2026-04-20',
  },
  {
    slug: 'what-is-zip-plus-4',
    title: 'What Is ZIP+4? The 4 Extra Digits Explained',
    short: 'What is ZIP+4?',
    excerpt: 'What the four digits after the hyphen identify, and when you should use them.',
    published: '2026-09-26',
  },
  {
    slug: 'zip-code-vs-postal-code',
    title: 'ZIP Code vs. Postal Code: What’s the Difference?',
    short: 'ZIP code vs. postal code',
    excerpt: 'Why the US says “ZIP code”, and how postal codes look around the world.',
    published: '2026-04-20',
  },
];
