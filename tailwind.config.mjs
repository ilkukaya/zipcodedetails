import typography from '@tailwindcss/typography';

/** @type {import('tailwindcss').Config} */
export default {
  content: ['./src/**/*.{astro,html,js,jsx,md,mdx,svelte,ts,tsx,vue}'],
  theme: {
    extend: {
      colors: {
        paper: '#F6F4EE',
        surface: '#FFFFFF',
        ink: {
          DEFAULT: '#142033',
          2: '#35404F',
          3: '#5E6878',
          4: '#8A92A0',
        },
        line: {
          DEFAULT: '#E2DDD1',
          strong: '#CFC8B8',
        },
        brand: {
          DEFAULT: '#2344A6',
          dark: '#162F7A',
          soft: '#E8EDFA',
        },
        stamp: {
          DEFAULT: '#C8412B',
          soft: '#FBE9E4',
        },
        manila: {
          DEFAULT: '#F2E7CD',
          dark: '#E6D5AE',
        },
        good: '#2E7A4F',
      },
      fontFamily: {
        sans: ['"Public Sans Variable"', 'system-ui', '-apple-system', 'Segoe UI', 'sans-serif'],
        display: ['"Source Serif 4 Variable"', 'Georgia', 'Cambria', 'serif'],
        mono: ['"IBM Plex Mono"', 'ui-monospace', 'SFMono-Regular', 'Menlo', 'monospace'],
      },
      maxWidth: {
        page: '76rem',
      },
      boxShadow: {
        card: '0 1px 0 rgba(20,32,51,0.04), 0 1px 2px rgba(20,32,51,0.06)',
        lift: '0 2px 4px rgba(20,32,51,0.06), 0 12px 32px -12px rgba(20,32,51,0.18)',
      },
      typography: ({ theme }) => ({
        DEFAULT: {
          css: {
            '--tw-prose-body': theme('colors.ink.2'),
            '--tw-prose-headings': theme('colors.ink.DEFAULT'),
            '--tw-prose-links': theme('colors.brand.DEFAULT'),
            '--tw-prose-bold': theme('colors.ink.DEFAULT'),
            '--tw-prose-bullets': theme('colors.stamp.DEFAULT'),
            '--tw-prose-counters': theme('colors.ink.3'),
            '--tw-prose-hr': theme('colors.line.DEFAULT'),
            '--tw-prose-quotes': theme('colors.ink.DEFAULT'),
            '--tw-prose-quote-borders': theme('colors.stamp.DEFAULT'),
            '--tw-prose-th-borders': theme('colors.line.strong'),
            '--tw-prose-td-borders': theme('colors.line.DEFAULT'),
            'h1, h2, h3': { fontFamily: theme('fontFamily.display').join(','), fontWeight: '600' },
            a: { textUnderlineOffset: '3px', textDecorationThickness: '1px' },
          },
        },
      }),
    },
  },
  plugins: [typography],
};
