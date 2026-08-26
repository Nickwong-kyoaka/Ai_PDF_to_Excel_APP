import type { Metadata } from 'next';
import { IBM_Plex_Mono, Manrope } from 'next/font/google';
import './globals.css';

const sans = Manrope({ variable: '--font-sans', subsets: ['latin'] });
const mono = IBM_Plex_Mono({ variable: '--font-mono', subsets: ['latin'], weight: ['400', '500', '600'] });
const siteOrigin = process.env.NEXT_PUBLIC_SITE_ORIGIN ?? 'https://formsight.internal';

export const metadata: Metadata = {
  metadataBase: new URL(siteOrigin),
  title: 'FormSight — Universal Questionnaire Scanner',
  description: 'Private Chinese and English questionnaire scanning with Qwen, YOLO, and human review.',
  openGraph: {
    title: 'FormSight',
    description: 'Trusted questionnaire data. Human-reviewed.',
    type: 'website',
    images: [{ url: '/og.png', width: 1672, height: 941, alt: 'FormSight questionnaire scanner' }],
  },
  twitter: {
    card: 'summary_large_image',
    title: 'FormSight',
    description: 'Trusted questionnaire data. Human-reviewed.',
    images: ['/og.png'],
  },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return <html lang="en"><body className={`${sans.variable} ${mono.variable}`}>{children}</body></html>;
}
