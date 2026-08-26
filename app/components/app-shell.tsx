'use client';

import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { createContext, useContext, useEffect, useMemo, useState } from 'react';
import { api } from '../lib/api';
import type { User } from '../lib/types';

type Locale = 'en' | 'zh';
type LocaleContextValue = { locale: Locale; setLocale: (value: Locale) => void; t: (en: string, zh: string) => string };
const LocaleContext = createContext<LocaleContextValue>({ locale: 'en', setLocale: () => {}, t: (en) => en });

export function useLocale() { return useContext(LocaleContext); }

const navigation = [
  { href: '/', icon: '▦', en: 'Dashboard', zh: '主頁' },
  { href: '/jobs', icon: '⌁', en: 'Scan jobs', zh: '掃描工作' },
  { href: '/review', icon: '✓', en: 'Review', zh: '人工覆核' },
  { href: '/annotations', icon: '⌗', en: 'Annotations', zh: '標註資料', adminOnly: true },
  { href: '/admin', icon: '⚙', en: 'Administration', zh: '系統管理', adminOnly: true },
];

export function AppShell({ children, reviewCount = 0 }: { children: React.ReactNode; reviewCount?: number }) {
  const pathname = usePathname();
  const router = useRouter();
  const [user, setUser] = useState<User | null>(null);
  const [locale, setLocaleState] = useState<Locale>(() =>
    typeof window !== 'undefined' && window.localStorage.getItem('formsight_locale') === 'zh' ? 'zh' : 'en'
  );

  useEffect(() => {
    api.me().then(setUser).catch(() => router.replace('/login'));
  }, [router]);

  const setLocale = (value: Locale) => {
    setLocaleState(value);
    window.localStorage.setItem('formsight_locale', value);
  };
  const localeValue = useMemo(() => ({ locale, setLocale, t: (en: string, zh: string) => locale === 'zh' ? zh : en }), [locale]);

  async function signOut() {
    await api.logout().catch(() => undefined);
    router.replace('/login');
  }

  return (
    <LocaleContext.Provider value={localeValue}>
      <main className="app-shell">
        <aside className="sidebar">
          <Link className="brand-mark" href="/" aria-label="FormSight">Q</Link>
          <nav aria-label="Primary">
            {navigation.filter((item) => !item.adminOnly || user?.role === 'admin').map((item) => {
              const active = item.href === '/' ? pathname === '/' : pathname.startsWith(item.href);
              return <Link key={item.href} className={`nav-item ${active ? 'active' : ''}`} href={item.href}>{item.icon}<span>{locale === 'zh' ? item.zh : item.en}</span>{item.href === '/review' && reviewCount > 0 ? <b>{reviewCount}</b> : null}</Link>;
            })}
          </nav>
          <div className="sidebar-bottom">
            <button className="nav-item sidebar-signout" onClick={signOut}>↪<span>{locale === 'zh' ? '登出' : 'Sign out'}</span></button>
            <div className="operator"><span>{user?.display_name.split(/\s+/).map((part) => part[0]).slice(0, 2).join('') || '…'}</span><div><strong>{user?.display_name || 'Loading…'}</strong><small>{user?.role || ''}</small></div></div>
          </div>
        </aside>
        <section className="workspace">
          <div className="shell-tools"><button className={locale === 'en' ? 'selected' : ''} onClick={() => setLocale('en')}>EN</button><span>/</span><button className={locale === 'zh' ? 'selected' : ''} onClick={() => setLocale('zh')}>繁</button></div>
          {children}
        </section>
      </main>
    </LocaleContext.Provider>
  );
}
