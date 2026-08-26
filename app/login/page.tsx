'use client';

import { FormEvent, useState } from 'react';
import { useRouter } from 'next/navigation';
import { api } from '../lib/api';

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  async function submit(event: FormEvent) {
    event.preventDefault(); setBusy(true); setError('');
    try { await api.login(email, password); router.replace('/'); }
    catch (reason) { setError(reason instanceof Error ? reason.message : 'Sign in failed'); }
    finally { setBusy(false); }
  }

  return (
    <main className="login-page">
      <section className="login-story">
        <div className="login-brand"><span>Q</span> FORMSIGHT</div>
        <div><p>PRIVATE · AUDITABLE · BILINGUAL</p><h1>Every mark matters.<br />Every correction<br />stays visible.</h1><div className="login-rule" /><p className="login-copy">Qwen reads the questionnaire. YOLO verifies the physical mark. Your reviewer makes the final decision.</p></div>
        <small>本地模型處理 · 問卷資料不會傳送至雲端</small>
      </section>
      <section className="login-panel">
        <form onSubmit={submit}>
          <p className="form-eyebrow">SECURE LAN ACCESS</p>
          <h2>Welcome back.<br /><span>歡迎回來。</span></h2>
          <label>Email / 電郵<input type="email" autoComplete="username" value={email} onChange={(event) => setEmail(event.target.value)} required /></label>
          <label>Password / 密碼<input type="password" autoComplete="current-password" value={password} onChange={(event) => setPassword(event.target.value)} required /></label>
          {error ? <p className="form-error" role="alert">{error}</p> : null}
          <button className="primary-action" disabled={busy}>{busy ? 'Signing in…' : 'Sign in  登入'}<span>→</span></button>
          <small>Access is limited to named accounts on your private network or VPN.</small>
        </form>
      </section>
    </main>
  );
}
