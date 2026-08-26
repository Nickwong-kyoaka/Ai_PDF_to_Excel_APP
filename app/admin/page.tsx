'use client';

import { FormEvent, useEffect, useState } from 'react';
import { AppShell, useLocale } from '../components/app-shell';
import { api } from '../lib/api';
import type { ModelProfile, User } from '../lib/types';

function AdminConsole() {
  const { t } = useLocale();
  const [tab, setTab] = useState('health');
  const [preflight, setPreflight] = useState<Record<string, unknown>>({});
  const [profiles, setProfiles] = useState<ModelProfile[]>([]);
  const [users, setUsers] = useState<User[]>([]);
  const [rules, setRules] = useState<Array<Record<string, unknown>>>([]);
  const [message, setMessage] = useState('');
  useEffect(() => { Promise.all([api.preflight(), api.profiles(), api.users(), api.rules()]).then(([health, profileData, userData, ruleData]) => { setPreflight(health); setProfiles(profileData); setUsers(userData); setRules(ruleData); }).catch((reason) => setMessage(String(reason))); }, []);

  async function addUser(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); const data = new FormData(event.currentTarget);
    try { const user = await api.createUser({ email: data.get('email'), display_name: data.get('name'), role: data.get('role'), password: data.get('password') }); setUsers((items) => [...items, user]); event.currentTarget.reset(); setMessage(t('User created.', '使用者已建立。')); }
    catch (reason) { setMessage(reason instanceof Error ? reason.message : String(reason)); }
  }
  async function addRule(event: FormEvent<HTMLFormElement>) {
    event.preventDefault(); const data = new FormData(event.currentTarget);
    try { const definition = JSON.parse(String(data.get('definition'))); const rule = await api.createRule({ name: data.get('name'), form_pattern: '*', severity: 'review', enabled: true, definition }); setRules((items) => [rule as Record<string, unknown>, ...items]); event.currentTarget.reset(); setMessage(t('Rule created.', '規則已建立。')); }
    catch (reason) { setMessage(reason instanceof Error ? reason.message : String(reason)); }
  }

  return <>
    <header className="page-header"><div><p>{t('Admin only', '只限管理員')}</p><h1>{t('System administration', '系統管理')}</h1><span>{t('Health, users, model profiles, validation rules and retention.', '健康狀態、使用者、模型設定、驗證規則及保留政策。')}</span></div></header>
    <div className="admin-tabs">{['health', 'profiles', 'users', 'rules'].map((item) => <button className={tab === item ? 'active' : ''} key={item} onClick={() => setTab(item)}>{item}</button>)}</div>
    {message ? <div className="admin-message">{message}</div> : null}
    {tab === 'health' ? <section className="health-grid"><HealthCard title="LM Studio" value={nested(preflight, 'lmstudio', 'status')} detail={nested(preflight, 'lmstudio', 'error') || 'Qwen model server'} /><HealthCard title="YOLO" value={nested(preflight, 'yolo', 'status')} detail={nested(preflight, 'yolo', 'error') || 'Custom mark detector'} /><HealthCard title="GPU" value={nested(preflight, 'gpu', 'status')} detail={String((nestedObject(preflight, 'gpu').devices as unknown[])?.length || 0) + ' device(s)'} /><HealthCard title="Worker" value={nested(preflight, 'worker', 'status')} detail={String(nested(preflight, 'worker', 'job_id') || 'No active job')} /><pre className="preflight-json">{JSON.stringify(preflight, null, 2)}</pre></section> : null}
    {tab === 'profiles' ? <section className="admin-card"><div className="section-title"><div><p>{t('Admin-approved choices', '管理員批准選項')}</p><h2>{t('Model profiles', '模型設定')}</h2></div></div><div className="profile-grid">{profiles.map((profile) => <article key={profile.id}><div><b>{profile.is_default ? 'DEFAULT' : 'APPROVED'}</b><span>{profile.verification_mode}</span></div><h3>{profile.name}</h3><dl><div><dt>Extractor</dt><dd>{profile.extractor_model_id}</dd></div><div><dt>Judge</dt><dd>{profile.judge_model_id}</dd></div><div><dt>Quantization</dt><dd>{profile.quantization}</dd></div></dl></article>)}</div></section> : null}
    {tab === 'users' ? <section className="admin-split"><div className="admin-card"><div className="section-title"><div><p>{t('Named accounts', '具名帳戶')}</p><h2>{t('Users', '使用者')}</h2></div></div><div className="user-list">{users.map((user) => <div key={user.id}><span>{user.display_name.split(/\s+/).map((part) => part[0]).slice(0, 2).join('')}</span><div><strong>{user.display_name}</strong><small>{user.email}</small></div><b>{user.role}</b></div>)}</div></div><form className="admin-form" onSubmit={addUser}><p>{t('Create user', '建立使用者')}</p><h2>{t('New named account', '新增具名帳戶')}</h2><label>{t('Display name', '顯示名稱')}<input name="name" required /></label><label>Email<input name="email" type="email" required /></label><label>{t('Role', '角色')}<select name="role"><option value="operator">Operator</option><option value="reviewer">Reviewer</option><option value="admin">Admin</option></select></label><label>{t('Temporary password', '臨時密碼')}<input name="password" type="password" minLength={12} required /></label><button className="primary-action">{t('Create account', '建立帳戶')} →</button></form></section> : null}
    {tab === 'rules' ? <section className="admin-split"><div className="admin-card"><div className="section-title"><div><p>{t('Deterministic before AI', 'AI 前確定性驗證')}</p><h2>{t('Reasonableness rules', '合理性規則')}</h2></div></div><div className="rule-list">{rules.map((rule) => <article key={String(rule.id)}><b>{String(rule.name)}</b><span>{String(rule.severity)}</span><pre>{JSON.stringify(rule.definition, null, 2)}</pre></article>)}{!rules.length ? <p className="table-empty">{t('No custom rules yet.', '尚未有自訂規則。')}</p> : null}</div></div><form className="admin-form" onSubmit={addRule}><p>{t('Safe JSON rule', '安全 JSON 規則')}</p><h2>{t('Add validation rule', '新增驗證規則')}</h2><label>{t('Rule name', '規則名稱')}<input name="name" required /></label><label>{t('Definition', '定義')}<textarea name="definition" defaultValue={'{\n  "question_id": "age",\n  "operator": "range",\n  "min": 0,\n  "max": 120,\n  "message": "Age is outside the allowed range"\n}'} required /></label><button className="primary-action">{t('Save rule', '儲存規則')} →</button></form></section> : null}
  </>;
}

function nestedObject(source: Record<string, unknown>, key: string): Record<string, unknown> { const value = source[key]; return value && typeof value === 'object' ? value as Record<string, unknown> : {}; }
function nested(source: Record<string, unknown>, key: string, child: string): unknown { return nestedObject(source, key)[child]; }
function HealthCard({ title, value, detail }: { title: string; value: unknown; detail: unknown }) { const online = ['online', 'ready', 'idle', 'processing'].includes(String(value)); return <article className="health-card"><div><span className={online ? 'health-dot online' : 'health-dot'} /><b>{String(value || 'unknown')}</b></div><h2>{title}</h2><p>{String(detail || '')}</p></article>; }

export default function AdminPage() { return <AppShell><AdminConsole /></AppShell>; }
